package ru.fix.zookeeper.lock;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.fix.zookeeper.transactional.TransactionalClient;
import ru.fix.zookeeper.utils.Marshaller;

import javax.annotation.concurrent.NotThreadSafe;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.UUID;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Lock uses persistent zk nodes.
 * After lock was acquired it will be active for given TTL
 * Owner should renew lock expiration time using {@link #expirableAcquire(Duration, Duration)}
 * <p>
 * WARNING! This class is not thread safe and should be used as distributed lock between different JVM's only.
 * <p>
 * WARNING!  Use {@link #checkAndProlongIfExpiresIn(Duration, Duration)} as much as possible
 * instead of {@link #checkAndProlong(Duration)} due to performance issue.
 * {@link #checkAndProlongIfExpiresIn(Duration, Duration)} - cheap operation.
 * {@link #checkAndProlong(Duration)} - heavy operation.
 *
 * @author Kamil Asfandiyarov
 */
@NotThreadSafe
public class PersistentExpiringDistributedLock implements AutoCloseable {
    final class State {
        final boolean isOwn;
        final boolean isExpired;

        private State(boolean isOwn, boolean isExpired) {
            this.isOwn = isOwn;
            this.isExpired = isExpired;
        }

        boolean isExpired() {
            return isExpired;
        }

        boolean isOwn() {
            return isOwn;
        }
    }

    private static final class LockWatcher implements AutoCloseable {
        private final NodeCache lockNodeCache;
        private final Semaphore lockNodeStateChanged = new Semaphore(0);
        private final LockIdentity lockId;

        public LockWatcher(CuratorFramework curatorFramework, LockIdentity lockId) throws Exception {
            this.lockId = lockId;
            lockNodeCache = new NodeCache(curatorFramework, lockId.getNodePath());
            lockNodeCache.getListenable().addListener(lockNodeStateChanged::release);
            lockNodeCache.start(true);
        }

        public void clearEvents() {
            lockNodeStateChanged.drainPermits();
        }

        public void waitForEventsAndReset(long maxWaitTime, TimeUnit timeUnit) throws InterruptedException {
            lockNodeStateChanged.tryAcquire(maxWaitTime, timeUnit);
            lockNodeStateChanged.drainPermits();
        }

        @Override
        public void close() {
            try {
                lockNodeCache.close();
            } catch (Exception exception) {
                logger.error("Failed to close NodeCache on lock {}", lockId, exception);
            }
        }
    }

    private static final Logger logger = LoggerFactory.getLogger(PersistentExpiringDistributedLock.class);


    private final CuratorFramework curatorFramework;
    private final LockIdentity lockId;
    private final String uuid;
    private volatile Instant expirationDate;
    private final LockWatcher lockWatcher;
    private final AtomicBoolean isClosed = new AtomicBoolean(false);

    /**
     * @param curatorFramework CuratorFramework
     * @param lockId           unique identifier for this instance of lock
     */
    public PersistentExpiringDistributedLock(
            CuratorFramework curatorFramework,
            LockIdentity lockId
    ) throws Exception {
        this.curatorFramework = curatorFramework;
        this.lockId = lockId;
        this.uuid = UUID.randomUUID().toString();
        this.lockWatcher = new LockWatcher(curatorFramework, lockId);
    }

    /**
     * Acquire the lock. Blocks until lock will be acquired, or the given acquiringTimeout expires.
     * Note: renews acquired period if current instance already holds the lock.
     *
     * @param acquirePeriod    time in millis for which current instance acquire the
     *                         lock (lock could be released with {@link #release()} method
     *                         or automatically when acquirePeriod expire)
     * @param acquiringTimeout max amount of time in millis which could be spend to try acquire the lock
     * @return true if the lock was acquired, false if not
     * @throws Exception ZK errors, connection interruptions
     */
    public boolean expirableAcquire(
            @NotNull Duration acquirePeriod,
            @NotNull Duration acquiringTimeout
    ) throws Exception {
        assertState();

        final Instant startTime = Instant.now();
        lockWatcher.clearEvents();

        while (true) { /* main loop */
            /*
             * read lock node status
             */
            Stat nodeStat = curatorFramework.checkExists().forPath(lockId.getNodePath());

            /*
             * check if node path exist
             */
            if (nodeStat == null) {
                /*
                 * lock node does not exist
                 */
                try {
                    Instant newExpirationDate = Instant.now().plus(acquirePeriod);
                    LockData lockData = new LockData(
                            uuid,
                            newExpirationDate,
                            lockId.getMetadata());

                    curatorFramework.create()
                            .creatingParentContainersIfNeeded()
                            .forPath(lockId.getNodePath(), encodeLockData(lockData));

                    expirationDate = newExpirationDate;
                    return true;
                } catch (KeeperException.NodeExistsException e) {
                    logger.debug("Node already exist", e);
                }
                /*
                 * if node already exist then continue with main loop
                 */
                continue;

            } else {
                /*
                 * lock node exist
                 */
                LockData lockData;
                try {
                    lockData = decodeLockData(curatorFramework.getData().forPath(lockId.getNodePath()));
                } catch (KeeperException.NoNodeException e) {
                    logger.debug("Node was removed by another actor between check exist and node data getting.", e);
                    return false;
                }

                if (lockData.isExpired()) {
                    /*
                     * lock expired
                     */
                    if (zkTxUpdateLockData(acquirePeriod, nodeStat)) {
                        return true;
                    }

                    /* tx failed, continue with main loop*/
                    continue;
                } else {
                    /*
                     * lock active, check owner
                     */
                    if (lockData.isOwnedBy(uuid)) {
                        /*
                         * we last lock owner
                         */
                        if (zkTxUpdateLockData(acquirePeriod, nodeStat)) {
                            return true;
                        }
                        /* tx failed, continue with main loop */
                        continue;

                    } else {
                        /*
                         * we are not last lock owner
                         */

                        /* lock expired after */
                        long alienLockTTL = Math.max(0, lockData.getExpirationTimestamp().toEpochMilli() - Instant.now().toEpochMilli());

                        /* acquiring try time expired after */
                        long acquiringTryTTL = Math.max(
                                0,
                                startTime.toEpochMilli() + acquiringTimeout.toMillis() - Instant.now().toEpochMilli());

                        long waitTime = Math.min(alienLockTTL, acquiringTryTTL);

                        if (waitTime > 0) {
                            final OffsetDateTime preWaitingTimeSnapshot = OffsetDateTime.now(ZoneOffset.UTC);
                            logger.debug(
                                    "Can't acquire lock={}. Lock expiration time: '{}', " +
                                            "current time: '{}'. Acquiring will be paused on {} ms",
                                    Marshaller.marshall(lockId), lockData.getExpirationTimestamp(),
                                    preWaitingTimeSnapshot, waitTime
                            );

                            // Wait in hope that lock will be released by current owner
                            lockWatcher.waitForEventsAndReset(waitTime, TimeUnit.MILLISECONDS);

                            final Duration logWaitingTime = Duration.between(
                                    preWaitingTimeSnapshot, OffsetDateTime.now(ZoneOffset.UTC)
                            );
                            logger.debug(
                                    "Actual waiting time for release lock {} is {}. Planned waiting time is {}ms.",
                                    Marshaller.marshall(lockId), logWaitingTime, waitTime
                            );
                        }

                        /* continue with main loop */

                    }
                }
            }

            final Duration actualAcquiringTime = Duration.between(startTime, Instant.now());
            /* check if acquiring try time expired */
            if (actualAcquiringTime.compareTo(acquiringTimeout) > 0) {
                logger.debug(
                        "Couldn't acquire lock for '{}' ms. Acquiring acquiringTimeout was expired. Lock id: {}",
                        actualAcquiringTime, Marshaller.marshall(lockId)
                );
                return false;
            }


        }
    }

    private void assertState() {
        if (isClosed.get()) {
            throw new IllegalStateException("Lock " + lockId + " already closed");
        }
    }

    /**
     * @param acquirePeriod time to hold lock
     * @param nodeStat      lock's node stat
     * @return true if lock updated in transaction successfully
     * @throws Exception when zk perform
     */
    private boolean zkTxUpdateLockData(Duration acquirePeriod, Stat nodeStat) throws Exception {
        try {
            Instant nextExpirationTimestamp = Instant.now().plus(acquirePeriod);
            LockData lockData = new LockData(uuid, nextExpirationTimestamp, lockId.getMetadata());

            TransactionalClient.createTransaction(curatorFramework)
                    .checkPathWithVersion(lockId.getNodePath(), nodeStat.getVersion())
                    .setData(lockId.getNodePath(), encodeLockData(lockData))
                    .commit();

            expirationDate = nextExpirationTimestamp;
            return true;
        } catch (KeeperException.BadVersionException | KeeperException.NoNodeException e) {
            logger.debug("Lock {} already acquired/modified", lockId, e);
        }
        return false;
    }

    /**
     * Check that caller still owns the lock and prolongs lock expiration time if so.
     * If lock is expired but not acquired by anybody else, this mean that there is no risk
     * to treat such lock as if caller still owns it. In this case checkAndProlong method
     * preserve ownership and prolongs lock to new duration.
     * <p>
     * Use {@link #checkAndProlongIfExpiresIn(Duration, Duration)} as much as possible
     * instead of {@link #checkAndProlong(Duration)} due to performance issue.
     * {@link #checkAndProlongIfExpiresIn(Duration, Duration)} makes zk node update only if needed,
     * {@link #checkAndProlong(Duration)} - always makes zk node update.
     * See details {@link #checkAndProlongIfExpiresIn(Duration, Duration)}
     *
     * @param prolongationPeriod time in millis which current instance will hold the lock until auto-expiration
     * @return {@code true} if current thread owns the lock (event if lock is expired) and prolongation was performed,
     * {@code false} if current instance doesn't owns the lock
     * @throws Exception ZK errors, connection interruptions
     */
    public boolean checkAndProlong(Duration prolongationPeriod) throws Exception {
        assertState();

        Stat nodeStat = curatorFramework.checkExists().forPath(lockId.getNodePath());
        try {
            LockData lockData = decodeLockData(curatorFramework.getData().forPath(lockId.getNodePath()));
            if (!lockData.isOwnedBy(uuid)) {
                return false;
            }
            return zkTxUpdateLockData(prolongationPeriod, nodeStat);
        } catch (KeeperException.NoNodeException noNodeException) {
            logger.debug("Node already removed while checkAndProlong.", noNodeException);
            return false;
        }
    }

    /**
     * Check if lock is not expired and in acquired state.
     */
    public State getState() throws Exception {
        assertState();

        try {
            LockData lockData = decodeLockData(curatorFramework.getData().forPath(lockId.getNodePath()));
            return new State(
                    lockData.isOwnedBy(uuid),
                    lockData.isExpired());
        } catch (KeeperException.NoNodeException noNodeException) {
            return new State(false, true);
        }
    }

    enum ReleaseResult {
        /**
         * Lock was owned and not expired, lock successful released
         */
        LOCK_RELEASED,
        /**
         * Lock is still owned but already expired, no operation performed
         */
        LOCK_STILL_OWNED_BUT_EXPIRED,
        /**
         * Lock is not owned or absent
         */
        LOCK_IS_LOST,
    }


    /**
     * Releases the lock. Has no effect if lock expired, removed or lock is owned by others.
     *
     * @throws Exception in case of connection or ZK errors
     */
    public ReleaseResult release() throws Exception {
        assertState();
        try {
            Stat nodeStat = curatorFramework.checkExists().forPath(lockId.getNodePath());
            if (nodeStat == null) {
                return ReleaseResult.LOCK_IS_LOST;
            }

            LockData lockData;
            try {
                lockData = decodeLockData(curatorFramework.getData().forPath(lockId.getNodePath()));
            } catch (KeeperException.NoNodeException e) {
                logger.debug("Node already removed on release.", e);
                return ReleaseResult.LOCK_IS_LOST;
            }

            // check if we are owner of the lock
            if (!lockData.isOwnedBy(uuid)) {
                return ReleaseResult.LOCK_IS_LOST;
            }

            if (lockData.isExpired()) {
                return ReleaseResult.LOCK_STILL_OWNED_BUT_EXPIRED;
            }

            try {
                TransactionalClient.createTransaction(curatorFramework)
                        .checkPathWithVersion(lockId.getNodePath(), nodeStat.getVersion())
                        .deletePath(lockId.getNodePath())
                        .commit();

                logger.trace("The lock={} has been released", Marshaller.marshall(lockId));
                return ReleaseResult.LOCK_RELEASED;

            } catch (KeeperException.NoNodeException | KeeperException.BadVersionException e) {
                logger.debug("Node={} already released.", Marshaller.marshall(lockId), e);
                return ReleaseResult.LOCK_IS_LOST;
            } finally {
                expirationDate = Instant.ofEpochMilli(0);
            }


        } catch (Exception e) {
            logger.error(
                    "Failed to release PersistentExpiringDistributedLock with lockId={}",
                    Marshaller.marshall(lockId), e
            );
            throw e;
        }
    }

    private byte[] encodeLockData(LockData lockData) {
        return Marshaller.marshall(lockData).getBytes(StandardCharsets.UTF_8);
    }

    private LockData decodeLockData(byte[] content) throws IOException {
        return Marshaller.unmarshall(new String(content, StandardCharsets.UTF_8), LockData.class);
    }

    @Override
    public void close() {
        if(!isClosed.compareAndExchange(false, true))
            return;

        try {
            release();
        } catch (Exception exception) {
            logger.error("Failed to close lock {}", lockId, exception);

        } finally {
            lockWatcher.close();
        }
    }

    /**
     * PersistentLock always keeps timestamp than indicates end of life.
     * After this timestamp lock is expired.
     * This method works only on owned locks.
     * You can call this method only after acquiring lock by {@link #expirableAcquire(Duration, Duration)}
     * <p>
     * If lock expires after expirationPeriod ms this method does not do anything.
     * If lock expires in less than expirationPeriod ms, then this method update increase expiration timestamp.
     * New expiration timestamp will be now() + prolongationPeriod
     * <p>
     * In other words:
     * Check if current instance owns the lock and prolongs ownership to
     * prolongationTime if until the end of ownership less than expirationPeriod.
     *
     * @param prolongationPeriod time in ms to prolong ownership
     * @param expirationPeriod   time in ms which controls is prolongation should be performed or not
     * @return true if current instance owns the lock, false otherwise
     * @throws Exception zk errors, connection interruptions
     */
    public boolean checkAndProlongIfExpiresIn(
            Duration prolongationPeriod,
            Duration expirationPeriod
    ) throws Exception {
        assertState();

        if (expirationDate.isBefore(Instant.now().plus(expirationPeriod))) {
            return checkAndProlong(prolongationPeriod);
        } else {
            return true;
        }
    }
}
