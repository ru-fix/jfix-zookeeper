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
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.UUID;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

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


    private static final Logger logger = LoggerFactory.getLogger(PersistentExpiringDistributedLock.class);
    private static final Charset charset = StandardCharsets.UTF_8;

    private final CuratorFramework curatorFramework;
    private final LockIdentity lockId;
    private volatile String version;

    private final Semaphore lockNodeStateChanged = new Semaphore(0);

    private final NodeCache nodeCache;
    private volatile Instant expirationDate;

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
        this.nodeCache = new NodeCache(curatorFramework, lockId.getNodePath());
        this.version = UUID.randomUUID().toString();

        init();
    }

    private void init() throws Exception {
        nodeCache.getListenable().addListener(lockNodeStateChanged::release);
        nodeCache.start(true);
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
        final Instant startTime = Instant.now();


        while (true) { /* main loop */

            /*
             * read lock data
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
                    expirationDate = Instant.now().plus(acquirePeriod);
                    version = UUID.randomUUID().toString();

                    LockData lockData = new LockData(
                            version,
                            expirationDate,
                            lockId.getData()
                    );
                    curatorFramework.create()
                            .creatingParentContainersIfNeeded()
                            .forPath(lockId.getNodePath(), encodeLockData(lockData));
                    return true;
                } catch (KeeperException.NodeExistsException e) {
                    logger.debug("Node already exist", e);
                }

                /*
                 * if node already exist then continue with main loop
                 */

            } else {
                /*
                 * lock node exist
                 */
                LockData lockData;
                try {
                    lockData = decodeLockData(curatorFramework.getData().forPath(lockId.getNodePath()));
                } catch (KeeperException.NoNodeException e) {
                    logger.warn("Node was removed by another between check exist and node data getting.", e);
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

                } else {
                    /*
                     * lock active
                     */
                    if (version.equals(lockData.getVersion())) {
                        /*
                         * we last lock owner
                         */
                        if (zkTxUpdateLockData(acquirePeriod, nodeStat)) {
                            return true;
                        }

                        /* tx failed, continue with main loop */

                    } else {
                        /*
                         * we are not last lock owner
                         */


                        /* lock expired after */
                        long lockTTL = Math.max(0, lockData.getExpirationTimestamp().toEpochMilli() - Instant.now().toEpochMilli());

                        /* acquiring try time expired after */
                        long acquiringTryTTL = Math.max(0, startTime.toEpochMilli() + acquiringTimeout.toMillis() - Instant.now().toEpochMilli());

                        long waitTime = Math.min(lockTTL, acquiringTryTTL);

                        if (waitTime > 0) {
                            final OffsetDateTime preWaitingTimeSnapshot = OffsetDateTime.now(ZoneOffset.UTC);
                            logger.debug(
                                    "Can't acquire lock={}. Lock expiration time: '{}', " +
                                            "current time: '{}'. Acquiring will be paused on {} ms",
                                    Marshaller.marshall(lockId), lockData.getExpirationTimestamp(),
                                    preWaitingTimeSnapshot, waitTime
                            );

                            // Wait in hope that lock will be released by current owner
                            lockNodeStateChanged.tryAcquire(waitTime, TimeUnit.MILLISECONDS);

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

    /**
     * @param acquirePeriod time to hold lock
     * @param nodeStat      lock's node stat
     * @return true if lock updated in transaction successfully
     * @throws Exception when zk perform
     */
    private boolean zkTxUpdateLockData(Duration acquirePeriod, Stat nodeStat) throws Exception {
        try {
            Instant nextExpirationDate = Instant.now().plus(acquirePeriod);
            version = UUID.randomUUID().toString();
            LockData lockData = new LockData(version, nextExpirationDate, lockId.getData());

            curatorFramework.inTransaction()
                    .check().withVersion(nodeStat.getVersion()).forPath(lockId.getNodePath()).and()
                    .setData().forPath(lockId.getNodePath(), encodeLockData(lockData)).and().commit();
            expirationDate = nextExpirationDate;
            return true;
        } catch (KeeperException.BadVersionException | KeeperException.NoNodeException e) {
            logger.debug("Lock already acquired/modified", e);
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
        Stat nodeStat = curatorFramework.checkExists().forPath(lockId.getNodePath());
        try {
            LockData lockData = decodeLockData(curatorFramework.getData().forPath(lockId.getNodePath()));
            if (!version.equals(lockData.getVersion())) {
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
        try {
            LockData lockData = decodeLockData(curatorFramework.getData().forPath(lockId.getNodePath()));
            return new State(
                    lockData.isOwnedBy(version),
                    lockData.isExpired());
        } catch (KeeperException.NoNodeException noNodeException) {
            return new State(false, true);
        }
    }

    final static class ReleaseResult {
        enum Status {
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
            /**
             * Network or other failure, see exception for details
             */
            FAILURE
        }

        private final Status status;
        private final Exception exception;

        private ReleaseResult(Status status, Exception exception) {
            this.status = status;
            this.exception = exception;
        }

        public Status getStatus() {
            return status;
        }

        public Exception getException() {
            return exception;
        }
    }

    /**
     * Releases the lock. Has no effect if lock expired, removed or lock is owned by others.
     */
    public ReleaseResult release() {
        try {
            Stat nodeStat = curatorFramework.checkExists().forPath(lockId.getNodePath());
            if (nodeStat == null) {
                return new ReleaseResult(ReleaseResult.Status.LOCK_IS_LOST, null);
            }

            LockData lockData;
            try {
                lockData = decodeLockData(curatorFramework.getData().forPath(lockId.getNodePath()));
            } catch (KeeperException.NoNodeException e) {
                logger.debug("Node already removed on release.", e);
                return new ReleaseResult(ReleaseResult.Status.LOCK_IS_LOST, null);
            }

            //TODO: delete in transaction with version

            // check if we are owner of the lock
            if (!version.equals(lockData.getVersion())) {
                return new ReleaseResult(ReleaseResult.Status.LOCK_IS_LOST, null);
            }

            if (lockData.isExpired()) {
                return new ReleaseResult(ReleaseResult.Status.LOCK_STILL_OWNED_BUT_EXPIRED, null);
            }

            try {
                TransactionalClient.createTransaction(curatorFramework)
                        .checkPathWithVersion(lockId.getNodePath(), nodeStat.getVersion())
                        .deletePath(lockId.getNodePath())
                        .commit();

                logger.trace("The lock={} has been released", Marshaller.marshall(lockId));
                return new ReleaseResult(ReleaseResult.Status.LOCK_RELEASED, null);

            } catch (KeeperException.NoNodeException | KeeperException.BadVersionException e) {
                logger.debug("Node={} already released.", Marshaller.marshall(lockId), e);
                return new ReleaseResult(ReleaseResult.Status.LOCK_IS_LOST, null);
            } finally {
                expirationDate = Instant.ofEpochMilli(0);
            }


        } catch (Exception e) {
            logger.error(
                    "Failed to release PersistentExpiringDistributedLock with lockId={}",
                    Marshaller.marshall(lockId), e
            );
            return new ReleaseResult(ReleaseResult.Status.FAILURE, e);
        }
    }

    private byte[] encodeLockData(LockData lockData) {
        return Marshaller.marshall(lockData).getBytes(charset);
    }

    private LockData decodeLockData(byte[] content) throws IOException {
        return Marshaller.unmarshall(new String(content, charset), LockData.class);
    }

    @Override
    public void close() {
        try {
            release();
        } finally {
            try {
                nodeCache.close();
            } catch (Exception exception) {
                logger.error("Failed to close nodeCashe on lock " + lockId.getNodePath(), exception);
            }
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
        if (expirationDate.isBefore(Instant.now().plus(expirationPeriod))) {
            return checkAndProlong(prolongationPeriod);
        } else {
            return true;
        }
    }

}
