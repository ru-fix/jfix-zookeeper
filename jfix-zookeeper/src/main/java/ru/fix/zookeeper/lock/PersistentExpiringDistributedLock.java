package ru.fix.zookeeper.lock;

import com.fasterxml.jackson.core.type.TypeReference;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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

    private static final Logger logger = LoggerFactory.getLogger(PersistentExpiringDistributedLock.class);
    private static final Charset charset = StandardCharsets.UTF_8;

    private final CuratorFramework curatorFramework;
    private final LockIdentity lockId;
    private String version;

    private final Object internalLock = new Object();

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
        nodeCache.getListenable().addListener(() -> {
                    synchronized (internalLock) {
                        internalLock.notifyAll();
                    }
                }
        );
        nodeCache.start();
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

        synchronized (internalLock) {
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

                    if (lockData.getExpirationDate().isBefore(Instant.now())) {
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
                            long lockTTL = Math.max(0, lockData.getExpirationDate().toEpochMilli() - Instant.now().toEpochMilli());

                            /* acquiring try time expired after */
                            long acquiringTryTTL = Math.max(0, startTime.toEpochMilli() + acquiringTimeout.toMillis() - Instant.now().toEpochMilli());

                            long waitTime = Math.min(lockTTL, acquiringTryTTL);

                            if (waitTime > 0) {
                                final OffsetDateTime preWaitingTimeSnapshot = OffsetDateTime.now(ZoneOffset.UTC);
                                logger.debug(
                                        "Can't acquire lock={}. Lock expiration time: '{}', " +
                                                "current time: '{}'. Acquiring will be paused on {} ms",
                                        Marshaller.marshall(lockId), lockData.getExpirationDate(),
                                        preWaitingTimeSnapshot, waitTime
                                );

                                // Wait in hope that lock will be released by current owner
                                internalLock.wait(waitTime);

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
    }

    /**
     * @param acquirePeriod time to hold lock
     * @param nodeStat      lock's node stat
     * @return true if lock updated in transaction successfully
     * @throws Exception when zk perform
     */
    @SuppressWarnings("deprecation")
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
     * <p>
     * WARNING! Use {@link #checkAndProlongIfExpiresIn(Duration, Duration)} as much as possible
     * instead of {@link #checkAndProlong(Duration)} due to performance issue.
     * {@link #checkAndProlongIfExpiresIn(Duration, Duration)} - cheap operation.
     * {@link #checkAndProlong(Duration)} - heavy operation.
     *
     * @param prolongationPeriod time in millis which current instance will hold the lock until auto-expiration
     * @return {@code true} if current thread owns the lock and prolongation was performed,
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
     * Release the lock.
     * Note: has no effect if timeout expired for lock (even if another thread acquired the lock because of that).
     *
     * @return false in case ZK errors, connection interruptions and other conditions when it is hard to clarify
     * result of operation
     */
    @SuppressWarnings("deprecation")
    public boolean release() {
        try {
            synchronized (internalLock) {
                Stat nodeStat = curatorFramework.checkExists().forPath(lockId.getNodePath());
                if (nodeStat != null) {

                    LockData lockData;
                    try {
                        lockData = decodeLockData(curatorFramework.getData().forPath(lockId.getNodePath()));
                    } catch (KeeperException.NoNodeException e) {
                        logger.debug("Node already removed on release.", e);
                        return true;
                    }

                    // check if we are owner of the lock
                    if (version.equals(lockData.getVersion())) {
                        try {
                            curatorFramework.inTransaction()
                                    .check().withVersion(nodeStat.getVersion()).forPath(lockId.getNodePath()).and()
                                    .delete().forPath(lockId.getNodePath()).and()
                                    .commit();
                            logger.trace("The lock={} has been released", Marshaller.marshall(lockId));
                        } catch (KeeperException.NoNodeException | KeeperException.BadVersionException e) {
                            logger.debug("Node={} already released.", Marshaller.marshall(lockId), e);
                        } finally {
                            expirationDate = Instant.ofEpochMilli(0);
                        }
                    }
                }
            }
            return true;
        } catch (Exception e) {
            logger.error(
                    "Failed to release PersistentExpiringDistributedLock with lockId={}",
                    Marshaller.marshall(lockId), e
            );
            return false;
        }
    }

    private byte[] encodeLockData(LockData lockData) {
        return Marshaller.marshall(lockData).getBytes(charset);
    }

    private LockData decodeLockData(byte[] content) throws IOException {
        return Marshaller.unmarshall(new String(content, charset), new TypeReference<>() {
        });
    }

    @Override
    public void close() throws Exception {
        try {
            release();
        } finally {
            nodeCache.close();
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
