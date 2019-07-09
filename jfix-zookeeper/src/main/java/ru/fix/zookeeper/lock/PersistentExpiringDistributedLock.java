package ru.fix.zookeeper.lock;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ExecutorService;

/**
 * Lock uses persistent zk nodes.
 * After lock was acquired it will be active for given TTL
 * Owner should renew lock expiration time using {@link #expirableAcquire(long, long)}
 * <p>
 * WARNING! This class is not thread safe and should be used as distributed lock between different JVM's only.
 * <p>
 * WARNING!  Use {@link #checkAndProlongIfExpiresIn(long, long)} as much as possible
 * instead of {@link #checkAndProlong(long)} due to performance issue.
 * {@link #checkAndProlongIfExpiresIn(long, long)} - cheap operation.
 * {@link #checkAndProlong(long)} - heavy operation.
 *
 * @author Kamil Asfandiyarov
 */
public class PersistentExpiringDistributedLock implements AutoCloseable {

    private static final Logger logger = LoggerFactory.getLogger(PersistentExpiringDistributedLock.class);
    private static Charset charset = StandardCharsets.UTF_8;

    private final CuratorFramework curatorFramework;
    private final String lockId;
    private final String path;

    private final Object internalLock = new Object();

    private final NodeCache nodeCache;
    private final ExecutorService notificationsExecutor;
    private volatile long expirationDate;
    private final String serverId;

    /**
     * @param curatorFramework      CuratorFramework
     * @param notificationsExecutor thread executor for notifying current lock instance
     *                              about some event from zk node
     * @param lockId                unique identifier for this instance of lock
     * @param path                  zk path to the lock node
     * @param serverId              unique identifier for this application instance
     */
    public PersistentExpiringDistributedLock(CuratorFramework curatorFramework,
                                             ExecutorService notificationsExecutor,
                                             String lockId,
                                             String path,
                                             String serverId) throws Exception {
        this.curatorFramework = curatorFramework;
        this.notificationsExecutor = notificationsExecutor;
        this.lockId = lockId;
        this.path = path;
        this.nodeCache = new NodeCache(curatorFramework, path);
        this.serverId = serverId;

        init();
    }

    private void init() throws Exception {
        nodeCache.getListenable().addListener(() -> {
            synchronized (internalLock) {
                internalLock.notifyAll();
            }
        }, notificationsExecutor);
        nodeCache.start();
    }

    /**
     * Acquire the lock. Blocks until lock will be acquired or the given timeout expires.
     * Note: renews acquired period if current instance already holds the lock.
     *
     * @param acquirePeriod time in millis for which current instance acquire the
     *                      lock (lock could be released with {@link #release()} method
     *                      or automatically when acquirePeriod expire)
     * @param timeout       max amount of time in millis which could be spend to try acquire the lock
     * @return true if the lock was acquired, false if not
     * @throws Exception ZK errors, connection interruptions
     */
    public boolean expirableAcquire(long acquirePeriod, final long timeout) throws Exception {
        final long startTime = System.currentTimeMillis();

        synchronized (internalLock) {

            while (true) { /** main loop */

                /**
                 * read lock data
                 */
                Stat nodeStat = curatorFramework.checkExists().forPath(path);

                /**
                 * check if node path exist
                 */
                if (nodeStat == null) {
                    /**
                     * lock node does not exist
                     */
                    try {
                        expirationDate = System.currentTimeMillis() + acquirePeriod;
                        LockData lockData = new LockData(lockId, expirationDate, serverId, logger);
                        curatorFramework.create()
                                .creatingParentContainersIfNeeded()
                                .forPath(path, encodeLockData(lockData));
                        return true;
                    } catch (KeeperException.NodeExistsException nodeExistExc) {
                        logger.trace("Node already exist", nodeExistExc);
                    }

                    /**
                     * if node already exist then continue with main loop
                     */

                } else {
                    /**
                     * lock node exist
                     */
                    LockData lockData;
                    try {
                        lockData = decodeLockData(curatorFramework.getData().forPath(path));
                    } catch (KeeperException.NoNodeException noNodeException) {
                        logger.trace("Node was created by another between check exist and decode.", noNodeException);
                        return false;
                    }

                    if (lockData.getExpirationDate() < System.currentTimeMillis()) {
                        /**
                         * lock expired
                         */
                        if (zkTxUpdateLockData(acquirePeriod, nodeStat)) {
                            return true;
                        }

                        /** tx failed, continue with main loop*/

                    } else {
                        /**
                         * lock active
                         */
                        if (lockId.equals(lockData.getUuid())) {
                            /**
                             * we last lock owner
                             */
                            if (zkTxUpdateLockData(acquirePeriod, nodeStat)) {
                                return true;
                            }

                            /** tx failed, continue with main loop */

                        } else {
                            /**
                             * we are not last lock owner
                             */


                            /** lock expired after */
                            long lockTTL = Math.max(0, lockData.getExpirationDate() - System.currentTimeMillis());

                            /** acquiring try time expired after*/
                            long acquiringTryTTL = Math.max(0, startTime + timeout - System.currentTimeMillis());

                            long waitTime = Math.min(lockTTL, acquiringTryTTL);

                            if (waitTime > 0) {
                                final long preWaitingTimeSnapshot = System.currentTimeMillis();
                                logger.trace("Can't acquire lock '{}'. Already acquired by worker '{}'. " +
                                                "Current lock id: '{}'. Lock expiration time: '{}', " +
                                                "current time: '{}'. Acquiring will be paused on {} ms",
                                        path, lockData.getUuid(), lockId, lockData.getExpirationDate(),
                                        preWaitingTimeSnapshot, waitTime);

                                // Wait in hope that lock will be released by current owner
                                internalLock.wait(waitTime);

                                final long logWaitingTime = System.currentTimeMillis() - preWaitingTimeSnapshot;
                                logger.trace("Actual waiting time for release lock {} is {} ms. Planned waiting time " +
                                        "is {}. Lock path: {}", lockId, logWaitingTime, waitTime, path);
                            }

                            /** continue with main loop */

                        }
                    }
                }

                final long actualAcquiringTime = System.currentTimeMillis() - startTime;
                /** check if acquiring try time expired */
                if (actualAcquiringTime > timeout) {
                    logger.trace("Couldn't acquire lock for '{}' ms. Acquiring timeout was expired. Lock path: {}, " +
                            "lock id: {}", actualAcquiringTime, path, lockId);
                    return false;
                }
            }

        }
    }

    /**
     * @param acquirePeriod
     * @param nodeStat
     * @return true if lock updated in transaction successfully
     * @throws Exception
     */
    private boolean zkTxUpdateLockData(long acquirePeriod, Stat nodeStat) throws Exception {
        try {
            long nextExpirationDate = System.currentTimeMillis() + acquirePeriod;
            LockData lockData = new LockData(lockId, nextExpirationDate, serverId, logger);
            curatorFramework.inTransaction()
                    .check().withVersion(nodeStat.getVersion()).forPath(path).and()
                    .setData().forPath(path, encodeLockData(lockData)).and().commit();
            expirationDate = nextExpirationDate;
            return true;
        } catch (KeeperException.BadVersionException | KeeperException.NoNodeException e) {
            logger.trace("Lock already acquired/modified", e);
        }
        return false;
    }

    /**
     * Check that caller still owns the lock and prolongs lock expiration time if so.
     * <p>
     * WARNING! Use {@link #checkAndProlongIfExpiresIn(long, long)} as much as possible
     * instead of {@link #checkAndProlong(long)} due to performance issue.
     * {@link #checkAndProlongIfExpiresIn(long, long)} - cheap operation.
     * {@link #checkAndProlong(long)} - heavy operation.
     *
     * @param prolongationPeriod time in millis which current instance will hold the lock until auto-expiration
     * @return {@code true} if current thread owns the lock and prolongation was performed,
     * {@code false} if current instance doesn't owns the lock
     * @throws Exception ZK errors, connection interruptions
     */
    public boolean checkAndProlong(long prolongationPeriod) throws Exception {
        Stat nodeStat = curatorFramework.checkExists().forPath(path);
        try {
            LockData lockData = decodeLockData(curatorFramework.getData().forPath(path));
            if (!lockId.equals(lockData.getUuid())) {
                return false;
            }
            return zkTxUpdateLockData(prolongationPeriod, nodeStat);
        } catch (KeeperException.NoNodeException noNodeException) {
            logger.trace("Node already removed while checkAndProlong.", noNodeException);
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
    public boolean release() {
        try {
            synchronized (internalLock) {
                Stat nodeStat = curatorFramework.checkExists().forPath(path);
                if (nodeStat != null) {

                    LockData lockData;
                    try {
                        lockData = decodeLockData(curatorFramework.getData().forPath(path));
                    } catch (KeeperException.NoNodeException noNodeException) {
                        logger.trace("Node already removed on release.", noNodeException);
                        return true;
                    }

                    // check if we are owner of the lock
                    if (lockId.equals(lockData.getUuid())) {
                        try {
                            curatorFramework.inTransaction()
                                    .check().withVersion(nodeStat.getVersion()).forPath(path).and()
                                    .delete().forPath(path).and()
                                    .commit();
                            logger.trace("The lock {} has been released by worker {}", path, lockId);
                        } catch (KeeperException.NoNodeException | KeeperException.BadVersionException
                                noNodeException) {
                            logger.trace("Node {} already released by server {}.", path, serverId, noNodeException);
                        } finally {
                            expirationDate = 0;
                        }
                    }
                }
            }
            return true;
        } catch (Exception exc) {
            logger.error("Failed to release PersistentExpiringDistributedLock, path: {}, id: {}", path, lockId, exc);
            return false;
        }
    }

    private byte[] encodeLockData(LockData lockData) throws JsonProcessingException {
        return Marshaller.marshall(lockData).getBytes(charset);
    }

    private LockData decodeLockData(byte[] content) throws IOException {
        return Marshaller.unmarshall(new String(content, charset), LockData.class);
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
     * You can call this method only after acquiring lock by {@link #expirableAcquire(long, long)}
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
    public boolean checkAndProlongIfExpiresIn(long prolongationPeriod, long expirationPeriod) throws Exception {
        if (expirationDate < System.currentTimeMillis() + expirationPeriod) {
            return checkAndProlong(prolongationPeriod);
        } else {
            return true;
        }
    }

}
