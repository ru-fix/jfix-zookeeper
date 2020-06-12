package ru.fix.zookeeper.lock;

import org.apache.curator.framework.CuratorFramework;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.fix.aggregating.profiler.Profiler;
import ru.fix.dynamic.property.api.DynamicProperty;
import ru.fix.stdlib.concurrency.threads.NamedExecutors;
import ru.fix.stdlib.concurrency.threads.ReschedulableScheduler;
import ru.fix.stdlib.concurrency.threads.Schedule;

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Acquires {@code PersistentExpiringDistributedLock} locks and automatically prolongs them.
 */
public class PersistentExpiringLockManager implements AutoCloseable {

    private static final Logger logger = LoggerFactory.getLogger(PersistentExpiringLockManager.class);

    private final CuratorFramework curatorFramework;
    private final DynamicProperty<PersistentExpiringLockManagerConfig> config;
    private final ConcurrentHashMap<LockIdentity, LockContainer> locks = new ConcurrentHashMap<>();
    private final ReschedulableScheduler lockProlongationScheduler;

    private static class LockContainer implements AutoCloseable {
        public final PersistentExpiringDistributedLock lock;
        public final LockProlongationFailedListener prolongationFailedListener;

        public LockContainer(
                PersistentExpiringDistributedLock lock,
                LockProlongationFailedListener prolongationFailedListener
        ) {
            this.lock = lock;
            this.prolongationFailedListener = prolongationFailedListener;
        }

        public boolean release() throws Exception {
            return lock.release();
        }

        public void close() {
            lock.close();
        }
    }

    public PersistentExpiringLockManager(
            CuratorFramework curatorFramework,
            DynamicProperty<PersistentExpiringLockManagerConfig> config,
            Profiler profiler
    ) {
        validateConfig(config.get());

        this.curatorFramework = curatorFramework;
        this.config = config;
        this.lockProlongationScheduler = NamedExecutors.newSingleThreadScheduler(
                "PersistentExpiringLockManager", profiler
        );
        this.lockProlongationScheduler.schedule(
                Schedule.withDelay(config.map(prop -> prop.getLockCheckAndProlongInterval().toMillis())),
                0,
                () -> locks.forEach((lockId, lockContainer) -> {
                    if (!checkAndProlongLockIfRequired(lockId, lockContainer.lock)) {
                        locks.remove(lockId);
                        logger.error("Failed lock prolongation for lock={}. Lock is removed from manager", lockId);
                        try {
                            lockContainer.prolongationFailedListener.onLockProlongationFailedAndRemoved(lockId);
                        } catch (Exception exc) {
                            logger.error("Failed to invoke ProlongationFailedListener on lock {}", lockId, exc);
                        }
                    }
                })
        );
    }

    private void validateConfig(PersistentExpiringLockManagerConfig config) {
        if (!(config.getLockAcquirePeriod().compareTo(config.getExpirationPeriod()) >= 0)) {
            throw new IllegalArgumentException("Invalid configuration." +
                    " acquirePeriod should be >= expirationPeriod");
        }
    }

    public boolean tryAcquire(LockIdentity lockId, LockProlongationFailedListener listener) {
        try {
            PersistentExpiringDistributedLock newPersistentLock = new PersistentExpiringDistributedLock(
                    curatorFramework,
                    lockId
            );

            Duration acquirePeriod = config.get().getLockAcquirePeriod();
            Duration acquiringTimeout = config.get().getAcquiringTimeout();
            if (!newPersistentLock.expirableAcquire(acquirePeriod, acquiringTimeout)) {
                logger.debug(
                        "Failed to acquire expirable lock. Acquire period: {}, timeout: {}, lockId: {}",
                        acquirePeriod, acquiringTimeout, lockId
                );
                newPersistentLock.close();
                return false;
            }

            LockContainer newLockContainer = new LockContainer(newPersistentLock, listener);
            LockContainer oldLockContainer = locks.put(lockId, newLockContainer);

            if (oldLockContainer != null) {
                logger.error("Illegal state of locking for lockId={}." +
                        " Lock already existed inside LockManager but expired. " +
                        " And was replaced by new lock.", lockId);
                oldLockContainer.close();
            }

            logger.info("Lock with lockId={} successfully acquired", lockId);
            return true;

        } catch (Exception e) {
            logger.error("Failed to create PersistentExpiringDistributedLock with lockId={}", lockId, e);
            return false;
        }
    }

    public boolean isLockManaged(LockIdentity lockId) {
        return locks.containsKey(lockId);
    }

    public Optional<PersistentExpiringDistributedLock.State> getLockState(LockIdentity lockId) throws Exception {
        LockContainer container = locks.get(lockId);
        if (container == null) return Optional.empty();

        return Optional.of(container.lock.getState());
    }

    public void release(LockIdentity lockId) {
        LockContainer container = locks.remove(lockId);
        try (container) {
            if (container == null) {
                logger.error("Illegal state. Persistent lock for lockId={} doesn't exist.", lockId);
                return;
            }
            if (!container.release()) {
                logger.warn("Failed to release lock {}", lockId);
            }
        } catch (Exception exception) {
            logger.error("Failed to release lock: " + lockId, exception);
        } finally {
            locks.remove(lockId);
        }
    }

    /**
     * @param lockId
     * @param lock
     * @return false in case of Exception of lock expiration or losing lock ownership
     */
    private boolean checkAndProlongLockIfRequired(LockIdentity lockId, PersistentExpiringDistributedLock lock) {
        try {
            logger.debug("Check and prolong lockId={}", lockId);
            return lock.checkAndProlongIfExpiresIn(
                    config.get().getLockAcquirePeriod(),
                    config.get().getExpirationPeriod());
        } catch (Exception e) {
            logger.error("Failed to checkAndProlongIfExpiresIn persistent locks with lockId {}", lockId, e);
            return false;
        }
    }

    @Override
    public void close() {
        locks.forEach((lockId, lockContainer) -> {
            if (lockId != null && lockContainer != null) {
                try {
                    logger.warn("Active not released lock with lockId={} closed.", lockId);
                    lockContainer.lock.close();
                } catch (Exception e) {
                    logger.error("Failed to close lock with lockId={}", lockId, e);
                }
            }
        });
        locks.clear();
        lockProlongationScheduler.close();
    }
}
