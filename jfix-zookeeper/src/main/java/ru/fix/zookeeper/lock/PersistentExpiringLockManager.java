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
import java.util.concurrent.atomic.AtomicBoolean;

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


    private final AtomicBoolean scheduler = new AtomicBoolean(false);
    private final AtomicBoolean main = new AtomicBoolean(false);

    private void lockAndWait(AtomicBoolean switcher) {
        switcher.set(true);
        while (switcher.get()) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            logger.info("wait til unlock {}", switcher);
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
                () -> locks.forEach(this::prolongOrRemoveLock)
        );
    }

    private void validateConfig(PersistentExpiringLockManagerConfig config) {
        if (config.getLockAcquirePeriod().compareTo(config.getExpirationPeriod()) < 0) {
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
        logger.error("Locking release method main thread!!!");
        lockAndWait(main);
        LockContainer container = locks.remove(lockId);
        try (container) { // here closing fucking lock!
            if (container == null) {
                logger.error("Illegal state. Persistent lock for lockId={} doesn't exist.", lockId);
                throw new IllegalStateException("Illegal state. Persistent lock for lockId=" + lockId + " doesn't exist." +
                        "Probably this lock was released when another manager acquired lock by same path.");
            }
            if (!container.release()) {
                logger.warn("Failed to release lock {}", lockId);
            }
        } catch (IllegalStateException exception) {
            throw exception;
        } catch (Exception exception) {
            logger.error("Failed to release lock: " + lockId, exception);
        }
        logger.error("Unlock scheduler thread!!!");
        scheduler.set(false);
        try {
            Thread.sleep(10000);
        } catch (InterruptedException e) {
            e.printStackTrace();
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

    /**
     * Try to prolong lock or else remove it from {@link #locks}
     */
    private void prolongOrRemoveLock(LockIdentity lockId, LockContainer lockContainer) {
        logger.error("Unlock main thread!!!!");
        main.set(false);
        logger.error("locking scheduler!!!!");
        lockAndWait(scheduler);
        boolean prolonged = false;
        try {
            logger.debug("Check and prolong lockId={}", lockId);
            prolonged = lockContainer.lock.checkAndProlongIfExpiresIn(
                    config.get().getLockAcquirePeriod(),
                    config.get().getExpirationPeriod()
            );
        } catch (Exception e) {
            // Lock could be already removed and closed with {@link #release(LockIdentity)} in another thread
            if (!locks.containsKey(lockId)) {
                logger.info(
                        "Failed to checkAndProlongIfExpiresIn lockId {} cause it has been deleted already", lockId, e
                );
                return;
            } else {
                logger.error("Failed to checkAndProlongIfExpiresIn persistent locks with lockId {}", lockId, e);
            }
        }
        if (!prolonged) {
            locks.remove(lockId);
            logger.error("Failed lock prolongation for lock={}. Lock is removed from manager", lockId);
            try {
                lockContainer.prolongationFailedListener.onLockProlongationFailedAndRemoved(lockId);
            } catch (Exception exc) {
                logger.error("Failed to invoke ProlongationFailedListener on lock {}", lockId, exc);
            }
        }
    }

}
