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

/**
 * Acquires {@code PersistentExpiringDistributedLock} locks and automatically prolongs them.
 */
public class PersistentExpiringLockManager implements AutoCloseable {

    private static final Logger logger = LoggerFactory.getLogger(PersistentExpiringLockManager.class);

    private final CuratorFramework curatorFramework;
    private final DynamicProperty<PersistentExpiringLockManagerConfig> config;
    private final ActiveLocksContainer locksContainer;
    private final ReschedulableScheduler lockProlongationScheduler;

    public PersistentExpiringLockManager(
            CuratorFramework curatorFramework,
            DynamicProperty<PersistentExpiringLockManagerConfig> config,
            Profiler profiler
    ) {
        validateConfig(config.get());
        this.curatorFramework = curatorFramework;
        this.config = config;
        this.locksContainer = new ActiveLocksContainer();
        this.lockProlongationScheduler = NamedExecutors.newSingleThreadScheduler(
                "PersistentExpiringLockManager", profiler
        );
        lockProlongationScheduler.schedule(
                Schedule.withDelay(config.map(prop -> prop.getLockCheckAndProlongInterval().toMillis())),
                0,
                () -> locksContainer.processAllLocks((lockId, lock, prolongFailedListener) -> {
                    boolean prolonged = false;
                    try {
                        logger.debug("Check and prolong lockId={}", lockId);
                        prolonged = lock.checkAndProlongIfExpiresIn(
                                config.get().getLockAcquirePeriod(),
                                config.get().getExpirationPeriod()
                        );
                    } catch (Exception e) {
                        logger.error(
                                "Failed to checkAndProlongIfExpiresIn persistent locks with lockId {}", lockId, e
                        );
                    }
                    if (!prolonged) {
                        logger.error("Failed lock prolongation for lock={}. Lock is removed from manager", lockId);
                        try {
                            prolongFailedListener.onLockProlongationFailedAndRemoved(lockId);
                        } catch (Exception exc) {
                            logger.error("Failed to invoke ProlongationFailedListener on lock {}", lockId, exc);
                        }
                    }
                    return prolonged
                            ? ProcessingLockResult.KEEP_LOCK_IN_CONTAINER
                            : ProcessingLockResult.REMOVE_LOCK_FROM_CONTAINER;
                })
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
            PersistentExpiringDistributedLock oldLock = locksContainer.put(lockId, newPersistentLock, listener);
            if (oldLock != null) {
                logger.error(
                        "Illegal state of locking for lockId={}." +
                                " Lock already existed inside LockManager but expired. " +
                                " And was replaced by new lock.",
                        lockId
                );
                oldLock.close();
            }
            logger.info("Lock with lockId={} successfully acquired", lockId);
            return true;

        } catch (Exception e) {
            logger.error("Failed to create PersistentExpiringDistributedLock with lockId={}", lockId, e);
            return false;
        }
    }

    public boolean isLockManaged(LockIdentity lockId) {
        return locksContainer.contains(lockId);
    }

    public Optional<PersistentExpiringDistributedLock.State> getLockState(LockIdentity lockId) throws Exception {
        PersistentExpiringDistributedLock lock = locksContainer.get(lockId);
        if (lock != null) {
            return Optional.of(lock.getState());
        } else {
            return Optional.empty();
        }
    }

    public void release(LockIdentity lockId) {
        PersistentExpiringDistributedLock lock = locksContainer.remove(lockId);
        try (lock) {
            if (lock == null) {
                logger.error("Illegal state. Persistent lock for lockId={} doesn't exist.", lockId);
                throw new IllegalStateException(
                        "Illegal state. Persistent lock for lockId=" + lockId + " doesn't exist." +
                                "Probably this lock was released when another manager acquired lock by same path."
                );
            }
            if (!lock.release()) {
                logger.warn("Failed to release lock {}", lockId);
            }
        } catch (IllegalStateException exception) {
            throw exception;
        } catch (Exception exception) {
            logger.error("Failed to release lock: " + lockId, exception);
        }
    }

    @Override
    public void close() {
        locksContainer.processAllLocks((lockId, lock, listener) -> {
            lock.close();
            try {
                logger.warn("Active not released lock with lockId={} closed.", lockId);
            } catch (Exception e) {
                logger.error("Failed to close lock with lockId={}", lockId, e);
            }
            return ProcessingLockResult.REMOVE_LOCK_FROM_CONTAINER;
        });
        lockProlongationScheduler.close();
    }

}
