package ru.fix.zookeeper.lock;

import kotlin.Unit;
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

    PersistentExpiringLockManager(
            CuratorFramework curatorFramework,
            DynamicProperty<PersistentExpiringLockManagerConfig> config,
            ActiveLocksContainer activeLocksContainer,
            ReschedulableScheduler lockProlongationScheduler
    ) {
        validateConfig(config.get());
        this.curatorFramework = curatorFramework;
        this.config = config;
        this.locksContainer = activeLocksContainer;
        this.lockProlongationScheduler = lockProlongationScheduler;
    }

    public PersistentExpiringLockManager(
            CuratorFramework curatorFramework,
            DynamicProperty<PersistentExpiringLockManagerConfig> config,
            Profiler profiler
    ) {
        this(
                curatorFramework,
                config,
                new ActiveLocksContainer(),
                NamedExecutors.newSingleThreadScheduler("PersistentExpiringLockManager", profiler)
        );
        lockProlongationScheduler.schedule(
                Schedule.withDelay(config.map(prop -> prop.getLockCheckAndProlongInterval().toMillis())),
                0,
                () -> locksContainer.processAllLocks((lockId, lock) -> {
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
                    return prolonged
                            ? ProcessingLockResult.KEEP_LOCK_IN_CONTAINER
                            : ProcessingLockResult.REMOVE_LOCK_FROM_CONTAINER;
                }, (lockId, listener, processingLockResult) -> {
                    if (processingLockResult == ProcessingLockResult.REMOVE_LOCK_FROM_CONTAINER) {
                        logger.error("Failed lock prolongation for lock={}. Lock is removed from manager", lockId);
                        try {
                            listener.onLockProlongationFailedAndRemoved(lockId);
                        } catch (Exception exc) {
                            logger.error("Failed to invoke ProlongationFailedListener on lock {}", lockId, exc);
                        }
                    } else {
                        // nothing to do
                    }
                    return Unit.INSTANCE;
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
            PersistentExpiringDistributedLock oldLock = locksContainer.putLock(lockId, newPersistentLock, listener);
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
        return Optional.ofNullable(locksContainer.getLockState(lockId));
    }

    public void release(LockIdentity lockId) {
        PersistentExpiringDistributedLock lock = locksContainer.removeLock(lockId);
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
        locksContainer.processAllLocks((lockId, lock) -> {
            lock.close();
            try {
                logger.warn("Active not released lock with lockId={} closed.", lockId);
            } catch (Exception e) {
                logger.error("Failed to close lock with lockId={}", lockId, e);
            }
            return ProcessingLockResult.REMOVE_LOCK_FROM_CONTAINER;
        }, (lockId, lockListener, processingLockResult) -> Unit.INSTANCE);
        locksContainer.close();
        lockProlongationScheduler.close();
    }

}
