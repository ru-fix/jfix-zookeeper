package ru.fix.zookeeper.lock;

import org.apache.curator.framework.CuratorFramework;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.fix.aggregating.profiler.Profiler;
import ru.fix.dynamic.property.api.DynamicProperty;
import ru.fix.stdlib.concurrency.threads.NamedExecutors;
import ru.fix.stdlib.concurrency.threads.ReschedulableScheduler;
import ru.fix.stdlib.concurrency.threads.Schedule;

import javax.annotation.Nullable;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiFunction;

import static ru.fix.zookeeper.lock.PersistentExpiringLockManager.ActiveLocksContainer.ProcessingLockResult.*;

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
                    return prolonged ? KEEP_LOCK_IN_CONTAINER : REMOVE_LOCK_FROM_CONTAINER;
                }, (lockId, listener, processingLockResult) -> {
                    if (processingLockResult == REMOVE_LOCK_FROM_CONTAINER) {
                        logger.error("Failed lock prolongation for lock={}. Lock is removed from manager", lockId);
                        try {
                            listener.onLockProlongationFailedAndRemoved(lockId);
                        } catch (Exception exc) {
                            logger.error("Failed to invoke ProlongationFailedListener on lock {}", lockId, exc);
                        }
                    }
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
        return locksContainer.getLockState(lockId);
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
            return REMOVE_LOCK_FROM_CONTAINER;
        }, (lockId, lockListener, processingLockResult) -> {
            // nothing to do
        });
        locksContainer.close();
        lockProlongationScheduler.close();
    }

    /**
     * Contains the locks and provide synced access to remove, prolong and getState operations under locks
     */
    static class ActiveLocksContainer implements AutoCloseable {
        /**
         * remove, prolong and getState operations MUST BE synced under {@link #globalLock}
         */
        private final ConcurrentMap<LockIdentity, LockContainer> locks = new ConcurrentHashMap<>();

        /**
         * all operations with accessing to {@link #locks} has been synced with this object
         * LockContainer is encapsulated inside this class so for performance issue this lock could be
         * replaced with syncing via LockContainer
         */
        private final ReentrantLock globalLock = new ReentrantLock(true);

        /**
         * @return - old value in the map
         */
        @Nullable
        public PersistentExpiringDistributedLock putLock(
                LockIdentity lockId,
                PersistentExpiringDistributedLock lock,
                LockProlongationFailedListener listener
        ) {
            LockContainer newLockContainer = new LockContainer(lock, listener);
            LockContainer oldLockContainer = locks.put(lockId, newLockContainer);
            if (oldLockContainer != null) {
                return oldLockContainer.lock;
            } else {
                return null;
            }
        }

        @Nullable
        public PersistentExpiringDistributedLock removeLock(LockIdentity lockId) {
            try {
                globalLock.lock();
                return Optional.ofNullable(locks.remove(lockId))
                        .map(lockContainer -> lockContainer.lock)
                        .orElse(null);
            } finally {
                globalLock.unlock();
            }
        }

        public boolean contains(LockIdentity lockId) {
            return locks.containsKey(lockId);
        }

        public Optional<PersistentExpiringDistributedLock.State> getLockState(LockIdentity lockId) throws Exception {
            try {
                globalLock.lock();
                LockContainer lockContainer = locks.get(lockId);
                if (lockContainer != null) {
                    return Optional.of(lockContainer.lock.getState());
                } else {
                    return Optional.empty();
                }
            } finally {
                globalLock.unlock();
            }
        }

        /**
         * @param lockProcessor           - processor that returns what to do with Lock in collection
         * @param lockResultPostProcessor - processor that does some work after processing and applied action
         */
        public void processAllLocks(
                BiFunction<LockIdentity, PersistentExpiringDistributedLock, ProcessingLockResult> lockProcessor,
                LockResultPostProcessor lockResultPostProcessor
        ) {
            locks.forEach((lockId, lockContainer) -> {
                try {
                    globalLock.lock();
                    final ProcessingLockResult processingLockResult;
                    if (locks.containsKey(lockId)) {
                        processingLockResult = lockProcessor.apply(lockId, lockContainer.lock);
                    } else {
                        processingLockResult = ALREADY_REMOVED;
                    }
                    applyProcessingLockResult(lockId, processingLockResult);
                    lockResultPostProcessor.apply(
                            lockId, lockContainer.prolongationFailedListener, processingLockResult
                    );
                } finally {
                    globalLock.unlock();
                }
            });
        }

        /**
         *
         */
        @Override
        public void close() {
            locks.clear();
        }

        private void applyProcessingLockResult(
                LockIdentity lockId,
                ProcessingLockResult processingLockResult
        ) {
            switch (processingLockResult) {
                case REMOVE_LOCK_FROM_CONTAINER:
                    locks.remove(lockId);
                    break;
                case KEEP_LOCK_IN_CONTAINER:
                    // do nothing
                    break;
                case ALREADY_REMOVED:
                    logger.trace("Lock with lockId {} already has been removed by other thread", lockId);
                    break;
                default:
                    throw new IllegalStateException(
                            "Unexpected value for processingLockResult " + processingLockResult
                    );
            }
        }

        private static class LockContainer {
            public final PersistentExpiringDistributedLock lock;
            public final LockProlongationFailedListener prolongationFailedListener;

            public LockContainer(
                    PersistentExpiringDistributedLock lock,
                    LockProlongationFailedListener prolongationFailedListener
            ) {
                this.lock = lock;
                this.prolongationFailedListener = prolongationFailedListener;
            }
        }

        @FunctionalInterface
        public interface LockResultPostProcessor {
            void apply(
                    LockIdentity lockId,
                    LockProlongationFailedListener lockListener,
                    ProcessingLockResult processingLockResult
            );
        }

        enum ProcessingLockResult {
            KEEP_LOCK_IN_CONTAINER,
            REMOVE_LOCK_FROM_CONTAINER,
            ALREADY_REMOVED
        }
    }

}
