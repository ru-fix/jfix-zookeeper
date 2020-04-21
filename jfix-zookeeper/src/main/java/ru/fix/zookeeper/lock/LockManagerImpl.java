package ru.fix.zookeeper.lock;

import org.apache.curator.framework.CuratorFramework;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.fix.aggregating.profiler.Profiler;
import ru.fix.dynamic.property.api.DynamicProperty;
import ru.fix.stdlib.concurrency.threads.NamedExecutors;
import ru.fix.stdlib.concurrency.threads.ReschedulableScheduler;
import ru.fix.stdlib.concurrency.threads.Schedule;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Acquires locks
 *
 * @author Kamil Asfandiyarov
 */
public class LockManagerImpl implements AutoCloseable, LockManager {

    private static final Logger log = LoggerFactory.getLogger(LockManagerImpl.class);

    public static final long DEFAULT_RESERVATION_PERIOD_MS = TimeUnit.MINUTES.toMillis(15);
    public static final long DEFAULT_ACQUIRING_TIMEOUT_MS = TimeUnit.SECONDS.toMillis(10);
    public static final long DEFAULT_EXPIRATION_PERIOD_MS = TimeUnit.MINUTES.toMillis(6);
    public static final long DEFAULT_LOCK_PROLONGATION_INTERVAL_MS = TimeUnit.MINUTES.toMillis(3);

    final CuratorFramework curatorFramework;
    final String workerId;
    final ExecutorService persistentLockExecutor;
    final ReschedulableScheduler lockProlongationTask;
    final Map<LockIdentity, LockContainer> locks = new ConcurrentHashMap<>();

    private static class LockContainer {
        public final PersistentExpiringDistributedLock lock;
        public final LockProlongationFailedListener prolongationListener;

        public LockContainer(
                PersistentExpiringDistributedLock lock,
                LockProlongationFailedListener prolongationListener
        ) {
            this.lock = lock;
            this.prolongationListener = prolongationListener;
        }

        public boolean release() {
            return this.lock.release();
        }

        public void close() throws Exception {
            lock.close();
        }
    }

    public LockManagerImpl(
            CuratorFramework curatorFramework,
            String workerId,
            Profiler profiler
    ) {
        lockProlongationTask = NamedExecutors.newSingleThreadScheduler("lock-prolongation", profiler);
        persistentLockExecutor = NamedExecutors.newSingleThreadPool("persistent-lock-executor", profiler);
        this.curatorFramework = curatorFramework;
        this.workerId = workerId;

        this.lockProlongationTask.schedule(
                DynamicProperty.of(Schedule.withDelay(DEFAULT_LOCK_PROLONGATION_INTERVAL_MS)),
                0,
                () -> locks.forEach((lockId, lockContainer) -> {
                    if (!checkAndProlongIfAvailable(lockId, lockContainer.lock)) {
                        log.info("Failed lock prolongation on wid={} for lockId={}", workerId, lockId.getId());
                        lockContainer.prolongationListener.onLockProlongationFailed();
                    }
                })
        );
    }

    @Override
    public boolean tryAcquire(LockIdentity lockId, LockProlongationFailedListener listener) {
        try {
            PersistentExpiringDistributedLock persistentLock = new PersistentExpiringDistributedLock(
                    curatorFramework,
                    persistentLockExecutor,
                    lockId,
                    workerId
            );
            if (!persistentLock.expirableAcquire(DEFAULT_RESERVATION_PERIOD_MS, DEFAULT_ACQUIRING_TIMEOUT_MS)) {
                log.debug("Failed to acquire expirable lock. Acquire period: {}, timeout: {}, lock path: {}",
                        DEFAULT_RESERVATION_PERIOD_MS, DEFAULT_ACQUIRING_TIMEOUT_MS, lockId.getNodePath());
                persistentLock.close();
                return false;
            }

            LockContainer oldLock = locks.put(lockId, new LockContainer(persistentLock, listener));
            if (oldLock != null) {
                log.error("Illegal state of locking for lockId={}. Lock already acquired.", lockId.getId());

                locks.remove(lockId);

                oldLock.close();
                persistentLock.close();
                return false;
            }
            log.info("Wid={}. Lock with lockId={} with successfully acquired", workerId, lockId.getId());
            return true;

        } catch (Exception e) {
            log.error("Failed to create PersistentExpiringDistributedLock with lockId={}", lockId.getId(), e);
            return false;
        }
    }

    @Override
    public boolean existsLock(LockIdentity lockId) {
        return locks.containsKey(lockId);
    }

    @Override
    public void release(LockIdentity lockId) {
        LockContainer persistentLockContainer = this.locks.get(lockId);
        if (persistentLockContainer == null || persistentLockContainer.lock == null) {
            log.error("Illegal state. Persistent lock for lockId={} doesn't exist.", lockId.getId());
            return;
        }

        try {
            persistentLockContainer.release();
            log.info("Worker with wid={} released lock with lockId={}", workerId, lockId.getId());
            locks.remove(lockId);
        } finally {
            try {
                persistentLockContainer.close();
            } catch (Exception e) {
                log.error("Error during closing lock with lockId={}", lockId.getId(), e);
            }
        }
    }

    private static boolean checkAndProlongIfAvailable(
            LockIdentity lockId,
            PersistentExpiringDistributedLock lock
    ) {
        try {
            log.info("Method checkAndProlong lockId={}", lockId.getId());
            return lock.checkAndProlongIfExpiresIn(DEFAULT_RESERVATION_PERIOD_MS, DEFAULT_EXPIRATION_PERIOD_MS);
        } catch (Exception e) {
            log.error("Failed to checkAndProlong persistent locks with lockId {} for workItem {}", lockId.getId(), e);
            return false;
        }
    }

    @Override
    public void close() {
        locks.forEach((lockId, lockContainer) -> {
            if (lockId != null) {
                try {
                    lockContainer.lock.close();
                } catch (Exception e) {
                    log.error("Failed to close lock with lockId={}", lockId, e);
                }
            }
        });
        locks.clear();
        persistentLockExecutor.shutdown();
        lockProlongationTask.shutdown();
    }
}
