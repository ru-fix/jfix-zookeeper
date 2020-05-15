package ru.fix.zookeeper.lock;

import org.apache.curator.framework.CuratorFramework;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.*;

/**
 * Acquires locks and releases them.
 * Periodically prolongs acquired locks.
 *
 * @author Kamil Asfandiyarov
 */
public class PersistentExpiringLockManager implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(PersistentExpiringLockManager.class);

    public static final long DEFAULT_RESERVATION_PERIOD_MS = TimeUnit.MINUTES.toMillis(15);
    public static final long DEFAULT_ACQUIRING_TIMEOUT_MS = TimeUnit.SECONDS.toMillis(10);
    public static final long DEFAULT_EXPIRATION_PERIOD_MS = TimeUnit.MINUTES.toMillis(6);
    public static final long DEFAULT_LOCK_PROLONGATION_INTERVAL_MS = TimeUnit.MINUTES.toMillis(3);

    protected final CuratorFramework curatorFramework;
    protected final String workerId;
    private final Map<LockIdentity, LockContainer> locks = new ConcurrentHashMap<>();
    private final ScheduledExecutorService defaultScheduledExecutorService;

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

    public PersistentExpiringLockManager(
            CuratorFramework curatorFramework,
            String workerId
    ) {
        this.curatorFramework = curatorFramework;
        this.workerId = workerId;

        Runnable locksProlongationTask = () -> locks.forEach((lockId, lockContainer) -> {
            if (!checkAndProlongIfAvailable(lockId, lockContainer.lock)) {
                log.info("Failed lock prolongation on wid={} for lockId={}", workerId, lockId.getId());
                lockContainer.prolongationListener.onLockProlongationFailed(lockId);
            }
        });
        defaultScheduledExecutorService = Executors.newSingleThreadScheduledExecutor(r ->
                new Thread(r, "lock-prolongation")
        );
        defaultScheduledExecutorService.scheduleWithFixedDelay(
                locksProlongationTask,
                0L,
                DEFAULT_LOCK_PROLONGATION_INTERVAL_MS,
                TimeUnit.MILLISECONDS
        );
    }

    public boolean tryAcquire(LockIdentity lockId, LockProlongationFailedListener listener) {
        try {
            PersistentExpiringDistributedLock persistentLock = new PersistentExpiringDistributedLock(
                    curatorFramework,
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

    public boolean isLiveLockExist(LockIdentity lockId) {
        return locks.containsKey(lockId);
    }

    public void release(LockIdentity lockId) {
        LockContainer persistentLockContainer = this.locks.get(lockId);
        if (persistentLockContainer == null || persistentLockContainer.lock == null) {
            log.error("Illegal state. Persistent lock for lockId={} doesn't exist.", lockId.getId());
            return;
        }

        if (persistentLockContainer.release()) {
            log.info("Worker with wid={} released lock with lockId={}", workerId, lockId.getId());
            locks.remove(lockId);
        } else {
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
            log.error("Failed to checkAndProlong persistent locks with lockId {}", lockId.getId(), e);
            return false;
        }
    }

    @Override
    public void close() {
        locks.forEach((lockId, lockContainer) -> {
            if (lockId != null && lockContainer != null) {
                try {
                    log.warn("Active not released lock with lockId={} closed.", lockId);
                    lockContainer.lock.close();
                } catch (Exception e) {
                    log.error("Failed to close lock with lockId={}", lockId, e);
                }
            }
        });
        locks.clear();

        ScheduledExecutorService service = this.defaultScheduledExecutorService;
        if (service != null) {
            service.shutdown();
        }
    }
}
