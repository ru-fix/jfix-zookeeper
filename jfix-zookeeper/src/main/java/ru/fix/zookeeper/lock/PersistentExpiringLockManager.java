package ru.fix.zookeeper.lock;

import org.apache.curator.framework.CuratorFramework;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.fix.zookeeper.utils.Marshaller;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Acquires locks and releases them.
 * Periodically prolongs acquired locks.
 *
 * @author Kamil Asfandiyarov
 */
public class PersistentExpiringLockManager implements AutoCloseable {

    private static final Logger logger = LoggerFactory.getLogger(PersistentExpiringLockManager.class);

    protected final CuratorFramework curatorFramework;
    protected final PersistentExpiringLockManagerConfig config;
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
            PersistentExpiringLockManagerConfig config
    ) {
        this.curatorFramework = curatorFramework;
        this.config = config;

        Runnable locksProlongationTask = () -> locks.forEach((lockId, lockContainer) -> {
            if (!checkAndProlongIfAvailable(lockId, lockContainer.lock)) {
                logger.info("Failed lock prolongation for lockId={}", Marshaller.marshall(lockId));
                lockContainer.prolongationListener.onLockProlongationFailed(lockId);
            }
        });
        defaultScheduledExecutorService = Executors.newSingleThreadScheduledExecutor(r ->
                new Thread(r, "lock-prolongation")
        );
        defaultScheduledExecutorService.scheduleWithFixedDelay(
                locksProlongationTask,
                0L,
                config.getLockProlongationInterval().toMillis(),
                TimeUnit.MILLISECONDS
        );
    }

    public boolean tryAcquire(LockIdentity lockId, LockProlongationFailedListener listener) {
        try {
            PersistentExpiringDistributedLock persistentLock = new PersistentExpiringDistributedLock(
                    curatorFramework,
                    lockId
            );
            Duration reservationPeriod = config.getReservationPeriod();
            Duration acquiringTimeout = config.getAcquiringTimeout();
            if (!persistentLock.expirableAcquire(reservationPeriod, acquiringTimeout)) {
                logger.debug(
                        "Failed to acquire expirable lock. Acquire period: {}, timeout: {}, lockId: {}",
                        reservationPeriod, acquiringTimeout, Marshaller.marshall(lockId)
                );
                persistentLock.close();
                return false;
            }

            LockContainer oldLock = locks.put(lockId, new LockContainer(persistentLock, listener));
            String marshalledLock = Marshaller.marshall(lockId);
            if (oldLock != null) {
                logger.error("Illegal state of locking for lockId={}. Lock already acquired.", marshalledLock);

                locks.remove(lockId);

                oldLock.close();
                persistentLock.close();
                return false;
            }
            logger.info("Lock with lockId={} with successfully acquired", marshalledLock);
            return true;

        } catch (Exception e) {
            logger.error(
                    "Failed to create PersistentExpiringDistributedLock with lockId={}",
                    Marshaller.marshall(lockId), e
            );
            return false;
        }
    }

    public boolean isLiveLockExist(LockIdentity lockId) {
        return locks.containsKey(lockId);
    }

    public void release(LockIdentity lockId) {
        LockContainer persistentLockContainer = this.locks.get(lockId);
        if (persistentLockContainer == null || persistentLockContainer.lock == null) {
            logger.error("Illegal state. Persistent lock for lockId={} doesn't exist.", Marshaller.marshall(lockId));
            return;
        }

        if (persistentLockContainer.release()) {
            logger.info("Released lock with lockId={}", Marshaller.marshall(lockId));
            locks.remove(lockId);
        } else {
            try {
                persistentLockContainer.close();
            } catch (Exception e) {
                logger.error("Error during closing lock with lockId={}", Marshaller.marshall(lockId), e);
            }
        }
    }

    private boolean checkAndProlongIfAvailable(
            LockIdentity lockId,
            PersistentExpiringDistributedLock lock
    ) {
        try {
            logger.info("Method checkAndProlong lockId={}", lockId.getId());
            return lock.checkAndProlongIfExpiresIn(config.getReservationPeriod(), config.getExpirationPeriod());
        } catch (Exception e) {
            logger.error("Failed to checkAndProlong persistent locks with lockId {}", Marshaller.marshall(lockId), e);
            return false;
        }
    }

    @Override
    public void close() {
        locks.forEach((lockId, lockContainer) -> {
            if (lockId != null && lockContainer != null) {
                try {
                    logger.warn("Active not released lock with lockId={} closed.", Marshaller.marshall(lockId));
                    lockContainer.lock.close();
                } catch (Exception e) {
                    logger.error("Failed to close lock with lockId={}", Marshaller.marshall(lockId), e);
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
