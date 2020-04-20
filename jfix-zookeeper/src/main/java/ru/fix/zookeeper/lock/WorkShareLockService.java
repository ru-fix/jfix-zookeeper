package ru.fix.zookeeper.lock;

public interface WorkShareLockService extends AutoCloseable {

    boolean tryAcquire(
            DistributedJob job,
            String workItem,
            WorkShareLockServiceImpl.LockProlongationFailedListener listener);

    boolean existsLock(DistributedJob job, String workItem);

    void release(DistributedJob job, String workItem);
}
