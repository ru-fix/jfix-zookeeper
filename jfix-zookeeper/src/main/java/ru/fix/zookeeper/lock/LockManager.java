package ru.fix.zookeeper.lock;

public interface LockManager extends AutoCloseable {

    boolean tryAcquire(LockIdentity lockId, LockProlongationFailedListener listener);

    boolean existsLock(LockIdentity lockId);

    void release(LockIdentity lockId);
}
