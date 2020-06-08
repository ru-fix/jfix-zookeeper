package ru.fix.zookeeper.lock;

@FunctionalInterface
public interface LockProlongationFailedListener {
    void onLockProlongationFailed(LockIdentity lockIdentity);
}
