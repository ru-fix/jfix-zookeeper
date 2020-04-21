package ru.fix.zookeeper.lock;

@FunctionalInterface
interface LockProlongationFailedListener {
    void onLockProlongationFailed();
}
