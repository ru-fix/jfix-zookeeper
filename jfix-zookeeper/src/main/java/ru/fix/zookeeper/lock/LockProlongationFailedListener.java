package ru.fix.zookeeper.lock;

@FunctionalInterface
public interface LockProlongationFailedListener {
    /**
     * {@link PersistentExpiringLockManager} failed to prolong lock.
     * Either due to connection problem.
     * Or lock was removed or expired.
     * Lock is removed from {@link PersistentExpiringLockManager}
     */
    void onLockProlongationFailedAndRemoved(LockIdentity lockIdentity);
}
