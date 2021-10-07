package ru.fix.zookeeper.lock

import mu.KotlinLogging
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentMap
import java.util.concurrent.locks.ReentrantLock

class ActiveLocksContainer {
    companion object {
        private val logger = KotlinLogging.logger { }
    }

    /**
     * remove, iterate operations MUST BE synced under [globalLock]
     */
    private val locks: ConcurrentMap<LockIdentity, LockContainer> = ConcurrentHashMap()

    /**
     * all operations with accessing to [locks] has been synced with this object
     * LockContainer is encapsulated inside this class so for performance issue this lock could be
     * replaced with syncing via LockContainer
     */
    private val globalLock = ReentrantLock(true)

    /**
     * @return - old value in the map that was replaced by [lock]
     * old value state could be changed in another thread
     */
    fun put(
            lockId: LockIdentity,
            lock: PersistentExpiringDistributedLock,
            listener: LockProlongationFailedListener
    ): PersistentExpiringDistributedLock? {
        val newLockContainer = LockContainer(lock, listener)
        val oldLockContainer = locks.put(lockId, newLockContainer)
        return oldLockContainer?.lock
    }

    fun remove(lockId: LockIdentity): PersistentExpiringDistributedLock? {
        globalLock.lock()
        return try {
            locks.remove(lockId)?.lock
        } finally {
            globalLock.unlock()
        }
    }

    operator fun contains(lockId: LockIdentity): Boolean {
        return locks.containsKey(lockId)
    }

    /**
     * @return - lock state could be changed in another thread
     */
    fun get(lockId: LockIdentity): PersistentExpiringDistributedLock? {
        return locks[lockId]?.lock
    }

    /**
     * @param lockProcessing - returns what to do with Lock in collection
     */
    fun processAllLocks(
            lockProcessing: (
                    LockIdentity, PersistentExpiringDistributedLock, LockProlongationFailedListener
            ) -> ProcessingLockResult
    ) {
        locks.forEach { (lockId: LockIdentity, lockContainer: LockContainer) ->
            globalLock.lock()
            try {
                if (locks.containsKey(lockId)) {
                    val processingLockResult = lockProcessing(
                            lockId, lockContainer.lock, lockContainer.prolongationFailedListener
                    )
                    when (processingLockResult) {
                        ProcessingLockResult.REMOVE_LOCK_FROM_CONTAINER -> locks.remove(lockId)
                        ProcessingLockResult.KEEP_LOCK_IN_CONTAINER -> {
                            // nothing to do
                        }
                    }
                } else {
                    logger.trace("Lock with lockId {} already has been removed by other thread", lockId)
                }
            } finally {
                globalLock.unlock()
            }
        }
    }

    private data class LockContainer(
            val lock: PersistentExpiringDistributedLock,
            val prolongationFailedListener: LockProlongationFailedListener
    )
}

enum class ProcessingLockResult {
    KEEP_LOCK_IN_CONTAINER, REMOVE_LOCK_FROM_CONTAINER
}
