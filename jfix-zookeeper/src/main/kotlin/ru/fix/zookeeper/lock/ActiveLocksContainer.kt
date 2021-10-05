package ru.fix.zookeeper.lock

import mu.KotlinLogging
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentMap
import java.util.concurrent.locks.ReentrantLock

class ActiveLocksContainer : AutoCloseable {
    companion object {
        private val logger = KotlinLogging.logger { }
    }

    /**
     * remove, prolong and getState operations MUST BE synced under [.globalLock]
     */
    private val locks: ConcurrentMap<LockIdentity, LockContainer> = ConcurrentHashMap()

    /**
     * all operations with accessing to [.locks] has been synced with this object
     * LockContainer is encapsulated inside this class so for performance issue this lock could be
     * replaced with syncing via LockContainer
     */
    private val globalLock = ReentrantLock(true)

    /**
     * @return - old value in the map
     */
    fun putLock(
            lockId: LockIdentity,
            lock: PersistentExpiringDistributedLock,
            listener: LockProlongationFailedListener
    ): PersistentExpiringDistributedLock? {
        val newLockContainer = LockContainer(lock, listener)
        val oldLockContainer = locks.put(lockId, newLockContainer)
        return oldLockContainer?.lock
    }

    fun removeLock(lockId: LockIdentity): PersistentExpiringDistributedLock? {
        return try {
            globalLock.lock()
            locks.remove(lockId)?.lock
        } finally {
            globalLock.unlock()
        }
    }

    operator fun contains(lockId: LockIdentity): Boolean {
        return locks.containsKey(lockId)
    }

    @Throws(Exception::class)
    fun getLockState(lockId: LockIdentity): PersistentExpiringDistributedLock.State? {
        return try {
            globalLock.lock()
            locks[lockId]?.lock?.state
        } finally {
            globalLock.unlock()
        }
    }

    /**
     * @param lockProcessing       - returns what to do with Lock in collection
     * @param resultPostProcessing - does some work after processing lock and applied action
     */
    fun processAllLocks(
            lockProcessing: (LockIdentity, PersistentExpiringDistributedLock) -> ProcessingLockResult,
            resultPostProcessing: (LockIdentity, LockProlongationFailedListener, ProcessingLockResult) -> Unit
    ) {
        locks.forEach { (lockId: LockIdentity, lockContainer: LockContainer) ->
            try {
                globalLock.lock()
                val processingLockResult = if (locks.containsKey(lockId)) {
                    lockProcessing(lockId, lockContainer.lock)
                } else {
                    ProcessingLockResult.ALREADY_REMOVED
                }
                when (processingLockResult) {
                    ProcessingLockResult.REMOVE_LOCK_FROM_CONTAINER -> locks.remove(lockId)
                    ProcessingLockResult.KEEP_LOCK_IN_CONTAINER -> {
                        // nothing to do
                    }
                    ProcessingLockResult.ALREADY_REMOVED ->
                        logger.trace("Lock with lockId {} already has been removed by other thread", lockId)
                }
                resultPostProcessing(lockId, lockContainer.prolongationFailedListener, processingLockResult)
            } finally {
                globalLock.unlock()
            }
        }
    }

    override fun close() {
        processAllLocks(
                lockProcessing = { lockId, lock ->
                    try {
                        lock.close()
                    } catch (e: Exception) {
                        logger.error(e) { "can't close lock with lockId = $lockId in ActiveLockContainer" }
                    }
                    ProcessingLockResult.REMOVE_LOCK_FROM_CONTAINER
                },
                resultPostProcessing = { _, _, _ ->
                    // nothing to do
                }
        )
        locks.clear()
    }

    private data class LockContainer(
            val lock: PersistentExpiringDistributedLock,
            val prolongationFailedListener: LockProlongationFailedListener
    )
}

enum class ProcessingLockResult {
    KEEP_LOCK_IN_CONTAINER, REMOVE_LOCK_FROM_CONTAINER, ALREADY_REMOVED
}
