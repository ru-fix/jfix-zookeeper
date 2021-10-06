package ru.fix.zookeeper.lock

import io.kotest.matchers.shouldBe
import io.kotest.matchers.shouldNotBe
import org.apache.curator.utils.ZKPaths
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import ru.fix.zookeeper.testing.ZKTestingServer

internal class ActiveLocksContainerTest {

    private lateinit var zkServer: ZKTestingServer

    @BeforeEach
    fun setup() {
        zkServer = ZKTestingServer()
                .withCloseOnJvmShutdown()
                .start()
    }

    @AfterEach
    fun shutdown() {
        zkServer.close()
    }

    @Test
    fun `test activeLockContainer put contains get remove getState operations`() {
        val activeLocksContainer = ActiveLocksContainer()
        val lockId = LockIdentity(ZKPaths.makePath("/locks", 1.toString()), "meta: ${PersistentExpiringLockManagerTest.LOCK_PATH}/id")
        val lock = PersistentExpiringDistributedLock(
                zkServer.client,
                lockId
        )

        val containsLockBeforePutting = lockId in activeLocksContainer
        val lockOfEmptyContainer = activeLocksContainer.put(lockId, lock, LockProlongationFailedListener {})
        val containsLockAfterPutting = lockId in activeLocksContainer
        val gotLockState = activeLocksContainer.get(lockId)
        val removedLock = activeLocksContainer.remove(lockId)
        val containsLockAfterRemoving = lockId in activeLocksContainer
        val gotLockStateAfterRemove = activeLocksContainer.get(lockId)
        val removeAfterRemoveLock = activeLocksContainer.remove(lockId)

        containsLockBeforePutting shouldBe false
        lockOfEmptyContainer shouldBe null
        containsLockAfterPutting shouldBe true
        gotLockState shouldNotBe null
        removedLock shouldBe lock
        containsLockAfterRemoving shouldBe false
        gotLockStateAfterRemove shouldBe null
        removeAfterRemoveLock shouldBe null
    }
}
