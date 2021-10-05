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

        val containsLockBeforePutting = activeLocksContainer.contains(lockId)
        val lockOfEmptyContainer = activeLocksContainer.putLock(lockId, lock, LockProlongationFailedListener {})
        val containsLockAfterPutting = activeLocksContainer.contains(lockId)
        val gotLockState = activeLocksContainer.getLockState(lockId)
        val removedLock = activeLocksContainer.removeLock(lockId)
        val containsLockAfterRemoving = activeLocksContainer.contains(lockId)
        val gotLockStateAfterRemove = activeLocksContainer.getLockState(lockId)
        val removeAfterRemoveLock = activeLocksContainer.removeLock(lockId)

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
