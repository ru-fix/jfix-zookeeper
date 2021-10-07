package ru.fix.zookeeper.lock

import io.kotest.matchers.shouldBe
import io.kotest.matchers.shouldNotBe
import org.apache.curator.utils.ZKPaths
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import ru.fix.zookeeper.testing.ZKTestingServer
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

internal class ActiveLocksContainerTest {
    companion object {
        val lastLockId = AtomicInteger(1)
    }

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
        val lockId = createLockId()
        val lock = PersistentExpiringDistributedLock(zkServer.client, lockId)

        (lockId in activeLocksContainer) shouldBe false
        val putValue = activeLocksContainer.put(lockId, lock, LockProlongationFailedListener {})
        putValue shouldBe null
        (lockId in activeLocksContainer) shouldBe true
        val getLock = activeLocksContainer.get(lockId)
        getLock shouldBe lock
        val removedLock = activeLocksContainer.remove(lockId)
        removedLock shouldBe lock
        (lockId in activeLocksContainer) shouldBe false
        activeLocksContainer.get(lockId) shouldBe null
        activeLocksContainer.remove(lockId) shouldBe null
    }

    @Test
    fun `concurrency iterate remove lock`() {
        val activeLocksContainer = ActiveLocksContainer()
        val lockId = createLockId()
        val lock = PersistentExpiringDistributedLock(zkServer.client, lockId)
        activeLocksContainer.put(lockId, lock, LockProlongationFailedListener {})
        val states = mutableListOf<PersistentExpiringDistributedLock.State>()

        val executor = Executors.newSingleThreadExecutor()
        executor.execute {
            activeLocksContainer.processAllLocks { lockIdentity, lock, _ ->
                TimeUnit.MILLISECONDS.sleep(100)
                states.add(lock.state)
                return@processAllLocks ProcessingLockResult.REMOVE_LOCK_FROM_CONTAINER
            }
        }
        TimeUnit.MILLISECONDS.sleep(20)
        activeLocksContainer.remove(lockId)

        states.isNotEmpty() shouldBe true
        states.first() shouldNotBe null
    }

    private fun createLockId() = LockIdentity(ZKPaths.makePath(
            "/locks", lastLockId.incrementAndGet().toString()),
            "meta: ${PersistentExpiringLockManagerTest.LOCK_PATH}/id"
    )

}
