package ru.fix.zookeeper.lock

import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import ru.fix.aggregating.profiler.NoopProfiler
import ru.fix.zookeeper.AbstractZookeeperTest
import java.util.concurrent.Executors

internal class LockManagerImplTest : AbstractZookeeperTest() {
    private lateinit var lockManager: PersistentExpiringLockManager

    @BeforeEach
    fun setUpSecond() {
        lockManager = lockManager()
    }

    @Test
    fun `node of lock created after acquiring and removed after releasing lock`() {
        val lockId = LockIdentity("lock-1", "$rootPath/locks/lock-1")
        lockManager.tryAcquire(lockId) {}

        println("After acquiring lock: \n" + zkTree())
        assertTrue(nodeExists(lockId.nodePath))

        lockManager.release(lockId)
        println("After releasing lock: \n" + zkTree())
        assertFalse(nodeExists(lockId.nodePath))
    }

    @Test
    fun `nodes of locks removed after close of zk client connection`() {
        val lockId1 = LockIdentity("lock-1", "$rootPath/locks/lock-1")
        val lockId2 = LockIdentity("lock-2", "$rootPath/locks/lock-2")
        lockManager.tryAcquire(lockId1) {}
        lockManager.tryAcquire(lockId2) {}

        println("After acquiring locks: \n" + zkTree())
        assertTrue(nodeExists(lockId1.nodePath))
        assertTrue(nodeExists(lockId2.nodePath))

        lockManager.close()
        println("After closing lock manager: \n" + zkTree())
        assertFalse(nodeExists(lockId1.nodePath))
        assertFalse(nodeExists(lockId2.nodePath))
    }

    @Test
    fun `start 30 parallel workers and try lock and unlock on every of them`() = runBlocking {
        val locksCount = 30
        val dispatcher = executor("lock-manager-launcher").asCoroutineDispatcher()

        val lockManagers = (1..locksCount).map {
            async(context = dispatcher) { lockManager() }
        }.awaitAll()

        lockManagers.mapIndexed { i, it ->
            async(context = dispatcher) {
                val lockId = LockIdentity("lock-$i", "$rootPath/locks/lock-$i")
                val acquired = it.tryAcquire(lockId) {}
                assertTrue(acquired)

                println("After acquiring lock: \n" + zkTree())
                assertTrue(nodeExists(lockId.nodePath))
            }
        }.awaitAll()

        assertEquals(locksCount, lockManagers.first().curatorFramework.children.forPath("$rootPath/locks").size)

        lockManagers.mapIndexed { i, it ->
            async(context = dispatcher) {
                val lockId = LockIdentity("lock-$i", "$rootPath/locks/lock-$i")
                it.release(lockId)

                println("After releasing lock: \n" + zkTree())
                assertFalse(nodeExists(lockId.nodePath))
            }
        }.awaitAll()

        assertEquals(0, lockManagers.first().curatorFramework.children.forPath("$rootPath/locks").size)
    }

    private fun executor(
            poolName: String,
            poolSize: Int = 12
    ) = Executors.newFixedThreadPool(poolSize) {
        Thread(it, poolName)
    }

    private fun lockManager() = PersistentExpiringLockManager(
            testingServer.createClient(),
            PersistentExpiringLockManagerConfig(),
            NoopProfiler()
    )

    private fun nodeExists(path: String) = testingServer.client.checkExists().forPath(path) != null
}