package ru.fix.zookeeper.lock

import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.async
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import ru.fix.aggregating.profiler.NoopProfiler
import ru.fix.dynamic.property.api.DynamicProperty
import ru.fix.stdlib.concurrency.threads.NamedExecutors
import ru.fix.zookeeper.AbstractZookeeperTest

internal class LockManagerImplTest : AbstractZookeeperTest() {
    private lateinit var lockManager: LockManager

    @BeforeEach
    fun setUpSecond() {
        lockManager = LockManagerImpl(testingServer.createClient(), "test-worker", NoopProfiler())
    }

    @Test
    fun test() {
        val lockId = LockIdentity("lock-1", "$rootPath/locks/lock-1")
        lockManager.tryAcquire(lockId) {}

        println("After acquiring lock: \n" + zkTree())
        assertTrue(nodeExists(lockId.nodePath))

        lockManager.release(lockId)
        println("After releasing lock: \n" + zkTree())
        assertFalse(nodeExists(lockId.nodePath))
    }

    @Test
    fun test1() {
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
    fun test3() {
        val locksCount = 30
        val dispatcher = NamedExecutors.newDynamicPool("lock-manager-pool", DynamicProperty.of(12), NoopProfiler())
                .asCoroutineDispatcher()
        (1..locksCount).map {
            GlobalScope.async(context = dispatcher) {
                lockManager = LockManagerImpl(testingServer.createClient(), "test-worker", NoopProfiler())
            }
        }

        val lockId = LockIdentity("lock-1", "$rootPath/locks/lock-1")
        lockManager.tryAcquire(lockId) {}

        println("After acquiring lock: \n" + zkTree())
        assertTrue(nodeExists(lockId.nodePath))

        lockManager.release(lockId)
        println("After releasing lock: \n" + zkTree())
        assertFalse(nodeExists(lockId.nodePath))
    }


    private fun nodeExists(path: String) = testingServer.client.checkExists().forPath(path) != null
}