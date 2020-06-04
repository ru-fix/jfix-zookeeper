package ru.fix.zookeeper.lock

import io.kotest.assertions.throwables.shouldThrow
import io.kotest.matchers.booleans.shouldBeFalse
import io.kotest.matchers.booleans.shouldBeTrue
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.runBlocking
import org.awaitility.Awaitility.await
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import ru.fix.aggregating.profiler.NoopProfiler
import ru.fix.dynamic.property.api.DynamicProperty
import ru.fix.zookeeper.testing.ZKTestingServer
import ru.fix.zookeeper.utils.ZkTreePrinter
import java.time.Duration
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit.MINUTES
import java.util.concurrent.atomic.AtomicInteger

internal class PersistentExpiringLockManagerTest {
    private lateinit var zkServer: ZKTestingServer
    private lateinit var manager1: PersistentExpiringLockManager
    private lateinit var manager2: PersistentExpiringLockManager

    companion object {
        private val lastLockId = AtomicInteger(0)
        fun nextLockId(): Int = lastLockId.incrementAndGet()

        const val LOCK_PATH = "/locks"
    }

    @BeforeEach
    fun `start zk and create managers`() {
        zkServer = ZKTestingServer()
                .withCloseOnJvmShutdown(true)
                .start()
        
        manager1 = createLockManager()
        manager2 = createLockManager()
    }

    @AfterEach
    fun `close zk and managers`(){
        manager1.close()
        manager2.close()

        zkServer.close()
    }

    @Test
    fun `acquire and release lock`() {
        val lockId = createLockIdentity()
        manager1.tryAcquire(lockId) { fail() }.shouldBeTrue()
        manager1.release(lockId)
    }

    @Test
    fun `after acquiring lock is managed`() {
        val lockId = createLockIdentity()
        manager1.tryAcquire(lockId) { fail() }.shouldBeTrue()
        manager1.isLockManaged(lockId).shouldBeTrue()
        manager1.release(lockId)
    }

    @Test
    fun `after acquiring lock state is owned and not expired`() {
        val lockId = createLockIdentity()
        manager1.tryAcquire(lockId) { fail() }.shouldBeTrue()
        manager1.getLockState(lockId).apply {
            isPresent.shouldBeTrue()
            get().apply {
                isOwn.shouldBeTrue()
                isExpired.shouldBeFalse()
            }
        }
        manager1.release(lockId)
    }

    @Test
    fun `acquiring already acquired lock leads to exception`() {
        manager1.tryAcquire(createLockIdentity()) { fail() }.shouldBeTrue()
        shouldThrow<IllegalStateException> {
            manager1.tryAcquire(createLockIdentity()) { fail() }
        }
    }

    @Test
    fun `first manager acquires lock, second manager fails to acquire same lock`() {
        val lockId = createLockIdentity()

        manager1.tryAcquire(lockId) { fail() }.shouldBeTrue()
        manager2.tryAcquire(lockId) { fail() }.shouldBeFalse()
        manager1.release(lockId)
    }

    @Test
    fun `lock node created after acquiring and removed after releasing lock`() {
        val lockId = LockIdentity("lock-1", "/locks/lock-1")
        manager1.tryAcquire(lockId) { fail() }

        println("After acquiring lock: \n" + zkTree())
        nodeExists(lockId.nodePath).shouldBeTrue()

        manager1.release(lockId)
        println("After releasing lock: \n" + zkTree())
        nodeExists(lockId.nodePath).shouldBeFalse()
    }


    @Test
    fun `locks nodes removed after close of zk client connection`() {
        val lockId1 = LockIdentity("lock-1", "/locks/lock-1")
        val lockId2 = LockIdentity("lock-2", "/locks/lock-2")
        manager1.tryAcquire(lockId1) { fail() }
        manager1.tryAcquire(lockId2) { fail() }

        println("After acquiring locks: \n" + zkTree())
        nodeExists(lockId1.nodePath).shouldBeTrue()
        nodeExists(lockId2.nodePath).shouldBeTrue()

        manager1.close()
        println("After closing lock manager: \n" + zkTree())
        nodeExists(lockId1.nodePath).shouldBeFalse()
        nodeExists(lockId2.nodePath).shouldBeFalse()
    }

    @Test
    fun `while manager1 lock is prolonged, manager2 can not acquire lock`() {
        val lockId = createLockIdentity()
        val fastFrequentManager = createLockManager(PersistentExpiringLockManagerConfig(
                lockAcquirePeriod = Duration.ofSeconds(2),
                expirationPeriod = Duration.ofSeconds(1),
                lockCheckAndProlongInterval = Duration.ofMillis(100)
        ))

        fastFrequentManager.tryAcquire(lockId) { fail() }.shouldBeTrue()

        Thread.sleep(5000)

        manager2.tryAcquire(lockId) { fail() }.shouldBeFalse()
        fastFrequentManager.release(lockId)

        await().atMost(1, MINUTES).until { manager2.tryAcquire(lockId) { fail() } }

        fastFrequentManager.close()
    }

    @Test
    fun `acquire and release 30 different locks in parallel`() = runBlocking {
        val locksCount = 30
        val dispatcher = Executors.newFixedThreadPool(12).asCoroutineDispatcher()

        val lockManagers = (1..locksCount).map {
            async(context = dispatcher) { createLockManager() }
        }.awaitAll()

        lockManagers.mapIndexed { i, it ->
            async(context = dispatcher) {
                val lockId = LockIdentity("lock-$i", "/locks/lock-$i")
                it.tryAcquire(lockId) { fail() }.shouldBeTrue()

                println("After acquiring lock: \n" + zkTree())
                assertTrue(nodeExists(lockId.nodePath))
            }
        }.awaitAll()

        assertEquals(locksCount, zkServer.client.children.forPath("/locks").size)

        lockManagers.mapIndexed { i, it ->
            async(context = dispatcher) {
                val lockId = LockIdentity("lock-$i", "/locks/lock-$i")
                it.release(lockId)

                println("After releasing lock: \n" + zkTree())
                assertFalse(nodeExists(lockId.nodePath))
            }
        }.awaitAll()

        assertEquals(0, zkServer.client.children.forPath("/locks").size)
    }

    fun createLockIdentity(id: Int = nextLockId()) = LockIdentity("$LOCK_PATH/id", "meta: $LOCK_PATH/id")

    private fun createLockManager(
            config: PersistentExpiringLockManagerConfig = PersistentExpiringLockManagerConfig()) =
            PersistentExpiringLockManager(
                    zkServer.createClient(),
                    DynamicProperty.of(config),
                    NoopProfiler()
            )

    private fun nodeExists(path: String) = zkServer.client.checkExists().forPath(path) != null

    private fun zkTree() = ZkTreePrinter(zkServer.client).print("/")
}