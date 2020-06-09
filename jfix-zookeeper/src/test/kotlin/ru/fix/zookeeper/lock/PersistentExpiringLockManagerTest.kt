package ru.fix.zookeeper.lock

import io.kotest.matchers.booleans.shouldBeFalse
import io.kotest.matchers.booleans.shouldBeTrue
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.runBlocking
import org.apache.curator.framework.state.ConnectionState
import org.apache.curator.utils.ZKPaths
import org.apache.logging.log4j.kotlin.logger
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
    private val logger = logger()
    private lateinit var zkServer: ZKTestingServer
    private lateinit var manager1: PersistentExpiringLockManager
    private lateinit var manager2: PersistentExpiringLockManager

    companion object {
        private val lastLockId = AtomicInteger(0)
        fun nextId(): Int = lastLockId.incrementAndGet()

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
    fun `close zk and managers`() {
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
    fun `can not acquire already acquired lock`() {
        val lockId = createLockIdentity()
        manager1.tryAcquire(lockId) { fail() }.shouldBeTrue()
        manager1.tryAcquire(lockId) { fail() }.shouldBeFalse()
    }

    @Test
    fun `first manager acquires lock, second manager fails to acquire same lock`() {
        val lockId = createLockIdentity()

        manager1.tryAcquire(lockId) { fail() }.shouldBeTrue()
        manager2.tryAcquire(lockId) { fail() }.shouldBeFalse()
        manager1.release(lockId)
    }

    @Test
    fun `first manager release lock, second manager acquires same lock`() {
        val lockId = createLockIdentity()

        manager1.tryAcquire(lockId) { fail() }.shouldBeTrue()
        manager2.tryAcquire(lockId) { fail() }.shouldBeFalse()
        manager1.release(lockId)
        manager2.tryAcquire(lockId) { fail()}.shouldBeTrue()
    }

    @Test
    fun `lock node created after acquiring and removed after releasing lock`() {
        val lockId = LockIdentity("/lock-1", "/locks/lock-1")
        manager1.tryAcquire(lockId) { fail() }

        logger.info("After acquiring lock: \n" + zkTree())
        nodeExists(lockId.nodePath).shouldBeTrue()

        manager1.release(lockId)
        logger.info("After releasing lock: \n" + zkTree())
        nodeExists(lockId.nodePath).shouldBeFalse()
    }


    @Test
    fun `locks nodes removed after close of zk client connection`() {
        val lockId1 = LockIdentity("/lock-1", "/locks/lock-1")
        val lockId2 = LockIdentity("/lock-2", "/locks/lock-2")
        manager1.tryAcquire(lockId1) { fail() }
        manager1.tryAcquire(lockId2) { fail() }

        logger.info("After acquiring locks: \n" + zkTree())
        nodeExists(lockId1.nodePath).shouldBeTrue()
        nodeExists(lockId2.nodePath).shouldBeTrue()

        manager1.close()
        logger.info("After closing lock manager: \n" + zkTree())
        nodeExists(lockId1.nodePath).shouldBeFalse()
        nodeExists(lockId2.nodePath).shouldBeFalse()
    }

    @Test
    fun `after manager1 lock prolongation, manager2 can not acquire lock`() {
        val lockId = createLockIdentity()
        val fastFrequentManager = createLockManager(PersistentExpiringLockManagerConfig(
                lockAcquirePeriod = Duration.ofSeconds(2),
                expirationPeriod = Duration.ofSeconds(1),
                lockCheckAndProlongInterval = Duration.ofMillis(100)
        ))

        fastFrequentManager.tryAcquire(lockId) { fail() }.shouldBeTrue()

        val lockData = zkServer.client.data.forPath(lockId.nodePath)

        //give time for manager1 to apply prolongation
        await().atMost(1, MINUTES).until {
            val newLockData = zkServer.client.data.forPath(lockId.nodePath)
            !newLockData!!.contentEquals(lockData)
        }

        manager2.tryAcquire(lockId) { fail() }.shouldBeFalse()
        fastFrequentManager.release(lockId)

        await().atMost(1, MINUTES).until { manager2.tryAcquire(lockId) { fail() } }

        fastFrequentManager.close()
    }


    @Test
    fun `acquire and release 30 different locks in parallel`() = runBlocking {
        val locksPath = "/locks" + nextId()
        fun createLockIdentityForIndex(index: Int) =
                LockIdentity(ZKPaths.makePath(locksPath, "lock-$index"), "meta: $index")

        val locksCount = 30
        val dispatcher = Executors.newFixedThreadPool(12).asCoroutineDispatcher()

        val lockManagers = (1..locksCount).map {
            async(context = dispatcher) { createLockManager() }
        }.awaitAll()

        lockManagers.mapIndexed { i, it ->
            async(context = dispatcher) {
                val lockId = createLockIdentityForIndex(i)
                it.tryAcquire(lockId) { fail() }.shouldBeTrue()

                logger.info("After acquiring lock: \n" + zkTree())
                assertTrue(nodeExists(lockId.nodePath))
            }
        }.awaitAll()

        assertEquals(locksCount, zkServer.client.children.forPath(locksPath).size)

        lockManagers.mapIndexed { i, it ->
            async(context = dispatcher) {
                val lockId = createLockIdentityForIndex(i)
                it.release(lockId)

                logger.info("After releasing lock: \n" + zkTree())
                assertFalse(nodeExists(lockId.nodePath))
            }
        }.awaitAll()

        assertEquals(0, zkServer.client.children.forPath(locksPath).size)
    }

    @Test
    fun `connection lost, lock expired, manager prolongs expired but owned lock`(){

        val proxyTcpCrusher = openProxyTcpCrusher()
        val zkProxyClient = createZkProxyClient(proxyTcpCrusher)
        val zkProxyState = startWatchZkProxyClientState(zkProxyClient)

        val id = idSequence.get()
        val lock1 = PersistentExpiringDistributedLock(zkProxyClient, LockIdentity(lockPath(id)))
        lock1.expirableAcquire(Duration.ofSeconds(100), Duration.ofSeconds(1)).shouldBeTrue()

        networkFailure.activate(proxyTcpCrusher)
        await().atMost(10, MINUTES).until { zkProxyState.get() == ConnectionState.LOST }

        val lock2 = PersistentExpiringDistributedLock(zkServer.client, LockIdentity(lockPath(id)))
        lock2.expirableAcquire(Duration.ofMillis(100), Duration.ofMillis(100)).shouldBeFalse()
        lock2.close()
    }

    @Test
    fun `connection lost, lock expired, manager fails to prolong lock and notifies client with callback`() {

    }

    private fun createLockIdentity(id: Int = nextId()) =
            LockIdentity(ZKPaths.makePath(LOCK_PATH, id.toString()), "meta: $LOCK_PATH/id")

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