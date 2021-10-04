package ru.fix.zookeeper.lock

import io.kotest.matchers.booleans.shouldBeFalse
import io.kotest.matchers.booleans.shouldBeTrue
import io.kotest.matchers.shouldBe
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.runBlocking
import org.apache.curator.framework.CuratorFramework
import org.apache.curator.utils.ZKPaths
import org.apache.logging.log4j.kotlin.logger
import org.awaitility.Awaitility.await
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import ru.fix.aggregating.profiler.NoopProfiler
import ru.fix.dynamic.property.api.DynamicProperty
import ru.fix.stdlib.concurrency.threads.NamedExecutors
import ru.fix.stdlib.concurrency.threads.Schedule
import ru.fix.zookeeper.lock.PersistentExpiringLockManager.ActiveLocksContainer.ProcessingLockResult
import ru.fix.zookeeper.testing.ZKTestingServer
import ru.fix.zookeeper.utils.ZkTreePrinter
import java.time.Duration
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeUnit.MINUTES
import java.util.concurrent.TimeUnit.SECONDS
import java.util.concurrent.atomic.AtomicBoolean
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
                .withCloseOnJvmShutdown()
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
        val callbackFailed = AtomicBoolean()

        manager1.tryAcquire(lockId) { callbackFailed.set(true) }.shouldBeTrue()
        manager1.release(lockId)

        callbackFailed.get().shouldBeFalse()
    }

    @Test
    fun `test activeLockContainer put contains get remove operations`() {
        val activeLocksContainer = PersistentExpiringLockManager.ActiveLocksContainer()
        val lockId = createLockIdentity()
        val lock = PersistentExpiringDistributedLock(
                zkServer.client,
                lockId
        )

        val containsLockBeforePutting = activeLocksContainer.contains(lockId)
        val lockOfEmptyContainer = activeLocksContainer.putLock(lockId, lock) {}
        val containsLockAfterPutting = activeLocksContainer.contains(lockId)
        val gotLockState = activeLocksContainer.getLockState(lockId)
        val removedLock = activeLocksContainer.removeLock(lockId)
        val containsLockAfterRemoving = activeLocksContainer.contains(lockId)
        val gotLockStateAfterRemove = activeLocksContainer.getLockState(lockId)
        val removeAfterRemoveLock = activeLocksContainer.removeLock(lockId)

        containsLockBeforePutting shouldBe false
        lockOfEmptyContainer shouldBe null
        containsLockAfterPutting shouldBe true
        gotLockState.isPresent shouldBe true
        lock shouldBe removedLock
        containsLockAfterRemoving shouldBe false
        gotLockStateAfterRemove.isEmpty shouldBe true
        removeAfterRemoveLock shouldBe null
    }

    @Test
    fun `concurrency releasing and scheduled prolonging lock`() {
        val prolongExceptions = ConcurrentHashMap.newKeySet<Exception>()
        val lockProlongStatus = AtomicBoolean(false)
        val latchMainThread = CountDownLatch(1)
        val latchReleaseOperation = CountDownLatch(1)
        val manager = createScheduleLatchedManager(
                latchReleaseOperation = latchReleaseOperation,
                latchMainThread = latchMainThread,
                prolongExceptions = prolongExceptions,
                lockProlongStatus = lockProlongStatus
        )

        val lockId = createLockIdentity()
        manager.tryAcquire(lockId) {}
        latchReleaseOperation.await(5, SECONDS) // wait manager's scheduler starting to process
        manager.release(lockId)
        latchMainThread.await(5, SECONDS) // wait manager's scheduler ending to process

        prolongExceptions.isEmpty() shouldBe true
        lockProlongStatus.get() shouldBe true
        manager.getLockState(lockId).isEmpty shouldBe true
    }

    @Test
    fun `after acquiring lock is managed`() {
        val lockId = createLockIdentity()
        val callbackFailed = AtomicBoolean()

        manager1.tryAcquire(lockId) { callbackFailed.set(true) }.shouldBeTrue()
        manager1.isLockManaged(lockId).shouldBeTrue()
        manager1.release(lockId)

        callbackFailed.get().shouldBeFalse()
    }

    @Test
    fun `after acquiring lock state is owned and not expired`() {
        val lockId = createLockIdentity()
        val callbackFailed = AtomicBoolean()

        manager1.tryAcquire(lockId) { callbackFailed.set(true) }.shouldBeTrue()
        manager1.getLockState(lockId).apply {
            isPresent.shouldBeTrue()
            get().apply {
                isOwn.shouldBeTrue()
                isExpired.shouldBeFalse()
            }
        }
        manager1.release(lockId)

        callbackFailed.get().shouldBeFalse()
    }

    @Test
    fun `can not acquire already acquired lock`() {
        val lockId = createLockIdentity()
        val callbackFailed = AtomicBoolean()

        manager1.tryAcquire(lockId) { callbackFailed.set(true) }.shouldBeTrue()
        manager1.tryAcquire(lockId) { callbackFailed.set(true) }.shouldBeFalse()

        callbackFailed.get().shouldBeFalse()
    }

    @Test
    fun `acquire already acquired but expired lock succeed but with error message in logs`() {
        val lockId = createLockIdentity()
        val callbackFailed = AtomicBoolean()

        val wronglyConfiguredManager = createLockManager(
                PersistentExpiringLockManagerConfig(
                        lockAcquirePeriod = Duration.ofSeconds(1),
                        expirationPeriod = Duration.ofSeconds(1),
                        lockCheckAndProlongInterval = Duration.ofSeconds(60)
                )
        )

        wronglyConfiguredManager.tryAcquire(lockId) { callbackFailed.set(true) }.shouldBeTrue()

        await().atMost(1, MINUTES).until {
            wronglyConfiguredManager.getLockState(lockId).map { it.isExpired }.orElse(false)
        }

        wronglyConfiguredManager.tryAcquire(lockId) { callbackFailed.set(true) }.shouldBeTrue()

        callbackFailed.get().shouldBeFalse()
    }

    @Test
    fun `first manager acquires lock, second manager fails to acquire same lock`() {
        val lockId = createLockIdentity()
        val callbackFailed = AtomicBoolean()

        manager1.tryAcquire(lockId) { callbackFailed.set(true) }.shouldBeTrue()
        manager2.tryAcquire(lockId) { callbackFailed.set(true) }.shouldBeFalse()
        manager1.release(lockId)

        callbackFailed.get().shouldBeFalse()
    }

    @Test
    fun `first manager release lock, second manager acquires same lock`() {
        val lockId = createLockIdentity()
        val callbackFailed = AtomicBoolean()

        manager1.tryAcquire(lockId) { callbackFailed.set(true) }.shouldBeTrue()
        manager2.tryAcquire(lockId) { callbackFailed.set(true) }.shouldBeFalse()
        manager1.release(lockId)
        manager2.tryAcquire(lockId) { callbackFailed.set(true) }.shouldBeTrue()

        callbackFailed.get().shouldBeFalse()
    }

    @Test
    fun `lock node created after acquiring and removed after releasing lock`() {
        val lockId = LockIdentity("/lock-1", "/locks/lock-1")
        val callbackFailed = AtomicBoolean()

        manager1.tryAcquire(lockId) { callbackFailed.set(true) }

        logger.info("After acquiring lock: \n" + zkTree())
        nodeExists(lockId.nodePath).shouldBeTrue()

        manager1.release(lockId)
        logger.info("After releasing lock: \n" + zkTree())
        nodeExists(lockId.nodePath).shouldBeFalse()

        callbackFailed.get().shouldBeFalse()
    }


    @Test
    fun `locks nodes removed after close of zk client connection`() {
        val lockId1 = LockIdentity("/lock-1", "/locks/lock-1")
        val lockId2 = LockIdentity("/lock-2", "/locks/lock-2")
        val callbackFailed = AtomicBoolean()

        manager1.tryAcquire(lockId1) { callbackFailed.set(true) }
        manager1.tryAcquire(lockId2) { callbackFailed.set(true) }

        logger.info("After acquiring locks: \n" + zkTree())
        nodeExists(lockId1.nodePath).shouldBeTrue()
        nodeExists(lockId2.nodePath).shouldBeTrue()

        manager1.close()
        logger.info("After closing lock manager: \n" + zkTree())
        nodeExists(lockId1.nodePath).shouldBeFalse()
        nodeExists(lockId2.nodePath).shouldBeFalse()

        callbackFailed.get().shouldBeFalse()
    }

    @Test
    fun `after manager1 lock prolongation, manager2 can not acquire lock`() {
        val lockId = createLockIdentity()
        val callbackFailed = AtomicBoolean()

        val fastFrequentManager = createLockManager(PersistentExpiringLockManagerConfig(
                lockAcquirePeriod = Duration.ofSeconds(2),
                expirationPeriod = Duration.ofSeconds(1),
                lockCheckAndProlongInterval = Duration.ofMillis(100)
        ))

        fastFrequentManager.tryAcquire(lockId) { callbackFailed.set(true) }.shouldBeTrue()

        val lockData = zkServer.client.data.forPath(lockId.nodePath)

        //give time for manager1 to apply prolongation
        await().atMost(1, MINUTES).until {
            val newLockData = zkServer.client.data.forPath(lockId.nodePath)
            !newLockData!!.contentEquals(lockData)
        }

        manager2.tryAcquire(lockId) { callbackFailed.set(true) }.shouldBeFalse()
        fastFrequentManager.release(lockId)

        await().atMost(1, MINUTES).until { manager2.tryAcquire(lockId) { callbackFailed.set(true) } }

        fastFrequentManager.close()

        callbackFailed.get().shouldBeFalse()
    }


    @Test
    fun `acquire and release 30 different locks in parallel`() = runBlocking {
        val locksPath = "/locks" + nextId()
        fun createLockIdentityForIndex(index: Int) =
                LockIdentity(ZKPaths.makePath(locksPath, "lock-$index"), "meta: $index")

        val callbackFailed = AtomicBoolean()

        val locksCount = 30
        val dispatcher = Executors.newFixedThreadPool(12).asCoroutineDispatcher()

        val lockManagers = (1..locksCount).map {
            async(context = dispatcher) { createLockManager() }
        }.awaitAll()

        lockManagers.mapIndexed { i, it ->
            async(context = dispatcher) {
                val lockId = createLockIdentityForIndex(i)
                it.tryAcquire(lockId) { callbackFailed.set(true) }.shouldBeTrue()

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
    fun `connection lost, manager failed to prolong, detaches lock and notifies client`() {
        val proxyTcpCrusher = zkServer.openProxyTcpCrusher()
        val zkProxyClient = zkServer.createZkProxyClient(proxyTcpCrusher)

        val lock = createLockIdentity()

        val proxiedManager = createLockManager(
                config = PersistentExpiringLockManagerConfig(
                        lockAcquirePeriod = Duration.ofSeconds(10),
                        expirationPeriod = Duration.ofSeconds(5),
                        lockCheckAndProlongInterval = Duration.ofSeconds(1)
                ),
                client = zkProxyClient)

        val prolongationFailedEventSlot = AtomicBoolean()

        proxiedManager.tryAcquire(lock) {
            prolongationFailedEventSlot.set(true)

        }.shouldBeTrue()

        proxyTcpCrusher.close()

        await().atMost(1, MINUTES).until { prolongationFailedEventSlot.get() }

        proxiedManager.isLockManaged(lock).shouldBeFalse()
    }

    private fun createLockIdentity(id: Int = nextId()) =
            LockIdentity(ZKPaths.makePath(LOCK_PATH, id.toString()), "meta: $LOCK_PATH/id")

    private fun createLockManager(
            config: PersistentExpiringLockManagerConfig = PersistentExpiringLockManagerConfig(),
            client: CuratorFramework = zkServer.client
    ) =
            PersistentExpiringLockManager(
                    client,
                    DynamicProperty.of(config),
                    NoopProfiler()
            )

    private fun nodeExists(path: String) = zkServer.client.checkExists().forPath(path) != null

    private fun zkTree() = ZkTreePrinter(zkServer.client).print("/")

    /**
     * @param latchReleaseOperation - latch that lock main thread before manager.release operation
     * @param latchMainThread - latch that lock main thread for waiting scheduler process locks (one lock this case)
     * @param prolongExceptions - set for adding any exceptions when prolong lock in scheduler
     * @param lockProlongStatus - result of prolonging lock in schdeduler
     *
     * @return manager with scheduler that release latchReleaseOperation latch, wait a little
     * and release latchMainThread latch. This concurrency sequence work once and for one lock in manager
     */
    private fun createScheduleLatchedManager(
            latchReleaseOperation: CountDownLatch,
            latchMainThread: CountDownLatch,
            prolongExceptions: ConcurrentHashMap.KeySetView<Exception, Boolean>,
            lockProlongStatus: AtomicBoolean
    ): PersistentExpiringLockManager {

        val managerConfig = DynamicProperty.of(PersistentExpiringLockManagerConfig(
                lockCheckAndProlongInterval = Duration.ofSeconds(1)
        ))
        val locksContainer = PersistentExpiringLockManager.ActiveLocksContainer()
        val scheduler = NamedExecutors.newSingleThreadScheduler(
                "test-PersistentExpiringLockManager", NoopProfiler()
        )
        scheduler.schedule(
                Schedule.withDelay(managerConfig.map { it.lockCheckAndProlongInterval.toMillis() }),
                0
        ) {
            locksContainer.processAllLocks({ _, lock ->
                var prolonged = false
                try {
                    latchReleaseOperation.countDown()
                    TimeUnit.MILLISECONDS.sleep(50) // wait a little for trying release lock in parallel
                    prolonged = lock.checkAndProlongIfExpiresIn(
                            managerConfig.get().lockAcquirePeriod,
                            managerConfig.get().expirationPeriod
                    )
                } catch (e: Exception) {
                    prolongExceptions.add(e)
                } finally {
                    lockProlongStatus.set(prolonged)
                    latchMainThread.countDown()
                }
                if (prolonged) ProcessingLockResult.KEEP_LOCK_IN_CONTAINER
                else ProcessingLockResult.REMOVE_LOCK_FROM_CONTAINER
            }) { _, _, lockProcessingResult ->
                if (lockProcessingResult == ProcessingLockResult.ALREADY_REMOVED) {
                    prolongExceptions.add(IllegalStateException("Impossible state"))
                }
            }
        }
        return PersistentExpiringLockManager(
                zkServer.client,
                managerConfig,
                locksContainer,
                scheduler
        )
    }

}
