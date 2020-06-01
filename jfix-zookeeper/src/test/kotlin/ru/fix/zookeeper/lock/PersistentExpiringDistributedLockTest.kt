package ru.fix.zookeeper.lock

import io.kotest.matchers.booleans.shouldBeFalse
import io.kotest.matchers.booleans.shouldBeTrue
import io.kotest.matchers.shouldBe
import io.kotest.matchers.nulls.shouldBeNull
import io.kotest.matchers.string.shouldContain
import org.apache.curator.framework.state.ConnectionState
import org.apache.curator.framework.state.ConnectionStateListener
import org.apache.logging.log4j.kotlin.logger
import org.awaitility.Awaitility.await
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.junit.jupiter.api.parallel.Execution
import org.junit.jupiter.api.parallel.ExecutionMode
import org.netcrusher.core.reactor.NioReactor
import org.netcrusher.tcp.TcpCrusher
import org.netcrusher.tcp.TcpCrusherBuilder
import ru.fix.stdlib.socket.SocketChecker
import ru.fix.zookeeper.lock.PersistentExpiringDistributedLock.ReleaseResult
import ru.fix.zookeeper.testing.ZKTestingServer
import ru.fix.zookeeper.utils.Marshaller
import ru.fix.zookeeper.utils.ZkTreePrinter
import java.net.InetAddress
import java.nio.charset.StandardCharsets
import java.time.Duration
import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.concurrent.TimeUnit.MINUTES
import java.util.concurrent.TimeUnit.SECONDS
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicReference
import java.util.function.Supplier

@Execution(ExecutionMode.CONCURRENT)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
internal class PersistentExpiringDistributedLockTest {
    private val logger = logger()

    val LOCKS_PATH = "/locks"

    lateinit var zkServer: ZKTestingServer
    val idSequence = object : Supplier<String> {
        val counter = AtomicInteger()
        override fun get(): String = counter.incrementAndGet().toString()
    }

    fun createLock(id: String = idSequence.get(),
                   path: String = "$LOCKS_PATH/$id",
                   data: String? = null): PersistentExpiringDistributedLock {
        return PersistentExpiringDistributedLock(
                zkServer.client,
                LockIdentity(id,
                        path,
                        data))
    }

    @BeforeAll
    fun startZkTestingServer() {
        zkServer = ZKTestingServer()
                .withCloseOnJvmShutdown(true)
                .start()
    }

    @AfterAll
    fun stopZkTestingServer() {
        zkServer.close()
    }

    @Test
    fun `lock in zookeeper is a node with text data that contains uid`() {
        val id = idSequence.get()
        val lock = PersistentExpiringDistributedLock(
                zkServer.client,
                LockIdentity(id,
                        "$LOCKS_PATH/$id",
                        data = null))

        lock.expirableAcquire(Duration.ofMinutes(1), Duration.ofMillis(1)).shouldBeTrue()

        logger.info(ZkTreePrinter(zkServer.client).print("/", true))

        val data = zkServer.client.data.forPath("$LOCKS_PATH/$id").toString(StandardCharsets.UTF_8)
        logger.info(data)
        data.shouldContain("version")

        lock.close()
    }

    @Test
    fun `acquired lock isAcquired, released lock is not Acquired`() {
        val lock = createLock()
        lock.expirableAcquire(Duration.ofSeconds(100), Duration.ofSeconds(2)).shouldBeTrue()
        lock.state.apply {
            isExpired.shouldBeFalse()
            isOwn.shouldBeTrue()
        }
        lock.release().apply {
            status.shouldBe(ReleaseResult.Status.LOCK_RELEASED)
            exception.shouldBeNull()
        }
        lock.state.apply {
            isOwn.shouldBeFalse()
            isExpired.shouldBeTrue()
        }
    }


    @Test
    fun `single actor acquires and unlocks twice`() {
        val lock = createLock()
        lock.expirableAcquire(Duration.ofMinutes(1), Duration.ofMillis(1)).shouldBeTrue()
        lock.release().apply {
            status.shouldBe(ReleaseResult.Status.LOCK_RELEASED)
            exception.shouldBeNull()
        }

        lock.expirableAcquire(Duration.ofMinutes(1), Duration.ofMillis(1)).shouldBeTrue()
        lock.release().apply {
            status.shouldBe(ReleaseResult.Status.LOCK_RELEASED)
            exception.shouldBeNull()
        }
    }

    @Test
    fun `single actor acquires twice`() {
        val lock = createLock()
        lock.expirableAcquire(Duration.ofMinutes(1), Duration.ofMillis(1)).shouldBeTrue()
        lock.expirableAcquire(Duration.ofMinutes(1), Duration.ofMillis(1)).shouldBeTrue()
        lock.release().apply {
            status.shouldBe(ReleaseResult.Status.LOCK_RELEASED)
            exception.shouldBeNull()
        }
    }

    @Test
    fun `two actors acquire same lock, only one succeed`() {
        val id = idSequence.get()

        val lock1 = createLock(id)
        val lock2 = createLock(id)

        lock1.expirableAcquire(Duration.ofMinutes(1), Duration.ofMillis(1)).shouldBeTrue()
        lock2.expirableAcquire(Duration.ofMinutes(1), Duration.ofMillis(1)).shouldBeFalse()
        lock1.close()
        lock2.expirableAcquire(Duration.ofMillis(1), Duration.ofMillis(1)).shouldBeTrue()
        lock2.close()
    }

    @Test
    fun `killing actor1 lock node in zk effectively release it for actor2`() {
        val id = idSequence.get()

        val lock1 = createLock(id)
        val lock2 = createLock(id)

        lock1.expirableAcquire(Duration.ofMinutes(1), Duration.ofMinutes(1)).shouldBeTrue()

        logger.info(ZkTreePrinter(zkServer.client).print("/", true))

        zkServer.client.delete().forPath("$LOCKS_PATH/$id")

        lock2.expirableAcquire(Duration.ofMinutes(1), Duration.ofMinutes(1)).shouldBeTrue()

        lock1.close()
        lock2.close()
    }

    @Test
    fun `killing active lock node in zk leads to failed release`() {
        val id = idSequence.get()
        val lock = createLock(path = "$LOCKS_PATH/$id")
        lock.expirableAcquire(Duration.ofMillis(1), Duration.ofMillis(1))
        zkServer.client.delete().forPath("$LOCKS_PATH/$id")

        lock.release().apply {
            status.shouldBe(ReleaseResult.Status.LOCK_IS_LOST)
            exception.shouldBeNull()
        }
    }

    @Test
    fun `expired lock in zk successfully overwritten and acquired by new lock`() {
        val lock1 = createLock()
        val lock2 = createLock()

        lock1.expirableAcquire(Duration.ofMillis(1), Duration.ofMillis(1)).shouldBeTrue()
        await().until { lock2.expirableAcquire(Duration.ofMillis(1), Duration.ofMillis(1)) }
    }

    @Test
    fun `expired lock is released with EXPIRED status`() {
        val lock1 = createLock()
        lock1.expirableAcquire(Duration.ofMillis(1), Duration.ofMillis(1)).shouldBeTrue()
        await().atMost(1, MINUTES).until { lock1.getState().isExpired }
        lock1.release().apply {
            status.shouldBe(ReleaseResult.Status.LOCK_STILL_OWNED_BUT_EXPIRED)
            exception.shouldBeNull()
        }
    }

    @Test
    fun `successful prolong of active lock`() {
        val lock = createLock()
        lock.expirableAcquire(Duration.ofSeconds(10), Duration.ofSeconds(10)).shouldBeTrue()
        lock.checkAndProlong(Duration.ofSeconds(10)).shouldBeTrue()
    }

    @Test
    fun `failed prolongation of active alien lock`() {
        val id1 = idSequence.get()
        val id2 = idSequence.get()
        val path = "$LOCKS_PATH/$id1"

        val lock1 = createLock(id1, path)
        val lock2 = createLock(id2, path)

        lock1.expirableAcquire(Duration.ofMillis(1), Duration.ofSeconds(10)).shouldBeTrue()

        logger.info(ZkTreePrinter(zkServer.client).print("/", true))

        await().atMost(1, MINUTES).until {
            lock2.expirableAcquire(Duration.ofSeconds(100), Duration.ofSeconds(10))
        }

        logger.info(ZkTreePrinter(zkServer.client).print("/", true))

        lock1.checkAndProlong(Duration.ofSeconds(10)).shouldBeFalse()
    }

    @Test
    fun `failed prolongation of expired alien lock`() {
        val id1 = idSequence.get()
        val id2 = idSequence.get()
        val path = "$LOCKS_PATH/$id1"

        val lock1 = createLock(id1, path)
        val lock2 = createLock(id2, path)

        lock1.expirableAcquire(Duration.ofMillis(1), Duration.ofSeconds(10)).shouldBeTrue()

        logger.info(ZkTreePrinter(zkServer.client).print("/", true))

        await().atMost(1, MINUTES).until {
            lock2.expirableAcquire(Duration.ofMillis(1), Duration.ofSeconds(10))
        }

        logger.info(ZkTreePrinter(zkServer.client).print("/", true))


        await().atMost(1, MINUTES).until {
            lock2.state.run { isExpired }
        }
        lock1.checkAndProlong(Duration.ofSeconds(10)).shouldBeFalse()
    }

    @Test
    fun `success prolongation of own expired lock when lock was not acquired by others`() {
        val lock1 = createLock()
        lock1.expirableAcquire(Duration.ofMillis(1), Duration.ofSeconds(10)).shouldBeTrue()

        await().atMost(10, SECONDS).until {
            lock1.state.isExpired
        }

        lock1.checkAndProlong(Duration.ofSeconds(10)).shouldBeTrue()
    }

    interface NetworkFailure {
        fun activate(crusher: TcpCrusher)
        fun deactivate(crusher: TcpCrusher)
    }

    @Test
    fun `temporary failed zookeeper connection do not invalidate lock (disconnection)`() {
        `temporary failed zookeeper connection do not invalidate lock`(
                object : NetworkFailure {
                    override fun activate(crusher: TcpCrusher) {
                        logger.info("disconnect")
                        crusher.close()
                    }

                    override fun deactivate(crusher: TcpCrusher) {
                        logger.info("restore connectivity")
                        crusher.open()
                    }
                }
        )
    }

    @Test
    fun `temporary failed zookeeper connection do not invalidate lock (freezing)`() {
        `temporary failed zookeeper connection do not invalidate lock`(
                object : NetworkFailure {
                    override fun activate(crusher: TcpCrusher) {
                        logger.info("freeze")
                        crusher.freeze()
                    }

                    override fun deactivate(crusher: TcpCrusher) {
                        logger.info("unfreeze")
                        crusher.unfreeze()
                    }
                })
    }

    fun `temporary failed zookeeper connection do not invalidate lock`(networkFailure: NetworkFailure) {
        val zkProxyCrusherPort = SocketChecker.getAvailableRandomPort()
        val proxyTcpCrusher = TcpCrusherBuilder.builder()
                .withReactor(NioReactor())
                .withBindAddress("localhost", zkProxyCrusherPort)
                .withConnectAddress("localhost", zkServer.port)
                .build()
        proxyTcpCrusher.open()

        val zkProxyClient = zkServer.createClient(
                "${proxyTcpCrusher.bindAddress.hostString}:${proxyTcpCrusher.bindAddress.port}",
                Duration.ofSeconds(5).toMillis().toInt(),
                Duration.ofSeconds(5).toMillis().toInt(),
                Duration.ofSeconds(5).toMillis().toInt())

        val zkProxyState = AtomicReference<ConnectionState>()
        zkProxyClient.connectionStateListenable.addListener(ConnectionStateListener { _, newState ->
            logger.info(newState)
            zkProxyState.set(newState)
        })

        val id = idSequence.get()
        val lock1 = PersistentExpiringDistributedLock(zkProxyClient, LockIdentity(id, "$LOCKS_PATH/$id"))
        lock1.expirableAcquire(Duration.ofSeconds(100), Duration.ofSeconds(1)).shouldBeTrue()

        networkFailure.activate(proxyTcpCrusher)
        await().atMost(10, MINUTES).until { zkProxyState.get() == ConnectionState.LOST }

        val lock2 = PersistentExpiringDistributedLock(zkServer.client, LockIdentity(id, "$LOCKS_PATH/$id"))
        lock2.expirableAcquire(Duration.ofMillis(100), Duration.ofMillis(100)).shouldBeFalse()
        lock2.close()

        networkFailure.deactivate(proxyTcpCrusher)
        await().atMost(10, MINUTES).until { zkProxyState.get() == ConnectionState.RECONNECTED }

        await().atMost(1, MINUTES).until { lock1.checkAndProlong(Duration.ofSeconds(10)) }
        lock1.close()

        proxyTcpCrusher.close()

    }

    @Test
    fun `lock stores information about ip and hostname of the owner`() {
        var hostIp = ""
        var hostName = ""
        try {
            val inetAddr = InetAddress.getLocalHost()
            hostIp = inetAddr.hostAddress.toString()
            hostName = inetAddr.hostAddress.toString()
        } catch (exc: Exception) {
            //leave host ip and name empty if failed to resolve host information
        }
        val id = idSequence.get()
        val lock = createLock(id = id)
        lock.expirableAcquire(Duration.ofMinutes(1), Duration.ofMinutes(1))

        val lockData = zkServer.client.data.forPath("$LOCKS_PATH/$id").toString(StandardCharsets.UTF_8)
        lockData.shouldContain(hostIp)
        lockData.shouldContain(hostName)
        lock.release().apply {
            status.shouldBe(ReleaseResult.Status.LOCK_RELEASED)
            exception.shouldBeNull()
        }
    }

    @Test
    fun `lock data serialized and deserialized as a json`() {

        val lockData = LockData(ip = "1.2.3.4",
                hostname = "hostName",
                uuid = "uuid",
                expirationDate = Instant.now().truncatedTo(ChronoUnit.MILLIS))
        val json = Marshaller.marshall(lockData)

        json.shouldContain("hostName")
        json.shouldContain("uui")

        val deserialized = Marshaller.unmarshall(json, LockData::class.java)
        deserialized.shouldBe(lockData)
    }
}