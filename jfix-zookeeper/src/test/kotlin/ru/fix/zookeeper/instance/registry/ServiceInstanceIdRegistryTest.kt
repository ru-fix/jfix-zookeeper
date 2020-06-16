package ru.fix.zookeeper.instance.registry

import io.kotest.matchers.shouldBe
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.runBlocking
import org.apache.curator.utils.ZKPaths
import org.apache.logging.log4j.kotlin.logger
import org.awaitility.Awaitility
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Test
import ru.fix.zookeeper.lock.PersistentExpiringDistributedLock.LockNodeState.EXPIRED_LOCK
import java.time.Duration
import java.util.concurrent.Executors

internal open class ServiceInstanceIdRegistryTest : AbstractServiceInstanceIdRegistryTest() {
    private val logger = logger()

    @Test
    fun `instances registered via single service registry`() {
        val registry = createInstanceIdRegistry()
        registry.register("abs-rate") shouldBe "1"
        registry.register("abs-rate") shouldBe "2"
        registry.register("drugkeeper") shouldBe "1"
        registry.register("abs-rate") shouldBe "3"
    }

    @Test
    fun `sequential startup of 3 service registry and register instances on each of them`() {
        createInstanceIdRegistry().register("a") shouldBe "1"
        createInstanceIdRegistry().register("a") shouldBe "2"
        createInstanceIdRegistry().register("b") shouldBe "1"
        createInstanceIdRegistry().register("a") shouldBe "3"
    }

    @Test
    fun `curator closed, instance id registry can't prolong lock of instance id and this lock will expire`() {
        val lockAcquirePeriod = Duration.ofSeconds(2)
        val client = testingServer.createClient()

        createInstanceIdRegistry(client = client, lockAcquirePeriod = lockAcquirePeriod).register("abs-rate") shouldBe "1"

        client.close()
        Awaitility.await()
                .timeout(Duration.ofSeconds(1))
                .until { !client.zookeeperClient.isConnected }

        val lockPath = ZKPaths.makePath(serviceRegistrationPath, "abs-rate", "1")
        waitLockNodeState(EXPIRED_LOCK, lockPath, testingServer.createClient())
    }

    @Test
    fun `parallel startup of 10 different service registries should register services with no collisions`() = runBlocking {
        val servicesCount = 10
        val dispatcher = Executors.newFixedThreadPool(12) {
            Thread(it, "discovery-pool")
        }.asCoroutineDispatcher()

        List(servicesCount) {
            async(context = dispatcher) {
                createInstanceIdRegistry(registrationRetryCount = 20, maxInstancesCount = 127).register("app")
            }
        }.awaitAll()

        val uniqueInstanceIds = testingServer.client.children
                .forPath(ZKPaths.makePath(serviceRegistrationPath, "app"))
                .map { it.toInt() }
                .toSet()

        assertEquals(servicesCount, uniqueInstanceIds.size)
    }

    @Test
    fun `parallel registration of 10 services should be performed without instance id collisions`() = runBlocking {
        val servicesCount = 10
        val dispatcher = Executors.newFixedThreadPool(12) {
            Thread(it, "discovery-pool")
        }.asCoroutineDispatcher()

        val registry = createInstanceIdRegistry(registrationRetryCount = 20, maxInstancesCount = 127)
        List(servicesCount) {
            async(context = dispatcher) {
                registry.register("app")
            }
        }.awaitAll()

        val uniqueInstanceIds = testingServer.client.children
                .forPath(ZKPaths.makePath(serviceRegistrationPath, "app"))
                .map { it.toInt() }
                .toSet()

        assertEquals(servicesCount, uniqueInstanceIds.size)
    }

    @Test
    fun `successful instance registration when registration path is already initialized by another service registry`() {
        createInstanceIdRegistry()
        createInstanceIdRegistry().register("abs-shake") shouldBe "1"
    }

    @Test
    fun `instance id should be in range from 1 to 10 because limit = 10, error thrown otherwise`() {
        val maxAvailableId = 10
        repeat(maxAvailableId) {
            createInstanceIdRegistry(20, testingServer.client, maxAvailableId).register("app")
        }

        assertThrows(AssertionError::class.java) {
            createInstanceIdRegistry(20, testingServer.client, maxAvailableId).register("app")
        }
        logger.info(zkTree())

        val uniqueInstanceIds = testingServer.client.children
                .forPath(ZKPaths.makePath(serviceRegistrationPath, "app"))
                .map { it.toInt() }
                .toSet()

        assertEquals(maxAvailableId, uniqueInstanceIds.size)
    }

    @Test
    fun `close of instance id registry should release locks`() {
        val registry = createInstanceIdRegistry()

        registry.register("app") shouldBe "1"
        registry.register("app") shouldBe "2"
        registry.register("b") shouldBe "1"

        registry.close()
        testingServer.client.data
                .forPath(serviceRegistrationPath).size shouldBe 0
    }

    @Test
    fun `unregistration of instance make available instance id for registration of another instances`() {
        val registry = createInstanceIdRegistry()

        registry.register("app") shouldBe "1"
        registry.register("app") shouldBe "2"
        registry.unregister("app", "2")
        registry.register("app") shouldBe "2"
    }

    @Test
    fun `unregistration of nonexistent instance id throws error`() {
        val registry = createInstanceIdRegistry()

        registry.register("app") shouldBe "1"
        assertThrows(IllegalStateException::class.java) {
            registry.unregister("app", "2")
        }
    }

}
