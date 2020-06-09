package ru.fix.zookeeper.instance.registry

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
import java.time.Duration
import java.util.*
import java.util.concurrent.Executors

internal open class ServiceInstanceIdRegistryTest : AbstractServiceInstanceIdRegistryTest() {
    private val logger = logger()

    @Test
    fun `instances registered via single service registry`() {
        val registry = createInstanceIdRegistry()
        assertEquals("1", registry.register("abs-rate"))
        assertEquals("2", registry.register("abs-rate"))
        assertEquals("3", registry.register("drugkeeper"))
        assertInstances(mapOf("abs-rate" to setOf("1", "2"), "drugkeeper" to setOf("3")))
    }

    @Test
    fun `sequential startup of 3 service registry and register instances on each of them`() {
        val instanceIds = listOf(
                createInstanceIdRegistry(),
                createInstanceIdRegistry(),
                createInstanceIdRegistry()
        ).map { it.register(UUID.randomUUID().toString()) }

        logger.info(zkTree())
        assertEquals("1", instanceIds[0])
        assertEquals("2", instanceIds[1])
        assertEquals("3", instanceIds[2])
    }

    @Test
    fun `curator closed, instance id registry can't prolong lock of instance id`() {
        val lockAcquirePeriod = Duration.ofSeconds(5)
        val client = testingServer.createClient()
        val client2 = testingServer.createClient()

        createInstanceIdRegistry(client = client, lockAcquirePeriod = lockAcquirePeriod).register("abs-rate")
        createInstanceIdRegistry(lockAcquirePeriod = lockAcquirePeriod).register("abs-rate")
        createInstanceIdRegistry(client = client2, lockAcquirePeriod = lockAcquirePeriod).register("drugkeeper")

        logger.info(zkTree())
        assertInstances(mapOf("abs-rate" to setOf("1", "2"), "drugkeeper" to setOf("3")))

        client2.blockUntilConnected()
        client.close()
        Awaitility.await()
                .timeout(Duration.ofSeconds(1))
                .until { !client.zookeeperClient.isConnected }

        Thread.sleep(lockAcquirePeriod.multipliedBy(2).toMillis())
        logger.info(zkTree())
        assertInstanceIdLocksExpiration(setOf("1" to false, "2" to true, "3" to true), lockAcquirePeriod)
    }

    @Test
    fun `parallel startup of 30 different service registries should register services with no collisions`() = runBlocking {
        val servicesCount = 30
        val dispatcher = Executors.newFixedThreadPool(12) {
            Thread(it, "discovery-pool")
        }.asCoroutineDispatcher()

        List(servicesCount) {
            async(context = dispatcher) {
                createInstanceIdRegistry(registrationRetryCount = 20, maxInstancesCount = 127).register("app")
            }
        }.awaitAll()

        logger.info(zkTree())
        val uniqueInstanceIds = testingServer.client.children
                .forPath(ZKPaths.makePath(rootPath, "services"))
                .map { it.toInt() }
                .toSet()

        assertEquals(servicesCount, uniqueInstanceIds.size)
    }

    @Test
    fun `parallel registration of 30 services should be performed without instance id collisions`() = runBlocking {
        val servicesCount = 30
        val dispatcher = Executors.newFixedThreadPool(12) {
            Thread(it, "discovery-pool")
        }.asCoroutineDispatcher()

        val registry = createInstanceIdRegistry(registrationRetryCount = 20, maxInstancesCount = 127)
        List(servicesCount) {
            async(context = dispatcher) {
                registry.register("app")
            }
        }.awaitAll()

        logger.info(zkTree())
        val uniqueInstanceIds = testingServer.client.children
                .forPath(ZKPaths.makePath(rootPath, "services"))
                .map { it.substringAfterLast("/").toInt() }
                .toSet()

        assertEquals(servicesCount, uniqueInstanceIds.size)
    }

    @Test
    fun `successful instance registration when registration path is already initialized by another service registry`() {
        createInstanceIdRegistry()
        createInstanceIdRegistry().register("abs-shake")

        logger.info(zkTree())
        assertInstances(mapOf("abs-shake" to setOf("1")))
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
                .forPath(ZKPaths.makePath(rootPath, "services"))
                .map { it.substringAfterLast("/").toInt() }
                .toSet()

        assertEquals(maxAvailableId, uniqueInstanceIds.size)
    }

    @Test
    fun `close of instance id registry should release lock`() {
        val registry = createInstanceIdRegistry()

        assertEquals("1", registry.register("app"))
        registry.close()
        logger.info(zkTree())

        Thread.sleep(6000)
        logger.info(zkTree())
    }
}
