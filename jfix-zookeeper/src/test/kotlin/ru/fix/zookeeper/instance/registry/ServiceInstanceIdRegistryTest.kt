package ru.fix.zookeeper.instance.registry

import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.runBlocking
import org.awaitility.Awaitility
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import java.time.Duration
import java.util.*
import java.util.concurrent.Executors

internal open class ServiceInstanceIdRegistryTest : AbstractServiceInstanceIdRegistryTest() {

    @Test
    fun `sequential startup of 3 instances should generate id from 1 to 3`() {
        val instances = listOf(
                createInstanceIdRegistry("abs-rate"),
                createInstanceIdRegistry("abs-rate"),
                createInstanceIdRegistry("drugkeeper")
        )

        println(zkTree())
        assertInstances(mapOf("abs-rate" to setOf("1", "2"), "drugkeeper" to setOf("3")))
        assertEquals("1", instances[0].instanceId)
        assertEquals("2", instances[1].instanceId)
        assertEquals("3", instances[2].instanceId)
    }

    @Test
    fun `curator closed, instance id registry can't prolong lock of instance id`() {
        val disconnectTimeout = Duration.ofSeconds(5)
        val client = testingServer.createClient()
        val client2 = testingServer.createClient()
        createInstanceIdRegistry("abs-rate", client = client)
        createInstanceIdRegistry("abs-rate")
        createInstanceIdRegistry("drugkeeper", client = client2)

        println(zkTree())
        assertInstances(mapOf("abs-rate" to setOf("1", "2"), "drugkeeper" to setOf("3")))

        client2.blockUntilConnected()
        client.close()
        Awaitility.await()
                .timeout(Duration.ofSeconds(1))
                .until { !client.zookeeperClient.isConnected }

        Thread.sleep(disconnectTimeout.multipliedBy(2).toMillis())
        println(zkTree())
        assertInstanceIdLocksExpiration(setOf("1" to false, "2" to true, "3" to true), disconnectTimeout)
    }

    @Test
    fun `parallel startup should be without instance id collisions`() = runBlocking {
        val servicesCount = 30
        val dispatcher = Executors.newFixedThreadPool(12) {
            Thread(it, "discovery-pool")
        }.asCoroutineDispatcher()

        List(servicesCount) {
            async(context = dispatcher) {
                createInstanceIdRegistry(registrationRetryCount = 20, maxInstancesCount = 127)
            }
        }.awaitAll()

        println(zkTree())
        val uniqueInstanceIds = testingServer.client.children
                .forPath("$rootPath/services")
                .map { it.substringAfterLast("/").toInt() }
                .toSet()

        assertEquals(servicesCount, uniqueInstanceIds.size)
    }

    @Test
    fun `successful instance creation when registration path is already initialized`() {
        createInstanceIdRegistry("abs-shake")
        createInstanceIdRegistry("abs-rate")

        println(zkTree())
        assertInstances(mapOf("abs-shake" to setOf("1"), "abs-rate" to setOf("2")))
    }

    @Test
    fun `instance id should be in range from 1 to 10, error thrown otherwise`() {
        val maxAvailableId = 10
        repeat(maxAvailableId) {
            createInstanceIdRegistry(UUID.randomUUID().toString(), 20, testingServer.client, maxAvailableId)
        }

        assertThrows(AssertionError::class.java) {
            createInstanceIdRegistry(UUID.randomUUID().toString(), 20, testingServer.client, maxAvailableId)
        }

        println(zkTree())
        val uniqueInstanceIds = testingServer.client.children
                .forPath("$rootPath/services")
                .map { it.substringAfterLast("/").toInt() }
                .toSet()

        assertEquals(maxAvailableId, uniqueInstanceIds.size)
    }

    @Test
    fun `close of instance id registry should release lock`() {
        val instances = listOf(
                createInstanceIdRegistry("abs-rate")
        )

        println(zkTree())
        assertEquals("1", instances[0].instanceId)
        instances[0].close()
        println(zkTree())
        Thread.sleep(6000)
        println(zkTree())

    }

    @Test
    fun `1`() {
        val limit = 32
        List(limit) {
            createInstanceIdRegistry(maxInstancesCount = limit)
        }.forEachIndexed { index, instance ->
            assertEquals(index + 1, instance.instanceId.toInt())
        }

        var instanceOverLimit: ServiceInstanceIdRegistry? = null
        assertThrows(java.lang.AssertionError::class.java) {
            instanceOverLimit = createInstanceIdRegistry(maxInstancesCount = limit)
        }
        assertNull(instanceOverLimit)
    }
}
