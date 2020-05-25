package ru.fix.zookeeper.discovery

import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Test
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
    fun `instance id in zk should disappear when this instance closed the session`() {
        val client = testingServer.createClient()
        createInstanceIdRegistry("abs-rate", client = client)
        createInstanceIdRegistry("abs-rate")
        createInstanceIdRegistry("drugkeeper")

        println(zkTree())
        assertInstances(mapOf("abs-rate" to setOf("1", "2"), "drugkeeper" to setOf("3")))
        client.close()

        println(zkTree())
        assertInstances(mapOf("abs-rate" to setOf("2"), "drugkeeper" to setOf("3")))
    }

    @Test
    fun `parallel startup should be without instance id collisions`() = runBlocking {
        val servicesCount = 30
        val dispatcher = Executors.newFixedThreadPool(12) {
            Thread(it, "discovery-pool")
        }.asCoroutineDispatcher()

        List(servicesCount) {
            async(context = dispatcher) {
                createInstanceIdRegistry(UUID.randomUUID().toString(), 20, maxInstancesCount = 127)
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
    fun `start and close ttl expired`() {
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
}
