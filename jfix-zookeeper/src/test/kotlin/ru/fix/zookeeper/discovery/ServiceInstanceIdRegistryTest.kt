package ru.fix.zookeeper.discovery

import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.runBlocking
import org.apache.curator.framework.CuratorFramework
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Test
import ru.fix.zookeeper.AbstractZookeeperTest
import ru.fix.zookeeper.utils.Marshaller
import java.util.*
import java.util.concurrent.Executors

internal class ServiceInstanceIdRegistryTest : AbstractZookeeperTest() {

    @Test
    fun `sequential startup of 3 instances should generate id from 1 to 3`() {
        val instances = listOf(
                createDiscovery("abs-rate"),
                createDiscovery("abs-rate"),
                createDiscovery("drugkeeper")
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
        createDiscovery("abs-rate", client = client)
        createDiscovery("abs-rate")
        createDiscovery("drugkeeper")

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
                createDiscovery(UUID.randomUUID().toString(), 20, maxInstancesCount = 127)
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
        createDiscovery("abs-shake")
        createDiscovery("abs-rate")

        println(zkTree())
        assertInstances(mapOf("abs-shake" to setOf("1"), "abs-rate" to setOf("2")))
    }


    @Test
    fun `instance id should be in range from 1 to 10, error thrown otherwise`() {
        val maxAvailableId = 10
        repeat(maxAvailableId) {
            createDiscovery(UUID.randomUUID().toString(), 20, testingServer.client, maxAvailableId)
        }

        assertThrows(AssertionError::class.java) {
            createDiscovery(UUID.randomUUID().toString(), 20, testingServer.client, maxAvailableId)
        }

        println(zkTree())
        val uniqueInstanceIds = testingServer.client.children
                .forPath("$rootPath/services")
                .map { it.substringAfterLast("/").toInt() }
                .toSet()

        assertEquals(maxAvailableId, uniqueInstanceIds.size)
    }

    private fun assertInstances(services: Map<String, Set<String>>) {
        val expected = services.flatMap {
            service -> service.value.map { service.key to it }
        }
        val actual = testingServer.client.children
                .forPath("$rootPath/services")
                .map {
                    val instancePath = "$rootPath/services/$it"
                    val serviceName = Marshaller.unmarshall(
                            testingServer.client.data.forPath(instancePath).toString(Charsets.UTF_8),
                            InstanceIdData::class.java
                    ).applicationName
                    serviceName to it
                }
        assertEquals(expected, actual)
    }

    private fun createDiscovery(
            appName: String = UUID.randomUUID().toString(),
            registrationRetryCount: Int = 5,
            client: CuratorFramework = testingServer.createClient(),
            maxInstancesCount: Int = Int.MAX_VALUE
    ) = ServiceInstanceIdRegistry(
            curatorFramework = client,
            instanceIdGenerator = MinFreeInstanceIdGenerator(maxInstancesCount),
            config = ServiceInstanceIdRegistryConfig(rootPath, appName, registrationRetryCount, maxInstancesCount)
    )
}
