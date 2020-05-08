package ru.fix.zookeeper.discovery

import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.runBlocking
import org.apache.curator.framework.CuratorFramework
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import ru.fix.zookeeper.AbstractZookeeperTest
import ru.fix.zookeeper.utils.Marshaller
import java.util.*
import java.util.concurrent.Executors

internal class ServiceDiscoveryTest : AbstractZookeeperTest() {

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
        val instance1 = createDiscovery("abs-rate")
        createDiscovery("abs-rate")
        createDiscovery("drugkeeper")

        println(zkTree())
        assertInstances(mapOf("abs-rate" to setOf("1", "2"), "drugkeeper" to setOf("3")))

        instance1.close()

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
                createDiscovery(UUID.randomUUID().toString(), 20)
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
        testingServer.client.create()
                .creatingParentsIfNeeded()
                .forPath("$rootPath/services")
        println(zkTree())
        createDiscovery("abs-rate")

        println(zkTree())
        assertInstances(mapOf("abs-rate" to setOf("1")))
    }


    @Test
    fun `instance id should be in range from 1 to 127, error thrown otherwise`() {
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
        val client = testingServer.client
        services.forEach { (service, instances) ->
            instances.forEach { instanceId ->
                val instancePath = "$rootPath/services/$instanceId"
                assertNotNull(client.checkExists().forPath(instancePath))
                val instanceIdDataInZkNode = Marshaller.unmarshall(
                        client.data.forPath(instancePath).toString(Charsets.UTF_8),
                        InstanceIdData::class.java
                )
                assertEquals(service, instanceIdDataInZkNode.applicationName)
            }
        }
    }

    private fun createDiscovery(
            appName: String = UUID.randomUUID().toString(),
            registrationRetryCount: Int = 5,
            client: CuratorFramework = testingServer.createClient(),
            maxInstancesCount: Int = Int.MAX_VALUE
    ) = ServiceDiscovery(
            curatorFramework = client,
            instanceIdGenerator = MinFreeInstanceIdGenerator(testingServer.client, "$rootPath/services"),
            config = ServiceDiscoveryConfig(rootPath, appName, registrationRetryCount),
            maxInstancesCount = maxInstancesCount
    )
}
