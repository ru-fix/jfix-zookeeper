package ru.fix.zookeeper.discovery

import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.async
import kotlinx.coroutines.runBlocking
import org.apache.curator.framework.CuratorFramework
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import ru.fix.aggregating.profiler.NoopProfiler
import ru.fix.dynamic.property.api.DynamicProperty
import ru.fix.stdlib.concurrency.threads.NamedExecutors
import ru.fix.zookeeper.AbstractZookeeperTest
import ru.fix.zookeeper.utils.Marshaller
import java.util.*

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
        val randomServiceNames = (1..servicesCount).map { UUID.randomUUID().toString() }
        val dispatcher = NamedExecutors.newDynamicPool("discovery-pool", DynamicProperty.of(12), NoopProfiler())
                .asCoroutineDispatcher()
        val services = randomServiceNames.map {
            GlobalScope.async(context = dispatcher) {
                createDiscovery(it, 20)
            }
        }

        services.forEach { it.await() }
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
        val maxAvailableId = 127
        repeat(maxAvailableId) {
            createDiscovery(UUID.randomUUID().toString(), 20, testingServer.client)
        }
        repeat(10) {
            assertThrows(AssertionError::class.java) {
                createDiscovery(UUID.randomUUID().toString(), 20, testingServer.client)
            }
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
            client: CuratorFramework = testingServer.createClient()
    ) = ServiceDiscovery(
            curatorFramework = client,
            instanceIdGenerator = MinFreeInstanceIdGenerator(testingServer.client, "$rootPath/services"),
            config = ServiceDiscoveryConfig(rootPath, appName, registrationRetryCount)
    )
}
