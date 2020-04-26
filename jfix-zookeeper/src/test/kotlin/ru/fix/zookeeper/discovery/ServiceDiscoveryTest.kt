package ru.fix.zookeeper.discovery

import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.async
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Test
import ru.fix.zookeeper.AbstractZookeeperTest
import java.util.*

internal class ServiceDiscoveryTest : AbstractZookeeperTest() {

    @Test
    fun `sequential startup of 3 instances should generate id from 1 to 3`() {
        createDiscovery("abs-rate")
        createDiscovery("abs-rate")
        createDiscovery("drugkeeper")

        println(zkTree())
        assertInstances(mapOf("abs-rate" to setOf("1", "2"), "drugkeeper" to setOf("3")))
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
        val services = randomServiceNames.map {
            GlobalScope.async {
                createDiscovery(it)
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

    private fun assertInstances(services: Map<String, Set<String>>) {
        val client = testingServer.client
        services.forEach { (service, instances) ->
            instances.forEach { instanceId ->
                val instancePath = "$rootPath/services/$instanceId"
                assertNotNull(client.checkExists().forPath(instancePath))
                assertEquals(service, client.data.forPath(instancePath).toString(Charsets.UTF_8))
            }
        }
    }


    private fun createDiscovery(
            appName: String = UUID.randomUUID().toString()
    ) = ServiceDiscovery(
            curatorFramework = testingServer.createClient(),
            rootPath = rootPath,
            applicationName = appName
    )
}
