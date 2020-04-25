package ru.fix.zookeeper.discovery

import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.async
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import ru.fix.zookeeper.AbstractZookeeperTest
import java.util.*

internal class ServiceDiscoveryTest : AbstractZookeeperTest() {

    @Test
    fun test() {
        createDiscovery("abs-rate")
        createDiscovery("abs-rate")
        createDiscovery()

        println(zkTree())
        assertInstances(mapOf("abs-rate" to setOf("1", "2"), "drugkeeper" to setOf("3")))
    }

    @Test
    fun test2() = runBlocking {
        val servicesCount = 10
        val randomServiceNames = (1..servicesCount).map { UUID.randomUUID().toString() }
        val services = randomServiceNames.map {
            GlobalScope.async {
                createDiscovery(it)
            }
        }

        services.forEach { it.await() }
        println(zkTree())
        val countUniqueInstanceId = testingServer.client.children
                .forPath("$rootPath/services")
                .map { it.substringAfterLast("/").toInt() }
                .toSet().size

        assertEquals(servicesCount, countUniqueInstanceId)
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
            appName: String = "drugkeeper"
    ) = ServiceDiscovery(
            curatorFramework = testingServer.createClient(),
            rootPath = rootPath,
            applicationName = appName
    )
}