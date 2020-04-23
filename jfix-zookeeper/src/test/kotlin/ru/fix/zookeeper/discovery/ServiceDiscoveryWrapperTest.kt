package ru.fix.zookeeper.discovery

import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Test
import ru.fix.zookeeper.AbstractZookeeperTest

internal class ServiceDiscoveryWrapperTest : AbstractZookeeperTest() {

    @Test
    fun test() {
        createDiscovery("abs-rate")
        createDiscovery("abs-rate")
        createDiscovery()

        println(zkTree())
        assertInstances(mapOf("abs-rate" to setOf("1", "2"), "drugkeeper" to setOf("3")))
    }

    private fun assertInstances(services: Map<String, Set<String>>) {
        val client = testingServer.client
        services.forEach { (service, instances) ->
            instances.forEach { instance ->
                assertNotNull(
                        client.checkExists()
                                .forPath("$rootPath/services/$service/$instance")
                )
            }
        }
    }


    private fun createDiscovery(
            appName: String = "drugkeeper"
    ) = ServiceDiscoveryWrapper(
            curatorFramework = testingServer.createClient(),
            rootPath = rootPath,
            applicationName = appName
    )
}
