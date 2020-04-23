package ru.fix.zookeeper.discovery

import org.junit.jupiter.api.Test
import ru.fix.zookeeper.AbstractZookeeperTest

internal class ServiceDiscoveryWrapperTest : AbstractZookeeperTest() {

    @Test
    fun test() {
        val s1 = createDiscovery("abs-rate")
        val s2 = createDiscovery("abs-rate")
        val s3 = createDiscovery()

        println(s1.serverId)
        println(s2.serverId)
        println(s3.serverId)

        println(zkTree())
    }


    private fun createDiscovery(
            appName: String = "drugkeeper"
    ) = ServiceDiscoveryWrapper(
            curatorFramework = zkTestingServer.createClient(),
            rootPath = rootPath,
            applicationName = appName
    )
}