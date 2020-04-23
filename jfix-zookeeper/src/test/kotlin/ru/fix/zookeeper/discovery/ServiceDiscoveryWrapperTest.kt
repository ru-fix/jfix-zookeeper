package ru.fix.zookeeper.discovery

import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import ru.fix.zookeeper.testing.ZKTestingServer
import ru.fix.zookeeper.utils.ZkTreePrinter

internal class ServiceDiscoveryWrapperTest {
    private lateinit var zkTestingServer: ZKTestingServer

    companion object {
        private const val rootPath = "/zk-cluster/wwp"
    }

    @BeforeEach
    fun setUp() {
        zkTestingServer = ZKTestingServer().start()
    }

    private fun createDiscovery(appName: String = "bookkeeper") = ServiceDiscoveryWrapper(
            curatorFramework = zkTestingServer.createClient(),
            rootPath = rootPath,
            applicationName = appName
    )

    @Test
    fun test() {
        val s1 = createDiscovery("abs-gate")
        val s2 = createDiscovery("abs-gate")
        val s3 = createDiscovery()

        println(s1.serverId)
        println(s2.serverId)
        println(s3.serverId)

        println(zkTree())
    }

    private fun zkTree() = ZkTreePrinter(zkTestingServer.client).print(rootPath)
}