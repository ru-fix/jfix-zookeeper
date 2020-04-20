package ru.fix.zookeeper.server

import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import ru.fix.zookeeper.testing.ZKTestingServer
import ru.fix.zookeeper.utils.ZkTreePrinter

internal class ServerIdManagerTest {
    private lateinit var zkTestingServer: ZKTestingServer

    companion object {
        private const val rootPath = "/zk-cluster/wwp"
    }

    @BeforeEach
    fun setUp() {
        zkTestingServer = ZKTestingServer()
        zkTestingServer.start()
    }

    @Test
    fun test() {
        println("Before" + ZkTreePrinter(zkTestingServer.client).print(rootPath, true))
        createServerIdManager()
        println("After 1" + ZkTreePrinter(zkTestingServer.client).print(rootPath, true))
        createServerIdManager()
        println("After 2" + ZkTreePrinter(zkTestingServer.client).print(rootPath, true))
        createServerIdManager("abs-rate")
        println("After 3" + ZkTreePrinter(zkTestingServer.client).print(rootPath, true))

    }

    private fun createServerIdManager(
            appName: String = "drugkeeper"
    ) = ServerIdManager(
            zkTestingServer.createClient(),
            rootPath,
            appName
    )
}
