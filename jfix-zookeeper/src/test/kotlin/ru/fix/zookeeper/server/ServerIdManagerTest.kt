package ru.fix.zookeeper.server

import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Disabled
import org.junit.jupiter.api.Test
import ru.fix.zookeeper.testing.ZKTestingServer
import ru.fix.zookeeper.utils.ZkTreePrinter
import java.util.concurrent.CompletableFuture

internal class ServerIdManagerTest {
    private lateinit var zkTestingServer: ZKTestingServer
    private lateinit var printer: ZkTreePrinter

    companion object {
        private const val rootPath = "/zk-cluster/wwp"
    }

    @BeforeEach
    fun setUp() {
        zkTestingServer = ZKTestingServer()
        printer = ZkTreePrinter(zkTestingServer.client)
        zkTestingServer.start()
    }

    @Test
    fun test() {
        println("Before" + printer.print(rootPath, true))
        createServerIdManager()
        println("After 1" + printer.print(rootPath, true))
        createServerIdManager()
        println("After 2" + printer.print(rootPath, true))
        createServerIdManager("abs-rate")
        println("After 3" + printer.print(rootPath, true))

    }

    @Disabled
    @Test
    fun test2() {
        val servers = (1..10)
                .map { CompletableFuture.runAsync { createServerIdManager() } }
        CompletableFuture.allOf(*servers.toTypedArray()).join()
        println("After " + printer.print(rootPath, true))
    }

    private fun createServerIdManager(
            appName: String = "drugkeeper"
    ) = ServerIdManager(
            zkTestingServer.createClient(),
            rootPath,
            appName
    )
}
