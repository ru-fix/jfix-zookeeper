package ru.fix.zookeeper

import org.junit.jupiter.api.BeforeEach
import ru.fix.zookeeper.testing.ZKTestingServer
import ru.fix.zookeeper.utils.ZkTreePrinter

abstract class AbstractZookeeperTest {
    protected lateinit var zkTestingServer: ZKTestingServer

    protected companion object {
        const val rootPath = "/zk-cluster/wwp"
    }

    @BeforeEach
    fun setUp() {
        zkTestingServer = ZKTestingServer()
                .withCloseOnJvmShutdown(true)
                .start()
    }

    protected fun zkTree(): String = ZkTreePrinter(zkTestingServer.client).print(rootPath)
}