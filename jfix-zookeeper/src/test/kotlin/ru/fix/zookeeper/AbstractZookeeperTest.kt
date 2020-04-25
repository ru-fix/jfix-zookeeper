package ru.fix.zookeeper

import org.junit.jupiter.api.BeforeEach
import ru.fix.zookeeper.testing.ZKTestingServer
import ru.fix.zookeeper.utils.ZkTreePrinter

abstract class AbstractZookeeperTest {
    protected lateinit var testingServer: ZKTestingServer

    protected companion object {
        const val rootPath = "/zk-cluster/wwp"
    }

    @BeforeEach
    fun setUp() {
        testingServer = ZKTestingServer()
                .withCloseOnJvmShutdown(true)
                .start()
    }

    protected fun zkTree(): String = ZkTreePrinter(testingServer.client).print(rootPath, true)
}