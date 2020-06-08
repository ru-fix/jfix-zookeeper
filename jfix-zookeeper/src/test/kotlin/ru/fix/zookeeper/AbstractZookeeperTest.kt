package ru.fix.zookeeper

import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import ru.fix.zookeeper.testing.ZKTestingServer
import ru.fix.zookeeper.utils.ZkTreePrinter

abstract class AbstractZookeeperTest {
    protected lateinit var testingServer: ZKTestingServer

    protected companion object {
        const val rootPath = "/"
    }

    @BeforeEach
    open fun setUp() {
        testingServer = ZKTestingServer()
                .withCloseOnJvmShutdown(true)
                .start()
    }

    @AfterEach
    open fun tearDown() {
        testingServer.close()
    }

    protected fun zkTree(): String = ZkTreePrinter(testingServer.client).print("/", true)
}