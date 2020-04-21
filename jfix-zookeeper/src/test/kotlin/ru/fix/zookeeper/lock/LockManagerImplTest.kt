package ru.fix.zookeeper.lock

import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import ru.fix.aggregating.profiler.NoopProfiler
import ru.fix.zookeeper.testing.ZKTestingServer
import ru.fix.zookeeper.utils.ZkTreePrinter

class LockManagerImplTest {
    private lateinit var zkTestingServer: ZKTestingServer
    private lateinit var printer: ZkTreePrinter
    private lateinit var lockManager: LockManager

    companion object {
        private const val rootPath = "/zk-cluster/wwp"
    }

    @BeforeEach
    fun setUp() {
        zkTestingServer = ZKTestingServer()
        printer = ZkTreePrinter(zkTestingServer.client)
        zkTestingServer.start()
        lockManager = LockManagerImpl(zkTestingServer.client, "worker", NoopProfiler())
    }


    @Test
    fun test() {
        lockManager.tryAcquire(LockIdentity("pain", "/path/pain")) { println("fallback") }
    }
}