package ru.fix.zookeeper.lock

import org.junit.jupiter.api.Test
import ru.fix.aggregating.profiler.NoopProfiler
import ru.fix.zookeeper.AbstractZookeeperTest

class LockManagerImplTest : AbstractZookeeperTest() {

    @Test
    fun test() {
        val lockManager = LockManagerImpl(testingServer.client, "worker", NoopProfiler())
        lockManager.tryAcquire(LockIdentity("pain", "/path/pain")) { println("fallback") }
    }

}