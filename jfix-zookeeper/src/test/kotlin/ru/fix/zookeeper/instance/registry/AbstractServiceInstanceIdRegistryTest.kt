package ru.fix.zookeeper.instance.registry

import org.apache.curator.framework.CuratorFramework
import org.apache.curator.utils.ZKPaths
import org.junit.jupiter.api.Assertions
import ru.fix.aggregating.profiler.NoopProfiler
import ru.fix.zookeeper.AbstractZookeeperTest
import ru.fix.zookeeper.lock.LockData
import ru.fix.zookeeper.utils.Marshaller
import java.time.Duration
import java.time.Instant

abstract class AbstractServiceInstanceIdRegistryTest : AbstractZookeeperTest() {

    protected fun assertInstances(services: Map<String, Set<String>>) {
        val expected = services.flatMap { service ->
            service.value.map { service.key to it }
        }
        val actual = testingServer.client.children
                .forPath(ZKPaths.makePath(rootPath, "services"))
                .asSequence()
                .map {
                    val instancePath = ZKPaths.makePath(rootPath, "services", it)
                    val nodeData = Marshaller.unmarshall(
                            testingServer.client.data.forPath(instancePath).toString(Charsets.UTF_8),
                            LockData::class.java
                    )
                    val instanceIdData = Marshaller.unmarshall(nodeData.metadata, InstanceIdData::class.java)
                    instanceIdData to it
                }
                .map { it.first.applicationName to it.second }
                .toList()

        Assertions.assertEquals(expected, actual)
    }

    protected fun isInstanceIdLockExpired(instanceId: String, disconnectTimeout: Duration): Boolean {
        val instancePath = ZKPaths.makePath(rootPath,"services", instanceId)
        val nodeData = Marshaller.unmarshall(
                testingServer.createClient().data.forPath(instancePath).toString(Charsets.UTF_8),
                LockData::class.java
        )
        val now = Instant.now()
        return nodeData.expirationTimestamp + disconnectTimeout < now
    }

    protected fun assertInstanceIdMapping(instances: Set<Pair<String, String>>) {
        instances.forEach {
            Assertions.assertEquals(it.second, it.first)
        }
    }

    protected fun assertInstanceIdLocksExpiration(
            instances: Set<Pair<String, Boolean>>,
            disconnectTimeout: Duration
    ) {
        instances.forEach {
            Assertions.assertEquals(it.second, !isInstanceIdLockExpired(it.first, disconnectTimeout))
        }
    }

    protected fun createInstanceIdRegistry(
            registrationRetryCount: Int = 5,
            client: CuratorFramework = testingServer.createClient(),
            maxInstancesCount: Int = Int.MAX_VALUE,
            disconnectTimeout: Duration = Duration.ofSeconds(10)
    ) = ServiceInstanceIdRegistry(
            curatorFramework = client,
            instanceIdGenerator = MinFreeInstanceIdGenerator(maxInstancesCount),
            config = ServiceInstanceIdRegistryConfig(
                    rootPath = rootPath,
                    countRegistrationAttempts = registrationRetryCount,
                    disconnectTimeout = disconnectTimeout
            ),
            profiler = NoopProfiler()
    )
}