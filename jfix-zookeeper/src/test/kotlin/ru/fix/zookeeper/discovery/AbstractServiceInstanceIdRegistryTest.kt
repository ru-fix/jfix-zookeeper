package ru.fix.zookeeper.discovery

import com.fasterxml.jackson.core.type.TypeReference
import org.apache.curator.framework.CuratorFramework
import org.junit.jupiter.api.Assertions
import ru.fix.zookeeper.AbstractZookeeperTest
import ru.fix.zookeeper.lock.LockData
import ru.fix.zookeeper.utils.Marshaller
import java.time.Duration
import java.time.Instant
import java.util.*
import kotlin.random.Random

abstract class AbstractServiceInstanceIdRegistryTest : AbstractZookeeperTest() {

    protected fun assertInstances(services: Map<String, Set<String>>) {
        val expected = services.flatMap { service ->
            service.value.map { service.key to it }
        }
        val actual = testingServer.client.children
                .forPath("$rootPath/services")
                .asSequence()
                .map {
                    val instancePath = "$rootPath/services/$it"
                    val nodeData = Marshaller.unmarshall(
                            testingServer.client.data.forPath(instancePath).toString(Charsets.UTF_8),
                            object : TypeReference<LockData>() {}
                    )
                    val instanceIdData = Marshaller.unmarshall(nodeData.data, object : TypeReference<InstanceIdData>() {})
                    instanceIdData to it
                }
                .map { it.first.applicationName to it.second }
                .toList()

        Assertions.assertEquals(expected, actual)
    }

    protected fun isInstanceIdLockExpired(instanceId: String, disconnectTimeout: Duration): Boolean {
        val instancePath = "$rootPath/services/$instanceId"
        val nodeData = Marshaller.unmarshall(
                testingServer.client.data.forPath(instancePath).toString(Charsets.UTF_8),
                object : TypeReference<LockData>() {}
        )
        return nodeData.expirationDate + disconnectTimeout < Instant.now()
    }

    protected fun assertInstanceIdMapping(instances: Set<Pair<ServiceInstanceIdRegistry, String>>) {
        instances.forEach {
            Assertions.assertEquals(it.second, it.first.instanceId)
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
            appName: String = UUID.randomUUID().toString(),
            registrationRetryCount: Int = 5,
            client: CuratorFramework = testingServer.createClient(),
            maxInstancesCount: Int = Int.MAX_VALUE,
            host: String = "localhost",
            port: Int = Random.nextInt(0, 65535),
            disconnectTimeout: Duration = Duration.ofSeconds(10)
    ) = ServiceInstanceIdRegistry(
            curatorFramework = client,
            instanceIdGenerator = MinFreeInstanceIdGenerator(maxInstancesCount),
            config = ServiceInstanceIdRegistryConfig(
                    rootPath = rootPath,
                    serviceName = appName,
                    countRegistrationAttempts = registrationRetryCount,
                    host = host,
                    port = port,
                    disconnectTimeout = disconnectTimeout
            )
    )
}