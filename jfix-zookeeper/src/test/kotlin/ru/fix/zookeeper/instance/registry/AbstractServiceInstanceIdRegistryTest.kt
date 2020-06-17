package ru.fix.zookeeper.instance.registry

import org.apache.curator.framework.CuratorFramework
import org.apache.curator.framework.state.ConnectionState
import org.apache.curator.utils.ZKPaths
import org.awaitility.Awaitility
import ru.fix.aggregating.profiler.NoopProfiler
import ru.fix.dynamic.property.api.DynamicProperty
import ru.fix.zookeeper.AbstractZookeeperTest
import ru.fix.zookeeper.lock.PersistentExpiringDistributedLock
import ru.fix.zookeeper.lock.PersistentExpiringLockManagerConfig
import java.time.Duration
import java.util.concurrent.atomic.AtomicReference

abstract class AbstractServiceInstanceIdRegistryTest : AbstractZookeeperTest() {

    protected companion object {
        val serviceRegistrationPath: String = ZKPaths.makePath(rootPath, "services")
    }

    protected fun instanceIdState(
            serviceName: String,
            instanceId: String,
            curator: CuratorFramework = testingServer.client
    ): PersistentExpiringDistributedLock.LockNodeState {
        val instancePath = ZKPaths.makePath(serviceRegistrationPath, serviceName, instanceId)
        return PersistentExpiringDistributedLock.readLockNodeState(curator, instancePath)
    }

    protected fun waitLockNodeState(
            expected: PersistentExpiringDistributedLock.LockNodeState,
            path: String,
            curator: CuratorFramework = testingServer.client,
            timeout: Duration = Duration.ofSeconds(10)
    ) {
        Awaitility.await()
                .timeout(timeout)
                .pollInterval(Duration.ofMillis(500))
                .until { expected == PersistentExpiringDistributedLock.readLockNodeState(curator, path) }
    }

    protected fun lockPath(serviceName: String, id: String): String =
            ZKPaths.makePath(serviceRegistrationPath, serviceName, id)

    protected fun createInstanceIdRegistry(
            registrationRetryCount: Int = 5,
            client: CuratorFramework = testingServer.createClient(),
            maxInstancesCount: Int = Int.MAX_VALUE,
            lockAcquirePeriod: Duration = Duration.ofSeconds(3),
            expirationPeriod: Duration = Duration.ofSeconds(2),
            lockCheckAndProlongInterval: Duration = Duration.ofMillis(1000),
            retryRestoreInstanceIdInterval: Duration = Duration.ofMillis(1)
    ) = ServiceInstanceIdRegistry(
            curatorFramework = client,
            instanceIdGenerator = MinFreeInstanceIdGenerator(maxInstancesCount),
            serviceRegistrationPath = serviceRegistrationPath,
            config = DynamicProperty.of(
                    ServiceInstanceIdRegistryConfig(
                            countRegistrationAttempts = registrationRetryCount,
                            persistentExpiringLockManagerConfig = PersistentExpiringLockManagerConfig(
                                    lockAcquirePeriod = lockAcquirePeriod,
                                    expirationPeriod = expirationPeriod,
                                    lockCheckAndProlongInterval = lockCheckAndProlongInterval,
                                    acquiringTimeout = Duration.ofSeconds(1)
                            ),
                            retryRestoreInstanceIdInterval = retryRestoreInstanceIdInterval
                    )),
            profiler = NoopProfiler()
    )

    protected fun waitDisconnectState(zkState: AtomicReference<ConnectionState>) {
        Awaitility.await()
                .timeout(Duration.ofSeconds(2))
                .until { zkState.get() == ConnectionState.SUSPENDED || zkState.get() == ConnectionState.LOST }
    }

    protected fun waitReconnectState(zkState: AtomicReference<ConnectionState>) {
        Awaitility.await()
                .timeout(Duration.ofSeconds(2))
                .until { zkState.get() == ConnectionState.RECONNECTED }
    }
}