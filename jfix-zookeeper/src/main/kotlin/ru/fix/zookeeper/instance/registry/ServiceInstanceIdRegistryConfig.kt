package ru.fix.zookeeper.instance.registry

import ru.fix.zookeeper.lock.PersistentExpiringLockManagerConfig
import java.time.Duration

class ServiceInstanceIdRegistryConfig(
        val countRegistrationAttempts: Int = 20,
        /**
         * Timeout during which the client can be disconnected and didn't lost instance id, that was before reconnection.
         */
        val persistentExpiringLockManagerConfig: PersistentExpiringLockManagerConfig = PersistentExpiringLockManagerConfig(
                lockAcquirePeriod = Duration.ofSeconds(90),
                expirationPeriod = Duration.ofSeconds(30),
                lockCheckAndProlongInterval =  Duration.ofSeconds(10),
                acquiringTimeout = Duration.ofSeconds(1)
        ),
        /**
         * How ofter try to acquire locks of instances, that was lost connection
         */
        val retryRestoreInstanceIdInterval: Duration = Duration.ofSeconds(10)
)