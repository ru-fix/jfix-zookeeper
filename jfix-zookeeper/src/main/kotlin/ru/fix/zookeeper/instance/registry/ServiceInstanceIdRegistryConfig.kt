package ru.fix.zookeeper.instance.registry

import ru.fix.zookeeper.lock.PersistentExpiringLockManagerConfig
import java.time.Duration

class ServiceInstanceIdRegistryConfig(
        val countRegistrationAttempts: Int = 20,
        /**
         * Timeout during which the client can be disconnected and didn't lost instance id, that was before reconnection.
         */
        val disconnectTimeout: Duration = Duration.ofSeconds(60),
        val persistentExpiringLockManagerConfig: PersistentExpiringLockManagerConfig = PersistentExpiringLockManagerConfig(
                lockAcquirePeriod = disconnectTimeout.dividedBy(2),
                expirationPeriod = disconnectTimeout.dividedBy(3),
                lockCheckAndProlongInterval = disconnectTimeout.dividedBy(4),
                acquiringTimeout = Duration.ofSeconds(1)
        )
)