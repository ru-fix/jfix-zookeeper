package ru.fix.zookeeper.instance.registry

import org.apache.curator.utils.ZKPaths
import java.time.Duration

class ServiceInstanceIdRegistryConfig(
        val rootPath: String,
        val countRegistrationAttempts: Int = 20,
        val serviceRegistrationPath: String = ZKPaths.makePath(rootPath,"services"),
        /**
         * Timeout during which the client can be disconnected and didn't lost instance id, that was before reconnection.
         */
        val disconnectTimeout: Duration = Duration.ofSeconds(60)
)