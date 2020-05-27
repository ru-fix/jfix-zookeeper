package ru.fix.zookeeper.instance.registry

import java.time.Duration

class ServiceInstanceIdRegistryConfig(
        val rootPath: String,
        val serviceName: String,
        val countRegistrationAttempts: Int = 20,
        val serviceRegistrationPath: String = "$rootPath/services",
        val disconnectTimeout: Duration = Duration.ofSeconds(10),
        val host: String,
        val port: Int
)