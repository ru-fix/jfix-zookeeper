package ru.fix.zookeeper.discovery

class ServiceInstanceIdRegistryConfig(
        val rootPath: String,
        val serviceName: String,
        val maxCountOfInstanceIds: Int = 128,
        val countRegistrationAttempts: Int = 20,
        val serviceRegistrationPath: String = "$rootPath/services"
)