package ru.fix.zookeeper.discovery

class ServiceDiscoveryConfig(
        val rootPath: String,
        val applicationName: String,
        val countRegistrationAttempts: Int = 5
)