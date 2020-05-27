package ru.fix.zookeeper.instance.registry

import java.time.Duration

class ServiceInstanceIdRegistryConfig(
        val rootPath: String,
        val serviceName: String,
        val countRegistrationAttempts: Int = 20,
        val serviceRegistrationPath: String = "$rootPath/services",
        /**
         * Pair host + port as application's primary key.
         */
        val host: String,
        val port: Int,
        /**
         * Timeout during which the client does not connect with the same host and port.
         * After this timeout expiration, new client can get instance id from zk,
         * if host and port of connected client and host and port of instance in zk are equal.
         */
        val disconnectTimeout: Duration = Duration.ofSeconds(30)
)