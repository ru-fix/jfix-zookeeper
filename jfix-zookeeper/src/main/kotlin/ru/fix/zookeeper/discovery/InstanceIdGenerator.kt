package ru.fix.zookeeper.discovery

import org.apache.curator.framework.CuratorFramework
import org.apache.curator.x.discovery.ServiceDiscovery

class InstanceIdGenerator(
        private val serviceDiscovery: ServiceDiscovery<Void>,
        private val curatorFramework: CuratorFramework,
        private val serviceRegistrationPath: String
) {

    fun nextId(): String {
        if (curatorFramework.checkExists().forPath(serviceRegistrationPath) == null) {
            curatorFramework.create()
                    .creatingParentsIfNeeded()
                    .forPath(serviceRegistrationPath)
        }

        val registeredServices = serviceDiscovery.queryForNames()
                .map { serviceName ->
                    serviceName to serviceDiscovery.serviceProviderBuilder()
                            .serviceName(serviceName)
                            .build()
                            .also { it.start() }
                }.toMap().toMutableMap()

        val nextInstanceId = ((registeredServices.flatMap { it.value.allInstances }
                .map { it.id.toInt() }
                .max() ?: 0) + 1).toString()
        registeredServices.values.forEach { it.close() }
        return nextInstanceId
    }
}
