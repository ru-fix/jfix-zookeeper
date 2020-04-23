package ru.fix.zookeeper.discovery

import org.apache.curator.framework.CuratorFramework
import org.apache.curator.x.discovery.ServiceDiscovery
import org.apache.curator.x.discovery.ServiceDiscoveryBuilder
import org.apache.curator.x.discovery.ServiceInstance

class ServiceDiscoveryWrapper(
        private val curatorFramework: CuratorFramework,
        rootPath: String,
        applicationName: String
) : AutoCloseable {
    private val serviceRegistrationPath = "$rootPath/services"
    private val discoveryService: ServiceDiscovery<Void> = ServiceDiscoveryBuilder
            .builder(Void::class.java)
            .basePath(serviceRegistrationPath)
            .client(curatorFramework)
            .build()
    lateinit var serverId: String
        private set

    init {
        discoveryService.start()
        val next = nextServerId()
        discoveryService.registerService(
                ServiceInstance.builder<Void>()
                        .name(applicationName)
                        .id(next)
                        .build()
        )
    }

    private fun nextServerId(): String {
        if (curatorFramework.checkExists().forPath(serviceRegistrationPath) == null) {
            curatorFramework.create()
                    .creatingParentsIfNeeded()
                    .forPath(serviceRegistrationPath)
        }

        val registeredServices = discoveryService.queryForNames()
                .map { serviceName ->
                    serviceName to discoveryService.serviceProviderBuilder()
                            .serviceName(serviceName)
                            .build()
                            .also { it.start() }
                }.toMap().toMutableMap()

        serverId = ((registeredServices.flatMap { it.value.allInstances }
                .map { it.id.toInt() }
                .max() ?: 0) + 1).toString()
        registeredServices.values.forEach { it.close() }
        return serverId
    }

    override fun close() {
        discoveryService.close()
    }

}
