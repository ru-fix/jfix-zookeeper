package ru.fix.zookeeper.discovery

import org.apache.curator.framework.CuratorFramework
import org.apache.curator.x.discovery.ServiceDiscovery
import org.apache.curator.x.discovery.ServiceDiscoveryBuilder
import org.apache.curator.x.discovery.ServiceInstance
import org.apache.curator.x.discovery.ServiceProvider

class ServiceDiscoveryWrapper(
        curatorFramework: CuratorFramework,
        rootPath: String,
        applicationName: String
) : AutoCloseable {
    private val serviceRegistrationPath = "$rootPath/services"
    private val discoveryService: ServiceDiscovery<Void> = ServiceDiscoveryBuilder
            .builder(Void::class.java)
            .basePath(serviceRegistrationPath)
            .client(curatorFramework)
            .build()
    private val registeredServices: Map<String, ServiceProvider<Void>>
    val serviceProvider: ServiceProvider<Void>

    init {
        discoveryService.start()

        serviceProvider = discoveryService.serviceProviderBuilder()
                .serviceName(applicationName)
                .build()
                .also { it.start() }

        registeredServices = discoveryService.queryForNames()
                .map { serviceName ->
                    serviceName to discoveryService.serviceProviderBuilder()
                            .serviceName(serviceName)
                            .build()
                            .also { it.start() }
                }.toMap().toMutableMap()

        discoveryService.registerService(
                ServiceInstance.builder<Void>()
                        .name(applicationName)
                        .id(nextServerId())
                        .build()
        )
        registeredServices.put(applicationName, serviceProvider)

    }

    private fun nextServerId(): String {
        return ((registeredServices.flatMap { it.value.allInstances }
                .also { println(it)
                    println("") }
                .map { it.id.toInt() }
                .max() ?: 0) + 1).toString()
    }

    override fun close() {
        discoveryService.close()
        registeredServices.forEach { it.value.close() }
        serviceProvider.close()
    }
}