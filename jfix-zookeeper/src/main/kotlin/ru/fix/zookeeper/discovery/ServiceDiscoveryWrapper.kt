package ru.fix.zookeeper.discovery

import org.apache.curator.framework.CuratorFramework
import org.apache.curator.x.discovery.ServiceDiscovery
import org.apache.curator.x.discovery.ServiceDiscoveryBuilder
import org.apache.curator.x.discovery.ServiceInstance
import org.slf4j.Logger
import org.slf4j.LoggerFactory

class ServiceDiscoveryWrapper(
        curatorFramework: CuratorFramework,
        rootPath: String,
        applicationName: String
) : AutoCloseable {
    private val serviceRegistrationPath = "$rootPath/services"
    private val serviceDiscovery: ServiceDiscovery<Void> = ServiceDiscoveryBuilder
            .builder(Void::class.java)
            .basePath(serviceRegistrationPath)
            .client(curatorFramework)
            .build()
    private val instanceIdGenerator = InstanceIdGenerator(
            serviceDiscovery, curatorFramework, serviceRegistrationPath
    )
    var serverId: String
        private set

    companion object {
        private val logger: Logger = LoggerFactory.getLogger(ServiceDiscoveryWrapper::class.java)
    }

    init {
        serviceDiscovery.start()
        serverId = instanceIdGenerator.nextId()
        logger.info("Service discovery started and generated id=$serverId for service=$applicationName")

        serviceDiscovery.registerService(
                ServiceInstance.builder<Void>()
                        .name(applicationName)
                        .id(serverId)
                        .build()
        )
        logger.info("Service discovery registered instance with id=$serverId for service=$applicationName")
    }

    override fun close() {
        serviceDiscovery.close()
        logger.info("Service discovery closed")
    }

}
