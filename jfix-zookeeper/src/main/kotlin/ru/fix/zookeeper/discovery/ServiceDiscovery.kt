package ru.fix.zookeeper.discovery

import org.apache.curator.framework.CuratorFramework
import org.apache.logging.log4j.kotlin.Logging
import org.apache.zookeeper.CreateMode

class ServiceDiscovery(
        private val curatorFramework: CuratorFramework,
        rootPath: String,
        private val applicationName: String
) : AutoCloseable {
    private val serviceRegistrationPath = "$rootPath/services"
    private val instanceIdGenerator: InstanceIdGenerator

    companion object : Logging

    init {
        instanceIdGenerator = SerialInstanceIdGenerator(curatorFramework, serviceRegistrationPath)
        initServiceRegistrationPath()
        initInstanceId()
    }

    override fun close() {
        curatorFramework.close()
        logger.info("Service discovery closed successfully")
    }

    private fun initServiceRegistrationPath() {
        try {
            val currentVersion = System.currentTimeMillis().toString().toByteArray()
            curatorFramework.create()
                    .creatingParentsIfNeeded()
                    .forPath(serviceRegistrationPath, currentVersion)
        } catch (e: Exception) {
            logger.debug("Node $serviceRegistrationPath is already initialized")
        }
    }

    private fun initInstanceId() {
        while (true) {
            val instanceIdPath = "$serviceRegistrationPath/${instanceIdGenerator.nextId()}"
            val create = curatorFramework.transactionOp().create()
                    .withMode(CreateMode.EPHEMERAL)
                    .forPath(instanceIdPath, applicationName.toByteArray())

            val newVersion = System.currentTimeMillis().toString().toByteArray()
            val updateVersion = curatorFramework.transactionOp().setData()
                    .forPath(serviceRegistrationPath, newVersion)

            try {
                curatorFramework.transaction().forOperations(create, updateVersion)
                break
            } catch (e: Exception) {
                logger.debug("Instance id creation in transaction failed", e)
                continue
            }
        }
    }
}
