package ru.fix.zookeeper.discovery

import org.apache.curator.framework.CuratorFramework
import org.apache.logging.log4j.kotlin.Logging
import org.apache.zookeeper.CreateMode
import ru.fix.zookeeper.utils.Marshaller
import ru.fix.zookeeper.utils.ZkTreePrinter

/**
 * This class provides functionality of instance id management.
 * When this class instantiated, it generates new instance id for this zookeeper client and registers this id in znode.
 * When zookeeper client died or connection closed, znode with generated instance id also deleted.
 * Service registration guarantees uniqueness of instance id in case of parallel startup on different JVMs.
 */
class ServiceDiscovery(
        private val curatorFramework: CuratorFramework,
        private val config: ServiceDiscoveryConfig
) : AutoCloseable {
    private val serviceRegistrationPath = "${config.rootPath}/services"
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

    /**
     * Try {@link #config.countRegistrationAttempts} times to register instance.
     * If unsuccessful, then throws an exception
     */
    private fun initInstanceId() {
        for (i in 1..config.countRegistrationAttempts) {
            val instanceIdPath = "$serviceRegistrationPath/${instanceIdGenerator.nextId()}"
            val instanceIdData = Marshaller.marshall(InstanceIdData(config.applicationName, System.currentTimeMillis()))
            val createInstanceIdNode = curatorFramework
                    .transactionOp().create()
                    .withMode(CreateMode.EPHEMERAL)
                    .forPath(instanceIdPath, instanceIdData.toByteArray())

            try {
                curatorFramework.transaction().forOperations(createInstanceIdNode)
                break
            } catch (e: Exception) {
                if (i < config.countRegistrationAttempts) {
                    logger.debug("Instance id creation in transaction failed", e)
                    continue
                } else {
                    logger.error("Failed to initialize service discovery and generate instance id. " +
                            "Number of attempts: ${config.countRegistrationAttempts}. " +
                            "Current registration node state: " +
                            ZkTreePrinter(curatorFramework).print(serviceRegistrationPath, true), e)
                    throw e
                }
            }
        }
    }
}
