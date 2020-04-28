package ru.fix.zookeeper.discovery

import org.apache.curator.framework.CuratorFramework
import org.apache.zookeeper.CreateMode
import org.apache.zookeeper.KeeperException
import org.slf4j.LoggerFactory
import ru.fix.zookeeper.transactional.TransactionalClient
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
        private val instanceIdGenerator: InstanceIdGenerator,
        private val config: ServiceDiscoveryConfig
) : AutoCloseable {
    private val serviceRegistrationPath = "${config.rootPath}/services"
    lateinit var instanceId: String
        private set

    companion object {
        private val logger = LoggerFactory.getLogger(ServiceDiscovery::class.java)
        private const val MAX_INSTANCES_COUNT = 127
    }

    init {
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
        } catch (e: KeeperException.NodeExistsException) {
            logger.debug("Node $serviceRegistrationPath is already initialized", e)
        } catch (e: Exception) {
            logger.error("Illegal state when create path: $serviceRegistrationPath", e)
        }
    }

    /**
     * Try {@link #config.countRegistrationAttempts} times to register instance.
     * Generate instance id, this id should be in range 1..127, assertion error will be thrown otherwise.
     * If unsuccessful, then log error
     */
    private fun initInstanceId() {
        TransactionalClient.tryCommit(curatorFramework, config.countRegistrationAttempts,
                { tx ->
                    val instanceId = instanceIdGenerator.nextId()
                    assert(instanceId.toInt() <= MAX_INSTANCES_COUNT) {
                        "Generated instance id has value $instanceId, but should be in range 1..$MAX_INSTANCES_COUNT"
                    }
                    val instanceIdPath = "$serviceRegistrationPath/$instanceId"
                    val instanceIdData = InstanceIdData(config.applicationName, System.currentTimeMillis())
                    tx.createPathWithMode(instanceIdPath, CreateMode.EPHEMERAL)
                            .setData(instanceIdPath, Marshaller.marshall(instanceIdData).toByteArray())
                    this.instanceId = instanceId
                },
                { error ->
                    logger.error("Failed to initialize service discovery and generate instance id. " +
                            "Number of attempts: ${config.countRegistrationAttempts}. " +
                            "Current registration node state: " +
                            ZkTreePrinter(curatorFramework).print(serviceRegistrationPath, true), error)
                }
        )
    }
}
