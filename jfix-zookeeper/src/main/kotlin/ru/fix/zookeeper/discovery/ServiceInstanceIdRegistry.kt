package ru.fix.zookeeper.discovery

import org.apache.curator.framework.CuratorFramework
import org.apache.zookeeper.CreateMode
import org.apache.zookeeper.KeeperException
import org.slf4j.LoggerFactory
import ru.fix.zookeeper.transactional.TransactionalClient
import ru.fix.zookeeper.utils.Marshaller
import ru.fix.zookeeper.utils.ZkTreePrinter
import java.time.OffsetDateTime
import java.time.ZoneOffset

/**
 * This class provides functionality of instance id management.
 * When this class instantiated, it generates new instance id for this zookeeper client and registers this id in znode.
 * When zookeeper client died or connection closed, znode with generated instance id also deleted.
 * Service registration guarantees uniqueness of instance id in case of parallel startup on different JVMs.
 */
class ServiceInstanceIdRegistry(
        private val curatorFramework: CuratorFramework,
        private val instanceIdGenerator: InstanceIdGenerator,
        private val config: ServiceInstanceIdRegistryConfig
) {
    lateinit var instanceId: String
        private set

    companion object {
        private val logger = LoggerFactory.getLogger(ServiceInstanceIdRegistry::class.java)
    }

    init {
        initServiceRegistrationPath()
        initInstanceId()
    }

    private fun initServiceRegistrationPath() {
        try {
            curatorFramework.create()
                    .creatingParentsIfNeeded()
                    .forPath(config.serviceRegistrationPath, byteArrayOf())
        } catch (e: KeeperException.NodeExistsException) {
            logger.debug("Node ${config.serviceRegistrationPath} is already initialized", e)
        } catch (e: Exception) {
            logger.error("Illegal state when create path: ${config.serviceRegistrationPath}", e)
        }
    }

    /**
     * Try {@link #config.countRegistrationAttempts} times to register instance.
     * Generate instance id, this id should be in range 1..maxInstancesCount, assertion error will be thrown otherwise.
     * If unsuccessful, then log error
     */
    private fun initInstanceId() {
        TransactionalClient.tryCommit(
                curatorFramework,
                config.countRegistrationAttempts,
                { tx ->
                    val alreadyRegisteredInstanceIds = curatorFramework.children.forPath(config.serviceRegistrationPath)
                    val instanceId = instanceIdGenerator.nextId(alreadyRegisteredInstanceIds)
                    val instanceIdPath = "${config.serviceRegistrationPath}/$instanceId"
                    val instanceIdData = InstanceIdData(config.serviceName, OffsetDateTime.now(ZoneOffset.UTC))
                    tx.createPathWithMode(instanceIdPath, CreateMode.EPHEMERAL)
                            .setData(instanceIdPath, Marshaller.marshall(instanceIdData).toByteArray())
                    this.instanceId = instanceId
                },
                { error ->
                    logger.error("Failed to initialize service id registry and generate instance id. " +
                            "Number of attempts: ${config.countRegistrationAttempts}. " +
                            "Current registration node state: " +
                            ZkTreePrinter(curatorFramework).print(config.serviceRegistrationPath, true),
                            error
                    )
                }
        )
    }
}
