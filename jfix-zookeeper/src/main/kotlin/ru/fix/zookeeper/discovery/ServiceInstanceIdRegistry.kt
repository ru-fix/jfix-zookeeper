package ru.fix.zookeeper.discovery

import com.fasterxml.jackson.core.type.TypeReference
import org.apache.curator.framework.CuratorFramework
import org.apache.curator.framework.recipes.cache.TreeCache
import org.apache.curator.framework.recipes.cache.TreeCacheEvent
import org.apache.curator.framework.recipes.cache.TreeCacheListener
import org.apache.curator.framework.state.ConnectionState
import org.apache.curator.framework.state.ConnectionStateListener
import org.apache.zookeeper.CreateMode
import org.apache.zookeeper.KeeperException
import org.slf4j.LoggerFactory
import ru.fix.zookeeper.lock.LockData
import ru.fix.zookeeper.lock.LockIdentity
import ru.fix.zookeeper.lock.PersistentExpiringLockManager
import ru.fix.zookeeper.lock.PersistentExpiringLockManagerConfig
import ru.fix.zookeeper.transactional.TransactionalClient
import ru.fix.zookeeper.utils.Marshaller
import ru.fix.zookeeper.utils.ZkTreePrinter
import java.time.Instant

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
) : AutoCloseable {
    lateinit var instanceIdPath: String
        private set
    lateinit var instanceId: String
        private set
    lateinit var lockIdentity: LockIdentity
        private set

    companion object {
        private val logger = LoggerFactory.getLogger(ServiceInstanceIdRegistry::class.java)
    }

    private val lockManager: PersistentExpiringLockManager = PersistentExpiringLockManager(
            curatorFramework, PersistentExpiringLockManagerConfig()
    )

    init {
        curatorFramework
                .connectionStateListenable
                .addListener(
                        ConnectionStateListener { client, newState ->
                            when (newState) {
                                ConnectionState.RECONNECTED -> reconnect1()
                                else -> Unit
                            }
                        })
        initServiceRegistrationPath()
        initInstanceId1()
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
    /*private fun initInstanceId() {
        TransactionalClient.tryCommit(
                curatorFramework,
                config.countRegistrationAttempts,
                { tx ->
                    val alreadyRegisteredInstanceIds = curatorFramework.children.forPath(config.serviceRegistrationPath)
                    val preparedInstanceId = instanceIdGenerator.nextId(alreadyRegisteredInstanceIds)

                    val instanceIdPath = "${config.serviceRegistrationPath}/$preparedInstanceId"
                    val instanceIdData = InstanceIdData(config.serviceName, Instant.now(), CONNECTED, host = config.host, port = config.port)
                    tx.createPathWithMode(instanceIdPath, CreateMode.EPHEMERAL)
                            .setData(instanceIdPath, Marshaller.marshall(instanceIdData).toByteArray())

                    this.instanceIdPath = instanceIdPath
                    this.instanceId = preparedInstanceId
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
    }*/

    private fun initInstanceId1() {
        val alreadyRegisteredInstanceIds = curatorFramework.children.forPath(config.serviceRegistrationPath)
        val preparedInstanceId = resolvePreviousConnection(alreadyRegisteredInstanceIds)
                ?: instanceIdGenerator.nextId(alreadyRegisteredInstanceIds)

        val instanceIdPath = "${config.serviceRegistrationPath}/$preparedInstanceId"
        val instanceIdData = InstanceIdData(config.serviceName, Instant.now(), host = config.host, port = config.port)
        val lockIdentity = LockIdentity(preparedInstanceId, instanceIdPath, Marshaller.marshall(instanceIdData))

        val result = lockManager.tryAcquire(lockIdentity) { lock ->
            logger.error("Failed to initialize service id registry and generate instance id. " +
                    "Lock id: ${Marshaller.marshall(lock)}. " +
                    "Current registration node state: " +
                    ZkTreePrinter(curatorFramework).print(config.serviceRegistrationPath, true)
            )
        }
        if (result) {
            this.instanceIdPath = instanceIdPath
            this.instanceId = preparedInstanceId
            this.lockIdentity = lockIdentity
        }
    }

    private fun resolvePreviousConnection(alreadyRegisteredInstanceIds: List<String>): String? {
        alreadyRegisteredInstanceIds.forEach ids@{
            val path = "${config.serviceRegistrationPath}/$it"
            val lockData = Marshaller.unmarshall(
                    curatorFramework.data.forPath(path).toString(Charsets.UTF_8),
                    object : TypeReference<LockData>() {}
            )
            if (lockData.data == null) {
                return@ids
            }
            val instanceIdData = Marshaller.unmarshall(lockData.data, object : TypeReference<InstanceIdData>() {})
            if (instanceIdData.host != config.host || instanceIdData.port != config.port) {
                return@ids
            }
            if (lockData.expirationDate.plus(config.disconnectTimeout).isBefore(Instant.now())) {
                return it
            }
        }
        return null
    }

    private fun reconnect1() {
        try {
            val lockData = Marshaller.unmarshall(
                    curatorFramework.data.forPath(instanceIdPath).toString(Charsets.UTF_8),
                    object : TypeReference<LockData>() {}
            )
            when {
                lockData.data == null -> {
                    logger.error("ServiceInstanceIdRegistry reconnected, but lock for its instance id not found.")
                }
                lockData.expirationDate.plus(config.disconnectTimeout).isAfter(Instant.now()) -> {
                    lockManager.release(lockIdentity)
                    lockManager.tryAcquire(lockIdentity) {
                        logger.error("Failed to acquire lock of instance id after reconnect " +
                                "Lock id: ${Marshaller.marshall(it)}. " +
                                "Current registration node state: " +
                                ZkTreePrinter(curatorFramework).print(config.serviceRegistrationPath, true)
                        )
                    }
                    logger.error("ServiceInstanceIdRegistry client reconnected after connection issues " +
                            "and got its previous instance id=$instanceId that have before reconnection")
                }
                else -> {
                    logger.error("Failed to reconnect after client disconnect." +
                            "Lock of instance id expired at ${lockData.expirationDate} " +
                            "and have disconnect timeout=${config.disconnectTimeout}" +
                            "Lock id: ${Marshaller.marshall(lockIdentity)}. " +
                            "Current registration node state: " +
                            ZkTreePrinter(curatorFramework).print(config.serviceRegistrationPath, true)
                    )
                }
            }
        } catch (e: Exception) {
            logger.error("Error during reconnection of instance id registry", e)
        }
    }

    override fun close() {
        TransactionalClient.tryCommit(
                curatorFramework, config.countRegistrationAttempts,
                {
                    it.deletePath(instanceIdPath)
                    logger.info("ServiceInstanceIdRegistry closed successfully")
                },
                {
                    logger.error("Error during ServiceInstanceIdRegistry close: ", it)
                }
        )
    }
}
