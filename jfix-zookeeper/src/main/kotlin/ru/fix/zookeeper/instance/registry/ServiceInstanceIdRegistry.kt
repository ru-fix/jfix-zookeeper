package ru.fix.zookeeper.instance.registry

import com.fasterxml.jackson.core.type.TypeReference
import org.apache.curator.framework.CuratorFramework
import org.apache.curator.framework.state.ConnectionState
import org.apache.curator.framework.state.ConnectionStateListener
import org.apache.zookeeper.KeeperException
import org.slf4j.LoggerFactory
import ru.fix.aggregating.profiler.Profiler
import ru.fix.dynamic.property.api.DynamicProperty
import ru.fix.zookeeper.lock.LockData
import ru.fix.zookeeper.lock.LockIdentity
import ru.fix.zookeeper.lock.PersistentExpiringLockManager
import ru.fix.zookeeper.lock.PersistentExpiringLockManagerConfig
import ru.fix.zookeeper.utils.Marshaller
import ru.fix.zookeeper.utils.ZkTreePrinter
import java.time.Duration
import java.time.Instant

/**
 * This class provides functionality of instance id management.
 * When this class instantiated, it generates new instance id for this zookeeper client and registers this id in znode.
 * When zookeeper client lost connection, instance will get previous instance after reconnection.
 * If new client connected, if disconnect timeout expired and client's config.host and config.port already contains in znodes,
 * it is seen as instance restart, and in this case instance get instance id, already contained in zk.
 * Service registration guarantees uniqueness of instance id in case of parallel startup on different JVMs.
 */
class ServiceInstanceIdRegistry(
        private val curatorFramework: CuratorFramework,
        private val instanceIdGenerator: InstanceIdGenerator,
        private val config: ServiceInstanceIdRegistryConfig,
        profiler: Profiler
) : AutoCloseable {
    lateinit var lockIdentity: LockIdentity
        private set
    lateinit var instanceIdPath: String
        private set
    lateinit var instanceId: String
        private set

    companion object {
        private val logger = LoggerFactory.getLogger(ServiceInstanceIdRegistry::class.java)
    }

    private val lockManager: PersistentExpiringLockManager = PersistentExpiringLockManager(
            curatorFramework,
            DynamicProperty.of(
                    PersistentExpiringLockManagerConfig(
                            reservationPeriod = config.disconnectTimeout.dividedBy(2),
                            expirationPeriod = config.disconnectTimeout.dividedBy(3),
                            lockProlongationInterval = config.disconnectTimeout.dividedBy(4),
                            acquiringTimeout = Duration.ofSeconds(1)
                    )
            ),
            profiler
    )

    init {
        curatorFramework
                .connectionStateListenable
                .addListener(
                        ConnectionStateListener { _, newState ->
                            when (newState) {
                                ConnectionState.RECONNECTED -> reconnect()
                                else -> Unit
                            }
                        })
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
     * If this is client with host and port, that already contains in instance's znodes, and timeout of this lock expired,
     * this situation seen as reconnect of instance, and previous instance id will be got.
     * Generate instance id, this id should be in range 1..maxInstancesCount, assertion error will be thrown otherwise.
     * If unsuccessful, then log error
     */
    private fun initInstanceId() {
        for (i in 1..config.countRegistrationAttempts) {
            val alreadyRegisteredInstanceIds = curatorFramework.children.forPath(config.serviceRegistrationPath)
            val preparedInstanceId = resolvePreviousConnection(alreadyRegisteredInstanceIds)
                    ?: instanceIdGenerator.nextId(alreadyRegisteredInstanceIds)

            val instanceIdPath = "${config.serviceRegistrationPath}/$preparedInstanceId"
            val instanceIdData = InstanceIdData(config.serviceName, Instant.now(), host = config.host, port = config.port)
            val lockIdentity = LockIdentity(preparedInstanceId, instanceIdPath, Marshaller.marshall(instanceIdData))

            val result = lockManager.tryAcquire(lockIdentity) { lock ->
                logger.debug("Failed to initialize service id registry and generate instance id. " +
                        "Lock id: ${Marshaller.marshall(lock)}. " +
                        "Current registration node state: " +
                        ZkTreePrinter(curatorFramework).print(config.serviceRegistrationPath, true)
                )
            }
            if (result) {
                this.instanceIdPath = instanceIdPath
                this.instanceId = preparedInstanceId
                this.lockIdentity = lockIdentity

                logger.info("Instance started with id=$instanceId")
                break
            } else if (i == config.countRegistrationAttempts) {
                logger.error("Failed to initialize instance id registry and get instance id. " +
                        "Limit=${config.countRegistrationAttempts} of instance id registration reached. " +
                        "Last try to get instance id was instanceId=$instanceId. " +
                        "Current registration node state: " +
                        ZkTreePrinter(curatorFramework).print(config.serviceRegistrationPath, true)
                )
            }
        }
    }

    /**
     * @return previous instance id if host and port of client same with data in zk node
     * and lock of this instance id expired, and null otherwise
     */
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
                logger.info("Instance started with host=${config.host} and port=${config.port}, " +
                        "disconnection timeout=${config.disconnectTimeout} not reached. Instance got id=$it")
                return it
            }
        }
        return null
    }

    private fun reconnect() {
        try {
            logger.info("Client reconnected after connection issues")
            val lockData = Marshaller.unmarshall(
                    curatorFramework.data.forPath(instanceIdPath).toString(Charsets.UTF_8),
                    object : TypeReference<LockData>() {}
            )
            when (lockData.data) {
                null -> {
                    logger.warn("ServiceInstanceIdRegistry reconnected, " +
                            "but lock for its instance id=$instanceId not found.")
                }
                else -> {
                    lockManager.release(lockIdentity)
                    if (lockManager.tryAcquire(lockIdentity) {
                                logger.error("Failed to acquire lock of instance id after reconnect " +
                                        "Lock id: ${Marshaller.marshall(it)}. " +
                                        "Current registration node state: " +
                                        ZkTreePrinter(curatorFramework).print(config.serviceRegistrationPath, true)
                                )
                            }) {
                        logger.info("ServiceInstanceIdRegistry client reconnected after connection issues " +
                                "and got its previous instance id=$instanceId that have before reconnection")
                    } else {
                        logger.error("Can't get previous instance id=$instanceId after reconnection")
                    }
                }
            }
        } catch (e: Exception) {
            logger.error("Error during reconnection of instance id registry", e)
        }
    }

    override fun close() {
        try {
            lockManager.release(lockIdentity)
            lockManager.close()
            logger.info("Instance id registry with instanceId=$instanceId closed successfully")
        } catch (e: Exception) {
            logger.error("Error during instance id registry with instanceId=$instanceId close ", e)
        }
    }
}
