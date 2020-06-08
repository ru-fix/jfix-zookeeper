package ru.fix.zookeeper.instance.registry

import org.apache.curator.framework.CuratorFramework
import org.apache.curator.framework.state.ConnectionState
import org.apache.curator.framework.state.ConnectionStateListener
import org.apache.curator.utils.ZKPaths
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
import java.util.concurrent.ConcurrentSkipListSet

/**
 * This class provides functionality of instance id registration.
 * When this class instantiated, it create service registration path (if not created).
 * When zookeeper client lost connection, instance will get previous instance after reconnection
 * if  disconnect timeout not expired.
 * Service registration guarantees uniqueness of instance id in case of parallel startup on different JVMs.
 */
class ServiceInstanceIdRegistry(
        private val curatorFramework: CuratorFramework,
        private val instanceIdGenerator: InstanceIdGenerator,
        private val config: ServiceInstanceIdRegistryConfig,
        profiler: Profiler
) : AutoCloseable {

    companion object {
        private val logger = LoggerFactory.getLogger(ServiceInstanceIdRegistry::class.java)
    }

    private val registeredInstanceIdLocks: MutableSet<LockIdentity> = ConcurrentSkipListSet()

    private val lockManager: PersistentExpiringLockManager = PersistentExpiringLockManager(
            curatorFramework,
            DynamicProperty.of(
                    PersistentExpiringLockManagerConfig(
                            lockAcquirePeriod = config.disconnectTimeout.dividedBy(2),
                            expirationPeriod = config.disconnectTimeout.dividedBy(3),
                            lockCheckAndProlongInterval = config.disconnectTimeout.dividedBy(4),
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
    }

    /**
     * @param serviceName name of service should be registered
     * @return generated instance id for registered service
     */
    fun register(serviceName: String): String {
        for (i in 1..config.countRegistrationAttempts) {
            val alreadyRegisteredInstanceIds = curatorFramework.children.forPath(config.serviceRegistrationPath)
            val preparedInstanceId = instanceIdGenerator.nextId(alreadyRegisteredInstanceIds)

            val instanceIdPath = ZKPaths.makePath(config.serviceRegistrationPath, preparedInstanceId)
            val instanceIdData = InstanceIdData(serviceName, Instant.now())
            val lockIdentity = LockIdentity(instanceIdPath, Marshaller.marshall(instanceIdData))

            val result = lockManager.tryAcquire(lockIdentity) { lock ->
                logger.warn("Failed to prolong lock=$lock for service=$serviceName")
            }
            if (result) {
                registeredInstanceIdLocks.add(lockIdentity)
                logger.info("Instance of service=$serviceName started with id=$preparedInstanceId")
                return preparedInstanceId
            } else if (i == config.countRegistrationAttempts) {
                logger.error("Failed to register service=$serviceName. " +
                        "Limit=${config.countRegistrationAttempts} of instance id registration reached. " +
                        "Last try to get instance id was instanceId=$preparedInstanceId. " +
                        "Current registration node state: " +
                        ZkTreePrinter(curatorFramework).print(config.serviceRegistrationPath, true)
                )
            }
        }
        throw Exception("Failed to register service=$serviceName.")
    }

    private fun initServiceRegistrationPath() {
        try {
            curatorFramework.create()
                    .creatingParentsIfNeeded()
                    .forPath(config.serviceRegistrationPath, byteArrayOf())
        } catch (e: KeeperException.NodeExistsException) {
            logger.debug("Node with path=${config.serviceRegistrationPath} is already initialized", e)
        } catch (e: Exception) {
            logger.error("Illegal state when create path: ${config.serviceRegistrationPath}", e)
            throw e
        }
    }

    private fun reconnect() {
        registeredInstanceIdLocks.forEach { lockIdentity ->
            val instanceId = ZKPaths.getNodeFromPath(lockIdentity.nodePath)
            try {
                logger.info("Client reconnected after connection issues")

                val instanceIdNodeData = curatorFramework.data.forPath(lockIdentity.nodePath).toString(Charsets.UTF_8)
                val lockData = Marshaller.unmarshall(instanceIdNodeData, LockData::class.java)
                when (lockData.metadata) {
                    null -> {
                        logger.warn("ServiceInstanceIdRegistry reconnected, " +
                                "but lock for its instance id=$instanceId not found.")
                    }
                    else -> {
                        if (lockData.expirationTimestamp.plus(config.disconnectTimeout).isBefore(Instant.now())) {
                            logger.error("Failed to reconnect with same instance id. Disconnect timeout expired!")
                            return@forEach
                        }

                        if (lockManager.tryAcquire(lockIdentity) { logger.warn("Failed to prolong lock=$it}") }) {
                            logger.info("ServiceInstanceIdRegistry client reconnected after connection issues " +
                                    "and got its previous instance id=$instanceId that have before reconnection")
                        } else {
                            logger.error("Can't get previous instance id=$instanceId after reconnection." +
                                    "Lock id: ${Marshaller.marshall(lockIdentity)}. " +
                                    "Current registration node state: " +
                                    ZkTreePrinter(curatorFramework).print(config.serviceRegistrationPath, true)
                            )
                        }
                    }
                }
            } catch (e: Exception) {
                logger.error("Error during reconnection of instance id registry", e)
            }
        }
    }

    override fun close() {
        try {
            registeredInstanceIdLocks.forEach { lockIdentity ->
                lockManager.release(lockIdentity)
            }
            registeredInstanceIdLocks.clear()
            lockManager.close()
            logger.info("Instance id registry with closed successfully.")
        } catch (e: Exception) {
            logger.error("Error during instance id registry close.")
        }
    }
}
