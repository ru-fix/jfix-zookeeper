package ru.fix.zookeeper.instance.registry

import org.apache.curator.framework.CuratorFramework
import org.apache.curator.framework.state.ConnectionState
import org.apache.curator.framework.state.ConnectionStateListener
import org.apache.curator.utils.ZKPaths
import org.apache.zookeeper.KeeperException
import org.slf4j.LoggerFactory
import ru.fix.aggregating.profiler.Profiler
import ru.fix.dynamic.property.api.DynamicProperty
import ru.fix.zookeeper.lock.LockIdentity
import ru.fix.zookeeper.lock.PersistentExpiringLockManager
import ru.fix.zookeeper.utils.Marshaller
import ru.fix.zookeeper.utils.ZkTreePrinter
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
        private val serviceRegistrationPath: String,
        private val config: DynamicProperty<ServiceInstanceIdRegistryConfig>,
        profiler: Profiler
) : AutoCloseable {

    companion object {
        private val logger = LoggerFactory.getLogger(ServiceInstanceIdRegistry::class.java)
    }

    private val registeredInstanceIdLocks: MutableSet<LockIdentity> = ConcurrentSkipListSet()

    private val lockManager: PersistentExpiringLockManager = PersistentExpiringLockManager(
            curatorFramework,
            config.map { it.persistentExpiringLockManagerConfig },
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
        val registrationAttempts = config.get().countRegistrationAttempts
        for (i in 1..registrationAttempts) {
            val alreadyRegisteredInstanceIds = lockManager.getNonExpiredLockNodesByPath(serviceRegistrationPath)
            val preparedInstanceId = instanceIdGenerator.nextId(alreadyRegisteredInstanceIds)

            val instanceIdPath = ZKPaths.makePath(serviceRegistrationPath, preparedInstanceId)
            val instanceIdData = InstanceIdData(serviceName, Instant.now())
            val lockIdentity = LockIdentity(instanceIdPath, Marshaller.marshall(instanceIdData))

            val result = lockManager.tryAcquire(lockIdentity) { lock ->
                logger.warn("Failed to prolong lock=$lock for service=$serviceName")
            }
            when {
                result -> {
                    registeredInstanceIdLocks.add(lockIdentity)
                    logger.info("Instance of service=$serviceName started with id=$preparedInstanceId")
                    return preparedInstanceId
                }
                i == 1 ->
                    logger.debug("Failed to register service=$serviceName during first attempt. Retry...")
                i in 2 until registrationAttempts ->
                    logger.warn("Failed to register service=$serviceName at attempt=$i. Retry...")
                i == registrationAttempts -> {
                    logger.error("Failed to register service=$serviceName. " +
                            "Limit=${registrationAttempts} of instance id registration reached. " +
                            "Last try to get instance id was instanceId=$preparedInstanceId. " +
                            "Current registration node state: " +
                            ZkTreePrinter(curatorFramework).print(serviceRegistrationPath, true)
                    )
                }
            }
        }
        throw Exception("Failed to register service=$serviceName.")
    }

    private fun initServiceRegistrationPath() {
        try {
            curatorFramework.create()
                    .creatingParentsIfNeeded()
                    .forPath(serviceRegistrationPath, byteArrayOf())
        } catch (e: KeeperException.NodeExistsException) {
            logger.debug("Node with path=$serviceRegistrationPath is already initialized", e)
        } catch (e: Exception) {
            logger.error("Illegal state when create path: $serviceRegistrationPath", e)
            throw e
        }
    }

    private fun reconnect() {
        registeredInstanceIdLocks.forEach { lockIdentity ->
            val instanceId = ZKPaths.getNodeFromPath(lockIdentity.nodePath)
            if (lockManager.tryAcquire(lockIdentity) {
                        logger.error("Failed to prolong lock=$it after reconnect." +
                                "Current registration node state: " +
                                ZkTreePrinter(curatorFramework).print(serviceRegistrationPath, true)
                        )
                    }
            ) {
                logger.info("ServiceInstanceIdRegistry client reconnected after connection issues " +
                        "and got its previous instance id=$instanceId that have before reconnection")
            } else {
                logger.error("Can't get previous instance id=$instanceId after reconnection. " +
                        "Probably lock of this instance was expired and new service was registered with this instance id. " +
                        "Lock id: $lockIdentity. Current registration node state: " +
                        ZkTreePrinter(curatorFramework).print(serviceRegistrationPath, true)
                )
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
