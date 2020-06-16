package ru.fix.zookeeper.instance.registry

import org.apache.curator.framework.CuratorFramework
import org.apache.curator.utils.ZKPaths
import org.apache.zookeeper.KeeperException
import org.slf4j.LoggerFactory
import ru.fix.aggregating.profiler.Profiler
import ru.fix.dynamic.property.api.DynamicProperty
import ru.fix.zookeeper.lock.LockIdentity
import ru.fix.zookeeper.lock.PersistentExpiringDistributedLock
import ru.fix.zookeeper.lock.PersistentExpiringLockManager
import ru.fix.zookeeper.utils.Marshaller
import ru.fix.zookeeper.utils.ZkTreePrinter
import java.time.Instant
import java.util.concurrent.ConcurrentHashMap

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

    private val registeredInstanceIdLocks: MutableSet<LockIdentity> = ConcurrentHashMap.newKeySet()

    private val lockManager: PersistentExpiringLockManager = PersistentExpiringLockManager(
            curatorFramework,
            config.map { it.persistentExpiringLockManagerConfig },
            profiler
    )

    init {
        initServiceRegistrationPath()
    }

    /**
     * @param serviceName name of service should be registered
     * @return generated instance id for registered service
     */
    fun register(serviceName: String): String {
        initServicePath(serviceName)
        val registrationAttempts = config.get().countRegistrationAttempts
        for (i in 1..registrationAttempts) {
            val alreadyRegisteredInstanceIds = getNonExpiredLockNodesInPathForService(serviceName)
            val preparedInstanceId = instanceIdGenerator.nextId(alreadyRegisteredInstanceIds)
            val lockIdentity = makeLockIdentity(serviceName, preparedInstanceId)

            val result = lockManager.tryAcquire(lockIdentity) { prolongationFallback(it, serviceName) }
            when {
                result -> {
                    registeredInstanceIdLocks.add(lockIdentity)
                    logger.info("Instance of service=$serviceName started with id=$preparedInstanceId")
                    return preparedInstanceId
                }
                i == registrationAttempts -> {
                    logger.error("Failed to register service=$serviceName. " +
                            "Limit=${registrationAttempts} of instance id registration reached. " +
                            "Last try to get instance id was instanceId=$preparedInstanceId. " +
                            "Current registration node state: " +
                            ZkTreePrinter(curatorFramework).print(serviceRegistrationPath, true)
                    )
                }
                i == 1 ->
                    logger.debug("Failed to register service=$serviceName during first attempt. Retry...")
                i in 2 until registrationAttempts ->
                    logger.warn("Failed to register service=$serviceName at attempt=$i. Retry...")
            }
        }
        throw Exception("Failed to register service=$serviceName. " +
                "Limit=${registrationAttempts} of instance id registration reached. " +
                "Current registration node state: " +
                ZkTreePrinter(curatorFramework).print(serviceRegistrationPath, true))
    }

    fun unregister(serviceName: String, instanceId: String) {
        try {
            val lockIdentity = makeLockIdentity(serviceName, instanceId)
            lockManager.release(lockIdentity)
            registeredInstanceIdLocks.remove(lockIdentity)
        } catch (e: Exception) {
            logger.error("Failed to unregister service=$serviceName with instanceId=$instanceId", e)
            throw  e
        }
    }

    /**
     * Try to acquire instanceId's lock that failed prolongation.  If acquiring failed, then log error.
     */
    private fun prolongationFallback(lockIdentity: LockIdentity, serviceName: String) {
        val instanceId = ZKPaths.getNodeFromPath(lockIdentity.nodePath)
        val acquired = lockManager.tryAcquire(lockIdentity) {
            logger.error("Failed to prolong lock=$it after for service=$serviceName. " +
                    "Current registration node state: " +
                    ZkTreePrinter(curatorFramework).print(serviceRegistrationPath, true))
        }

        if (!acquired) {
            logger.error("Can't acquire lock of instance id=$instanceId after lock prolongation fail. " +
                    "Probably lock of this instance was expired and new service was registered with this instance id. " +
                    "Lock id: $lockIdentity. Current registration node state: " +
                    ZkTreePrinter(curatorFramework).print(serviceRegistrationPath, true)
            )
        }
    }

    private fun makeLockIdentity(serviceName: String, instanceId: String) : LockIdentity {
        val instanceIdPath = ZKPaths.makePath(serviceRegistrationPath, serviceName, instanceId)
        val instanceIdData = InstanceIdData(Instant.now())
        return LockIdentity(instanceIdPath, Marshaller.marshall(instanceIdData))
    }

    private fun initServiceRegistrationPath() {
        initServicePath("")
    }

    private fun initServicePath(serviceName: String) {
        val path = ZKPaths.makePath(serviceRegistrationPath, serviceName)
        try {
            curatorFramework.create()
                    .creatingParentsIfNeeded()
                    .forPath(path, byteArrayOf())
        } catch (e: KeeperException.NodeExistsException) {
            logger.debug("Node with path=$path is already initialized", e)
        } catch (e: Exception) {
            logger.error("Illegal state when create path: $path", e)
            throw e
        }
    }

    private fun getNonExpiredLockNodesInPathForService(serviceName: String): List<String> {
        return curatorFramework.children
                .forPath(ZKPaths.makePath(serviceRegistrationPath, serviceName))
                .asSequence()
                .map {
                    it to PersistentExpiringDistributedLock.readLockNodeState(
                            curatorFramework, ZKPaths.makePath(serviceRegistrationPath, serviceName, it)
                    )
                }
                .filter { it.second == PersistentExpiringDistributedLock.LockNodeState.NOT_EXPIRED_LOCK }
                .map { it.first }
                .toList()
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
            logger.error("Error during instance id registry close.", e)
        }
    }
}
