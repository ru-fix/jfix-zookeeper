package ru.fix.zookeeper.instance.registry

import org.apache.curator.framework.CuratorFramework
import org.apache.curator.utils.ZKPaths
import org.apache.zookeeper.KeeperException
import org.slf4j.LoggerFactory
import ru.fix.aggregating.profiler.Profiler
import ru.fix.dynamic.property.api.DynamicProperty
import ru.fix.stdlib.concurrency.threads.NamedExecutors
import ru.fix.stdlib.concurrency.threads.ReschedulableScheduler
import ru.fix.stdlib.concurrency.threads.Schedule
import ru.fix.zookeeper.lock.LockIdentity
import ru.fix.zookeeper.lock.PersistentExpiringDistributedLock
import ru.fix.zookeeper.lock.PersistentExpiringLockManager
import ru.fix.zookeeper.utils.Marshaller
import ru.fix.zookeeper.utils.ZkTreePrinter
import java.time.Instant
import java.util.concurrent.ConcurrentHashMap

/**
 * This class provides functionality of instance id registration.
 * When zookeeper client lost connection, instance will get previous instance after reconnection.
 * When this class instantiated, it create service registration path (if not created).
 * When called {@link #register(String)}, registry creates sub-node with serviceName for service instance registration.
 * It checks which instanceIds already taken and not expired for this service, and based on them generates new instance id.
 *
 * When registry had connection problems and because of this there expired lock was appeared,
 * another registry can acquire lock of instance id, that lock was expired.
 * Exists case, when first registry reestablished connection, but instance, that have expired lock, already not in self management.
 * First registry will be log errors until one of registries (first or second) will not be closed and released lock.
 * Scheduler of first or second registry (which not closed) will reacquire lost instance id's lock and stop logs error.
 *
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
    private val prolongFailedInstanceIdLocks: MutableSet<Pair<String, LockIdentity>> = ConcurrentHashMap.newKeySet()

    private var disconnectedInstancesRestorer: ReschedulableScheduler = NamedExecutors.newSingleThreadScheduler(
            "disconnected-instances-restorer", profiler
    )
    private val lockManager: PersistentExpiringLockManager = PersistentExpiringLockManager(
            curatorFramework,
            config.map { it.persistentExpiringLockManagerConfig },
            profiler
    )

    init {
        initServiceRegistrationPath()
        disconnectedInstancesRestorer.schedule(
                Schedule.withDelay(
                        config.map { it.retryRestoreInstanceIdInterval.toMillis() }
                )
        ) {
            prolongFailedInstanceIdLocks.forEach { (serviceName, lockIdentity) ->
                prolongationFallback(lockIdentity, serviceName)
            }
        }
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

            val result = lockManager.tryAcquire(lockIdentity) {
                prolongationFallback(it, serviceName)
                prolongFailedInstanceIdLocks.add(serviceName to lockIdentity)
            }
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

    /**
     * Releases lock of instanceId and removed this lock from registry's management
     * @param serviceName name of service should be unregistered
     * @param instanceId generated id of service should be unregistered
     * @throws Exception when failed to release lock of instance id
     */
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
     * Try to acquire instanceId's lock that failed prolongation.
     * If acquiring failed, then log error.
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
        } else {
            prolongFailedInstanceIdLocks.remove(serviceName to lockIdentity)
            logger.info("Connection restored for service=$serviceName with lock=$lockIdentity")
        }
    }

    private fun makeLockIdentity(serviceName: String, instanceId: String): LockIdentity {
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
                .filter { it.second == PersistentExpiringDistributedLock.LockNodeState.LIVE_LOCK }
                .map { it.first }
                .toList()
    }

    override fun close() {
        registeredInstanceIdLocks.forEach { lockIdentity ->
            try {
                lockManager.release(lockIdentity)
            } catch (e: Exception) {
                logger.error("Error during instance id registry close.", e)
            }
        }
        registeredInstanceIdLocks.clear()
        prolongFailedInstanceIdLocks.clear()

        disconnectedInstancesRestorer.close()
        lockManager.close()

        logger.info("Instance id registry closed successfully.")
    }
}
