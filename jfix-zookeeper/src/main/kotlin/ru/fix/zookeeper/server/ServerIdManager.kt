package ru.fix.zookeeper.server

import org.apache.curator.framework.CuratorFramework
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import ru.fix.aggregating.profiler.Profiler
import ru.fix.zookeeper.lock.LockIdentity
import ru.fix.zookeeper.lock.LockManagerImpl
import ru.fix.zookeeper.utils.Marshaller
import ru.fix.zookeeper.transactional.TransactionalClient
import java.util.*
import java.util.concurrent.ConcurrentLinkedQueue

class ServerIdManager(
        private val curatorFramework: CuratorFramework,
        rootPath: String,
        private val applicationName: String,
        private val profiler: Profiler
) : AutoCloseable {
    private val pathHelper = ServerIdPathHelper(rootPath)
    private val lockManagerImpl = LockManagerImpl(curatorFramework, UUID.randomUUID().toString(), profiler)

    companion object {
        private val logger: Logger = LoggerFactory.getLogger(ServerIdManager::class.java)
    }

    var serverId: Int = 0
        get() = if (field != 0) field else error("Server id didn't initialized yet")
        private set

    init {
        TransactionalClient.tryCommit(curatorFramework, 10) {
            this
                    .initPath(it)
                    .writeAvailableServerId(it)
                    .updateVersion(it)
            it.commit()
        }
    }

    private fun initPath(transactionalClient: TransactionalClient): ServerIdManager {
        if (curatorFramework.checkExists().forPath(pathHelper.aliveServersPath) == null) {
            val marshalledVersion = Marshaller.marshall(System.currentTimeMillis())
            transactionalClient
                    .createPathWithParentsIfNeeded(pathHelper.aliveServersPath)
                    .setData(pathHelper.aliveServersPath, marshalledVersion.toByteArray())
                    .commitAndClearTransaction()
        }
        return this
    }

    private fun writeAvailableServerId(transactionalClient: TransactionalClient): ServerIdManager {
        val aliveServerIds = curatorFramework.children.forPath(pathHelper.aliveServersPath)
        val availableServerId = findAvailableServerId(aliveServerIds)
        val generatedServerIdPath = "${pathHelper.aliveServersPath}/$availableServerId"
        val marshalledServerIdData = Marshaller.marshall(ServerIdNodeData(applicationName, System.currentTimeMillis()))

        val c = curatorFramework.transactionOp().check().forPath("")
        curatorFramework.transactionOp().create()
        ConcurrentLinkedQueue<String>().poll()
        
        transactionalClient
                .createPath(generatedServerIdPath)
                .setData(generatedServerIdPath, marshalledServerIdData.toByteArray())

        return this
    }

    private fun updateVersion(transactionalClient: TransactionalClient): ServerIdManager {
        val marshalledVersion = Marshaller.marshall(System.currentTimeMillis())
        transactionalClient
                .setData(pathHelper.aliveServersPath, marshalledVersion.toByteArray())
        return this
    }

    private fun findAvailableServerId(aliveServers: List<String>): Int {
        return (aliveServers
                .map { it.toInt() }
                .max() ?: 0) + 1
    }

    override fun close() {
        curatorFramework.close()
    }
}