package ru.fix.zookeeper.server

import org.apache.curator.framework.CuratorFramework
import ru.fix.zookeeper.utils.Marshaller
import ru.fix.zookeeper.transactional.TransactionalClient

class ServerIdManager(
        private val curatorFramework: CuratorFramework,
        rootPath: String,
        private val applicationName: String
) : AutoCloseable {
    private val transactionalClient = TransactionalClient.createTransaction(curatorFramework)
    private val pathHelper = ServerIdPathHelper(rootPath)

    var serverId: Int = 0
        get() = if (field != 0) field else error("Server id didn't initialized yet")
        private set

    init {
        this
                .initPath()
                .writeAvailableServerId()
                .updateVersion()
                .transactionalClient
                .commit()
    }

    private fun initPath(): ServerIdManager {
        if (curatorFramework.checkExists().forPath(pathHelper.aliveServersPath) == null) {
            val marshalledVersion = Marshaller.marshall(System.currentTimeMillis())
            transactionalClient
                    .createPathWithParentsIfNeeded(pathHelper.aliveServersPath)
                    .setData(pathHelper.aliveServersPath, marshalledVersion.toByteArray())
                    .commitAndClearTransaction()
        }
        return this
    }

    private fun writeAvailableServerId(): ServerIdManager {
        val aliveServerIds = curatorFramework.children.forPath(pathHelper.aliveServersPath)
        val availableServerId = findAvailableServerId(aliveServerIds)
        serverId = availableServerId
        val generatedServerIdPath = "${pathHelper.aliveServersPath}/$availableServerId"
        val marshalledServerIdData = Marshaller.marshall(ServerIdNodeData(applicationName, System.currentTimeMillis()))

        transactionalClient
                .createPath(generatedServerIdPath)
                .setData(generatedServerIdPath, marshalledServerIdData.toByteArray())

        return this
    }

    private fun updateVersion(): ServerIdManager {
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