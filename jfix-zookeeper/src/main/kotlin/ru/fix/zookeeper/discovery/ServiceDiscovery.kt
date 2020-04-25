package ru.fix.zookeeper.discovery

import org.apache.curator.framework.CuratorFramework
import org.apache.zookeeper.CreateMode

class ServiceDiscovery(
        private val curatorFramework: CuratorFramework,
        rootPath: String,
        applicationName: String
) {
    private val serviceRegistrationPath = "$rootPath/services"

    init {
        val check = curatorFramework.transactionOp().check().forPath(serviceRegistrationPath)
        val write = curatorFramework.transactionOp().create().forPath(serviceRegistrationPath)

        try {
            val currentVersion = System.currentTimeMillis().toString().toByteArray()
            curatorFramework.create()
                    .creatingParentsIfNeeded()
                    .forPath(serviceRegistrationPath, currentVersion)
        } catch (e: Exception) {

        }

        while (true) {

            val create = curatorFramework.transactionOp().create()
                    .withMode(CreateMode.EPHEMERAL)
                    .forPath("$serviceRegistrationPath/${nextId()}", applicationName.toByteArray())

            val newVersion = System.currentTimeMillis().toString().toByteArray()
            val updVersion = curatorFramework.transactionOp().setData()
                    .forPath(serviceRegistrationPath, newVersion)

            try {
                curatorFramework.transaction()
                        .forOperations(create, updVersion)
                break
            } catch (e: Exception) {
                println(e.message)
                continue
            }
        }

    }

    private fun nextId(): String {
        return (curatorFramework.children.forPath(serviceRegistrationPath)
                .map { it.toInt() }.max() ?: 0)
                .plus(1).toString()
    }
}