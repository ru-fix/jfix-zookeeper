package ru.fix.zookeeper.discovery

import org.apache.curator.framework.CuratorFramework

class SerialInstanceIdGenerator(
        private val curatorFramework: CuratorFramework,
        private val serviceRegistrationPath: String
) : InstanceIdGenerator {

    override fun nextId(): String {
        return (curatorFramework.children
                .forPath(serviceRegistrationPath)
                .map { it.toInt() }
                .max() ?: 0)
                .plus(1).toString()
    }
}
