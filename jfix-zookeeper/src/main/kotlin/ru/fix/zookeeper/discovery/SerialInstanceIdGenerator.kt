package ru.fix.zookeeper.discovery

import org.apache.curator.framework.CuratorFramework

class SerialInstanceIdGenerator(
        private val curatorFramework: CuratorFramework,
        private val serviceRegistrationPath: String
) : InstanceIdGenerator {

    /**
     * For example, there are 3 instances:
     * └ services
     *    └ 1
     *    └ 2
     *    └ 5
     * @return  6 in this example, max + 1 value of already registered instances
     */
    override fun nextId(): String {
        return (curatorFramework.children
                .forPath(serviceRegistrationPath)
                .map { it.toInt() }
                .max() ?: 0)
                .plus(1).toString()
    }
}
