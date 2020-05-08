package ru.fix.zookeeper.discovery

import org.apache.curator.framework.CuratorFramework

class MinFreeInstanceIdGenerator(
        private val curatorFramework: CuratorFramework,
        private val serviceRegistrationPath: String
) : InstanceIdGenerator {

    /**
     * For example, there are 3 instances:
     * └ services
     *    └ 1
     *    └ 2
     *    └ 5
     * @return  3 in this example, minimal free instance id
     */
    override fun nextId(): String {
        return curatorFramework.children
                .forPath(serviceRegistrationPath)
                .mapTo(ArrayList()) { it.toInt() }
                .apply {
                    sort()
                }.fold(1) { acc, id ->
                    if (id != acc) {
                        return acc.toString()
                    }

                    acc + 1
                }.toString()
    }
}
