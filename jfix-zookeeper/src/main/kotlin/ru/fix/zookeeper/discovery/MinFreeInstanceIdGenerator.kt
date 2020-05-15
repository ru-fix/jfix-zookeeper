package ru.fix.zookeeper.discovery

class MinFreeInstanceIdGenerator(
        private val maxCountOfInstanceIds: Int
) : InstanceIdGenerator {

    /**
     * For example, there are 3 instances:
     * └ services
     *    └ 1
     *    └ 2
     *    └ 5
     * @return  3 in this example, minimal free instance id
     */
    override fun nextId(instanceIds: List<String>): String {
        val newInstanceId = instanceIds
                .mapTo(ArrayList()) { it.toInt() }
                .apply {
                    sort()
                }.fold(1) { acc, id ->
                    if (id != acc) {
                        return acc.toString()
                    }
                    acc + 1
                }.toString()
        assert(newInstanceId.toInt() <= maxCountOfInstanceIds) {
            "Generated instance id has value $newInstanceId," +
                    " but should be in range 1..${maxCountOfInstanceIds}"
        }
        return newInstanceId
    }
}
