package ru.fix.zookeeper.instance.registry

class MinFreeInstanceIdGenerator(maxCountOfInstanceIds: Int) : InstanceIdGenerator {
    private val instanceIdValidator = InstanceIdValidator(maxCountOfInstanceIds)

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
        return newInstanceId.also {
            instanceIdValidator.validate(it)
        }
    }
}
