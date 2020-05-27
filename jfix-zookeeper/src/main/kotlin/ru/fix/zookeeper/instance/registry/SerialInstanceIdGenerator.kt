package ru.fix.zookeeper.instance.registry

class SerialInstanceIdGenerator(maxCountOfInstanceIds: Int) : InstanceIdGenerator {
    private val instanceIdValidator = InstanceIdValidator(maxCountOfInstanceIds)

    /**
     * For example, there are 3 instances:
     * └ services
     *    └ 1
     *    └ 2
     *    └ 5
     * @return  6 in this example, max + 1 value of already registered instances
     */
    override fun nextId(instanceIds: List<String>): String {
        return (instanceIds
                .asSequence()
                .map { it.toInt() }
                .max() ?: 0)
                .plus(1).toString()
                .also {
                    instanceIdValidator.validate(it)
                }
    }
}
