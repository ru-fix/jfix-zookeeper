package ru.fix.zookeeper.instance.registry

class InstanceIdValidator(private val maxInstancesCount: Int) {

    fun validate(instanceId: String) {
        assert(instanceId.toInt() <= maxInstancesCount) {
            "Generated instance id has value $instanceId," +
                    " but should be in range 1..${maxInstancesCount}"
        }
    }
}