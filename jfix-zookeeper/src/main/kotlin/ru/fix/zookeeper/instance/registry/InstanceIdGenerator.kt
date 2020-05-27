package ru.fix.zookeeper.instance.registry

interface InstanceIdGenerator {
    fun nextId(instanceIds: List<String>) : String
}