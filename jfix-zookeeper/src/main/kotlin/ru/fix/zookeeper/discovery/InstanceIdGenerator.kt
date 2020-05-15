package ru.fix.zookeeper.discovery

interface InstanceIdGenerator {
    fun nextId(instanceIds: List<String>) : String
}