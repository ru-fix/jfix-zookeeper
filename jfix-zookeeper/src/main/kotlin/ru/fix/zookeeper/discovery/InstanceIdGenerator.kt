package ru.fix.zookeeper.discovery

interface InstanceIdGenerator {
    fun nextId() : String
}