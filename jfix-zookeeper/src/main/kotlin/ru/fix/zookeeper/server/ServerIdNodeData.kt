package ru.fix.zookeeper.server

data class ServerIdNodeData(
        val app: String,
        val acquireTimestamp: Long
)