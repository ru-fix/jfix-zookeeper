package ru.fix.zookeeper.discovery

import java.time.OffsetDateTime

data class InstanceIdData(
        val applicationName: String,
        val timestamp: OffsetDateTime
)