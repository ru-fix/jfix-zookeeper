package ru.fix.zookeeper.instance.registry

import com.fasterxml.jackson.annotation.JsonFormat
import java.time.Instant

data class InstanceIdData(
        val applicationName: String,
        @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd'T'HH:mm:ss.SSSXXX", timezone = "UTC")
        val created: Instant,
        @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd'T'HH:mm:ss.SSSXXX", timezone = "UTC")
        val disconnectInstant: Instant? = null,
        val host: String,
        val port: Int
)