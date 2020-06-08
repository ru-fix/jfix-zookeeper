package ru.fix.zookeeper.instance.registry

import com.fasterxml.jackson.annotation.JsonFormat
import java.time.Instant

data class InstanceIdData(
        val serviceName: String,
        @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd'T'HH:mm:ss.SSSXXX", timezone = "UTC")
        val registered: Instant
)