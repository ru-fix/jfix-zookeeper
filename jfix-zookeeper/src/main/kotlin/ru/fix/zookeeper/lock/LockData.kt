package ru.fix.zookeeper.lock

import com.fasterxml.jackson.annotation.JsonFormat
import org.slf4j.LoggerFactory
import java.net.InetAddress
import java.net.UnknownHostException
import java.time.Instant

data class LockData(
        val version: String,
        @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd'T'HH:mm:ss.SSSXXX", timezone = "UTC")
        val expirationDate: Instant,
        val data: String? = null
) {
    var ip: String = "Unknown ip"
        private set
    var hostname: String = "Unknown hostname"
        private set

    companion object {
        private val logger = LoggerFactory.getLogger(LockData::class.java)
    }

    init {
        try {
            val inetAddress = InetAddress.getLocalHost()
            ip = inetAddress.hostAddress
            hostname = inetAddress.hostName
        } catch (e: UnknownHostException) {
            logger.debug("Host is invalid when lock data instantiated", e)
        }
    }

    constructor(
            ip: String,
            hostname: String,
            uuid: String,
            expirationDate: Instant
    ) : this(uuid, expirationDate) {
        this.ip = ip
        this.hostname = hostname
    }
}
