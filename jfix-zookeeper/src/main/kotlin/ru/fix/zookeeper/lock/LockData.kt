package ru.fix.zookeeper.lock

import org.slf4j.LoggerFactory
import java.net.InetAddress
import java.net.UnknownHostException

data class LockData(
        val uuid: String,
        val expirationDate: Long,
        val serverId: String
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
            logger.trace("Node[serverId={}, uuid={}] already removed on release.", serverId, uuid, e)
        }
    }

    constructor(
            ip: String,
            hostname: String,
            serverId: String,
            uuid: String,
            expirationDate: Long
    ) : this(uuid, expirationDate, serverId) {
        this.ip = ip
        this.hostname = hostname
    }
}
