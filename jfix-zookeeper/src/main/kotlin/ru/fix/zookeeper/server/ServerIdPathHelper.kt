package ru.fix.zookeeper.server

internal class ServerIdPathHelper(private val rootPath: String) {
    val aliveServersPath = "$rootPath/alive-servers"
    val updateVersionPath = "$aliveServersPath/update-version"

    fun serverIdPath(serverId: String) = "$aliveServersPath/serverId"
}
