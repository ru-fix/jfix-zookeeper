package ru.fix.zookeeper.discovery

import org.awaitility.Awaitility.await
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.netcrusher.core.reactor.NioReactor
import org.netcrusher.tcp.TcpCrusher
import org.netcrusher.tcp.TcpCrusherBuilder
import ru.fix.stdlib.socket.SocketChecker
import java.time.Duration

internal class ServiceInstanceIdRegistryConnectionProblemsTest : AbstractServiceInstanceIdRegistryTest() {
    private val reactor: NioReactor = NioReactor()

    @Test
    fun `client of service instance id registry lost connection and should`() {
        val disconnectTimeout = Duration.ofSeconds(4)
        val proxyPort = SocketChecker.getAvailableRandomPort()
        val crusher = tcpCrusher(proxyPort)

        val proxyClient = testingServer.createClient("localhost:${proxyPort}", 1000, 1000, 1000)

        val instances = listOf(
                createInstanceIdRegistry("abs-rate", client = proxyClient, disconnectTimeout = disconnectTimeout),
                createInstanceIdRegistry("abs-rate"),
                createInstanceIdRegistry("drugkeeper")
        )
        println(zkTree())
        assertInstances(mapOf("abs-rate" to setOf("1", "2"), "drugkeeper" to setOf("3")))
        assertInstanceIdMapping(setOf(instances[0] to "1", instances[1] to "2", instances[2] to "3"))

        crusher.freeze()
        await()
                .timeout(Duration.ofSeconds(2))
                .until { !proxyClient.zookeeperClient.isConnected }
        println(zkTree())

        Thread.sleep(disconnectTimeout.toMillis() / 2)
        assertInstanceIdLocksExpiration(setOf("1" to true, "2" to true, "3" to true), disconnectTimeout)

        crusher.unfreeze()
        Thread.sleep(3000)
        await()
                .timeout(Duration.ofSeconds(2))
                .until { proxyClient.zookeeperClient.isConnected }

        println(zkTree())
        assertInstances(mapOf("abs-rate" to setOf("1", "2"), "drugkeeper" to setOf("3")))
        assertTrue(false)
    }

    @Test
    fun `client disconnected, disconnect timeout not reached, register new service with new instance id`() {
        val disconnectTimeout = Duration.ofSeconds(4)
        val proxyPort = SocketChecker.getAvailableRandomPort()
        val crusher = tcpCrusher(proxyPort)

        val proxyClient = testingServer.createClient("localhost:${proxyPort}", 1000, 1000, 1000)

        val instances = mutableListOf(
                createInstanceIdRegistry("abs-rate", client = proxyClient, disconnectTimeout = disconnectTimeout),
                createInstanceIdRegistry("abs-rate"),
                createInstanceIdRegistry("drugkeeper")
        )
        println(zkTree())
        assertInstances(mapOf("abs-rate" to setOf("1", "2"), "drugkeeper" to setOf("3")))
        assertInstanceIdMapping(setOf(instances[0] to "1", instances[1] to "2", instances[2] to "3"))

        crusher.freeze()
        await()
                .timeout(Duration.ofSeconds(2))
                .until { !proxyClient.zookeeperClient.isConnected }

        println(zkTree())
        Thread.sleep(disconnectTimeout.toMillis() / 2)
        println(zkTree())
        Assertions.assertFalse(isInstanceIdLockExpired("1", disconnectTimeout))

        instances.add(createInstanceIdRegistry("abs-rate"))
        await()
                .timeout(Duration.ofSeconds(1))
                .until { isAliveInstance(instances[3]) }
        println(zkTree())

        assertInstanceIdLocksExpiration(setOf("1" to true, "2" to true, "3" to true, "4" to true), disconnectTimeout)

        crusher.unfreeze()
        Thread.sleep(disconnectTimeout.toMillis() / 2)
        println(zkTree())

        assertInstanceIdLocksExpiration(setOf("1" to true, "2" to true, "3" to true, "4" to true), disconnectTimeout)



        assertTrue(false)

    }

    @Test
    fun `client disconnected, disconnect timeout reached, register new service with instance id that lock expired`() {
        val disconnectTimeout = Duration.ofSeconds(4)
        val proxyPort = SocketChecker.getAvailableRandomPort()
        val crusher = tcpCrusher(proxyPort)

        val proxyClient = testingServer.createClient("localhost:${proxyPort}", 1000, 1000, 1000)

        val instances = mutableListOf(
                createInstanceIdRegistry("abs-rate", client = proxyClient, disconnectTimeout = disconnectTimeout),
                createInstanceIdRegistry("abs-rate"),
                createInstanceIdRegistry("drugkeeper")
        )
        println(zkTree())
        assertInstances(mapOf("abs-rate" to setOf("1", "2"), "drugkeeper" to setOf("3")))
        assertInstanceIdMapping(setOf(instances[0] to "1", instances[1] to "2", instances[2] to "3"))

        crusher.freeze()
        await()
                .timeout(Duration.ofSeconds(2))
                .until { !proxyClient.zookeeperClient.isConnected }
        println(zkTree())

        Thread.sleep(disconnectTimeout.toMillis() * 2)
        crusher.unfreeze()
        println(zkTree())
        assertTrue(isInstanceIdLockExpired("1", disconnectTimeout))

        val client = testingServer.createClient()
        instances.add(createInstanceIdRegistry("abs-rate", client = client))
        await()
                .timeout(Duration.ofSeconds(1))
                .until { client.zookeeperClient.isConnected }
        println(zkTree())

        assertInstanceIdLocksExpiration(setOf("1" to true, "2" to true, "3" to true), disconnectTimeout)


        assertTrue(false)
    }

    @Test
    fun `prolongation of instance id's lock works fine after reconnect`() {
        val disconnectTimeout = Duration.ofSeconds(4)
        val proxyPort = SocketChecker.getAvailableRandomPort()
        val crusher = tcpCrusher(proxyPort)

        val proxyClient = testingServer.createClient("localhost:${proxyPort}", 1000, 1000, 1000)

        createInstanceIdRegistry("abs-rate", client = proxyClient, disconnectTimeout = disconnectTimeout)

        println(zkTree())
        assertInstances(mapOf("abs-rate" to setOf("1")))

        crusher.reopen()
        println(zkTree())

        Thread.sleep(disconnectTimeout.toMillis())
        println(zkTree())
        assertInstances(mapOf("abs-rate" to setOf("1")))
        assertFalse(isInstanceIdLockExpired("1", disconnectTimeout))
    }

    private fun isAliveInstance(instance: ServiceInstanceIdRegistry): Boolean {
        return testingServer.client.checkExists().forPath(instance.instanceIdPath) != null
    }

    private fun tcpCrusher(proxyPort: Int): TcpCrusher =
            TcpCrusherBuilder.builder()
                    .withReactor(reactor)
                    .withBindAddress("localhost", proxyPort)
                    .withConnectAddress("localhost", testingServer.port)
                    .buildAndOpen()

}
