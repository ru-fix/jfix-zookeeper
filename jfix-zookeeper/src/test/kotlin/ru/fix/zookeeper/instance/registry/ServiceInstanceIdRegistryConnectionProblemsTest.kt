package ru.fix.zookeeper.instance.registry

import org.apache.logging.log4j.kotlin.logger
import org.awaitility.Awaitility.await
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.netcrusher.core.reactor.NioReactor
import org.netcrusher.tcp.TcpCrusher
import org.netcrusher.tcp.TcpCrusherBuilder
import ru.fix.stdlib.socket.SocketChecker
import java.time.Duration

internal class ServiceInstanceIdRegistryConnectionProblemsTest : AbstractServiceInstanceIdRegistryTest() {
    private val logger = logger()
    private val reactor: NioReactor = NioReactor()

    @Test
    fun `client lost connection and reconnect with same instance id when disconnect timeout didn't reached`() {
        val disconnectTimeout = Duration.ofSeconds(4)
        val proxyPort = SocketChecker.getAvailableRandomPort()
        val crusher = tcpCrusher(proxyPort)

        val proxyClient = testingServer.createClient("localhost:${proxyPort}", 1000, 1000, 1000)

        val instanceIds = listOf(
                createInstanceIdRegistry(client = proxyClient, disconnectTimeout = disconnectTimeout).register("abs-rate"),
                createInstanceIdRegistry().register("abs-rate"),
                createInstanceIdRegistry().register("drugkeeper")
        )
        logger.info(zkTree())
        assertInstances(mapOf("abs-rate" to setOf("1", "2"), "drugkeeper" to setOf("3")))
        assertInstanceIdMapping(setOf(instanceIds[0] to "1", instanceIds[1] to "2", instanceIds[2] to "3"))

        crusher.freeze()
        await()
                .timeout(Duration.ofSeconds(2))
                .until { !proxyClient.zookeeperClient.isConnected }
        logger.info(zkTree())

        Thread.sleep(disconnectTimeout.toMillis() / 2)
        assertInstanceIdLocksExpiration(setOf("1" to true, "2" to true, "3" to true), disconnectTimeout)

        crusher.unfreeze()
        Thread.sleep(3000)
        await()
                .timeout(Duration.ofSeconds(2))
                .until { proxyClient.zookeeperClient.isConnected }

        logger.info(zkTree())
        assertInstances(mapOf("abs-rate" to setOf("1", "2"), "drugkeeper" to setOf("3")))
    }

    @Test
    fun `client disconnected, disconnect timeout not reached, register new service with new instance id`() {
        val disconnectTimeout = Duration.ofSeconds(4)
        val proxyPort = SocketChecker.getAvailableRandomPort()
        val crusher = tcpCrusher(proxyPort)

        val proxyClient = testingServer.createClient("localhost:${proxyPort}", 1000, 1000, 1000)

        val instances = mutableListOf(
                createInstanceIdRegistry(client = proxyClient, disconnectTimeout = disconnectTimeout).register("abs-rate"),
                createInstanceIdRegistry().register("abs-rate"),
                createInstanceIdRegistry().register("drugkeeper")
        )
        logger.info(zkTree())
        assertInstances(mapOf("abs-rate" to setOf("1", "2"), "drugkeeper" to setOf("3")))
        assertInstanceIdMapping(setOf(instances[0] to "1", instances[1] to "2", instances[2] to "3"))

        crusher.freeze()
        await()
                .timeout(Duration.ofSeconds(2))
                .until { !proxyClient.zookeeperClient.isConnected }

        logger.info(zkTree())
        Thread.sleep(disconnectTimeout.toMillis() / 2)
        logger.info(zkTree())
        assertFalse(isInstanceIdLockExpired("1", disconnectTimeout))

        val registry = createInstanceIdRegistry()
        registry.register("extra-service")
        logger.info(zkTree())

        assertInstanceIdLocksExpiration(setOf("1" to true, "2" to true, "3" to true, "4" to true), disconnectTimeout)

        crusher.unfreeze()
        Thread.sleep(disconnectTimeout.toMillis() / 2)
        logger.info(zkTree())

        assertInstanceIdLocksExpiration(setOf("1" to true, "2" to true, "3" to true, "4" to true), disconnectTimeout)
    }

    @Test
    fun `client disconnected, disconnect timeout reached, register new another service with free instance id`() {
        val disconnectTimeout = Duration.ofSeconds(4)
        val proxyPort = SocketChecker.getAvailableRandomPort()
        val crusher = tcpCrusher(proxyPort)

        val proxyClient = testingServer.createClient("localhost:${proxyPort}", 1000, 1000, 1000)
        val instances = mutableListOf(
                createInstanceIdRegistry(
                        client = proxyClient,
                        disconnectTimeout = disconnectTimeout
                ).register("abs-rate"),
                createInstanceIdRegistry().register("abs-rate"),
                createInstanceIdRegistry().register("drugkeeper")
        )
        logger.info(zkTree())
        assertInstances(mapOf("abs-rate" to setOf("1", "2"), "drugkeeper" to setOf("3")))
        assertInstanceIdMapping(setOf(instances[0] to "1", instances[1] to "2", instances[2] to "3"))

        crusher.freeze()
        await()
                .timeout(Duration.ofSeconds(1))
                .until { !proxyClient.zookeeperClient.isConnected }

        logger.info(zkTree())
        Thread.sleep(disconnectTimeout.toMillis() * 2)

        logger.info(zkTree())
        assertTrue(isInstanceIdLockExpired("1", disconnectTimeout))

        val client = testingServer.createClient()
        createInstanceIdRegistry(client = client, disconnectTimeout = disconnectTimeout).register("wow-service")
        await()
                .timeout(Duration.ofSeconds(1))
                .until { client.zookeeperClient.isConnected }
        logger.info(zkTree())

        assertInstanceIdLocksExpiration(setOf("1" to false, "2" to true, "3" to true, "4" to true), disconnectTimeout)

        crusher.close()
    }

    @Test
    fun `prolongation of instance id's lock works fine after reconnect`() {
        val disconnectTimeout = Duration.ofSeconds(4)
        val proxyPort = SocketChecker.getAvailableRandomPort()
        val crusher = tcpCrusher(proxyPort)

        val proxyClient = testingServer.createClient("localhost:${proxyPort}", 1000, 1000, 1000)

        createInstanceIdRegistry(client = proxyClient, disconnectTimeout = disconnectTimeout).register("app")

        logger.info(zkTree())
        assertInstances(mapOf("app" to setOf("1")))

        crusher.reopen()
        logger.info(zkTree())

        Thread.sleep(disconnectTimeout.toMillis())
        logger.info(zkTree())
        assertInstances(mapOf("app" to setOf("1")))
        assertFalse(isInstanceIdLockExpired("1", disconnectTimeout))
    }

    @Test
    fun `instances registered by same service have all expired locks after connection lost and timeout reached`() {
        val disconnectTimeout = Duration.ofSeconds(4)
        val proxyPort = SocketChecker.getAvailableRandomPort()
        val crusher = tcpCrusher(proxyPort)

        val proxyClient = testingServer.createClient("localhost:${proxyPort}", 1000, 1000, 1000)

        val registry = createInstanceIdRegistry(client = proxyClient, disconnectTimeout = disconnectTimeout)
        registry.register("app")
        registry.register("app")
        registry.register("app")

        logger.info(zkTree())
        assertInstances(mapOf("app" to setOf("1", "2", "3")))
        assertInstanceIdLocksExpiration(setOf("1" to true, "2" to true, "3" to true), disconnectTimeout)

        crusher.freeze()
        Thread.sleep(disconnectTimeout.toMillis() * 2)
        logger.info(zkTree())
        assertInstanceIdLocksExpiration(setOf("1" to false, "2" to false, "3" to false), disconnectTimeout)

        crusher.unfreeze()
        logger.info(zkTree())
        assertInstanceIdLocksExpiration(setOf("1" to false, "2" to false, "3" to false), disconnectTimeout)
    }

    private fun tcpCrusher(proxyPort: Int): TcpCrusher =
            TcpCrusherBuilder.builder()
                    .withReactor(reactor)
                    .withBindAddress("localhost", proxyPort)
                    .withConnectAddress("localhost", testingServer.port)
                    .buildAndOpen()

}
