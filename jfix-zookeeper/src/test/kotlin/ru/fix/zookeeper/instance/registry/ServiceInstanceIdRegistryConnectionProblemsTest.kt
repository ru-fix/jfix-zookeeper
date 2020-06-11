package ru.fix.zookeeper.instance.registry

import org.apache.curator.framework.state.ConnectionState
import org.apache.logging.log4j.kotlin.logger
import org.awaitility.Awaitility.await
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Test
import java.time.Duration
import java.util.concurrent.atomic.AtomicReference

internal class ServiceInstanceIdRegistryConnectionProblemsTest : AbstractServiceInstanceIdRegistryTest() {
    private val logger = logger()

    @Test
    fun `client lost connection and reconnect with same instance id when lock of this instance id not expired`() {
        val lockAcquirePeriod = Duration.ofSeconds(3)
        val crusher = testingServer.openProxyTcpCrusher()
        val proxyClient = testingServer.createZkProxyClient(crusher)
        val zkProxyState = testingServer.startWatchClientState(proxyClient)

        val instanceIds = listOf(
                createInstanceIdRegistry(client = proxyClient, lockAcquirePeriod = lockAcquirePeriod).register("abs-rate"),
                createInstanceIdRegistry().register("abs-rate"),
                createInstanceIdRegistry().register("drugkeeper")
        )
        logger.info(zkTree())
        assertInstances(mapOf("abs-rate" to setOf("1", "2"), "drugkeeper" to setOf("3")))
        assertInstanceIdMapping(setOf(instanceIds[0] to "1", instanceIds[1] to "2", instanceIds[2] to "3"))

        crusher.close()
        waitDisconnectState(zkProxyState)
        logger.info(zkTree())

        Thread.sleep(lockAcquirePeriod.toMillis() / 2)
        assertInstanceIdLocksExpiration(setOf("1" to true, "2" to true, "3" to true), lockAcquirePeriod)

        crusher.open()
        waitReconnectState(zkProxyState)

        logger.info(zkTree())
        assertInstanceIdLocksExpiration(setOf("1" to true, "2" to true, "3" to true), lockAcquirePeriod)
        assertInstances(mapOf("abs-rate" to setOf("1", "2"), "drugkeeper" to setOf("3")))
    }

    @Test
    fun `when instance reconnected after connection failure and lock not expired, error shouldn't be logged`() {
        val lockAcquirePeriod = Duration.ofSeconds(3)
        val crusher = testingServer.openProxyTcpCrusher()
        val proxyClient = testingServer.createZkProxyClient(crusher)

        val registry = createInstanceIdRegistry(client = proxyClient, lockAcquirePeriod = lockAcquirePeriod)
        registry.register("my-service")

        assertInstances(mapOf("my-service" to setOf("1")))

        crusher.close()
        Thread.sleep(3000)
        assertInstanceIdLocksExpiration(setOf("1" to true), lockAcquirePeriod)

        crusher.open()
        Thread.sleep(1000)
        assertInstanceIdLocksExpiration(setOf("1" to true), lockAcquirePeriod)
    }

    @Test
    fun `client disconnected, lock of this instance id not expired, register new service with new instance id`() {
        val lockAcquirePeriod = Duration.ofSeconds(3)
        val crusher = testingServer.openProxyTcpCrusher()
        val proxyClient = testingServer.createZkProxyClient(crusher)
        val zkProxyState = testingServer.startWatchClientState(proxyClient)

        val instances = mutableListOf(
                createInstanceIdRegistry(client = proxyClient, lockAcquirePeriod = lockAcquirePeriod).register("abs-rate"),
                createInstanceIdRegistry().register("abs-rate"),
                createInstanceIdRegistry().register("drugkeeper")
        )
        logger.info(zkTree())
        assertInstances(mapOf("abs-rate" to setOf("1", "2"), "drugkeeper" to setOf("3")))
        assertInstanceIdMapping(setOf(instances[0] to "1", instances[1] to "2", instances[2] to "3"))

        crusher.close()
        waitDisconnectState(zkProxyState)

        logger.info(zkTree())
        Thread.sleep(lockAcquirePeriod.toMillis() / 2)
        logger.info(zkTree())
        assertFalse(isInstanceIdLockExpired("1", lockAcquirePeriod))

        val registry = createInstanceIdRegistry()
        registry.register("extra-service")
        logger.info(zkTree())

        assertInstanceIdLocksExpiration(setOf("1" to true, "2" to true, "3" to true, "4" to true), lockAcquirePeriod)

        Thread.sleep(lockAcquirePeriod.toMillis() / 2)
        crusher.open()
        waitReconnectState(zkProxyState)
        logger.info(zkTree())

        assertInstanceIdLocksExpiration(setOf("1" to true, "2" to true, "3" to true, "4" to true), lockAcquirePeriod)
    }

    @Test
    fun `client disconnected, instance id's lock expired, register new another service with instance id of expired lock`() {
        val lockAcquirePeriod = Duration.ofSeconds(3)
        val crusher = testingServer.openProxyTcpCrusher()
        val proxyClient = testingServer.createZkProxyClient(crusher)
        val zkProxyState = testingServer.startWatchClientState(proxyClient)

        val instances = mutableListOf(
                createInstanceIdRegistry(
                        client = proxyClient,
                        lockAcquirePeriod = lockAcquirePeriod
                ).register("abs-rate"),
                createInstanceIdRegistry().register("abs-rate"),
                createInstanceIdRegistry().register("drugkeeper")
        )
        logger.info(zkTree())
        assertInstances(mapOf("abs-rate" to setOf("1", "2"), "drugkeeper" to setOf("3")))
        assertInstanceIdMapping(setOf(instances[0] to "1", instances[1] to "2", instances[2] to "3"))

        crusher.close()
        waitDisconnectState(zkProxyState)

        logger.info(zkTree())
        Thread.sleep(lockAcquirePeriod.toMillis() * 2)

        logger.info(zkTree())
        assertInstanceIdLocksExpiration(setOf("1" to false, "2" to true, "3" to true), lockAcquirePeriod)

        val client = testingServer.createClient()
        createInstanceIdRegistry(client = client, lockAcquirePeriod = lockAcquirePeriod).register("wow-service")
        client.blockUntilConnected()

        logger.info(zkTree())
        assertInstanceIdLocksExpiration(setOf("1" to true, "2" to true, "3" to true), lockAcquirePeriod)
    }

    @Test
    fun `client disconnected, instance id's lock expired, register instance on expired instance id, client of expired lock reconnected with error logged`() {
        val lockAcquirePeriod = Duration.ofSeconds(3)
        val crusher = testingServer.openProxyTcpCrusher()
        val proxyClient = testingServer.createZkProxyClient(crusher)
        val zkProxyState = testingServer.startWatchClientState(proxyClient)

        val instances = mutableListOf(
                createInstanceIdRegistry(
                        client = proxyClient,
                        lockAcquirePeriod = lockAcquirePeriod
                ),
                createInstanceIdRegistry(),
                createInstanceIdRegistry()
        )
        instances.forEach { it.register("my-service") }
        logger.info(zkTree())
        assertInstances(mapOf("my-service" to setOf("1", "2", "3")))

        crusher.close()
        waitDisconnectState(zkProxyState)

        logger.info(zkTree())
        Thread.sleep(lockAcquirePeriod.toMillis() * 2)

        logger.info(zkTree())
        assertInstanceIdLocksExpiration(setOf("1" to false, "2" to true, "3" to true), lockAcquirePeriod)

        val client = testingServer.createClient()
        createInstanceIdRegistry(client = client, lockAcquirePeriod = lockAcquirePeriod).register("wow-service")
        client.blockUntilConnected()


        logger.info(zkTree())
        assertInstanceIdLocksExpiration(setOf("1" to true, "2" to true, "3" to true), lockAcquirePeriod)

        crusher.open()
        /**
         *  Here was errors logged with period = lock prolongation interval, because 2 registry manages same instance.
         */
        Thread.sleep(3000)

        instances[0].close()
        /**
         * No error logs, when reconnected registry closed
         */
        Thread.sleep(3000)
    }

    @Test
    fun `prolongation of instance id's lock works fine after reconnect`() {
        val lockAcquirePeriod = Duration.ofSeconds(3)
        val crusher = testingServer.openProxyTcpCrusher()
        val proxyClient = testingServer.createZkProxyClient(crusher)

        createInstanceIdRegistry(client = proxyClient, lockAcquirePeriod = lockAcquirePeriod).register("app")

        logger.info(zkTree())
        assertInstances(mapOf("app" to setOf("1")))

        crusher.reopen()
        logger.info(zkTree())

        Thread.sleep(lockAcquirePeriod.toMillis())
        logger.info(zkTree())
        assertInstances(mapOf("app" to setOf("1")))
        assertFalse(isInstanceIdLockExpired("1", lockAcquirePeriod))
    }

    @Test
    fun `instances registered by same service have all expired locks after connection lost and all prolonged after reconnect`() {
        val lockAcquirePeriod = Duration.ofSeconds(3)
        val crusher = testingServer.openProxyTcpCrusher()
        val proxyClient = testingServer.createZkProxyClient(crusher)
        val zkProxyState = testingServer.startWatchClientState(proxyClient)

        val registry = createInstanceIdRegistry(
                client = proxyClient,
                lockAcquirePeriod = lockAcquirePeriod,
                expirationPeriod = Duration.ofMillis(500),
                lockCheckAndProlongInterval = Duration.ofMillis(300)
        )
        registry.register("app")
        registry.register("app")
        registry.register("app")

        logger.info(zkTree())
        assertInstances(mapOf("app" to setOf("1", "2", "3")))
        assertInstanceIdLocksExpiration(setOf("1" to true, "2" to true, "3" to true), lockAcquirePeriod)

        crusher.close()
        Thread.sleep(lockAcquirePeriod.toMillis() * 2)
        logger.info(zkTree())
        assertInstanceIdLocksExpiration(setOf("1" to false, "2" to false, "3" to false), lockAcquirePeriod)

        crusher.open()
        waitReconnectState(zkProxyState)
        Thread.sleep(1000)

        logger.info(zkTree())
        assertInstanceIdLocksExpiration(setOf("1" to true, "2" to true, "3" to true), lockAcquirePeriod)
    }

    private fun waitDisconnectState(zkState: AtomicReference<ConnectionState>) {
        await()
                .timeout(Duration.ofSeconds(2))
                .until { zkState.get() == ConnectionState.SUSPENDED || zkState.get() == ConnectionState.LOST }
    }

    private fun waitReconnectState(zkState: AtomicReference<ConnectionState>) {
        await()
                .timeout(Duration.ofSeconds(2))
                .until { zkState.get() == ConnectionState.RECONNECTED }
    }
}
