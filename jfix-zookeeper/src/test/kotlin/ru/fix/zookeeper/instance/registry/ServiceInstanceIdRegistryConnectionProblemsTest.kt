package ru.fix.zookeeper.instance.registry

import io.kotest.matchers.shouldBe
import org.apache.curator.utils.ZKPaths
import org.apache.logging.log4j.kotlin.logger
import org.junit.jupiter.api.Test
import ru.fix.zookeeper.lock.PersistentExpiringDistributedLock.LockNodeState.EXPIRED_LOCK
import ru.fix.zookeeper.lock.PersistentExpiringDistributedLock.LockNodeState.LIVE_LOCK
import java.time.Duration

internal class ServiceInstanceIdRegistryConnectionProblemsTest : AbstractServiceInstanceIdRegistryTest() {
    private val logger = logger()

    @Test
    fun `client lost connection and reconnect with same instance id when lock of this instance id not expired`() {
        val lockAcquirePeriod = Duration.ofSeconds(4)
        val crusher = testingServer.openProxyTcpCrusher()
        val proxyClient = testingServer.createZkProxyClient(crusher)
        val zkProxyState = testingServer.startWatchClientState(proxyClient)

        val registry = createInstanceIdRegistry(client = proxyClient, lockAcquirePeriod = lockAcquirePeriod)
        registry.register("a") shouldBe "1"
        logger.info(zkTree())

        crusher.close()
        waitDisconnectState(zkProxyState)

        Thread.sleep(lockAcquirePeriod.toMillis() / 2)
        logger.info(zkTree())
        instanceIdState("a", "1") shouldBe LIVE_LOCK

        crusher.open()
        waitReconnectState(zkProxyState)

        Thread.sleep(lockAcquirePeriod.toMillis())
        logger.info(zkTree())
        instanceIdState("a", "1") shouldBe LIVE_LOCK
    }

    @Test
    fun `client disconnected, lock of this instance id not expired, register new service with new instance id`() {
        val lockAcquirePeriod = Duration.ofSeconds(4)
        val crusher = testingServer.openProxyTcpCrusher()
        val proxyClient = testingServer.createZkProxyClient(crusher)
        val zkProxyState = testingServer.startWatchClientState(proxyClient)

        createInstanceIdRegistry(client = proxyClient, lockAcquirePeriod = lockAcquirePeriod).register("abs-rate") shouldBe "1"
        createInstanceIdRegistry().register("abs-rate") shouldBe "2"
        logger.info(zkTree())

        crusher.close()
        waitDisconnectState(zkProxyState)

        instanceIdState("abs-rate", "1") shouldBe LIVE_LOCK

        val registry = createInstanceIdRegistry()
        registry.register("abs-rate") shouldBe "3"
    }

    @Test
    fun `client disconnected, instance id's lock expired, register new another service with instance id of expired lock`() {
        val lockAcquirePeriod = Duration.ofSeconds(3)
        val crusher = testingServer.openProxyTcpCrusher()
        val proxyClient = testingServer.createZkProxyClient(crusher)
        val zkProxyState = testingServer.startWatchClientState(proxyClient)

        createInstanceIdRegistry(client = proxyClient, lockAcquirePeriod = lockAcquirePeriod).register("abs-rate") shouldBe "1"
        createInstanceIdRegistry().register("abs-rate") shouldBe "2"

        crusher.close()
        waitDisconnectState(zkProxyState)

        waitLockNodeState(EXPIRED_LOCK, ZKPaths.makePath(serviceRegistrationPath, "abs-rate", "1"))
        instanceIdState("abs-rate", "1") shouldBe EXPIRED_LOCK

        val client = testingServer.createClient()
        createInstanceIdRegistry(client = client, lockAcquirePeriod = lockAcquirePeriod).register("abs-rate") shouldBe "1"
        client.blockUntilConnected()

        instanceIdState("abs-rate", "1") shouldBe LIVE_LOCK
    }

    /**
     * Here is close of the first registry to resolve problem of uniqueness of instance id of same service
     */
    @Test
    fun `client disconnected, instance id's lock expired, register instance on expired instance id, client of expired lock reconnected with error logged`() {
        val lockAcquirePeriod = Duration.ofSeconds(2)
        val crusher = testingServer.openProxyTcpCrusher()
        val proxyClient = testingServer.createZkProxyClient(crusher)
        val zkProxyState = testingServer.startWatchClientState(proxyClient)

        val registry = createInstanceIdRegistry(
                client = proxyClient,
                lockAcquirePeriod = lockAcquirePeriod
        )
        registry.register("my-service")
        logger.info(zkTree())

        crusher.close()
        waitDisconnectState(zkProxyState)

        logger.info(zkTree())
        waitLockNodeState(EXPIRED_LOCK, ZKPaths.makePath(serviceRegistrationPath, "my-service", "1"))

        val client = testingServer.createClient()
        createInstanceIdRegistry(client = client, lockAcquirePeriod = lockAcquirePeriod).register("my-service") shouldBe "1"
        client.blockUntilConnected()

        logger.info(zkTree())
        instanceIdState("my-service", "1") shouldBe LIVE_LOCK

        crusher.open()
        /**
         *  Here was errors, because 2 registry manages same instance.
         */
        Thread.sleep(5000)

        registry.close()
        /**
         * No error logs, when reconnected registry closed
         */
        Thread.sleep(5000)
        logger.info(zkTree())
        instanceIdState("my-service", "1") shouldBe LIVE_LOCK
    }

    /**
     * Here is close of second registry to resolve problem of uniqueness of instance id of same service
     */
    @Test
    fun `1st registry have expired lock after connection loss, 2nd registry acquire this lock and close, 1st registry stop log errors`() {
        val lockAcquirePeriod = Duration.ofSeconds(2)
        val crusher1 = testingServer.openProxyTcpCrusher()
        val proxyClient1 = testingServer.createZkProxyClient(crusher1)
        val zkProxyState1 = testingServer.startWatchClientState(proxyClient1)

        val registry1 = createInstanceIdRegistry(client = proxyClient1, lockAcquirePeriod = lockAcquirePeriod)
        registry1.register("my-service") shouldBe "1"
        logger.info(zkTree())

        crusher1.close()
        waitDisconnectState(zkProxyState1)

        logger.info(zkTree())
        waitLockNodeState(EXPIRED_LOCK, lockPath("my-service", "1"))

        logger.info(zkTree())

        val registry2 = createInstanceIdRegistry()
        registry2.register("my-service") shouldBe "1"

        crusher1.open()
        Thread.sleep(5000)
        /**
         *  Here was errors logged, because 2 registry manages same instance.
         */

        registry2.close()
        /**
         * No error logs, when reconnected registry closed
         */
        Thread.sleep(5000)
        instanceIdState("my-service", "1") shouldBe LIVE_LOCK
    }

    @Test
    fun `instance id's lock didn't lost after reconnect`() {
        val lockAcquirePeriod = Duration.ofSeconds(3)
        val crusher = testingServer.openProxyTcpCrusher()
        val proxyClient = testingServer.createZkProxyClient(crusher)
        val zkProxyState = testingServer.startWatchClientState(proxyClient)

        createInstanceIdRegistry(client = proxyClient, lockAcquirePeriod = lockAcquirePeriod).register("app") shouldBe "1"
        crusher.reopen()

        waitDisconnectState(zkProxyState)
        instanceIdState("app", "1") shouldBe LIVE_LOCK
    }

    @Test
    fun `registries registered by same service have all expired locks after connection lost and all prolonged after reconnect`() {
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
        registry.register("app") shouldBe "1"
        registry.register("app") shouldBe "2"
        registry.register("app") shouldBe "3"

        instanceIdState("app", "1") shouldBe LIVE_LOCK

        crusher.close()
        waitLockNodeState(EXPIRED_LOCK, ZKPaths.makePath(serviceRegistrationPath, "app", "3"))
        instanceIdState("app", "1") shouldBe EXPIRED_LOCK
        instanceIdState("app", "2") shouldBe EXPIRED_LOCK

        crusher.open()
        waitReconnectState(zkProxyState)
        waitLockNodeState(LIVE_LOCK, ZKPaths.makePath(serviceRegistrationPath, "app", "3"))

        instanceIdState("app", "1") shouldBe LIVE_LOCK
        instanceIdState("app", "2") shouldBe LIVE_LOCK
        instanceIdState("app", "3") shouldBe LIVE_LOCK
    }

}
