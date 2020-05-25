package ru.fix.zookeeper.discovery

import com.fasterxml.jackson.core.type.TypeReference
import org.awaitility.Awaitility.await
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import org.netcrusher.core.reactor.NioReactor
import org.netcrusher.tcp.TcpCrusher
import org.netcrusher.tcp.TcpCrusherBuilder
import org.slf4j.LoggerFactory
import ru.fix.stdlib.socket.SocketChecker
import ru.fix.zookeeper.lock.LockData
import ru.fix.zookeeper.utils.Marshaller
import java.time.Duration
import java.util.concurrent.TimeUnit
import kotlin.random.Random

internal class ServiceInstanceIdRegistryConnectionProblemsTest : AbstractServiceInstanceIdRegistryTest() {
    private val reactor: NioReactor = NioReactor()

    @Test
    fun `client of service instance id registry lost connection and should`() {
        val disconnectTimeout = Duration.ofSeconds(3)
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
//        Thread.sleep(15000)
        await()
                .timeout(Duration.ofSeconds(2))
                .until { !proxyClient.zookeeperClient.isConnected }
//        assertInstances(mapOf("abs-rate" to setOf("2"), "drugkeeper" to setOf("3")))
        println(zkTree())

        Thread.sleep(disconnectTimeout.toMillis())

        crusher.unfreeze()
        Thread.sleep(3000)
        await()
                .timeout(Duration.ofSeconds(2))
                .until { proxyClient.zookeeperClient.isConnected }

        println(zkTree())
//        assertInstances(mapOf("abs-rate" to setOf("1", "2"), "drugkeeper" to setOf("3")))
        Assertions.assertTrue(false)
    }

    @Test
    fun `2`() {
        val sessionTimeoutMs = 3000
        val proxyPort = SocketChecker.getAvailableRandomPort()
        val crusher = tcpCrusher(proxyPort)

        val proxyClient = testingServer.createClient("localhost:${proxyPort}", sessionTimeoutMs, 1000, 1000)

        val instances = mutableListOf(
                createInstanceIdRegistry("abs-rate", client = proxyClient),
                createInstanceIdRegistry("abs-rate"),
                createInstanceIdRegistry("drugkeeper")
        )
        println(zkTree())
//        assertInstances(mapOf("abs-rate" to setOf("1", "2"), "drugkeeper" to setOf("3")))
//        assertInstanceIdMapping(setOf(instances[0] to "1", instances[1] to "2", instances[2] to "3"))

        crusher.freeze()
        Thread.sleep(5000)
/*        await()
                .timeout(Duration.ofSeconds(10))
                .until { !isAliveInstance(instances[0]) }*/

        println(zkTree())
        Thread.sleep(15000)
        println(zkTree())
//        assertInstances(mapOf("abs-rate" to setOf("2"), "drugkeeper" to setOf("3")))

        println(zkTree())
        instances.add(createInstanceIdRegistry("abs-rate"))
        Thread.sleep(10000)
        /*await()
                .timeout(Duration.ofSeconds(10))
                .until { isAliveInstance(instances[3]) }*/
        println(zkTree())
//        assertInstances(mapOf("abs-rate" to setOf("2", "4"), "drugkeeper" to setOf("3")))

        crusher.unfreeze()
        Thread.sleep(3000)
        /*await()
                .timeout(Duration.ofSeconds(3))
                .until { isAliveInstance(instances[0]) }*/

        println(zkTree())
//        assertInstances(mapOf("abs-rate" to setOf("1", "2"), "drugkeeper" to setOf("3")))
        Assertions.assertTrue(false)

    }


    @Test
    fun `3`() {
        val sessionTimeoutMs = 3000
        val proxyPort = SocketChecker.getAvailableRandomPort()
//        val crusher = tcpCrusher(proxyPort)

//        val proxyClient = testingServer.createClient("localhost:${proxyPort}", sessionTimeoutMs, 1000, 1000)

        val port = Random.nextInt(0, 65535)
        val instances = mutableListOf(
                createInstanceIdRegistry("abs-rate", port = port),
                createInstanceIdRegistry("abs-rate")
        )
        println(zkTree())
//        assertInstances(mapOf("abs-rate" to setOf("1", "2"), "drugkeeper" to setOf("3")))
//        assertInstanceIdMapping(setOf(instances[0] to "1", instances[1] to "2", instances[2] to "3"))

//        crusher.freeze()
        Thread.sleep(5000)
/*        await()
                .timeout(Duration.ofSeconds(10))
                .until { !isAliveInstance(instances[0]) }*/

        instances[0].close()
        println(zkTree())
        Thread.sleep(1500)
        println(zkTree())
        instances.add(createInstanceIdRegistry("abs-rate", port = port))
//        assertInstances(mapOf("abs-rate" to setOf("2"), "drugkeeper" to setOf("3")))

        println(zkTree())
        Thread.sleep(10000)
        /*await()
                .timeout(Duration.ofSeconds(10))
                .until { isAliveInstance(instances[3]) }*/
        println(zkTree())
//        assertInstances(mapOf("abs-rate" to setOf("2", "4"), "drugkeeper" to setOf("3")))

//        crusher.unfreeze()
        Thread.sleep(3000)
        /*await()
                .timeout(Duration.ofSeconds(3))
                .until { isAliveInstance(instances[0]) }*/

        println(zkTree())
//        assertInstances(mapOf("abs-rate" to setOf("1", "2"), "drugkeeper" to setOf("3")))
        Assertions.assertTrue(false)
    }

    @Test
    fun `4`() {
        val sessionTimeoutMs = 3000
        val proxyPort = SocketChecker.getAvailableRandomPort()
        val crusher = tcpCrusher(proxyPort)

        val proxyClient = testingServer.createClient("localhost:${proxyPort}", sessionTimeoutMs, 1000, 1000)

        val port = Random.nextInt(0, 65535)
        val instances = mutableListOf(
                createInstanceIdRegistry("abs-rate", client = proxyClient, port = port),
                createInstanceIdRegistry("abs-rate")
        )
        println(zkTree())
//        assertInstances(mapOf("abs-rate" to setOf("1", "2"), "drugkeeper" to setOf("3")))
//        assertInstanceIdMapping(setOf(instances[0] to "1", instances[1] to "2", instances[2] to "3"))

        crusher.freeze()
        Thread.sleep(1000)
/*        await()
                .timeout(Duration.ofSeconds(10))
                .until { !isAliveInstance(instances[0]) }*/


        println(zkTree())
        Thread.sleep(5000)
        instances.add(createInstanceIdRegistry("abs-rate", port = port))
//        assertInstances(mapOf("abs-rate" to setOf("2"), "drugkeeper" to setOf("3")))

        println(zkTree())
        Thread.sleep(1000)
        /*await()
                .timeout(Duration.ofSeconds(10))
                .until { isAliveInstance(instances[3]) }*/
        println(zkTree())
//        assertInstances(mapOf("abs-rate" to setOf("2", "4"), "drugkeeper" to setOf("3")))

//        crusher.unfreeze()
        Thread.sleep(1000)
        /*await()
                .timeout(Duration.ofSeconds(3))
                .until { isAliveInstance(instances[0]) }*/

        println(zkTree())
//        assertInstances(mapOf("abs-rate" to setOf("1", "2"), "drugkeeper" to setOf("3")))
        Assertions.assertTrue(false)
    }

  /*  private fun isAliveInstance(instance: ServiceInstanceIdRegistry): Boolean {
        if (testingServer.client.checkExists().forPath(instance.instanceIdPath) == null) {
            return false
        }
        val nodeData = Marshaller.unmarshall(
                testingServer.client.data.forPath(instance.instanceIdPath).toString(Charsets.UTF_8),
                object : TypeReference<LockData>() {}
        )
        val instanceIdData = Marshaller.unmarshall(nodeData.data, object : TypeReference<InstanceIdData>() {})
        return instanceIdData.c
    }*/

    private fun tcpCrusher(proxyPort: Int): TcpCrusher =
            TcpCrusherBuilder.builder()
                    .withReactor(reactor)
                    .withBindAddress("localhost", proxyPort)
                    .withConnectAddress("localhost", testingServer.port)
                    .buildAndOpen()

}
