package ru.fix.zookeeper.testing

import org.apache.curator.framework.CuratorFramework
import org.apache.curator.framework.CuratorFrameworkFactory
import org.apache.curator.framework.state.ConnectionState
import org.apache.curator.framework.state.ConnectionStateListener
import org.apache.curator.retry.ExponentialBackoffRetry
import org.apache.curator.retry.RetryNTimes
import org.apache.curator.test.InstanceSpec
import org.apache.curator.test.TestingServer
import org.netcrusher.core.reactor.NioReactor
import org.netcrusher.tcp.TcpCrusher
import org.netcrusher.tcp.TcpCrusherBuilder
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import ru.fix.stdlib.socket.SocketChecker
import java.nio.file.Files
import java.nio.file.Path
import java.time.Duration
import java.util.*
import java.util.concurrent.atomic.AtomicReference


/**
 * Runs single instance zookeeper server.
 *
 * Stores node state in temp folder that will be removed during [close]
 *
 * Creates CuratorFramework client that can be used in tests.
 *
 * This way server will be shutdown on by JVM shutdown hook
 * ```
 * ZkTestingServer server = new ZkTestingServer()
 *      .withCloseOnJvmShutdown()
 *      .start();
 *  ```
 *
 * This way server shutdown is explicit<br></br>
 * ```
 * ZkTestingServer server = new ZkTestingServer()
 *      .start();
 *
 * // -- run tests --
 *
 * server.close();
 * ```
 *
 * This way you can test for network problem
 * ```
 * val proxyTcpCrusher = zkServer.openProxyTcpCrusher()
 * val zkProxyClient = zkServer.createZkProxyClient(proxyTcpCrusher)
 * val zkProxyState = zkServer.startWatchClientState(zkProxyClient)
 * // do something
 * proxyTcpCrusher.close()
 * // do something after connection lost
 * await().atMost(10, MINUTES).until {
 * zkProxyState.get() == ConnectionState.SUSPENDED ||
 * zkProxyState.get() == ConnectionState.LOST
 * ```
 *
 */
class ZKTestingServer : AutoCloseable {
    companion object {
        private val logger: Logger = LoggerFactory.getLogger(ZKTestingServer::class.java)
    }

    lateinit var server: TestingServer
        private set

    var port = 0
        private set

    private lateinit var tmpDir: Path

    /**
     * Managed client for this server
     *
     * @return pre-created and initialized curator framework
     */
    lateinit var client: CuratorFramework
        private set

    private lateinit var rootPathUuid: String
    private var closeOnJvmShutdown = false

    /**
     * Register shutdown hook [Runtime.addShutdownHook] and close on jvm exit.
     */
    fun withCloseOnJvmShutdown(): ZKTestingServer {
        closeOnJvmShutdown = true
        return this
    }

    private fun init() {
        initZkServer()

        if (closeOnJvmShutdown) {
            Runtime.getRuntime().addShutdownHook(Thread(Runnable { close() }))
        }

        initZkClient()
    }

    private fun initZkClient() {
        initRootUuidPath()
        this.client = createClient()
    }

    private fun initRootUuidPath() {
        rootPathUuid = UUID.randomUUID().toString()
        CuratorFrameworkFactory.builder()
                .connectString(server.connectString)
                .retryPolicy(ExponentialBackoffRetry(1000, 10))
                .build().use { client ->
                    client.start()
                    client.create().forPath("/$rootPathUuid")
                }
    }

    private fun initZkServer() {
        tmpDir = Files.createTempDirectory("zkTestSrvTmpDir")

        val LAST_ATTEMPT_NUMBER = 15
        for (i in 1..LAST_ATTEMPT_NUMBER) {
            try {
                val instanceSpec = InstanceSpec(
                        tmpDir.toFile(),
                        SocketChecker.getAvailableRandomPort(),
                        SocketChecker.getAvailableRandomPort(),
                        SocketChecker.getAvailableRandomPort(),
                        true,
                        1)
                port = instanceSpec.port
                server = TestingServer(instanceSpec, true)
                return
            } catch (e: Exception) {
                if (i == LAST_ATTEMPT_NUMBER) {
                    throw e
                } else {
                    logger.warn("Failed to create zk testing server", e)
                }
            }
        }
    }

    /**
     * Close server and remove temp directory
     */
    override fun close() {
        try {
            client.close()
        } catch (e: Exception) {
            logger.error("Failed to close zk client", e)
        }
        try {

            server.close()
        } catch (e: Exception) {
            logger.error("Failed to close zk testing server", e)
        }
        try {
            Files.deleteIfExists(tmpDir)
        } catch (e: Exception) {
            logger.error("Failed to delete {}", tmpDir, e)
        }
    }

    fun start(): ZKTestingServer {
        init()
        return this
    }

    /**
     * Creates new client. Users should manually close this client.
     *
     * @return fully initialized curator framework
     */
    fun createClient(sessionTimeoutMs: Int = 60000,
                     connectionTimeoutMs: Int = 15000): CuratorFramework {
        return createClient(
                connectionString = server.connectString,
                sessionTimeoutMs = sessionTimeoutMs,
                connectionTimeoutMs = connectionTimeoutMs)
    }

    fun createClient(
            connectionString: String = server.connectString,
            sessionTimeoutMs: Int = 60000,
            connectionTimeoutMs: Int = 15000): CuratorFramework {

        val newClient = CuratorFrameworkFactory.builder()
                .connectString("$connectionString/$rootPathUuid")
                .retryPolicy(RetryNTimes(3, 1000))
                .sessionTimeoutMs(sessionTimeoutMs)
                .connectionTimeoutMs(connectionTimeoutMs)
                .build()
        newClient.start()
        return newClient
    }

    fun startWatchClientState(zkClient: CuratorFramework): AtomicReference<ConnectionState> {
        val state = AtomicReference<ConnectionState>()
        zkClient.connectionStateListenable.addListener(ConnectionStateListener { _, newState ->
            logger.debug("State changed to {}", newState)
            state.set(newState)
        })
        return state
    }

    fun createZkProxyClient(proxyTcpCrusher: TcpCrusher): CuratorFramework {
        return createClient(
                "${proxyTcpCrusher.bindAddress.hostString}:${proxyTcpCrusher.bindAddress.port}",
                Duration.ofSeconds(5).toMillis().toInt(),
                Duration.ofSeconds(5).toMillis().toInt())
    }

    fun openProxyTcpCrusher(): TcpCrusher {
        val zkProxyCrusherPort = SocketChecker.getAvailableRandomPort()
        val proxyTcpCrusher = TcpCrusherBuilder.builder()
                .withReactor(NioReactor())
                .withBindAddress("localhost", zkProxyCrusherPort)
                .withConnectAddress("localhost", port)
                .build()

        proxyTcpCrusher.open()
        return proxyTcpCrusher
    }
}