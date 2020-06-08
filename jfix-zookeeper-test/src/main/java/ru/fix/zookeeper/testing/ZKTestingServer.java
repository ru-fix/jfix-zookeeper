package ru.fix.zookeeper.testing;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.InstanceSpec;
import org.apache.curator.test.TestingServer;
import org.slf4j.Logger;
import ru.fix.stdlib.socket.SocketChecker;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.UUID;

import static org.slf4j.LoggerFactory.getLogger;

/**
 * Runs single instance zookeeper server. <br>
 * Stores node state in temp folder that will be removed during {@link #close()} <br>
 * Creates CuratorFramework client that can be used in tests. <br>
 * <br>
 * This way server will be shutdown on by JVM shutdown hook <br>
 * <pre>{@code
 *   ZkTestingServer server = new ZkTestingServer()
 *              .withCloseOnJvmShutdown()
 *              .start();
 * }
 * </pre>
 * <br>
 * This way server shutdown is explicit<br>
 * <pre>{@code
 *   ZkTestingServer server = new ZkTestingServer()
 *              .start();
 *
 *   // -- run tests --
 *
 *   server.close();
 *
 * }
 * </pre>
 */
public class ZKTestingServer implements AutoCloseable {

    private static final Logger logger = getLogger(ZKTestingServer.class);

    private TestingServer zkServer;
    private Path tmpDir;
    private int port;

    private CuratorFramework curatorFramework;
    private String uuid;

    private boolean closeOnJvmShutdown = false;

    /**
     * Register shutdown hook {@link Runtime#addShutdownHook(Thread)} and close on jvm exit.
     */
    public ZKTestingServer withCloseOnJvmShutdown(boolean closeOnJvmShutdown) {
        this.closeOnJvmShutdown = closeOnJvmShutdown;
        return this;
    }

    private void init() throws IOException {
        tmpDir = Files.createTempDirectory("tmpDir");

        for (int i = 0; i < 15; i++) {
            try {
                InstanceSpec instanceSpec = new InstanceSpec(tmpDir.toFile(), SocketChecker.getAvailableRandomPort(),
                        SocketChecker.getAvailableRandomPort(), SocketChecker.getAvailableRandomPort(),
                        true, 1);
                port = instanceSpec.getPort();

                zkServer = new TestingServer(instanceSpec, true);
                break;
            } catch (Exception e) {
                logger.debug("Failed to create zk testing server", e);
            }
        }

        if (closeOnJvmShutdown) {
            Runtime.getRuntime().addShutdownHook(new Thread(this::close));
        }
    }

    /**
     * Close server and remove temp directory
     */
    @Override
    public void close() {
        try {
            zkServer.close();
        } catch (Exception e) {
            logger.error("Failed to close zk testing server", e);
        }

        try {
            Files.deleteIfExists(tmpDir);
        } catch (Exception e) {
            logger.error("Failed to delete {}", tmpDir, e);
        }
    }


    public ZKTestingServer start() throws Exception {
        init();
        uuid = UUID.randomUUID().toString();

        CuratorFramework client = createClient("");
        client.create().forPath("/" + uuid);
        client.close();

        curatorFramework = createClient();

        return this;
    }

    public int getPort() {
        return port;
    }

    public TestingServer getZkServer() {
        return zkServer;
    }

    /**
     * Creates new client. Users should manually close this client.
     *
     * @return fully initialized curator framework
     */
    public CuratorFramework createClient() {
        return createClient(uuid);
    }

    public CuratorFramework createClient(String connectionString, int sessionTimeoutMs,
                                         int connectionTimeoutMs, int maxRetrySleepMs) {
        return createClient(connectionString, uuid, sessionTimeoutMs, connectionTimeoutMs, maxRetrySleepMs);
    }

    private CuratorFramework createClient(String root) {
        return createClient(zkServer.getConnectString(), root, 60_000,
                15_000, Integer.MAX_VALUE);
    }

    private CuratorFramework createClient(String connectionString, String root, int sessionTimeoutMs,
                                          int connectionTimeoutMs, int maxRetrySleepMs) {
        CuratorFramework newClient = CuratorFrameworkFactory.builder()
                .connectString(connectionString + "/" + root)
                .retryPolicy(new ExponentialBackoffRetry(1000, 10, maxRetrySleepMs))
                .sessionTimeoutMs(sessionTimeoutMs)
                .connectionTimeoutMs(connectionTimeoutMs)
                .build();
        newClient.start();
        return newClient;
    }

    /**
     * Managed client for this server
     *
     * @return pre-created and initialized curator framework
     */
    public CuratorFramework getClient() {
        return curatorFramework;
    }

}
