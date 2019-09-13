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
 * -Run test zk server
 * -Create client CuratorFramework
 *
 * Example of using:
 *
 * -Common way:
 *
 * ZkTestingServer server = new ZkTestingServer();
 * server.start();
 *
 * -In Junit5:
 *
 * private ZkTestingServer server;
 *
 * `@BeforeEach`
 * public void setUp() {
 *     server = new ZkTestingServer();
 *     server.start();
 * }
 *  or
 *
 * private ZkTestingServer server = new ZkTestingServer();
 * `@BeforeAll`
 * public void setUp() {
 *      server.start();
 * }
 * `
 */
public class ZKTestingServer {

    private static final Logger log = getLogger(ZKTestingServer.class);

    private TestingServer zkServer;
    private Path tmpDir;
    private int port;

    private CuratorFramework curatorFramework;
    private String uuid;

    private boolean closeOnJvmShutdown = true;
    private ZKTestingServer withCloseOnJvmShutdown(boolean closeOnJvmShutdown){
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
                log.debug("Failed to create zk testing server", e);
            }
        }


        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                zkServer.close();
            } catch (Exception e) {
                log.error("Failed to close zk testing server", e);
            }

            try {
                Files.deleteIfExists(tmpDir);
            } catch (Exception e) {
                log.error("Failed to delete {}", tmpDir, e);
            }
        }));
    }


    public void start() throws Exception {
        init();
        uuid = UUID.randomUUID().toString();

        CuratorFramework client = createClient("");
        client.create().forPath("/" + uuid);
        client.close();

        curatorFramework = createClient();
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
