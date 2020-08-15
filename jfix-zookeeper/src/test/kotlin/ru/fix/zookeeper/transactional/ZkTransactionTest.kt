package ru.fix.zookeeper.transactional

import org.apache.curator.framework.CuratorFramework
import org.apache.zookeeper.KeeperException
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import ru.fix.zookeeper.testing.ZKTestingServer

import java.util.ArrayList
import java.util.HashSet
import java.util.List
import java.util.Set
import java.util.concurrent.*

import static org.junit.jupiter.api.Assertions.*


class ZkTransactionTest {

    lateinit var zkServer: ZKTestingServer

    @BeforeEach
    fun startZkServer(){
        zkServer = new ZKTestingServer()
                .withCloseOnJvmShutdown();
        zkServer.start();
    }

    @AfterEach
    fun stopZkServer(){
        zkServer.close();
    }


    @Test
    fun deletePathWithChildrenIfNeeded(){
        zkServer.client.create().creatingParentsIfNeeded().forPath("/1/2/3/4/5/6/7");

        ZkTransaction.createTransaction(zkServer.client)
                .deletePathWithChildrenIfNeeded("/1/2/3")
                .deletePathWithChildrenIfNeeded("/1/2")
                .commit();

        assertNull(zkServer.client.checkExists().forPath("/1/2"));
        assertNotNull(zkServer.client.checkExists().forPath("/1"));
    }

    @Test
    fun createPathWithParentsIfNeeded() {
        zkServer.client.create().creatingParentsIfNeeded().forPath("/2/03");

        ZkTransaction.createTransaction(zkServer.client)
                .createPathWithParentsIfNeeded("/1/01/001")
                .checkPath("/1/01/001")
                .createPathWithParentsIfNeeded("/1/01/001/0001/00001")
                .createPathWithParentsIfNeeded("/2/03/003")
                .commit();

        assertNotNull(zkServer.client.checkExists().forPath("/1/01/001"));
        assertNotNull(zkServer.client.checkExists().forPath("/1/01/001/0001/00001"));
        assertNotNull(zkServer.client.checkExists().forPath("/2/03/003"));
    }

    @Test
    fun `createPathWithParentsIfNeeded throws NodeExistsException if node already exist`() {
        zkServer.client.create().creatingParentsIfNeeded().forPath("/2/03");
        assertThrows(
                KeeperException.NodeExistsException.class,
                { ZkTransaction.createTransaction(zkServer.client)
                    .createPathWithParentsIfNeeded("/2/03")
                    .commit()
                }
        );
    }

    @Test
    public void testCheckPathWithVersion() throws Exception {
        ZkTransaction.createTransaction(zkServer.client)
                .createPathWithParentsIfNeeded("/1/01/001")
                .checkPath("/1/01/001")
                .checkPathWithVersion("/1/01/001", 0)
                .setData("/1/01/001", new byte[]{101})
                .checkPathWithVersion("/1/01/001", 1)
                .commit();

        assertNotNull(zkServer.client.checkExists().forPath("/1/01/001"));
        assertArrayEquals(new byte[]{101}, zkServer.client.getData().forPath("/1/01/001"));
    }

    @Test
    public void testCheckPathWithIncorrectVersion() throws Exception {
        assertThrows(
                KeeperException.BadVersionException.class,
                () -> ZkTransaction.createTransaction(zkServer.client)
                        .createPathWithParentsIfNeeded("/1/01/001")
                        .checkPath("/1/01/001")
                        .checkPathWithVersion("/1/01/001", 0)
                        .setData("/1/01/001", new byte[]{101})
                        .checkPathWithVersion("/1/01/001", 2)
                        .commit()
        );
    }

    /**
     * Test case for unsupported delete and create operations mix
     */
    @Test
    public void testMixedCreateDelete_Failure() throws Exception {
        zkServer.client.create()
                .creatingParentsIfNeeded()
                .forPath("/1/2/3/4/5");

        assertThrows(
                KeeperException.NoNodeException.class,
                () -> ZkTransaction.createTransaction(zkServer.client)
                        .deletePathWithChildrenIfNeeded("/1/2/3")
                        .createPathWithParentsIfNeeded("/1/2/3/4")
                        .commit()
        );
    }


    @Test
    public void check_version_WHEN_multiply_updating_THEN_only_one_succeed() throws Exception {
        String lockPath = "/lock";
        CuratorFramework curator = zkServer.client;
        curator.create().creatingParentsIfNeeded().forPath(lockPath);

        Set<String> expectedNodes = new CopyOnWriteArraySet<>();
        expectedNodes.add("lock");

        int quantityOfTransactions = 10;
        CountDownLatch countDownLatch = new CountDownLatch(quantityOfTransactions);
        List<CompletableFuture<Void>> futures = new ArrayList<>(quantityOfTransactions);
        ExecutorService executor = Executors.newFixedThreadPool(quantityOfTransactions);

        for (int i = 0; i < quantityOfTransactions; i++) {
            final int transactionNumber = i;
            futures.add(CompletableFuture.runAsync(() -> {
                try {
                    ZkTransaction.tryCommit(zkServer.client, 1, transaction -> {
                        transaction.checkAndUpdateVersion(lockPath);
                        transaction.createPath("/" + transactionNumber);
                        countDownLatch.countDown();
                        countDownLatch.await();
                    });
                    expectedNodes.add(String.valueOf(transactionNumber));
                } catch (Exception ignored) {
                }
            }, executor));
        }
        CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new)).join();
        executor.shutdown();

        assertEquals(expectedNodes.size(), 2);
        assertEquals(expectedNodes, new HashSet<>(curator.getChildren().forPath("/")));
    }

    @Test
    public void transaction_with_only_changeAndUpdateVersionOnChange_does_nothing(){
        ZkTransaction.tryCommit(zkServer.client, 1, tx->{
        })
    }

    @Test
    public void transaction_with_creatPath_that_already_created_does_nothing(){
        ZkTransaction.tryCommit(zkServer.client, 1, tx->{
        })
    }

}