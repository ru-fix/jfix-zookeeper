package ru.fix.zookeeper.transactional

import org.apache.curator.framework.recipes.cache.ChildData
import org.apache.curator.framework.recipes.cache.CuratorCache
import org.apache.curator.framework.recipes.cache.CuratorCacheListener
import org.apache.logging.log4j.kotlin.Logging
import org.apache.zookeeper.KeeperException
import org.awaitility.Awaitility.await
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import ru.fix.zookeeper.testing.ZKTestingServer
import java.lang.Thread.sleep

import java.util.ArrayList
import java.util.concurrent.*

import java.util.concurrent.atomic.AtomicBoolean


class ZkTransactionTest {
    companion object : Logging

    lateinit var zkServer: ZKTestingServer

    @BeforeEach
    fun startZkServer() {
        zkServer = ZKTestingServer()
                .withCloseOnJvmShutdown();
        zkServer.start();
    }

    @AfterEach
    fun stopZkServer() {
        zkServer.close();
    }


    @Test
    fun deletePathWithChildrenIfNeeded() {
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
        assertThrows<KeeperException.NodeExistsException> {
            ZkTransaction.createTransaction(zkServer.client)
                    .createPathWithParentsIfNeeded("/2/03")
                    .commit()
        }
    }

    @Test
    fun checkPathWithVersion() {
        ZkTransaction.createTransaction(zkServer.client)
                .createPathWithParentsIfNeeded("/1/01/001")
                .checkPath("/1/01/001")
                .checkPathWithVersion("/1/01/001", 0)
                .setData("/1/01/001", byteArrayOf(101))
                .checkPathWithVersion("/1/01/001", 1)
                .commit()

        assertNotNull(zkServer.client.checkExists().forPath("/1/01/001"))
        assertArrayEquals(byteArrayOf(101), zkServer.client.getData().forPath("/1/01/001"))
    }

    @Test
    fun checkPathWithIncorrectVersion() {
        assertThrows<KeeperException.BadVersionException> {
            ZkTransaction.createTransaction(zkServer.client)
                    .createPathWithParentsIfNeeded("/1/01/001")
                    .checkPath("/1/01/001")
                    .checkPathWithVersion("/1/01/001", 0)
                    .setData("/1/01/001", byteArrayOf(101))
                    .checkPathWithVersion("/1/01/001", 2)
                    .commit()
        }
    }

    /**
     * Test case for unsupported delete and create operations mix
     */
    @Test
    fun `Mixed create or delete with parents or children fails`() {
        zkServer.client.create()
                .creatingParentsIfNeeded()
                .forPath("/1/2/3/4/5");

        assertThrows<KeeperException.NoNodeException> {
            ZkTransaction.createTransaction(zkServer.client)
                    .deletePathWithChildrenIfNeeded("/1/2/3")
                    .createPathWithParentsIfNeeded("/1/2/3/4")
                    .commit()
        }
    }


    @Test
    fun `with checkVersion only one of multiple updating transactions succeed`() {
        val lockPath = "/lock";
        val curator = zkServer.client;
        curator.create().creatingParentsIfNeeded().forPath(lockPath);

        val expectedNodes = CopyOnWriteArraySet<String>();
        expectedNodes.add("lock");

        val quantityOfTransactions = 10;
        val countDownLatch = CountDownLatch(quantityOfTransactions);
        val futures = ArrayList<CompletableFuture<Void>>(quantityOfTransactions);
        val executor = Executors.newFixedThreadPool(quantityOfTransactions);

        for (transactionNumber in (0 until quantityOfTransactions)) {
            futures.add(CompletableFuture.runAsync(Runnable {
                try {
                    ZkTransaction.tryCommit(zkServer.client, 1) { transaction ->
                        transaction.readVersionThenCheckAndUpdateInTransaction(lockPath);
                        transaction.createPath("/$transactionNumber");
                        countDownLatch.countDown();
                        countDownLatch.await();
                    };
                    expectedNodes.add(transactionNumber.toString());
                } catch (ignored: Exception) {
                }
            }, executor))
        }
        CompletableFuture.allOf(*futures.toTypedArray()).join();
        executor.shutdown();

        assertEquals(expectedNodes.size, 2);
        assertEquals(expectedNodes, curator.children.forPath("/").toHashSet());
    }

    @Test
    fun `transaction with only changeAndUpdateVersionOnChange does nothing`() {
        val curator = zkServer.client
        curator.create().creatingParentsIfNeeded().forPath("/version")

        val changeInZkOccurred = AtomicBoolean()
        val zkListener = CuratorCache.build(curator, "/")
        zkListener.listenable().addListener(CuratorCacheListener { type: CuratorCacheListener.Type,
                                                                   oldData: ChildData?,
                                                                   newData: ChildData? ->
            logger.info("Change $type, ${oldData?.path}, ${newData?.path}")
            changeInZkOccurred.set(true)
        })
        zkListener.start()
        sleep(1000)

        changeInZkOccurred.set(false)
        ZkTransaction.tryCommit(curator, 1) { tx ->
            tx.readVersionThenCheckAndUpdateInTransactionIfItMutatesZkState("/version")
        }
        sleep(1000)
        assertFalse(changeInZkOccurred.get())


        changeInZkOccurred.set(false)
        ZkTransaction.tryCommit(curator, 1){ tx ->
            tx.readVersionThenCheckAndUpdateInTransactionIfItMutatesZkState("/version")
            tx.createPath("/new-path")
        }
        await().until { changeInZkOccurred.get() }

        changeInZkOccurred.set(false)
        ZkTransaction.tryCommit(curator, 1) { tx ->
            tx.readVersionThenCheckAndUpdateInTransactionIfItMutatesZkState("/version")
            tx.setData("/new-path", byteArrayOf(42))
        }
        await().until{ changeInZkOccurred.get() }


        changeInZkOccurred.set(false)
        ZkTransaction.tryCommit(curator, 1) { tx ->
            tx.readVersionThenCheckAndUpdateInTransactionIfItMutatesZkState("/version")
            assertNotNull(curator.checkExists().forPath("/new-path"))
        }
        sleep(1000)
        assertFalse(changeInZkOccurred.get())


        changeInZkOccurred.set(false)
        ZkTransaction.tryCommit(curator, 1){ tx ->
            tx.readVersionThenCheckAndUpdateInTransactionIfItMutatesZkState("/version")
            tx.deletePath("/new-path")
        }
        await().until { changeInZkOccurred.get() }
    }
}