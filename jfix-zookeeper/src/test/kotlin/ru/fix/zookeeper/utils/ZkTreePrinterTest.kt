package ru.fix.zookeeper.utils

import io.kotest.matchers.string.shouldBeEmpty
import io.kotest.matchers.string.shouldContain
import io.kotest.matchers.string.shouldNotContain
import org.junit.jupiter.api.*
import ru.fix.zookeeper.testing.ZKTestingServer

internal class ZkTreePrinterTest{

    lateinit var zkServer: ZKTestingServer

    @BeforeEach
    fun startZkTestingServer() {
        zkServer = ZKTestingServer()
                .withCloseOnJvmShutdown(true)
                .start()
    }

    @AfterEach
    fun stopZkTestingServer() {
        zkServer.close()
    }

    @Test
    fun `print empty root`() {
        val dump = ZkTreePrinter(zkServer.client).print("/")
        println(dump)
        dump.trim().shouldBeEmpty()
    }

    @Test
    fun `print root with two children`() {
        for(path in listOf("/child1", "/child2")){
            zkServer.client.create().creatingParentsIfNeeded().forPath("$path")
        }
        val dump = ZkTreePrinter(zkServer.client).print("/")
        println(dump)
        dump.shouldContain("child1")
        dump.shouldContain("child2")
    }

    @Test
    fun `print root with twice deep node`() {
        zkServer.client.create().creatingParentsIfNeeded().forPath("/parent/child")
        val dump = ZkTreePrinter(zkServer.client).print("/")
        println(dump)
        dump.shouldContain("parent")
        dump.shouldContain("child")
    }

    @Test
    fun `print subtree`() {
        zkServer.client.create().creatingParentsIfNeeded().forPath("/parent/child")
        val dump = ZkTreePrinter(zkServer.client).print("/parent")
        println(dump)
        dump.shouldNotContain("parent")
        dump.shouldContain("child")
    }

    @Test
    fun `print empty root with data`() {
        val dump = ZkTreePrinter(zkServer.client).print("/", true)
        println(dump)
        dump.trim().shouldBeEmpty()
    }

    @Test
    fun `print root with two children with data`() {
        zkServer.client.create().creatingParentsIfNeeded().forPath("/child1")
        zkServer.client.create().creatingParentsIfNeeded().forPath("/child2", "child2-data".toByteArray())

        val dump = ZkTreePrinter(zkServer.client).print("/", true)
        println(dump)
        dump.shouldContain("child1")
        dump.shouldContain("child2")
        dump.shouldContain("child2-data")
    }

    @Test
    fun `print root with twice deep node with data`() {
        zkServer.client.create().creatingParentsIfNeeded().forPath("/parent/child", "data".toByteArray())
        val dump = ZkTreePrinter(zkServer.client).print("/", true)
        println(dump)
        dump.shouldContain("parent")
        dump.shouldContain("child")
        dump.shouldContain("data")
    }

    @Test
    fun `print subtree with data`() {
        zkServer.client.create().creatingParentsIfNeeded().forPath("/parent/child", "data".toByteArray())
        val dump = ZkTreePrinter(zkServer.client).print("/parent", true)
        println(dump)
        dump.shouldNotContain("parent")
        dump.shouldContain("child")
        dump.shouldContain("data")
    }
}