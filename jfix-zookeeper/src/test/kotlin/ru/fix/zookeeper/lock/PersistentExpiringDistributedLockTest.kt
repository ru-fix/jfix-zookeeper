package ru.fix.zookeeper.lock

import io.kotest.assertions.throwables.shouldThrow
import io.kotest.matchers.booleans.shouldBeFalse
import io.kotest.matchers.booleans.shouldBeTrue
import io.kotest.matchers.string.shouldContain
import org.awaitility.Awaitility.await
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.junit.jupiter.api.parallel.Execution
import org.junit.jupiter.api.parallel.ExecutionMode
import ru.fix.zookeeper.testing.ZKTestingServer
import ru.fix.zookeeper.utils.ZkTreePrinter
import java.nio.charset.StandardCharsets
import java.time.Duration
import java.util.concurrent.atomic.AtomicInteger
import java.util.function.Supplier

@Execution(ExecutionMode.CONCURRENT)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
internal class PersistentExpiringDistributedLockTest {
    val LOCKS_PATH = "/locks"

    lateinit var zkServer: ZKTestingServer
    val idSequence = object : Supplier<String> {
        val counter = AtomicInteger()
        override fun get(): String = counter.incrementAndGet().toString()
    }

    @BeforeAll
    fun startZkTestingServer() {
        zkServer = ZKTestingServer()
                .withCloseOnJvmShutdown(true)
                .start()
    }

    @AfterAll
    fun stopZkTestingServer() {
        zkServer.close()
    }

    @Test
    fun `lock in zookeeper is a node with text data that contains uid`() {
        val id = idSequence.get()
        val lock = PersistentExpiringDistributedLock(
                zkServer.client,
                LockIdentity(id,
                        "$LOCKS_PATH/id",
                        data = null))

        lock.expirableAcquire(Duration.ofMinutes(1), Duration.ofMillis(1)).shouldBeTrue()

        println(ZkTreePrinter(zkServer.client).print("/", true))

        val data = zkServer.client.data.forPath("$LOCKS_PATH/$id").toString(StandardCharsets.UTF_8)
        println(data)
        data.shouldContain("version")

        lock.close()
    }


    @Test
    fun `single actor acquires and unlocks twice`() {
        val lock = createLock()
        lock.expirableAcquire(Duration.ofMinutes(1), Duration.ofMillis(1)).shouldBeTrue()
        lock.release()

        lock.expirableAcquire(Duration.ofMinutes(1), Duration.ofMillis(1)).shouldBeTrue()
        lock.release()
    }

    @Test
    fun `single actor acquires twice`() {
        val lock = createLock()
        lock.expirableAcquire(Duration.ofMinutes(1), Duration.ofMillis(1)).shouldBeTrue()
        lock.expirableAcquire(Duration.ofMinutes(1), Duration.ofMillis(1)).shouldBeTrue()
        lock.release()
    }

    @Test
    fun `two actors acquire same lock, only one succeed`() {
        val id = idSequence.get()

        val lock1 = createLock(id)
        val lock2 = createLock(id)

        lock1.expirableAcquire(Duration.ofMinutes(1), Duration.ofMillis(1)).shouldBeTrue()
        lock2.expirableAcquire(Duration.ofMinutes(1), Duration.ofMillis(1)).shouldBeFalse()
        lock1.close()
        lock2.expirableAcquire(Duration.ofMillis(1), Duration.ofMillis(1)).shouldBeTrue()
        lock2.close()
    }

    @Test
    fun `killing actor1 lock node in zk effectively release it for actor2`() {
        val id = idSequence.get()

        val lock1 = createLock(id)
        val lock2 = createLock(id)

        lock1.expirableAcquire(Duration.ofMinutes(1), Duration.ofMillis(1)).shouldBeTrue()

        println(ZkTreePrinter(zkServer.client).print("/", true))

        zkServer.client.delete().forPath("$LOCKS_PATH/$id")

        lock2.expirableAcquire(Duration.ofMinutes(1), Duration.ofMillis(1)).shouldBeFalse()

        lock1.close()
        lock2.close()
    }

    @Test
    fun `killing active lock node in zk leads to exception in lock release`() {
        val id = idSequence.get()
        val lock = createLock(path = "$LOCKS_PATH/$id")
        lock.expirableAcquire(Duration.ofMillis(1), Duration.ofMillis(1))
        zkServer.client.delete().forPath("$LOCKS_PATH/$id")

        shouldThrow<Exception>{
            lock.release()
        }.message.shouldContain("expired")
    }

    @Test
    fun `expired lock in zk successfully overwritten and acquired by new lock`() {
        val lock1 = createLock()
        val lock2 = createLock()

        lock1.expirableAcquire(Duration.ofMillis(1), Duration.ofMillis(1)).shouldBeTrue()
        await().until { lock2.expirableAcquire(Duration.ofMillis(1), Duration.ofMillis(1)) }
    }

    @Test
    fun `successfull prolong of active lock`(){

    }

    @Test
    fun `failed prolongation of active alien lock`(){

    }

    @Test
    fun `failed prolongation of expired alien lock`(){

    }

    @Test
    fun `failed prolongation of own expired lock`(){

    }

    private fun createLock(id: String = idSequence.get(),
                           path: String = "$LOCKS_PATH/$id",
                           data: String? = null): PersistentExpiringDistributedLock {
        return PersistentExpiringDistributedLock(
                zkServer.client,
                LockIdentity(id,
                        path,
                        data))
    }
}