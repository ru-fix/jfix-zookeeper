package ru.fix.zookeeper.discovery

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import ru.fix.zookeeper.AbstractZookeeperTest

internal class SerialInstanceIdGeneratorTest : AbstractZookeeperTest() {
    private lateinit var idGenerator: InstanceIdGenerator
    private lateinit var registrationPath: String

    @BeforeEach
    fun setUpSecond() {
        registrationPath = "$rootPath/services"
        idGenerator = SerialInstanceIdGenerator(testingServer.createClient(), registrationPath)
        testingServer.client.create().orSetData()
                .creatingParentsIfNeeded()
                .forPath(registrationPath)
    }

    @ValueSource(ints = [0, 1, 10, 100])
    @ParameterizedTest
    fun `generate next instance id, when ${count} instances is already initiated`(count: Int) {
        initInstanceIds(count)
        val instanceId = idGenerator.nextId()
        assertEquals(count.plus(1).toString(), instanceId)
    }

    @Test
    fun `next instance id should be greater by 1 than max instance id that already initiated`() {
        initInstanceIds(20)
        testingServer.client
                .create()
                .forPath("$registrationPath/200")
        val instanceId = idGenerator.nextId()
        assertEquals("201", instanceId)
    }

    private fun initInstanceIds(count: Int) {
        (1..count).forEach {
            testingServer.client
                    .create()
                    .forPath("$registrationPath/$it")
        }
    }
}
