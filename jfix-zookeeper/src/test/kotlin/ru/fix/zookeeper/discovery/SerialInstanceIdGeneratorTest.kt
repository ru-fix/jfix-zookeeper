package ru.fix.zookeeper.discovery

import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import java.lang.AssertionError

internal class SerialInstanceIdGeneratorTest {
    private lateinit var idGenerator: InstanceIdGenerator

    @BeforeEach
    fun setUp() {
        idGenerator = SerialInstanceIdGenerator(127)
    }

    @Test
    fun `generate next instance id, when no registered instance ids`() {
        val instanceId = idGenerator.nextId(listOf())
        assertEquals("1", instanceId)
    }

    @ValueSource(ints = [1, 10, 100])
    @ParameterizedTest
    fun `generate next instance id, when ${count} instances is already initiated`(count: Int) {
        val instanceIds = (1..count).map { it.toString() }.toList()
        val instanceId = idGenerator.nextId(instanceIds)
        assertEquals(count.plus(1).toString(), instanceId)
    }

    @Test
    fun `next instance id should be greater by 1 than max instance id that already initiated`() {
        val instanceId = idGenerator.nextId(listOf("20"))
        assertEquals("21", instanceId)
    }

    @Test
    fun `instance id not greater limit of instance id`() {
        var instanceId: String? = null
        Assertions.assertThrows(AssertionError::class.java) {
            instanceId = idGenerator.nextId(listOf("127"))
        }
        Assertions.assertNull(instanceId)
    }
}
