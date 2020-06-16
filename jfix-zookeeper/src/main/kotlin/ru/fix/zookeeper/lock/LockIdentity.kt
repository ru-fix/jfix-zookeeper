package ru.fix.zookeeper.lock

import org.apache.curator.utils.PathUtils
import java.util.*


data class LockIdentity(
        val nodePath: String,
        val metadata: String? = null
) : Comparable<LockIdentity> {

    init {
        PathUtils.validatePath(nodePath)
    }

    override fun compareTo(other: LockIdentity) = compareValuesBy(this, other, { it.nodePath })

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as LockIdentity

        return other.nodePath == this.nodePath
    }

    override fun hashCode(): Int = Objects.hash(nodePath)
}
