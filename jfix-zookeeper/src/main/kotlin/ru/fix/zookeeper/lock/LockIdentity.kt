package ru.fix.zookeeper.lock

import org.apache.curator.utils.PathUtils


data class LockIdentity(
        val nodePath: String,
        val metadata: String? = null
) : Comparable<LockIdentity> {

    init {
        PathUtils.validatePath(nodePath)
    }

    override fun compareTo(other: LockIdentity) = compareValuesBy(this, other, { it.nodePath }, { it.nodePath })

}
