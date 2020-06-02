package ru.fix.zookeeper.lock

data class LockIdentity(
        val nodePath: String,
        val metadata: String? = null
) : Comparable<LockIdentity> {


    override fun compareTo(other: LockIdentity) = compareValuesBy(this, other, { it.nodePath }, { it.nodePath })

}
