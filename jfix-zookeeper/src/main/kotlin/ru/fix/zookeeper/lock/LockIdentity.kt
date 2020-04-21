package ru.fix.zookeeper.lock

data class LockIdentity(
        val id: String,
        val nodePath: String
) : Comparable<LockIdentity> {

    override fun compareTo(other: LockIdentity) = compareValuesBy(this, other, { it.id }, { it.id })

}
