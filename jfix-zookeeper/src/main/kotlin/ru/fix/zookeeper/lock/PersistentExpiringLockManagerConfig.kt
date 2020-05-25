package ru.fix.zookeeper.lock

import java.time.Duration

data class PersistentExpiringLockManagerConfig(
        val reservationPeriod: Duration = Duration.ofSeconds(3),
        val acquiringTimeout: Duration = Duration.ofSeconds(3),
        val expirationPeriod: Duration = Duration.ofSeconds(0),
        val lockProlongationInterval: Duration = Duration.ofSeconds(2)
)