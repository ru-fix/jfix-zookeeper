package ru.fix.zookeeper.lock

import java.time.Duration

data class PersistentExpiringLockManagerConfig(
        /**
         *  How long to hold lock
         */
        val reservationPeriod: Duration = Duration.ofSeconds(60),
        /**
         * how long lock acquiring can be performed
         */
        val acquiringTimeout: Duration = Duration.ofSeconds(2),
        /**
         * time for checking lock expiration.
         * If lock expires after lock.expirationTime + expiration period,
         * then prolong this lock, else do nothing
         */
        val expirationPeriod: Duration = Duration.ofSeconds(20),
        /**
         * how often to prolong locks
         */
        val lockProlongationInterval: Duration = Duration.ofSeconds(10)
)