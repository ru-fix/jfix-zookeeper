package ru.fix.zookeeper.lock

import java.time.Duration

data class PersistentExpiringLockManagerConfig(
        /**
         *  For how long to acquire the lock.
         *  @see PersistentExpiringDistributedLock#expirableAcquire
         */
        val lockAcquirePeriod: Duration = Duration.ofSeconds(90),
        /**
         * How many time can be spend during acquiring attempt
         * @see PersistentExpiringDistributedLock#expirableAcquire
         */
        val acquiringTimeout: Duration = Duration.ofSeconds(3),
        /**
         * If lock is going to expire after this period then lock will be prolonged
         * @see PersistentExpiringDistributedLock#checkAndProlongIfExpiresIn
         */
        val expirationPeriod: Duration = Duration.ofSeconds(30),
        /**
         * How often manager will check locks for theirs expirationPeriod
         */
        val lockCheckAndProlongInterval: Duration = Duration.ofSeconds(10)
)