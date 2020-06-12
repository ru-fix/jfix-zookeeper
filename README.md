# jfix-zookeeper
Provides handy functionality for transactions, persistent locks and testing  
[![Maven Central](https://img.shields.io/maven-central/v/ru.fix/jfix-zookeeper.svg)](http://search.maven.org/#search%7Cga%7C1%7Cg%3A%22ru.fix%22)

## Persistent Lock
Zookeeper ephemeral nodes die whe session lost.  
But what if there was temporal problem in the network.  
With `PersistentExpiringDistributedLock` you can fix that problem
by using persistent Zookeeper nodes that contain locks expiration timestamp inside.  
```kotlin
val curatorFramework = zkServer.client
val lock = PersistentExpiringDistributedLock(
        curatorFramework, 
        LockIdentity("/locks/resource1", "my-service resource1 optional metadata")
)
if(lock.expirableAcquire(Duration.ofSeconds(60), Duration.ofSeconds(3))){
    //Do some work that takes less that 60 seconds.
    //It is ok if you temporary lose connection to zookeeper and your session timeouts.
    //As long as you get your connectoin back at the moment of lock prolongation. 
    if(lock.checkAndProlong(Duration.ofSeconds(30))){
        //do some other work, now you have 30 seconds before lock is expired
    }
}
lock.close()
//now other process can acquire same lock
```

## Transactions
`ZkTransaction` simplifies atomic change that involves many zookeeper nodes 
```java
int ATTEMPTS = 3
var operationsResults = ZkTransaction.tryCommit(curatorFramework, ATTEMPTS, transaction -> {
        transaction.checkAndUpdateVersion("/tree/version");
        transaction.createPath("/tree/new-child");
        transaction.deletePath("/tree/old-child");
    });
```
## Testing
Setup embedded Zookeeper Server for  `@Before` and `@After` testing stages. 
```kotlin
val server = ZkTestingServer()
     .withCloseOnJvmShutdown()
     .start();
// -- run tests --
server.close();
```
This way you can test for network problem
```kotlin
val proxyTcpCrusher = zkServer.openProxyTcpCrusher()
val zkProxyClient = zkServer.createZkProxyClient(proxyTcpCrusher)
val zkProxyState = zkServer.startWatchClientState(zkProxyClient)

// do something with CuratorFramework zkProxyClient

proxyTcpCrusher.close()

// do something after connection lost for CuratorFramework zkProxyClient

await().atMost(10, MINUTES).until {
    zkProxyState.get() == ConnectionState.SUSPENDED ||
    zkProxyState.get() == ConnectionState.LOST
}
```