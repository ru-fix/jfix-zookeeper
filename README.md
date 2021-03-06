# jfix-zookeeper
Provides handy functionality for transactions, persistent locks and testing  
[![Maven Central](https://img.shields.io/maven-central/v/ru.fix/jfix-zookeeper.svg)](http://search.maven.org/#search%7Cga%7C1%7Cg%3A%22ru.fix%22)

## Persistent Lock
To implement locks in Zookeeper you can use ephemeral nodes. 
They exist as long as your zookeeper session alive and die on session loss.
For some cases this is actually what you need. 
 
If there was a temporal connectivity loss in the network.
Your application will lose zookeeper session and all ephemeral nodes will be gone.
Immediately after that another application can acquire same lock if it was based on ephemeral nodes. 
But what if previous owner of the lock did not complete it's work on time. 
   
With `PersistentExpiringDistributedLock` you can fix that problem
by using persistent Zookeeper nodes that contain locks expiration timestamp inside. Persisten Zookeeper nodes does not dies on session loss. Lock leasing time is decoupled from zookeeper session timeout.
  
```kotlin
val curatorFramework: CuratorFramework = ... 
val lock = PersistentExpiringDistributedLock(
        curatorFramework, 
        LockIdentity("/locks/resource1", "my-service resource1 optional metadata")
)
if(lock.expirableAcquire(Duration.ofSeconds(60), Duration.ofSeconds(3))){
    //Do some work that takes less that 60 seconds.
    //It is ok if you temporary lose connection to zookeeper and your session timeouts.
    //As long as you get your connection back at the moment of lock prolongation. 
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
## Embedded Zookeeper Testing Server
Setup embedded Zookeeper Server in `@Before` and `@After` testing stages. 
```kotlin
val server = ZkTestingServer()
     .withCloseOnJvmShutdown()
     .start();
// -- run tests --
server.close();
```
This way you can test for network problems between you application and zookeeper
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

## ServiceInstanceIdRegistry
Allows receiving unique identifiers for each service instance in the cluster. 

Suppose you have 5 applications. 
All of them sending events. 
Each event should have unique identifier of an application that sent it. 

 
```kotlin
val registry = ServiceInstanceIdRegistry(
            curatorFramework = ...,
            instanceIdGenerator = MinFreeInstanceIdGenerator(10),
            serviceRegistrationPath = "/my-registry")
```
```kotlin
//in application A
val uniqueId = registry.register("my-service")

//1
println(uniqueId)

sendEvent(Event(applicationId = uniqueId))
```
```kotlin
//in application B
val unique = registry.register("my-service")

//2
println(unique)

sendEvent(Event(applicationId = uniqueId))
```
Id acquireing techniqe usefull for application instance identification in  
DistributedJobManager https://github.com/ru-fix/distributed-job-manager  
AtomicIdGenerator https://github.com/ru-fix/jfix-stdlib


