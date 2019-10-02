package ru.fix.zookeeper.transactional;

import org.apache.zookeeper.KeeperException;
import org.junit.Before;
import org.junit.Test;
import ru.fix.zookeeper.testing.ZKTestingServer;

import static org.junit.Assert.*;

public class TransactionalClientIT {

    private ZKTestingServer zkTestingServer;

    @Before
    public void setUp() throws Exception {
        zkTestingServer = new ZKTestingServer()
                .withCloseOnJvmShutdown(true);
        zkTestingServer.start();
    }


    @Test
    public void testDeletePathWithChildrenIfNeeded() throws Exception {
        zkTestingServer.getClient().create().creatingParentsIfNeeded().forPath("/1/2/3/4/5/6/7");

        TransactionalClient.createTransaction(zkTestingServer.getClient())
                .deletePathWithChildrenIfNeeded("/1/2/3")
                .deletePathWithChildrenIfNeeded("/1/2")
                .commit();

        assertNull(zkTestingServer.getClient().checkExists().forPath("/1/2"));
        assertNotNull(zkTestingServer.getClient().checkExists().forPath("/1"));
    }

    @Test
    public void testCreatePathWithParentsIfNeeded() throws Exception {
        zkTestingServer.getClient().create().creatingParentsIfNeeded().forPath("/2/03");

        TransactionalClient.createTransaction(zkTestingServer.getClient())
                .createPathWithParentsIfNeeded("/1/01/001")
                .checkPath("/1/01/001")
                .createPathWithParentsIfNeeded("/1/01/001/0001/00001")
                .createPathWithParentsIfNeeded("/2/03/003")
                .commit();

        assertNotNull(zkTestingServer.getClient().checkExists().forPath("/1/01/001"));
        assertNotNull(zkTestingServer.getClient().checkExists().forPath("/1/01/001/0001/00001"));
        assertNotNull(zkTestingServer.getClient().checkExists().forPath("/2/03/003"));
    }

    @Test(expected = KeeperException.NodeExistsException.class)
    public void testCreatePathWithParentsIfNeeded_ForExistingNode() throws Exception {
        zkTestingServer.getClient().create().creatingParentsIfNeeded().forPath("/2/03");
        TransactionalClient.createTransaction(zkTestingServer.getClient())
                .createPathWithParentsIfNeeded("/2/03")
                .commit();
    }

    @Test
    public void testCheckPathWithVersion() throws Exception {
        TransactionalClient.createTransaction(zkTestingServer.getClient())
                .createPathWithParentsIfNeeded("/1/01/001")
                .checkPath("/1/01/001")
                .checkPathWithVersion("/1/01/001", 0)
                .setData("/1/01/001", new byte[]{101})
                .checkPathWithVersion("/1/01/001", 1)
                .commit();

        assertNotNull(zkTestingServer.getClient().checkExists().forPath("/1/01/001"));
        assertArrayEquals(new byte[]{101}, zkTestingServer.getClient().getData().forPath("/1/01/001"));
    }

    @Test(expected = KeeperException.BadVersionException.class)
    public void testCheckPathWithIncorrectVersion() throws Exception {
        TransactionalClient.createTransaction(zkTestingServer.getClient())
                .createPathWithParentsIfNeeded("/1/01/001")
                .checkPath("/1/01/001")
                .checkPathWithVersion("/1/01/001", 0)
                .setData("/1/01/001", new byte[]{101})
                .checkPathWithVersion("/1/01/001", 2)
                .commit();
    }

    /**
     * Test case for unsupported delete and create operations mix
     */
    @Test(expected = KeeperException.NoNodeException.class)
    public void testMixedCreateDelete_Failure() throws Exception {
        zkTestingServer.getClient().create().creatingParentsIfNeeded().forPath("/1/2/3/4/5");

        TransactionalClient.createTransaction(zkTestingServer.getClient())
                .deletePathWithChildrenIfNeeded("/1/2/3")
                .createPathWithParentsIfNeeded("/1/2/3/4")
                .commit();
    }


}