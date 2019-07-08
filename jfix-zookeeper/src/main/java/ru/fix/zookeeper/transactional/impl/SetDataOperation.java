package ru.fix.zookeeper.transactional.impl;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.transaction.CuratorTransaction;
import org.apache.curator.framework.api.transaction.CuratorTransactionBridge;

public class SetDataOperation extends AbstractPathOperation {

    private byte[] data;

    public SetDataOperation(CuratorFramework curatorFramework, String path, byte[] data) {
        super(curatorFramework, path);
        this.data = data;
    }

    public byte[] getData() {
        return data;
    }

    @Override
    public CuratorTransactionBridge appendToTransaction(CuratorTransaction curatorTransaction,
                                                        OperationsContext operationsContext) throws Exception {
        return curatorTransaction.setData().forPath(getPath(), data);
    }

}
