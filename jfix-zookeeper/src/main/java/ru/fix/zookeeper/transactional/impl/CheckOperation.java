package ru.fix.zookeeper.transactional.impl;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.transaction.CuratorTransaction;
import org.apache.curator.framework.api.transaction.CuratorTransactionBridge;

public class CheckOperation extends AbstractPathOperation {

    private Integer version;

    public CheckOperation(CuratorFramework curatorFramework, String path, Integer version) {
        super(curatorFramework, path);
        this.version = version;
    }

    @Override
    public CuratorTransactionBridge appendToTransaction(CuratorTransaction curatorTransaction,
                                                        OperationsContext operationsContext) throws Exception {
        if (version != null) {
            return curatorTransaction.check().withVersion(version).forPath(getPath());
        } else {
            return curatorTransaction.check().forPath(getPath());
        }
    }

}
