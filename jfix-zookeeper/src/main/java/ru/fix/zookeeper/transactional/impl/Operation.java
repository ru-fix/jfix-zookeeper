package ru.fix.zookeeper.transactional.impl;

import org.apache.curator.framework.api.transaction.CuratorTransaction;
import org.apache.curator.framework.api.transaction.CuratorTransactionBridge;

public interface Operation {

    /**
     * Path for given operation (i.e. create node for path, check node at path, delete node with path)
     *
     * @return path
     */
    String getPath();

    /**
     * Append operation to transaction
     *
     * @param curatorTransaction transaction to append to
     * @param operationsContext  local operations context
     */
    CuratorTransactionBridge appendToTransaction(CuratorTransaction curatorTransaction,
                                                 OperationsContext operationsContext) throws Exception;

}
