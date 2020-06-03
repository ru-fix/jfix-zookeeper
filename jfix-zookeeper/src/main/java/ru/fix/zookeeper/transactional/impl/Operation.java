package ru.fix.zookeeper.transactional.impl;

import org.apache.curator.framework.api.transaction.CuratorOp;

import java.util.List;

public interface Operation {

    /**
     * @param operationsContext  local operations context
     */
    List<CuratorOp> buildOperations(OperationsContext operationsContext) throws Exception;

}
