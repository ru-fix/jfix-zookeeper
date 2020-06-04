package ru.fix.zookeeper.transactional;

import org.apache.curator.framework.api.transaction.CuratorOp;

import java.util.List;

interface Operation {

    /**
     * @param operationsContext  local operations context
     */
    List<CuratorOp> buildOperations(OperationsContext operationsContext) throws Exception;

}
