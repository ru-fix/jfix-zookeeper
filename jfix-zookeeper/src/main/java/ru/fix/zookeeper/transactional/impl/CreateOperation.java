package ru.fix.zookeeper.transactional.impl;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.transaction.CuratorTransaction;
import org.apache.curator.framework.api.transaction.CuratorTransactionBridge;
import org.apache.curator.utils.PathUtils;
import org.apache.curator.utils.ZKPaths;
import org.apache.zookeeper.CreateMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class CreateOperation extends AbstractPathOperation {

    private Logger logger = LoggerFactory.getLogger(CreateOperation.class);

    private boolean createWithParents;
    private CreateMode createMode;

    public CreateOperation(CuratorFramework curatorFramework, String path, CreateMode createMode,
                           boolean createWithParents) {
        super(curatorFramework, path);
        this.createMode = createMode;
        this.createWithParents = createWithParents;
    }

    public boolean isCreateWithParents() {
        return createWithParents;
    }

    @Override
    public CuratorTransactionBridge appendToTransaction(CuratorTransaction curatorTransaction,
                                                        OperationsContext operationsContext) throws Exception {
        String path = PathUtils.validatePath(getPath());
        if (createWithParents) {
            List<String> pathParts = ZKPaths.split(path);

            // collect all create operations for parents
            String currentParentNode = null;
            for (int i = 0; i < pathParts.size() - 1; i++) {
                currentParentNode = ZKPaths.makePath(currentParentNode, pathParts.get(i));
                if (getCuratorFramework().checkExists().forPath(currentParentNode) == null) {
                    if (!operationsContext.isCreated(currentParentNode)) {
                        operationsContext.markAsCreated(currentParentNode);
                        curatorTransaction.create().forPath(currentParentNode);
                        logger.trace("Appended create operation: {}", currentParentNode);
                    }
                } else {
                    break;
                }
            }
        }

        // append original node
        operationsContext.markAsCreated(path);
        logger.trace("Appended create operation: {}", path);
        return curatorTransaction.create().withMode(createMode).forPath(path);
    }

}

