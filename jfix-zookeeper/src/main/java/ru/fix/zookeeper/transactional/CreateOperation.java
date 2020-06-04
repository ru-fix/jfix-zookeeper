package ru.fix.zookeeper.transactional;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.transaction.CuratorOp;
import org.apache.curator.utils.PathUtils;
import org.apache.curator.utils.ZKPaths;
import org.apache.zookeeper.CreateMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

class CreateOperation implements Operation{

    private final Logger logger = LoggerFactory.getLogger(CreateOperation.class);

    private final CuratorFramework curatorFramework;
    private final boolean createWithParents;
    private final CreateMode createMode;
    private final String path;

    public CreateOperation(CuratorFramework curatorFramework,
                           String path,
                           CreateMode createMode,
                           boolean createWithParents) {
        this.curatorFramework = curatorFramework;
        this.path = PathUtils.validatePath(path);
        this.createMode = createMode;
        this.createWithParents = createWithParents;
    }

    @Override
    public List<CuratorOp> buildOperations(OperationsContext operationsContext) throws Exception {
        ArrayList<CuratorOp> operations = new ArrayList<>();
        if (createWithParents) {
            List<String> pathParts = ZKPaths.split(path);

            // collect all create operations for parents
            String currentParentNode = null;
            for (int i = 0; i < pathParts.size() - 1; i++) {
                currentParentNode = ZKPaths.makePath(currentParentNode, pathParts.get(i));
                if (curatorFramework.checkExists().forPath(currentParentNode) == null) {
                    if (!operationsContext.isCreated(currentParentNode)) {
                        operationsContext.markAsCreated(currentParentNode);
                        operations.add(curatorFramework.transactionOp().create().forPath(currentParentNode));
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
        operations.add(curatorFramework.transactionOp().create().withMode(createMode).forPath(path));
        return operations;
    }
}

