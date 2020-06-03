package ru.fix.zookeeper.transactional.impl;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.transaction.CuratorOp;
import org.apache.curator.utils.PathUtils;
import org.apache.curator.utils.ZKPaths;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

public class DeleteOperation implements Operation {

    private final Logger logger = LoggerFactory.getLogger(DeleteOperation.class);

    private final CuratorFramework curatorFramework;
    private final String path;
    private boolean deleteWithChildren;

    public DeleteOperation(CuratorFramework curatorFramework, String path, boolean deleteWithChildren) {
        this.curatorFramework = curatorFramework;
        this.path = PathUtils.validatePath(path);
        this.deleteWithChildren = deleteWithChildren;
    }

    @Override
    public List<CuratorOp> buildOperations(OperationsContext operationsContext) throws Exception {
        ArrayList<CuratorOp> operations = new ArrayList<CuratorOp>();
        if (deleteWithChildren) {
            Set<String> localNodesToDelete = new TreeSet<>();
            collectAllChildren(path, localNodesToDelete);

            List<String> reversedNodeToDelete = localNodesToDelete.stream()
                    .sorted(Collections.reverseOrder()).collect(Collectors.toList());
            for (String nodeToDelete : reversedNodeToDelete) {
                if (!operationsContext.isDeleted(nodeToDelete)) {
                    operationsContext.markAsDeleted(nodeToDelete);
                    operations.add(curatorFramework.transactionOp().delete().forPath(nodeToDelete));
                    logger.trace("Appended delete operation: {}", nodeToDelete);
                }
            }
        }

        // append operation for current node without checks
        operationsContext.markAsDeleted(path);
        logger.trace("Appended delete operation: {}", path);
        operations.add(curatorFramework.transactionOp().delete().forPath(path));
        return operations;
    }

    private void collectAllChildren(String path, Collection<String> collectedChildren) throws Exception {
        List<String> children = curatorFramework.getChildren().forPath(path);
        for (String item : children) {
            String itemPath = ZKPaths.makePath(path, item);
            collectedChildren.add(itemPath);
            collectAllChildren(itemPath, collectedChildren);
        }
    }
}
