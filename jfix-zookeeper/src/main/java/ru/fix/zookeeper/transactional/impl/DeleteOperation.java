package ru.fix.zookeeper.transactional.impl;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.transaction.CuratorTransaction;
import org.apache.curator.framework.api.transaction.CuratorTransactionBridge;
import org.apache.curator.utils.PathUtils;
import org.apache.curator.utils.ZKPaths;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

public class DeleteOperation extends AbstractPathOperation {

    private Logger logger = LoggerFactory.getLogger(DeleteOperation.class);

    private boolean deleteWithChildren;

    public DeleteOperation(CuratorFramework curatorFramework, String path, boolean deleteWithChildren) {
        super(curatorFramework, path);
        this.deleteWithChildren = deleteWithChildren;
    }

    public boolean isDeleteWithChildren() {
        return deleteWithChildren;
    }

    @Override
    public CuratorTransactionBridge appendToTransaction(CuratorTransaction curatorTransaction,
                                                        OperationsContext operationsContext) throws Exception {
        String path = PathUtils.validatePath(getPath());
        if (deleteWithChildren) {
            Set<String> localNodesToDelete = new TreeSet<>();
            collectAllChildren(path, localNodesToDelete);

            List<String> reversedNodeToDelete = localNodesToDelete.stream()
                    .sorted(Collections.reverseOrder()).collect(Collectors.toList());
            for (String nodeToDelete : reversedNodeToDelete) {
                if (!operationsContext.isDeleted(nodeToDelete)) {
                    operationsContext.markAsDeleted(nodeToDelete);
                    curatorTransaction.delete().forPath(nodeToDelete);
                    logger.trace("Appended delete operation: {}", nodeToDelete);
                }
            }
        }

        // append operation for current node without checks
        operationsContext.markAsDeleted(path);
        logger.trace("Appended delete operation: {}", path);
        return curatorTransaction.delete().forPath(path);
    }

    private void collectAllChildren(String path, Collection<String> collectedChildren) throws Exception {
        List<String> children = getCuratorFramework().getChildren().forPath(path);
        for (String item : children) {
            String itemPath = ZKPaths.makePath(path, item);
            collectedChildren.add(itemPath);
            collectAllChildren(itemPath, collectedChildren);
        }
    }
}
