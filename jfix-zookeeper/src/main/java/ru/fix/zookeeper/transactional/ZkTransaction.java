package ru.fix.zookeeper.transactional;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.transaction.*;
import org.apache.zookeeper.CreateMode;
import ru.fix.zookeeper.transactional.impl.*;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Transactional client with createWithParents/deleteWithChildren operations
 * <p>
 * !WARN. Functionality of mixing createWithParents/deleteWithChildren limited and won't work in cases like this:
 * </p>
 * Current state: 1/2/3
 * <pre>
 * Transaction:
 * DELETE 1/2/3
 * DELETE 1/2
 * CREATE WITH PARENTS 1/2/3/4
 * </pre>
 */
public class ZkTransaction {

    @FunctionalInterface
    public interface TransactionCreator {
        void fillTransaction(ZkTransaction zkTransaction) throws Exception;
    }

    @FunctionalInterface
    public interface TransactionCreationErrorHandler {
        void onError(Exception e) throws Exception;
    }

    private final CuratorFramework curatorFramework;
    private final List<CuratorOp> operations = new ArrayList<>();
    private final OperationsContext operationsContext = new OperationsContext();

    private ZkTransaction(CuratorFramework curatorFramework) {
        Objects.requireNonNull(curatorFramework);
        this.curatorFramework = curatorFramework;
    }

    public static ZkTransaction createTransaction(CuratorFramework curatorFramework) {
        return new ZkTransaction(curatorFramework);
    }

    public ZkTransaction checkPath(String path) throws Exception {
        operations.add(curatorFramework.transactionOp().check().forPath(path));
        return this;
    }

    public ZkTransaction checkPathWithVersion(String path, Integer version) throws Exception {
        operations.add(curatorFramework.transactionOp().check().withVersion(version).forPath(path));
        return this;
    }

    public ZkTransaction createPath(String path) throws Exception {
        operations.addAll(
                new CreateOperation(curatorFramework, path, CreateMode.PERSISTENT, false)
                        .buildOperations(operationsContext)
        );
        return this;
    }

    public ZkTransaction createPathWithMode(String path, CreateMode mode) throws Exception {
        operations.addAll(
                new CreateOperation(curatorFramework, path, mode, false)
                        .buildOperations(operationsContext));
        return this;
    }

    public ZkTransaction setData(String path, byte[] data) throws Exception {
        operations.add(curatorFramework.transactionOp().setData().forPath(path, data));
        return this;
    }

    public ZkTransaction deletePath(String path) throws Exception {
        operations.addAll(
                new DeleteOperation(curatorFramework, path, false)
                        .buildOperations(operationsContext));
        return this;
    }

    public ZkTransaction deletePathWithChildrenIfNeeded(String path) throws Exception {
        operations.addAll(
                new DeleteOperation(curatorFramework, path, true)
                        .buildOperations(operationsContext));
        return this;
    }

    public ZkTransaction createPathWithParentsIfNeeded(String path) throws Exception {
        operations.addAll(
                new CreateOperation(curatorFramework, path, CreateMode.PERSISTENT, true)
                        .buildOperations(operationsContext));
        return this;
    }

    /**
     * Node by given path should exist, value of given node will be overwritten.
     * Usage: invoke this method with the same parameter during different transactions,
     * which must not succeed in parallel.
     *
     * @param path node, which version should be checked and updated
     * @return previous version of updating node
     */
    public int checkAndUpdateVersion(String path) throws Exception {
        int version = curatorFramework.checkExists().forPath(path).getVersion();
        checkPathWithVersion(path, version);
        setData(path, new byte[]{});
        return version;
    }


    public static List<CuratorTransactionResult> tryCommit(
            CuratorFramework curatorFramework,
            int times,
            TransactionCreator transactionCreator
    ) throws Exception {
        return tryCommit(curatorFramework, times, transactionCreator, e -> {
            throw e;
        });
    }

    public static List<CuratorTransactionResult> tryCommit(
            CuratorFramework client,
            int times,
            TransactionCreator inTransaction,
            TransactionCreationErrorHandler onError
    ) throws Exception {
        for (int i = 0; i < times; i++) {
            try {
                ZkTransaction transaction = createTransaction(client);
                inTransaction.fillTransaction(transaction);
                return transaction.commit();
            } catch (Exception e) {
                if (i == times - 1) {
                    onError.onError(e);
                }
            }
        }
        //TODO fix api
        return Collections.emptyList();
    }

    public List<CuratorTransactionResult> commit() throws Exception {
        if (operations.isEmpty()) {
            throw new IllegalStateException("Transaction is empty");
        }
        return curatorFramework.transaction().forOperations(operations);
    }
}
