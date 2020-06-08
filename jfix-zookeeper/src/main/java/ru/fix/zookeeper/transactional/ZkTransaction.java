package ru.fix.zookeeper.transactional;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.transaction.CuratorOp;
import org.apache.curator.framework.api.transaction.CuratorTransactionResult;
import org.apache.zookeeper.CreateMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
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
    private final static Logger logger = LoggerFactory.getLogger(ZkTransaction.class);

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


    /**
     * @param curatorFramework {@link CuratorFramework}
     * @param times            How many times to retry
     * @param inTransaction    transaction operations
     * @return list or operation results in case of success
     * @throws Exception last retry exception in case of failure
     */
    public static List<CuratorTransactionResult> tryCommit(
            CuratorFramework curatorFramework,
            int times,
            TransactionCreator inTransaction
    ) throws Exception {
        return tryCommit(curatorFramework, times, inTransaction, exc -> {
        });
    }

    /**
     * @param curatorFramework {@link CuratorFramework}
     * @param times            How many times to retry
     * @param inTransaction    transaction operations
     * @param onError          Callback that will be invoked for each failed retry
     * @return list or operation results in case of success
     * @throws Exception last retry exception in case of failure
     */
    public static List<CuratorTransactionResult> tryCommit(
            CuratorFramework curatorFramework,
            int times,
            TransactionCreator inTransaction,
            TransactionCreationErrorHandler onError
    ) throws Exception {
        for (int i = 0; i < times; i++) {
            try {
                ZkTransaction transaction = createTransaction(curatorFramework);
                inTransaction.fillTransaction(transaction);
                return transaction.commit();
            } catch (Exception exc) {
                logger.debug("Failed to commit transaction on retry {}", i, exc);
                onError.onError(exc);
                if (i == times - 1) {
                    throw exc;
                }
            }
        }
        throw new IllegalStateException();
    }

    public List<CuratorTransactionResult> commit() throws Exception {
        if (operations.isEmpty()) {
            throw new IllegalStateException("Transaction is empty");
        }
        return curatorFramework.transaction().forOperations(operations);
    }
}
