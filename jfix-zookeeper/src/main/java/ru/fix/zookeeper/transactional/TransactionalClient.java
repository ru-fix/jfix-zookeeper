package ru.fix.zookeeper.transactional;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.transaction.CuratorTransaction;
import org.apache.curator.framework.api.transaction.CuratorTransactionBridge;
import org.apache.zookeeper.CreateMode;
import ru.fix.zookeeper.transactional.impl.*;

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
public class TransactionalClient {

    private CuratorFramework curatorFramework;

    private List<Operation> operations = new ArrayList<>();

    private TransactionalClient(CuratorFramework curatorFramework) {
        Objects.requireNonNull(curatorFramework);
        this.curatorFramework = curatorFramework;
    }

    public static TransactionalClient createTransaction(CuratorFramework curatorFramework) {
        return new TransactionalClient(curatorFramework);
    }

    public TransactionalClient checkPath(String path) throws Exception {
        operations.add(new CheckOperation(curatorFramework, path, null));
        return this;
    }

    public TransactionalClient checkPathWithVersion(String path, Integer version) throws Exception {
        operations.add(new CheckOperation(curatorFramework, path, version));
        return this;
    }

    public TransactionalClient createPath(String path) throws Exception {
        operations.add(new CreateOperation(curatorFramework, path, CreateMode.PERSISTENT, false));
        return this;
    }

    public TransactionalClient createPathWithMode(String path, CreateMode mode) throws Exception {
        operations.add(new CreateOperation(curatorFramework, path, mode, false));
        return this;
    }

    public TransactionalClient setData(String path, byte[] data) throws Exception {
        operations.add(new SetDataOperation(curatorFramework, path, data));
        return this;
    }

    public TransactionalClient deletePath(String path) throws Exception {
        operations.add(new DeleteOperation(curatorFramework, path, false));
        return this;
    }

    public TransactionalClient deletePathWithChildrenIfNeeded(String path) throws Exception {
        operations.add(new DeleteOperation(curatorFramework, path, true));
        return this;
    }

    public TransactionalClient createPathWithParentsIfNeeded(String path) throws Exception {
        operations.add(new CreateOperation(curatorFramework, path, CreateMode.PERSISTENT, true));
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


    public static void tryCommit(
            CuratorFramework curatorFramework,
            int times,
            TransactionCreator transactionCreator
    ) throws Exception {
        tryCommit(curatorFramework, times, transactionCreator, e -> {
            throw e;
        });
    }

    public static void tryCommit(
            CuratorFramework client,
            int times,
            TransactionCreator inTransaction,
            TransactionCreationErrorHandler onError
    ) throws Exception {
        for (int i = 0; i < times; i++) {
            try {
                TransactionalClient transaction = createTransaction(client);
                inTransaction.fillTransaction(transaction);
                transaction.commit();
                break;
            } catch (Exception e) {
                if (i == times - 1) {
                    onError.onError(e);
                }
            }
        }
    }


    public void commit() throws Exception {
        if (operations.isEmpty()) {
            throw new IllegalStateException("Transaction is empty");
        }

        CuratorTransactionBridge bridge = null;
        CuratorTransaction transaction = curatorFramework.inTransaction();
        OperationsContext operationsContext = new OperationsContext();
        for (Operation operation : operations) {
            bridge = operation.appendToTransaction(transaction, operationsContext);
        }

        Objects.requireNonNull(bridge);
        bridge.and().commit();
    }

    @FunctionalInterface
    public interface TransactionCreator {
        void fillTransaction(TransactionalClient transactionalClient) throws Exception;
    }

    @FunctionalInterface
    public interface TransactionCreationErrorHandler {
        void onError(Exception e) throws Exception;
    }

}
