package ru.fix.zookeeper.transactional.impl;

import java.util.Set;
import java.util.TreeSet;

public class OperationsContext {

    private Set<String> localNodesToCreate = new TreeSet<>();
    private Set<String> localNodesToDelete = new TreeSet<>();

    public void markAsCreated(String node) {
        localNodesToCreate.add(node);
    }

    public void markAsDeleted(String node) {
        localNodesToDelete.add(node);
    }

    public boolean isCreated(String node) {
        return localNodesToCreate.contains(node);
    }

    public boolean isDeleted(String node) {
        return localNodesToDelete.contains(node);
    }

}
