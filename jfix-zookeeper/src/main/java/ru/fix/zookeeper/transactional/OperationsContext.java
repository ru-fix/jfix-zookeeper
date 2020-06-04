package ru.fix.zookeeper.transactional;

import java.util.Set;
import java.util.TreeSet;

class OperationsContext {

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
