package ru.fix.zookeeper.transactional.impl;

import org.apache.curator.framework.CuratorFramework;

public abstract class AbstractPathOperation implements Operation {

    private CuratorFramework curatorFramework;
    private String path;

    public AbstractPathOperation(CuratorFramework curatorFramework, String path) {
        this.curatorFramework = curatorFramework;
        this.path = path;
    }

    public CuratorFramework getCuratorFramework() {
        return curatorFramework;
    }

    @Override
    public String getPath() {
        return path;
    }

}
