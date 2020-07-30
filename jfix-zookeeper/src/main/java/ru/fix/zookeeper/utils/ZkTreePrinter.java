package ru.fix.zookeeper.utils;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.utils.ZKPaths;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;

/**
 * Simple printer of Zookeeper node tree.
 * The implementation depends of Apache Curator Framework.
 */
public class ZkTreePrinter {

    private static final Logger log = LoggerFactory.getLogger(ZkTreePrinter.class);

    private final CuratorFramework client;

    public ZkTreePrinter(CuratorFramework client) {
        this.client = client;
    }

    /**
     * node's recursive printer
     *
     * @param path node path
     * @return tree's string representation
     */
    public String print(String path) {
        return print(path, true);
    }

    public String print(String path, boolean includeData) {
        StringBuilder out = new StringBuilder();
        out.append("\n");

        print(path, out, 0, includeData);
        return out.toString();
    }

    private void print(String path, StringBuilder out, int level, boolean includeData) {
        for (String child : getChildren(path)) {
            out.append(" ".repeat(Math.max(0, level)));
            try {
                String data = includeData ? " " + new String(client.getData().forPath(ZKPaths.makePath(path, child))) : "";
                out.append("└ ").append(child).append(data);
            } catch (Exception e) {
                log.debug("Error while trying print znode for path " + path, e);
            }
            out.append("\n");

            String childPath = ZKPaths.makePath(path, child);
            print(childPath, out, level + 1, includeData);
        }
    }

    private List<String> getChildren(String path) {
        try {
            return client.getChildren().forPath(path);
        } catch (KeeperException.NoNodeException e) {
            log.debug("Cannot read children from path '{}', reason {}", path, e.getMessage(), e);
            return Collections.emptyList();
        } catch (Exception e) {
            log.warn("Cannot read children from path '{}', reason {}", path, e.getMessage(), e);
            return Collections.emptyList();
        }
    }

}
