package ru.fix.zookeeper.lock;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;
import org.slf4j.Logger;

import java.net.InetAddress;
import java.net.UnknownHostException;

@Data
class LockData {

    @JsonProperty
    private String ip = "Unknown ip";

    @JsonProperty
    private String hostname = "Unknown hostname";

    @JsonProperty
    private String serverId;

    @JsonProperty
    private String uuid;

    @JsonProperty
    private long expirationDate;

    @JsonCreator
    LockData(@JsonProperty("ip") String ip, @JsonProperty("hostname") String hostname,
                    @JsonProperty("serverId") String serverId, @JsonProperty("uuid") String uuid,
                    @JsonProperty("expirationDate") long expirationDate) {
        this.ip = ip;
        this.hostname = hostname;
        this.serverId = serverId;
        this.uuid = uuid;
        this.expirationDate = expirationDate;
    }

    LockData(String uuid, long expirationDate, String serverId, Logger logger) {
        this.uuid = uuid;
        this.expirationDate = expirationDate;
        this.serverId = serverId;

        try {
            InetAddress inetAddr = InetAddress.getLocalHost();
            this.ip = inetAddr.getHostAddress();
            this.hostname = inetAddr.getHostName();
        } catch (UnknownHostException ignore) {
            logger.trace("Node[serverId={}, uuid={}] already removed on release.", serverId, uuid, ignore);
        }
    }
}
