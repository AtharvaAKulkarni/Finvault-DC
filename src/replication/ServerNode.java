package replication;

import java.io.Serializable;

public class ServerNode implements Serializable {

    private static final long serialVersionUID = 1L;

    private final int nodeId;
    private final String host;

    public ServerNode(int nodeId, String host) {
        this.nodeId = nodeId;
        this.host = host;
    }

    public int getNodeId() {
        return nodeId;
    }

    public String getHost() {
        return host;
    }

    @Override
    public String toString() {
        return "ServerNode{" +
                "nodeId=" + nodeId +
                ", host='" + host + '\'' +
                '}';
    }
}