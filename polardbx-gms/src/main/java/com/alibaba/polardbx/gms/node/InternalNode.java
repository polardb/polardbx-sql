/*
 * Copyright [2013-2021], Alibaba Group Holding Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.polardbx.gms.node;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.net.URI;
import java.util.Objects;

import static com.alibaba.polardbx.util.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

/**
 * A node is a server in a cluster than can process queries.
 */
public class InternalNode implements Node {
    private String nodeIdentifier;
    private final String cluster;
    private final String host;
    private final int port;
    private final int rpcPort;
    private final URI httpUri;
    private final NodeVersion nodeVersion;
    private final String instId;
    private boolean leader;
    private final boolean coordinator;
    private final boolean worker;
    private boolean inBlacklist;
    private boolean htap;
    private final NodeServer nodeServer;
    private transient boolean master;

    public InternalNode(String nodeIdentifier, String cluster, String instId, String host, int port, int rpcPort,
                        NodeVersion nodeVersion, boolean coordinator, boolean worker, boolean inBlacklist,
                        boolean htap) {
        this.nodeIdentifier = requireNonNull(nodeIdentifier, "nodeIdentifier is null");
        this.cluster = cluster;
        this.instId = instId;
        this.host = host;
        this.port = port;
        this.rpcPort = rpcPort;
        this.httpUri = URI.create("http://" + host + ":" + rpcPort);
        this.nodeVersion = requireNonNull(nodeVersion, "nodeVersion is null");
        this.coordinator = coordinator;
        this.worker = worker;
        this.inBlacklist = inBlacklist;
        this.htap = htap;
        this.nodeServer = new NodeServer(this.host, this.rpcPort);
    }

    @JsonCreator
    public InternalNode(@JsonProperty("nodeIdentifier") String nodeIdentifier,
                        @JsonProperty("cluster") String cluster,
                        @JsonProperty("instId") String instId,
                        @JsonProperty("host") String host,
                        @JsonProperty("port") int port,
                        @JsonProperty("rpcPort") int rpcPort,
                        @JsonProperty("httpUri") URI httpUri,
                        @JsonProperty("nodeVersion") NodeVersion nodeVersion,
                        @JsonProperty("leader") boolean leader,
                        @JsonProperty("coordinator") boolean coordinator,
                        @JsonProperty("worker") boolean worker,
                        @JsonProperty("inBlacklist") boolean inBlacklist,
                        @JsonProperty("htap") boolean htap,
                        @JsonProperty("nodeServer") NodeServer nodeServer) {
        this.nodeIdentifier = nodeIdentifier;
        this.cluster = cluster;
        this.instId = instId;
        this.host = host;
        this.port = port;
        this.rpcPort = rpcPort;
        this.httpUri = httpUri;
        this.nodeVersion = nodeVersion;
        this.leader = leader;
        this.coordinator = coordinator;
        this.worker = worker;
        this.inBlacklist = inBlacklist;
        this.htap = htap;
        this.nodeServer = nodeServer;
    }

    @Override
    @JsonProperty
    public NodeServer getNodeServer() {
        return nodeServer;
    }

    @Override
    @JsonProperty
    public String getNodeIdentifier() {
        return nodeIdentifier;
    }

    public void setNodeIdentifier(String nodeIdentifier) {
        this.nodeIdentifier = nodeIdentifier;
    }

    @Override
    @JsonProperty
    public String getCluster() {
        return cluster;
    }

    @Override
    @JsonProperty
    public URI getHttpUri() {
        return httpUri;
    }

    @Override
    public HostAddress getHostAndPort() {
        return HostAddressCache.fromUri(httpUri);
    }

    @Override
    public String getVersion() {
        return nodeVersion.getVersion();
    }

    @Override
    @JsonProperty
    public boolean isLeader() {
        return leader;
    }

    @Override
    public void setLeader(boolean leader) {
        this.leader = leader;
    }

    @Override
    @JsonProperty
    public boolean isCoordinator() {
        return coordinator;
    }

    @Override
    @JsonProperty
    public boolean isWorker() {
        return worker;
    }

    @Override
    @JsonProperty
    public boolean isHtap() {
        return htap;
    }

    public void setHtap(boolean htap) {
        this.htap = htap;
    }

    @Override
    @JsonProperty
    public boolean isInBlacklist() {
        return inBlacklist;
    }

    @Override
    public void setInBlacklist(boolean inBlacklist) {
        this.inBlacklist = inBlacklist;
    }

    @JsonProperty
    public NodeVersion getNodeVersion() {
        return nodeVersion;
    }

    @Override
    @JsonProperty
    public String getHost() {
        return host;
    }

    @Override
    @JsonProperty
    public int getRpcPort() {
        return rpcPort;
    }

    @Override
    @JsonProperty
    public int getPort() {
        return port;
    }

    @Override
    @JsonProperty
    public String getInstId() {
        return instId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof InternalNode)) {
            return false;
        }
        InternalNode that = (InternalNode) o;
        return nodeIdentifier.equals(that.nodeIdentifier) &&
            Objects.equals(cluster, that.cluster);
    }

    @Override
    public int hashCode() {
        return Objects.hash(nodeIdentifier, cluster);
    }

    @Override
    public String toString() {
        return toStringHelper(this)
            .add("nodeIdentifier", nodeIdentifier)
            .add("nodeVersion", nodeVersion)
            .add("nodeServer", nodeServer)
            .add("leader", leader)
            .add("coordinator", coordinator)
            .add("worker", worker)
            .add("htap", htap)
            .toString();
    }

    public String getHostMppPort() {
        return host + ":" + rpcPort;
    }

    @Override
    public String getHostPort() {
        return host + ":" + port;
    }

    public boolean isMaster() {
        return master;
    }

    public void setMaster(boolean master) {
        this.master = master;
    }
}
