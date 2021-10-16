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

package com.alibaba.polardbx.gms.node;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.net.URI;

public class NodeServer {

    public final static NodeServer EMPTY_NODE_SERVER = new NodeServer("", -1);

    private final String host;
    private final int httpPort;

    @JsonCreator
    public NodeServer(
        @JsonProperty("host") String host,
        @JsonProperty("httpPort") int httpPort) {
        this.host = host;
        this.httpPort = httpPort;
    }

    @JsonProperty
    public String getHost() {
        return host;
    }

    @Override
    public int hashCode() {
        int result = getHost() != null ? getHost().hashCode() : 0;
        result = 31 * result + getHttpPort();
        return result;
    }

    @JsonProperty
    public int getHttpPort() {
        return httpPort;
    }

    @Override
    public boolean equals(Object ob) {
        if (ob == this) {
            return true;
        }
        if (ob == null || !(ob instanceof NodeServer)) {
            return false;
        }
        NodeServer that = (NodeServer) ob;
        return host.equals(that.getHost())
            && httpPort == that.getHttpPort();
    }

    @Override
    public String toString() {
        return String.format("%s:%d", host, httpPort);
    }

    public URI getTaskURI() {
        return URI.create(String.format("http://%s:%d/v1/task", host, httpPort));
    }

}