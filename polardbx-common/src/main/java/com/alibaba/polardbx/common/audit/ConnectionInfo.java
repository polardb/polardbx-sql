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

package com.alibaba.polardbx.common.audit;

public class ConnectionInfo {
    private final String instanceId;
    private final String user;
    private final String host;
    private final int port;
    private final String schema;
    private final String traceId;

    public ConnectionInfo(String instanceId, String user, String host, int port, String schema,
                          String traceId) {
        this.instanceId = instanceId;
        this.user = user;
        this.host = host;
        this.port = port;
        this.schema = schema;
        this.traceId = traceId;
    }

    public String getInstanceId() {
        return instanceId;
    }

    public String getUser() {
        return user;
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public String getSchema() {
        return schema;
    }

    public String getTraceId() {
        return traceId;
    }
}
