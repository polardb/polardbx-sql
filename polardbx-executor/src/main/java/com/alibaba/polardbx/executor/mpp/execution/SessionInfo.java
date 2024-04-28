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

package com.alibaba.polardbx.executor.mpp.execution;

import com.alibaba.polardbx.util.MoreObjects;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Map;

// Deliver the session Variables based this class from Server to Worker.
public class SessionInfo {
    private String schema;
    private String user;
    private Map<String, Object> serverVariables;
    private Map<String, Object> userDefVariables;

    @JsonCreator
    public SessionInfo(
        @JsonProperty("schema") String schema,
        @JsonProperty("user") String user,
        @JsonProperty("serverVariables") Map<String, Object> serverVariables,
        @JsonProperty("userDefVariables") Map<String, Object> userDefVariables) {
        this.schema = schema;
        this.user = user;
        this.serverVariables = serverVariables;
        this.userDefVariables = userDefVariables;
    }

    @JsonProperty
    public String getSchema() {
        return schema;
    }

    @JsonProperty
    public String getUser() {
        return user;
    }

    @JsonProperty
    public Map<String, Object> getServerVariables() {
        return serverVariables;
    }

    @JsonProperty
    public Map<String, Object> getUserDefVariables() {
        return userDefVariables;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
            .add("schema", schema)
            .toString();
    }
}
