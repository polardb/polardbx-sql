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

package com.alibaba.polardbx.executor.mpp.client;

public class LocalResultRequest {
    private String queryId;
    private long token;
    private String uriInfo;

    public LocalResultRequest() {
    }

    @Override
    public String toString() {
        return "LocalResultRequest[queryId:" + queryId + ", token:" + token + ", uriInfo:" + uriInfo;
    }

    public String getQueryId() {
        return queryId;
    }

    public void setQueryId(String queryId) {
        this.queryId = queryId;
    }

    public long getToken() {
        return token;
    }

    public void setToken(long token) {
        this.token = token;
    }

    public String getUriInfo() {
        return uriInfo;
    }

    public void setUriInfo(String uriInfo) {
        this.uriInfo = uriInfo;
    }
}
