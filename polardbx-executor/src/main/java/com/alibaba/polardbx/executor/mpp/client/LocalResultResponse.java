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

public class LocalResultResponse {
    private long token;
    private QueryResults results;
    private Exception e;

    public LocalResultResponse() {
    }

    public long getToken() {
        return token;
    }

    public void setToken(long token) {
        this.token = token;
    }

    public QueryResults getResults() {
        return results;
    }

    public void setResults(QueryResults results) {
        this.results = results;
    }

    public Exception getException() {
        return e;
    }

    public void setException(Exception e) {
        this.e = e;
    }
}
