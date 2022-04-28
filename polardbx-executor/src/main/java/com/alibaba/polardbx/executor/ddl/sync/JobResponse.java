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

package com.alibaba.polardbx.executor.ddl.sync;

import java.util.HashMap;
import java.util.Map;

public class JobResponse {

    public enum ResponseType {
        SUCCESS, WARNING, ERROR
    }

    private String schemaName;
    private Map<Long, Response> responses = new HashMap<>();

    public void addResponse(Long jobId, Response response) {
        this.responses.put(jobId, response);
    }

    public void mergeResponses(JobResponse jobResponse) {
        this.responses.putAll(jobResponse.getResponses());
    }

    public Response getResponse(Long jobId) {
        return responses.get(jobId);
    }

    public int size() {
        return responses.size();
    }

    public String getSchemaName() {
        return schemaName;
    }

    public void setSchemaName(String schemaName) {
        this.schemaName = schemaName;
    }

    public Map<Long, Response> getResponses() {
        return responses;
    }

    public void setResponses(Map<Long, Response> responses) {
        this.responses = responses;
    }

    public static class Response {

        private ResponseType responseType;
        private Object responseContent;

        public Response() {
        }

        public Response(ResponseType responseType, Object responseContent) {
            this.responseType = responseType;
            this.responseContent = responseContent;
        }

        public ResponseType getResponseType() {
            return responseType;
        }

        public void setResponseType(ResponseType responseType) {
            this.responseType = responseType;
        }

        public Object getResponseContent() {
            return responseContent;
        }

        public void setResponseContent(Object responseContent) {
            this.responseContent = responseContent;
        }
    }

}
