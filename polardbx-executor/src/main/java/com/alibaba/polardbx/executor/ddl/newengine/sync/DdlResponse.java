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

package com.alibaba.polardbx.executor.ddl.newengine.sync;

import com.alibaba.fastjson.annotation.JSONCreator;
import com.alibaba.polardbx.optimizer.statis.SQLTracer;

import java.util.HashMap;
import java.util.Map;

public class DdlResponse {

    public enum ResponseType {
        SUCCESS, WARNING, ERROR
    }

    /**
     * Job ID -> Response
     */
    private Map<Long, Response> responses = new HashMap<>();

    public void addResponse(Long jobId, Response response) {
        this.responses.put(jobId, response);
    }

    public Response getResponse(Long jobId) {
        return responses.get(jobId);
    }

    public Response removeResponse(Long jobId) {
        return responses.remove(jobId);
    }

    public Map<Long, Response> getResponses() {
        return responses;
    }

    public void setResponses(Map<Long, Response> responses) {
        this.responses = responses;
    }

    public static class Response {

        private long jobId;
        private String schemaName;
        private String objectName;
        private String ddlType;
        private ResponseType responseType;
        private String responseContent;


        private String ddlStmt;
        private String startTime;
        private String endTime;

        private Object warning;
        private SQLTracer tracer;

        public Response(long jobId,
                        String schemaName,
                        String objectName,
                        String ddlType,
                        ResponseType responseType,
                        String responseContent) {
            this.jobId = jobId;
            this.schemaName = schemaName;
            this.objectName = objectName;
            this.ddlType = ddlType;
            this.responseType = responseType;
            this.responseContent = responseContent;
        }

        @JSONCreator
        public Response(long jobId,
                        String schemaName,
                        String objectName,
                        String ddlType,
                        ResponseType responseType,
                        String responseContent,
                        String ddlStmt,
                        String startTime,
                        String endTime,
                        Object warning,
                        SQLTracer tracer) {
            this.jobId = jobId;
            this.schemaName = schemaName;
            this.objectName = objectName;
            this.ddlType = ddlType;
            this.responseType = responseType;
            this.responseContent = responseContent;
            this.ddlStmt = ddlStmt;
            this.startTime = startTime;
            this.endTime = endTime;
            this.warning = warning;
            this.tracer = tracer;
        }

        public long getJobId() {
            return this.jobId;
        }

        public void setJobId(final long jobId) {
            this.jobId = jobId;
        }

        public ResponseType getResponseType() {
            return responseType;
        }

        public void setResponseType(ResponseType responseType) {
            this.responseType = responseType;
        }

        public String getSchemaName() {
            return this.schemaName;
        }

        public void setSchemaName(final String schemaName) {
            this.schemaName = schemaName;
        }

        public String getObjectName() {
            return this.objectName;
        }

        public void setObjectName(final String objectName) {
            this.objectName = objectName;
        }

        public String getDdlType() {
            return this.ddlType;
        }

        public void setDdlType(final String ddlType) {
            this.ddlType = ddlType;
        }

        public String getResponseContent() {
            return this.responseContent;
        }

        public void setResponseContent(final String responseContent) {
            this.responseContent = responseContent;
        }

        public Object getWarning() {
            return this.warning;
        }

        public void setWarning(final Object warning) {
            this.warning = warning;
        }

        public SQLTracer getTracer() {
            return this.tracer;
        }

        public void setTracer(final SQLTracer tracer) {
            this.tracer = tracer;
        }


        public String getDdlStmt() {
            return ddlStmt;
        }

        public void setDdlStmt(String ddlStmt) {
            this.ddlStmt = ddlStmt;
        }

        public String getStartTime() {
            return startTime;
        }

        public void setStartTime(String startTime) {
            this.startTime = startTime;
        }

        public String getEndTime() {
            return endTime;
        }

        public void setEndTime(String endTime) {
            this.endTime = endTime;
        }
    }

}
