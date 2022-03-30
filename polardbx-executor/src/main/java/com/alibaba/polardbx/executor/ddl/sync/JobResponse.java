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
