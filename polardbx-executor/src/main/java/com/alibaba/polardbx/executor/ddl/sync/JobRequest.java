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

import com.alibaba.polardbx.common.ddl.Job;
import com.alibaba.polardbx.common.ddl.Job.JobSource;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class JobRequest {

    // 前端连接ID
    public static final String TEST_MODE = "TEST_MODE";

    private List<Job> jobs;
    private JobSource source;
    private int fromNodeId;
    private String schemaName;
    private Map<String, Object> dataPassed = new HashMap<>();

    public List<Job> getJobs() {
        return jobs;
    }

    public void setJobs(List<Job> jobs) {
        this.jobs = jobs;
    }

    public JobSource getSource() {
        return source;
    }

    public void setSource(JobSource source) {
        this.source = source;
    }

    public int getFromNodeId() {
        return fromNodeId;
    }

    public void setFromNodeId(int fromNodeId) {
        this.fromNodeId = fromNodeId;
    }

    public String getSchemaName() {
        return schemaName;
    }

    public void setSchemaName(String schemaName) {
        this.schemaName = schemaName;
    }

    public Object getDataPassed(String key) {
        return dataPassed.get(key);
    }

    public void addDataPassed(String key, Object value) {
        dataPassed.put(key, value);
    }

    public Map<String, Object> getDataPassed() {
        return dataPassed;
    }

    public void setDataPassed(Map<String, Object> dataPassed) {
        this.dataPassed = dataPassed;
    }

    @Override
    public String toString() {
        return "JobRequest{" +
            "jobs=" + jobs +
            ", source=" + source +
            ", fromNodeId=" + fromNodeId +
            ", schemaName='" + schemaName + '\'' +
            '}';
    }
}
