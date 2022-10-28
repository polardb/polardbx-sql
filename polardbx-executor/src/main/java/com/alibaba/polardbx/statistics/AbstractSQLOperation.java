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

package com.alibaba.polardbx.statistics;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.alibaba.polardbx.common.jdbc.Parameters;
import com.alibaba.polardbx.optimizer.statis.SQLOperation;

/**
 * @author mengshi.sunmengshi 2014年6月24日 下午3:12:09
 * @since 5.1.0
 */
public abstract class AbstractSQLOperation implements SQLOperation {

    private Parameters params;
    private long timeCost;
    private long physicalCloseCost;
    private long totalTimeCost;

    protected String groupName;
    protected String dbKeyName;
    private String thread;

    private float getConnectionTimeCost;
    private Long rowsCount = 0L;
    private Long timestamp = 0L;
    private Long grpConnId = 0L;
    private String traceId = "";

    public AbstractSQLOperation(String groupName, String dbKeyName, Long timestamp) {
        this.groupName = groupName;
        this.dbKeyName = dbKeyName;
        this.timestamp = timestamp;
    }

    public AbstractSQLOperation(Parameters params, long timeCost, String groupName, String dbKeyName,
                                String thread, float getConnectionTimeCost, Long rowsCount, Long timestamp) {
        this.params = params;
        this.timeCost = timeCost;
        this.groupName = groupName;
        this.dbKeyName = dbKeyName;
        this.thread = thread;
        this.getConnectionTimeCost = getConnectionTimeCost;
        this.rowsCount = rowsCount;
        this.timestamp = timestamp;
    }

    @Override
    @JsonProperty
    public String getGroupName() {
        return groupName;
    }

    public void setGroupName(String groupName) {
        this.groupName = groupName;
    }

    public void setParams(Parameters params) {
        this.params = params;
    }

    @Override
    public String getParamsStr() {
        if (params == null) {
            return null;
        }

        if (params.isBatch()) {
            if (params.getBatchSize() == 0) {
                return null;
            }
        }
        return params.toString();
    }

    @JsonProperty
    public Parameters getParams() {
        return params;
    }

    @Override
    @JsonProperty
    public long getTimeCost() {
        return timeCost;
    }

    public void setTimeCost(long timeCost) {
        this.timeCost = timeCost;
    }

    @Override
    @JsonProperty
    public Long getRowsCount() {
        return rowsCount;
    }

    public void setRowsCount(Long rowsCount) {
        this.rowsCount = rowsCount;
    }

    @Override
    @JsonProperty
    public String getThreadName() {
        return this.thread;
    }

    public void setThreadName(String thread) {
        this.thread = thread;
    }

    @Override
    @JsonProperty
    public float getGetConnectionTimeCost() {
        return getConnectionTimeCost;
    }

    public void setGetConnectionTimeCost(float getConnectionTimeCost) {
        this.getConnectionTimeCost = getConnectionTimeCost;
    }

    @Override
    @JsonProperty
    public String getDbKeyName() {
        return dbKeyName;
    }

    public void setDbKeyName(String dbKeyName) {
        this.dbKeyName = dbKeyName;
    }

    @Override
    @JsonProperty
    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    @JsonProperty
    public long getPhysicalCloseCost() {
        return physicalCloseCost;
    }

    public void setPhysicalCloseCost(long physicalCloseCost) {
        this.physicalCloseCost = physicalCloseCost;
    }

    @Override
    @JsonProperty
    public long getTotalTimeCost() {
        return totalTimeCost;
    }

    public void setTotalTimeCost(long totalTimeCost) {
        this.totalTimeCost = totalTimeCost;
    }

    @Override
    public Long getGrpConnId() {
        return grpConnId;
    }

    public void setGrpConnId(Long grpConnId) {
        this.grpConnId = grpConnId;
    }

    @Override
    public String getTraceId() {
        return traceId;
    }

    public void setTraceId(String traceId) {
        this.traceId = traceId;
    }
}
