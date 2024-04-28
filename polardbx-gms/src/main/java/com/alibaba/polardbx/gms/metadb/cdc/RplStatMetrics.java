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

package com.alibaba.polardbx.gms.metadb.cdc;

import com.alibaba.polardbx.gms.metadb.record.SystemTableRecord;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;

public class RplStatMetrics implements SystemTableRecord {
    private Long id;
    private Long taskId;
    private String channel;
    private String subChannel;
    private String taskType;
    private Timestamp gmtCreated;
    private Timestamp gmtModified;
    private Long inEps;
    private Long outRps;
    private Long applyCount;
    private Long outBps;
    private Long inBps;
    private Long outInsertRps;
    private Long outUpdateRps;
    private Long outDeleteRps;
    private Long receiveDelay;
    private Long processDelay;
    private Long mergeBatchSize;
    private Long rt;
    private Long skipCounter;
    private Long skipExceptionCounter;
    private Long persistMsgCounter;
    private Long msgCacheSize;
    private Integer cpuUseRatio;
    private Integer memUseRatio;
    private Long fullGcCount;
    private String workerIp;

    @Override
    public RplStatMetrics fill(ResultSet rs) throws SQLException {
        this.id = rs.getLong("id");
        this.taskId = rs.getLong("task_id");
        this.channel = rs.getString("channel");
        this.subChannel = rs.getString("sub_channel");
        this.taskType = rs.getString("task_type");
        this.gmtCreated = rs.getTimestamp("gmt_created");
        this.gmtModified = rs.getTimestamp("gmt_modified");
        this.inEps = rs.getLong("in_eps");
        this.outRps = rs.getLong("out_rps");
        this.applyCount = rs.getLong("apply_count");
        this.outBps = rs.getLong("out_bps");
        this.inBps = rs.getLong("in_bps");
        this.outInsertRps = rs.getLong("out_insert_rps");
        this.outUpdateRps = rs.getLong("out_update_rps");
        this.outDeleteRps = rs.getLong("out_delete_rps");
        this.receiveDelay = rs.getLong("receive_delay");
        this.processDelay = rs.getLong("process_delay");
        this.mergeBatchSize = rs.getLong("merge_batch_size");
        this.rt = rs.getLong("rt");
        this.skipCounter = rs.getLong("skip_counter");
        this.skipExceptionCounter = rs.getLong("skip_exception_counter");
        this.persistMsgCounter = rs.getLong("persist_msg_counter");
        this.msgCacheSize = rs.getLong("msg_cache_size");
        this.cpuUseRatio = rs.getInt("cpu_use_ratio");
        this.memUseRatio = rs.getInt("mem_use_ratio");
        this.fullGcCount = rs.getLong("full_gc_count");
        this.workerIp = rs.getString("worker_ip");
        return this;
    }

    public Long getId() {
        return id;
    }

    public Long getTaskId() {
        return taskId;
    }

    public Timestamp getGmtCreated() {
        return gmtCreated;
    }

    public Timestamp getGmtModified() {
        return gmtModified;
    }

    public Long getInEps() {
        return inEps;
    }

    public Long getOutRps() {
        return outRps;
    }

    public Long getApplyCount() {
        return applyCount;
    }

    public Long getOutBps() {
        return outBps;
    }

    public Long getInBps() {
        return inBps;
    }

    public Long getOutInsertRps() {
        return outInsertRps;
    }

    public Long getOutUpdateRps() {
        return outUpdateRps;
    }

    public Long getOutDeleteRps() {
        return outDeleteRps;
    }

    public Long getReceiveDelay() {
        return receiveDelay;
    }

    public Long getProcessDelay() {
        return processDelay;
    }

    public Long getMergeBatchSize() {
        return mergeBatchSize;
    }

    public Long getRt() {
        return rt;
    }

    public Long getSkipCounter() {
        return skipCounter;
    }

    public Long getSkipExceptionCounter() {
        return skipExceptionCounter;
    }

    public Long getPersistMsgCounter() {
        return persistMsgCounter;
    }

    public Long getMsgCacheSize() {
        return msgCacheSize;
    }

    public Integer getCpuUseRatio() {
        return cpuUseRatio;
    }

    public Integer getMemUseRatio() {
        return memUseRatio;
    }

    public Long getFullGcCount() {
        return fullGcCount;
    }

    public String getWorkerIp() {
        return workerIp;
    }

    public String getChannel() {
        return channel;
    }

    public String getSubChannel() {
        return subChannel;
    }

    public String getTaskType() {
        return taskType;
    }
}
