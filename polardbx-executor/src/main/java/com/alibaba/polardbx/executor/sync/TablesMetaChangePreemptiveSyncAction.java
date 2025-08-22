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

package com.alibaba.polardbx.executor.sync;

import com.alibaba.fastjson.annotation.JSONCreator;
import com.alibaba.polardbx.executor.cursor.ResultCursor;
import com.alibaba.polardbx.optimizer.config.table.PreemptiveTime;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.SchemaManager;

import java.util.List;

/**
 * For PolarDB-X only.
 */
public class TablesMetaChangePreemptiveSyncAction implements ISyncAction {
    private String schemaName;
    private List<String> logicalTables;


    private PreemptiveTime preemptiveTime;

    private Long connId;

    private Boolean sameTableGroup;
    private Boolean forceSyncFailed;

    public TablesMetaChangePreemptiveSyncAction() {

    }

    @JSONCreator
    public TablesMetaChangePreemptiveSyncAction(String schemaName, List<String> logicalTables, PreemptiveTime preemptiveTime, Long connId, Boolean sameTableGroup, Boolean forceSyncFailed) {
        this.schemaName = schemaName;
        this.logicalTables = logicalTables;
        this.preemptiveTime = preemptiveTime;
        this.connId = connId;
        this.sameTableGroup = sameTableGroup;
        this.forceSyncFailed = forceSyncFailed;
    }

    public TablesMetaChangePreemptiveSyncAction(String schemaName, List<String> logicalTables, PreemptiveTime preemptiveTime, Long connId, Boolean sameTableGroup) {
        this.schemaName = schemaName;
        this.logicalTables = logicalTables;
        this.preemptiveTime = preemptiveTime;
        this.connId = connId;
        this.sameTableGroup = sameTableGroup;
        this.forceSyncFailed = false;
    }

    public TablesMetaChangePreemptiveSyncAction(String schemaName, List<String> logicalTables, PreemptiveTime preemptiveTime) {
        this.schemaName = schemaName;
        this.logicalTables = logicalTables;
        this.preemptiveTime = preemptiveTime;
        this.sameTableGroup = true;
        this.forceSyncFailed = false;
    }

    public TablesMetaChangePreemptiveSyncAction(String schemaName, List<String> logicalTables, PreemptiveTime preemptiveTime, boolean forceSyncFailed) {
        this.schemaName = schemaName;
        this.logicalTables = logicalTables;
        this.preemptiveTime = preemptiveTime;
        this.sameTableGroup = true;
        this.forceSyncFailed = forceSyncFailed;
    }


    @Override
    public ResultCursor sync() {
        SchemaManager oldSchemaManager = OptimizerContext.getContext(schemaName).getLatestSchemaManager();
        // TODO(luoyanxin) optimize single-version schema-change
        oldSchemaManager.toNewVersionInTrx(logicalTables, true, preemptiveTime, connId, true,
            sameTableGroup, forceSyncFailed);
        return null;
    }

    public String getSchemaName() {
        return schemaName;
    }

    public void setSchemaName(String schemaName) {
        this.schemaName = schemaName;
    }

    public List<String> getLogicalTables() {
        return logicalTables;
    }

    public void setLogicalTables(List<String> logicalTables) {
        this.logicalTables = logicalTables;
    }

    public PreemptiveTime getPreemptiveTime() {
        return preemptiveTime;
    }

    public void setPreemptiveTime(PreemptiveTime preemptiveTime) {
        this.preemptiveTime = preemptiveTime;
    }

    public Long getConnId() {
        return connId;
    }

    public void setConnId(Long connId) {
        this.connId = connId;
    }

    public Boolean getSameTableGroup() {
        return sameTableGroup;
    }

    public void setSameTableGroup(Boolean sameTableGroup) {
        this.sameTableGroup = sameTableGroup;
    }

    public Boolean getForceSyncFailed() {
        return forceSyncFailed;
    }

    public void setForceSyncFailed(Boolean forceSyncFailed) {
        this.forceSyncFailed = forceSyncFailed;
    }

}
