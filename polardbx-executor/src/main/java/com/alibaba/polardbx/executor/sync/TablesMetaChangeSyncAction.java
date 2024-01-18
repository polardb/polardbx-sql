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
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.cursor.ResultCursor;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.SchemaManager;

import java.util.List;

/**
 * For PolarDB-X only.
 */
public class TablesMetaChangeSyncAction implements ISyncAction {
    protected final static Logger logger = LoggerFactory.getLogger(TablesMetaChangeSyncAction.class);

    private String schemaName;
    private List<String> logicalTables;

    private Long connId;

    public TablesMetaChangeSyncAction() {

    }

    public TablesMetaChangeSyncAction(String schemaName, List<String> logicalTables) {
        this.schemaName = schemaName;
        this.logicalTables = logicalTables;
        this.connId = -1L;
    }

    @JSONCreator
    public TablesMetaChangeSyncAction(String schemaName, List<String> logicalTables, Long connId) {
        this.schemaName = schemaName;
        this.logicalTables = logicalTables;
        this.connId = connId;
    }

    @Override
    public ResultCursor sync() {
        SchemaManager oldSchemaManager = OptimizerContext.getContext(schemaName).getLatestSchemaManager();
        oldSchemaManager.toNewVersionInTrx(logicalTables, connId, true);
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

    public Long getConnId() {
        return connId;
    }

    public void setConnId(Long connId) {
        this.connId = connId;
    }
}
