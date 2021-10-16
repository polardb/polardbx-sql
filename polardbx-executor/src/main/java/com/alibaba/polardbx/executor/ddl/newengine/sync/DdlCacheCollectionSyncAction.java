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

import com.alibaba.polardbx.common.TddlNode;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.executor.cursor.ResultCursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.ddl.newengine.DdlEngineDagExecutorMap;
import com.alibaba.polardbx.executor.sync.ISyncAction;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import org.apache.commons.collections.CollectionUtils;

import java.util.List;

import static com.alibaba.polardbx.common.ddl.newengine.DdlConstants.EMPTY_CONTENT;
import static com.alibaba.polardbx.common.ddl.newengine.DdlConstants.ENGINE_TYPE_DAG;
import static com.alibaba.polardbx.common.ddl.newengine.DdlConstants.NONE;
import static com.alibaba.polardbx.executor.ddl.newengine.DdlEngineDagExecutorMap.DdlEngineDagExecutionInfo;

public class DdlCacheCollectionSyncAction implements ISyncAction {

    private String schemaName;

    public DdlCacheCollectionSyncAction() {
    }

    public DdlCacheCollectionSyncAction(String schemaName) {
        this.schemaName = schemaName;
    }

    @Override
    public ResultCursor sync() {
        ArrayResultCursor resultCursor = buildResultCursor();

        String serverInfo = TddlNode.getNodeInfo();

        List<DdlEngineDagExecutionInfo> ddlExecutionInfoList = DdlEngineDagExecutorMap.getAllDdlJobCaches(schemaName);
        if (CollectionUtils.isNotEmpty(ddlExecutionInfoList)) {
            for (DdlEngineDagExecutionInfo ddlJobCache : ddlExecutionInfoList) {
                resultCursor.addRow(buildRow(ddlJobCache, serverInfo));
            }
        }

        return resultCursor;
    }

    public static ArrayResultCursor buildResultCursor() {
        ArrayResultCursor resultCursor = new ArrayResultCursor("DDL_JOB_CACHES");
        resultCursor.addColumn("ENGINE", DataTypes.StringType);
        resultCursor.addColumn("SERVER", DataTypes.StringType);
        resultCursor.addColumn("JOB_ID", DataTypes.StringType);
        resultCursor.addColumn("SCHEMA_NAME", DataTypes.StringType);
        resultCursor.addColumn("RESOURCES", DataTypes.StringType);
        resultCursor.addColumn("STATE", DataTypes.StringType);
        resultCursor.addColumn("INTERRUPTED", DataTypes.StringType);
        resultCursor.addColumn("LEGACY_ENGINE_INFO", DataTypes.StringType);
        resultCursor.initMeta();
        return resultCursor;
    }

    private Object[] buildRow(DdlEngineDagExecutionInfo ddlJobCache, String nodeInfo) {
        return new Object[] {
            ENGINE_TYPE_DAG,
            nodeInfo,
            ddlJobCache.jobId,
            ddlJobCache.schemaName,
            ddlJobCache.resources,
            ddlJobCache.state != null ? ddlJobCache.state.name() : EMPTY_CONTENT,
            ddlJobCache.interrupted,
            NONE
        };
    }

    public String getSchemaName() {
        return schemaName;
    }

    public void setSchemaName(String schemaName) {
        this.schemaName = schemaName;
    }

}
