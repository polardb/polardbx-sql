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
import com.alibaba.polardbx.executor.ddl.newengine.meta.DdlEngineResourceManager;
import com.alibaba.polardbx.gms.metadb.misc.PersistentReadWriteLock;
import com.alibaba.polardbx.executor.sync.ISyncAction;
import com.alibaba.polardbx.optimizer.context.DdlContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.google.common.base.Joiner;
import com.google.common.collect.Sets;
import org.apache.commons.collections.CollectionUtils;

import java.util.List;
import java.util.Set;

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

    private PersistentReadWriteLock lockManager = PersistentReadWriteLock.create();

    @Override
    public ResultCursor sync() {
        ArrayResultCursor resultCursor = buildResultCursor();

        String serverInfo = TddlNode.getNodeInfo();

        // All DDls
        List<DdlContext> ddlAcquiringLocksList = DdlEngineResourceManager.getAllDdlAcquiringLocks(schemaName);
        if (CollectionUtils.isNotEmpty(ddlAcquiringLocksList)){
            for(DdlContext ddlContext: ddlAcquiringLocksList){
                resultCursor.addRow(buildRowFromDdlContext(ddlContext, serverInfo));
            }
        }

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
        resultCursor.addColumn("DDL_STMT", DataTypes.StringType);
        resultCursor.addColumn("STATE", DataTypes.StringType);
        resultCursor.addColumn("INTERRUPTED", DataTypes.StringType);
        resultCursor.addColumn("LEGACY_ENGINE_INFO", DataTypes.StringType);
        resultCursor.initMeta();
        return resultCursor;
    }

    private Object[] buildRowFromDdlContext(DdlContext ddlContext, String nodeInfo) {
        Set<String> blockers =
            lockManager.queryBlocker(Sets.union(Sets.newHashSet(ddlContext.getSchemaName()), ddlContext.getResources()));
        return new Object[] {
            ENGINE_TYPE_DAG,
            nodeInfo,
            ddlContext.getJobId(),
            ddlContext.getSchemaName(),
            ddlContext.getDdlStmt(),
            String.format("Acquiring DDL Locks, S:%s, X:%s, Blockers:[%s]",
                ddlContext.getSchemaName(),
                setToString(ddlContext.getResources()),
                setToString(blockers)
            ),
            ddlContext.isInterrupted(),
            NONE
        };
    }

    private Object[] buildRow(DdlEngineDagExecutionInfo ddlJobCache, String nodeInfo) {
        return new Object[] {
            ENGINE_TYPE_DAG,
            nodeInfo,
            ddlJobCache.jobId,
            ddlJobCache.schemaName,
            ddlJobCache.ddlStmt,
            ddlJobCache.state != null ? ddlJobCache.state.name() : NONE,
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

    private String setToString(Set<String> lockSet){
        if(CollectionUtils.isEmpty(lockSet)){
            return "";
        }
        return Joiner.on(",").join(lockSet);
    }

}
