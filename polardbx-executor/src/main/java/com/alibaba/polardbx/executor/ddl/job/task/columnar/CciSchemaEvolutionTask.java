/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the License);
 * you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.polardbx.executor.ddl.job.task.columnar;

import com.alibaba.fastjson.annotation.JSONCreator;
import com.alibaba.polardbx.executor.ddl.job.meta.TableMetaChanger;
import com.alibaba.polardbx.executor.ddl.job.task.BaseDdlTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.executor.utils.DdlUtils;
import com.alibaba.polardbx.executor.utils.failpoint.FailPoint;
import com.alibaba.polardbx.gms.metadb.table.ColumnarTableMappingRecord;
import com.alibaba.polardbx.gms.metadb.table.ColumnarTableStatus;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import lombok.Getter;

import java.sql.Connection;
import java.util.Map;

@Getter
@TaskName(name = "CciSchemaEvolutionTask")
public class CciSchemaEvolutionTask extends BaseDdlTask {

    private final String primaryTableName;
    private final String columnarTableName;
    private final Long versionId;
    private final ColumnarTableStatus afterStatus;
    private final Map<String, String> options;

    private ColumnarTableMappingRecord tableMappingRecord;

    @JSONCreator
    private CciSchemaEvolutionTask(String schemaName, String primaryTableName, String columnarTableName, Long versionId,
                                   ColumnarTableStatus afterStatus, ColumnarTableMappingRecord tableMappingRecord,
                                   Map<String, String> options) {
        super(schemaName);
        this.primaryTableName = primaryTableName;
        this.columnarTableName = columnarTableName;
        this.versionId = versionId;
        this.afterStatus = afterStatus;
        this.options = options;
        this.tableMappingRecord = tableMappingRecord;
    }

    @Override
    protected void duringTransaction(Connection metaDbConnection, ExecutionContext executionContext) {
        switch (afterStatus) {
        case CREATING:
        case CHECKING:
            this.tableMappingRecord = TableMetaChanger.addCreateCciSchemaEvolutionMeta(metaDbConnection,
                schemaName,
                primaryTableName,
                columnarTableName,
                options,
                versionId,
                jobId);
            break;
        case DROP:
            updateSupportedCommands(true, false, metaDbConnection);
            TableMetaChanger.addDropCciSchemaEvolutionMeta(metaDbConnection,
                schemaName,
                primaryTableName,
                columnarTableName,
                versionId,
                jobId);
            break;
        default:
            LOGGER.warn(String.format("CciSchemaEvolutionTask does nothing for afterStatus: %s", afterStatus));
        }

        FailPoint.injectRandomExceptionFromHint(executionContext);
        FailPoint.injectRandomSuspendFromHint(executionContext);
    }

    @Override
    protected void duringRollbackTransaction(Connection metaDbConnection, ExecutionContext executionContext) {
        switch (afterStatus) {
        case CREATING:
        case CHECKING:
            // Need new version id for rollback, cause version id is the primary key of columnar_table_evolution table
            final long rollbackVersionId = DdlUtils.generateVersionId(executionContext);
            TableMetaChanger.addRollbackCreateCciSchemaEvolutionMeta(metaDbConnection,
                tableMappingRecord,
                rollbackVersionId,
                jobId);
            break;
        default:
        }

        FailPoint.injectRandomExceptionFromHint(executionContext);
        FailPoint.injectRandomSuspendFromHint(executionContext);
    }

    public void duringTransaction(Long ddlJobId, Connection metaDbConnection, ExecutionContext executionContext) {
        setJobId(ddlJobId);
        duringTransaction(metaDbConnection, executionContext);
    }

    public void duringRollbackTransaction(Long ddlJobId, Connection metaDbConnection,
                                          ExecutionContext executionContext) {
        setJobId(ddlJobId);
        duringRollbackTransaction(metaDbConnection, executionContext);
    }

    public static CciSchemaEvolutionTask dropCci(String schemaName, String logicalTableName, String columnarTableName,
                                                 Long versionId) {
        return new CciSchemaEvolutionTask(schemaName, logicalTableName, columnarTableName, versionId,
            ColumnarTableStatus.DROP, null, null);
    }

    public static CciSchemaEvolutionTask createCci(String schemaName, String logicalTableName, String columnarTableName,
                                                   Map<String, String> options, Long versionId) {
        return new CciSchemaEvolutionTask(schemaName, logicalTableName, columnarTableName, versionId,
            ColumnarTableStatus.CREATING, null, options);
    }

    @Override
    protected String remark() {
        return String.format("|ddlVersionId: %s |afterStatus: %s", versionId, afterStatus);
    }
}
