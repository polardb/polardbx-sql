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

package com.alibaba.polardbx.executor.handler.ddl;

import com.alibaba.polardbx.common.exception.TddlNestableRuntimeException;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.executor.ddl.job.builder.DdlPhyPlanBuilder;
import com.alibaba.polardbx.executor.ddl.job.builder.RenameTableBuilder;
import com.alibaba.polardbx.executor.ddl.job.converter.PhysicalPlanData;
import com.alibaba.polardbx.executor.ddl.job.factory.RenameTableJobFactory;
import com.alibaba.polardbx.executor.ddl.job.meta.TableMetaChanger;
import com.alibaba.polardbx.executor.ddl.job.task.basic.ShowTableMetaTask;
import com.alibaba.polardbx.executor.ddl.job.task.gsi.ValidateTableVersionTask;
import com.alibaba.polardbx.executor.ddl.job.validator.TableValidator;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlJob;
import com.alibaba.polardbx.executor.ddl.newengine.job.ExecutableDdlJob;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.gms.metadb.MetaDbDataSource;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.ddl.BaseDdlOperation;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalRenameTable;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.RenameTablePreparedData;
import org.apache.calcite.sql.SqlRenameTable;

import java.sql.Connection;
import java.util.HashMap;
import java.util.Map;

public class LogicalRenameTableHandler extends LogicalCommonDdlHandler {

    public LogicalRenameTableHandler(IRepository repo) {
        super(repo);
    }

    @Override
    protected DdlJob buildDdlJob(BaseDdlOperation logicalDdlPlan, ExecutionContext executionContext) {
        LogicalRenameTable logicalRenameTable = (LogicalRenameTable) logicalDdlPlan;
        if (executionContext.getParamManager().getBoolean(ConnectionParams.FLASHBACK_RENAME)) {
            makeTableVisible(logicalRenameTable.getSchemaName(), logicalRenameTable.getTableName(), executionContext);
        }
        logicalRenameTable.prepareData();
        return buildRenameTableJob(logicalRenameTable, executionContext);
    }

    @Override
    protected boolean validatePlan(BaseDdlOperation logicalDdlPlan, ExecutionContext executionContext) {
        SqlRenameTable sqlRenameTable = (SqlRenameTable) logicalDdlPlan.getNativeSqlNode();

        final String schemaName = logicalDdlPlan.getSchemaName();

        final String sourceTableName = sqlRenameTable.getOriginTableName().getLastName();
        TableValidator.validateTableName(sourceTableName);
        TableValidator.validateTableExistence(schemaName, sourceTableName, executionContext);

        final String targetTableName = sqlRenameTable.getOriginNewTableName().getLastName();
        TableValidator.validateTableName(targetTableName);
        TableValidator.validateTableNameLength(targetTableName);
        TableValidator.validateTableNonExistence(schemaName, targetTableName, executionContext);

        TableValidator.validateTableNamesForRename(schemaName, sourceTableName, targetTableName);

        return false;
    }

    private DdlJob buildRenameTableJob(LogicalRenameTable logicalRenameTable, ExecutionContext executionContext) {
        RenameTablePreparedData renameTablePreparedData = logicalRenameTable.getRenameTablePreparedData();
        DdlPhyPlanBuilder renameTableBuilder =
            RenameTableBuilder.create(logicalRenameTable.relDdl,
                renameTablePreparedData,
                executionContext).build();
        PhysicalPlanData physicalPlanData = renameTableBuilder.genPhysicalPlanData();
        if (executionContext.getParamManager().getBoolean(ConnectionParams.FLASHBACK_RENAME)) {
            physicalPlanData.setFlashbackRename(true);
        }

        Map<String, Long> tableVersions = new HashMap<>();

        tableVersions.put(renameTablePreparedData.getTableName(),
            renameTablePreparedData.getTableVersion());
        ValidateTableVersionTask validateTableVersionTask =
            new ValidateTableVersionTask(renameTablePreparedData.getSchemaName(), tableVersions);

        ExecutableDdlJob result = new RenameTableJobFactory(physicalPlanData, executionContext).create();
        result.addTask(validateTableVersionTask);
        result.addTaskRelationship(validateTableVersionTask, result.getHead());

        return result;
    }

    public static void makeTableVisible(String schemaName, String tableName, ExecutionContext executionContext) {
        ShowTableMetaTask showTableMetaTask = new ShowTableMetaTask(schemaName, tableName);
        try (Connection metaDbConn = MetaDbDataSource.getInstance().getConnection()) {
            try {
                MetaDbUtil.beginTransaction(metaDbConn);
                showTableMetaTask.executeImpl(metaDbConn, executionContext);
                MetaDbUtil.commit(metaDbConn);
            } catch (Throwable t) {
                MetaDbUtil.rollback(metaDbConn, new RuntimeException(t), null, null);
                throw new TddlNestableRuntimeException(t);
            } finally {
                MetaDbUtil.endTransaction(metaDbConn, null);
            }
            TableMetaChanger.afterNewTableMeta(showTableMetaTask.getSchemaName(), showTableMetaTask.getLogicalTableName());
        } catch (Throwable t) {
            throw new TddlNestableRuntimeException(t);
        }
    }
}
