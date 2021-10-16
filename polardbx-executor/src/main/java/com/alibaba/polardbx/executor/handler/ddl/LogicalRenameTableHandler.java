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

import com.alibaba.polardbx.executor.ddl.job.builder.DdlPhyPlanBuilder;
import com.alibaba.polardbx.executor.ddl.job.builder.RenameTableBuilder;
import com.alibaba.polardbx.executor.ddl.job.converter.PhysicalPlanData;
import com.alibaba.polardbx.executor.ddl.job.factory.RenameTableJobFactory;
import com.alibaba.polardbx.executor.ddl.job.validator.TableValidator;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlJob;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.ddl.BaseDdlOperation;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalRenameTable;
import org.apache.calcite.sql.SqlRenameTable;

public class LogicalRenameTableHandler extends LogicalCommonDdlHandler {

    public LogicalRenameTableHandler(IRepository repo) {
        super(repo);
    }

    @Override
    protected DdlJob buildDdlJob(BaseDdlOperation logicalDdlPlan, ExecutionContext executionContext) {
        LogicalRenameTable logicalRenameTable = (LogicalRenameTable) logicalDdlPlan;
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
        DdlPhyPlanBuilder renameTableBuilder =
            RenameTableBuilder.create(logicalRenameTable.relDdl,
                logicalRenameTable.getRenameTablePreparedData(),
                executionContext).build();
        PhysicalPlanData physicalPlanData = renameTableBuilder.genPhysicalPlanData();

        return new RenameTableJobFactory(physicalPlanData, executionContext).create();
    }

}
