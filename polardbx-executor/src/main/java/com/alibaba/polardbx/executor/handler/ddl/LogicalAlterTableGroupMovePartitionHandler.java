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

import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.ddl.job.factory.AlterTableGroupMovePartitionJobFactory;
import com.alibaba.polardbx.executor.ddl.job.validator.TableValidator;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlJob;
import com.alibaba.polardbx.executor.ddl.newengine.job.TransientDdlJob;
import com.alibaba.polardbx.executor.partitionmanagement.AlterTableGroupUtils;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.ddl.BaseDdlOperation;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalAlterTableGroupMovePartition;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.ddl.AlterTableGroupMovePartition;
import org.apache.calcite.sql.SqlAlterTableGroup;
import org.apache.calcite.sql.SqlAlterTableGroupMovePartition;

public class LogicalAlterTableGroupMovePartitionHandler extends LogicalCommonDdlHandler {

    public LogicalAlterTableGroupMovePartitionHandler(IRepository repo) {
        super(repo);
    }

    @Override
    protected DdlJob buildDdlJob(BaseDdlOperation logicalDdlPlan, ExecutionContext executionContext) {
        LogicalAlterTableGroupMovePartition logicalAlterTableGroupMovePartition =
            (LogicalAlterTableGroupMovePartition) logicalDdlPlan;

        AlterTableGroupMovePartition alterTableGroupMovePartition =
            (AlterTableGroupMovePartition) logicalDdlPlan.relDdl;
        SqlAlterTableGroup sqlAlterTableGroup = (SqlAlterTableGroup) logicalDdlPlan.relDdl.getSqlNode();

        assert sqlAlterTableGroup.getAlters().size() == 1;
        assert sqlAlterTableGroup.getAlters().get(0) instanceof SqlAlterTableGroupMovePartition;

        SqlAlterTableGroupMovePartition sqlAlterTableGroupMovePartition =
            (SqlAlterTableGroupMovePartition) sqlAlterTableGroup.getAlters().get(0);

        if (GeneralUtil.isEmpty(sqlAlterTableGroupMovePartition.getTargetPartitions())) {
            return new TransientDdlJob();
        }

        logicalAlterTableGroupMovePartition.preparedData(executionContext);
        //CheckOSSArchiveUtil.checkWithoutOSS(logicalAlterTableGroupMovePartition.getPreparedData());
        return AlterTableGroupMovePartitionJobFactory
            .create(logicalAlterTableGroupMovePartition.relDdl, logicalAlterTableGroupMovePartition.getPreparedData(),
                executionContext);
    }

    @Override
    protected boolean validatePlan(BaseDdlOperation logicalDdlPlan, ExecutionContext executionContext) {
        AlterTableGroupUtils.alterTableGroupPreCheck(
            (SqlAlterTableGroup) logicalDdlPlan.relDdl.getSqlNode(),
            logicalDdlPlan.getSchemaName(),
            executionContext);
        return false;
    }

    @Override
    public Cursor handle(RelNode logicalPlan, ExecutionContext executionContext) {
        BaseDdlOperation logicalDdlPlan = (BaseDdlOperation) logicalPlan;

        executionContext.getServerVariables().put("foreign_key_checks", false);
        initDdlContext(logicalDdlPlan, executionContext);

        // Validate the plan on file storage first
        TableValidator.validateTableEngine(logicalDdlPlan, executionContext);
        // Validate the plan first and then return immediately if needed.
        boolean returnImmediately = validatePlan(logicalDdlPlan, executionContext);

        boolean isNewPartDb = DbInfoManager.getInstance().isNewPartitionDb(logicalDdlPlan.getSchemaName());

        if (isNewPartDb) {
            setPartitionDbIndexAndPhyTable(logicalDdlPlan);
        } else {
            setDbIndexAndPhyTable(logicalDdlPlan);
        }

        // Build a specific DDL job by subclass.
        DdlJob ddlJob = returnImmediately ?
            new TransientDdlJob() :
            buildDdlJob(logicalDdlPlan, executionContext);

        // Validate the DDL job before request.
        validateJob(logicalDdlPlan, ddlJob, executionContext);

        if (executionContext.getDdlContext().getExplain()) {
            return buildExplainResultCursor(logicalDdlPlan, ddlJob, executionContext);
        }

        // Handle the client DDL request on the worker side.
        handleDdlRequest(ddlJob, executionContext);

        if (executionContext.getDdlContext().isSubJob()) {
            return buildSubJobResultCursor(ddlJob, executionContext);
        }
        return buildResultCursor(logicalDdlPlan, ddlJob, executionContext);
    }

}
