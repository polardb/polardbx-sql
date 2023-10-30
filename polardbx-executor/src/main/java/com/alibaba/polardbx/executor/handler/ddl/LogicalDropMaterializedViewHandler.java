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

import com.alibaba.polardbx.common.ddl.newengine.DdlType;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.AffectRowCursor;
import com.alibaba.polardbx.executor.ddl.job.validator.TableValidator;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlJob;
import com.alibaba.polardbx.executor.ddl.newengine.job.TransientDdlJob;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.executor.sync.DropViewSyncAction;
import com.alibaba.polardbx.executor.sync.SyncManagerHelper;
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.context.DdlContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.planner.ExecutionPlan;
import com.alibaba.polardbx.optimizer.core.planner.Planner;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalDropMaterializedView;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalDropTable;
import com.alibaba.polardbx.optimizer.parse.FastsqlParser;
import com.alibaba.polardbx.optimizer.view.DrdsSystemTableView;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlDropTable;

import java.util.ArrayList;

public class LogicalDropMaterializedViewHandler extends LogicalDropTableHandler {

    public LogicalDropMaterializedViewHandler(IRepository repo) {
        super(repo);
    }

    @Override
    public Cursor handle(RelNode logicalPlan, ExecutionContext executionContext) {

        LogicalDropMaterializedView logicalCreateTable = (LogicalDropMaterializedView) logicalPlan;

        String schemaName = logicalCreateTable.getSchemaName();
        if (TStringUtil.isEmpty(schemaName)) {
            schemaName = executionContext.getSchemaName();
        }

        String tableName = logicalCreateTable.getViewName();

        DdlContext ddlContext =
            DdlContext.create(schemaName, tableName, DdlType.DROP_TABLE, executionContext);
        executionContext.setDdlContext(ddlContext);

        boolean isNewPartDb = DbInfoManager.getInstance().isNewPartitionDb(schemaName);

        //generate drop table
        String targetDropTableSql = String.format("drop table if exists %s", tableName);

        //ddl
        final SqlDropTable targetTableAst = (SqlDropTable)
            new FastsqlParser().parse(targetDropTableSql, executionContext).get(0);

        PlannerContext plannerContext = PlannerContext.fromExecutionContext(executionContext);

        ExecutionPlan dropTableSqlPlan = Planner.getInstance().getPlan(targetTableAst, plannerContext);
        LogicalDropTable logicalDropRelNode = (LogicalDropTable) dropTableSqlPlan.getPlan();

        logicalDropRelNode.prepareData();

        DdlJob ddlJob = null;

        if (logicalDropRelNode.ifExists()) {
            if (!TableValidator.checkIfTableExists(logicalDropRelNode.getSchemaName(),
                logicalDropRelNode.getTableName())) {
                ddlJob = new TransientDdlJob();
            }
        }

        if (ddlJob == null) {
            if (!isNewPartDb) {
                ddlJob = buildDropTableJob(logicalDropRelNode, executionContext);
            } else {
                ddlJob = buildDropPartitionTableJob(logicalDropRelNode, executionContext);
            }
        }

        // Handle the client DDL request on the worker side.
        handleDdlRequest(ddlJob, executionContext);

        //sync all nodes
        syncView(schemaName, tableName + "_Materialized");

        return new AffectRowCursor(new int[] {0});
    }

    public void syncView(String schemaName, String viewName) {

        DrdsSystemTableView.Row row = OptimizerContext.getContext(schemaName).getViewManager().select(viewName);
        if (row == null) {
            throw new TddlRuntimeException(ErrorCode.ERR_VIEW, "Unknown view " + viewName);
        }

        boolean success = OptimizerContext.getContext(schemaName).getViewManager().delete(viewName);

        if (!success) {
            throw new TddlRuntimeException(ErrorCode.ERR_VIEW,
                "drop view fail for " + DrdsSystemTableView.TABLE_NAME + " can not write");
        }

        ArrayList<String> viewList = new ArrayList<>();
        viewList.add(viewName);
        SyncManagerHelper.sync(new DropViewSyncAction(schemaName, viewList), schemaName);
    }
}
