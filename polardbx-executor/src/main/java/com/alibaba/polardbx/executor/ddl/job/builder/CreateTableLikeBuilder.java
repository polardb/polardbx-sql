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

package com.alibaba.polardbx.executor.ddl.job.builder;

import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.executor.common.ExecutorContext;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.planner.ExecutionPlan;
import com.alibaba.polardbx.optimizer.core.planner.Planner;
import com.alibaba.polardbx.optimizer.core.rel.dal.LogicalShow;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalCreateTable;
import com.alibaba.polardbx.optimizer.core.row.Row;
import com.alibaba.polardbx.optimizer.parse.FastsqlParser;
import org.apache.calcite.sql.SqlCreateTable;
import org.apache.calcite.sql.SqlShowCreateTable;
import org.apache.commons.lang.StringUtils;

public abstract class CreateTableLikeBuilder {

    private final static String CREATE_TABLE = "CREATE TABLE";
    private final static String CREATE_TABLE_IF_NOT_EXISTS = "CREATE TABLE IF NOT EXISTS";

    protected static LogicalCreateTable genCreateTableLikeDdl(ExecutionContext executionContext) {
        // For `create table like xx` statement, do show create table
        // query first to get the referenced table architecture, which
        // is used for creating new table later on.

        String createTableSql = genCreateTableDdl(executionContext);

        SqlCreateTable createTableAst = (SqlCreateTable) new FastsqlParser().parse(createTableSql).get(0);
        //createTableAst.setTargetTable(logicalCreateTable.relDdl.getTableName());

        // Remove auto-increment start point,mapping rules and global
        // index before creating table
        if (createTableAst.getAutoIncrement() != null) {
            createTableAst.getAutoIncrement().setStart(null);
        }
        createTableAst.setMappingRules(null);
        createTableAst.setGlobalKeys(null);
        createTableAst.setGlobalUniqueKeys(null);

        PlannerContext pc = PlannerContext.fromExecutionContext(executionContext);

        return (LogicalCreateTable) Planner.getInstance().getPlan(createTableAst, pc).getPlan();
    }

    private static String genCreateTableDdl(ExecutionContext executionContext) {
        SqlShowCreateTable sqlShowCreateTable = null;
        //SqlShowCreateTable.create(SqlParserPos.ZERO, logicalCreateTable.relDdl.getLikeTableName());

        ExecutionPlan showCreateTablePlan = Planner.getInstance().getPlan(sqlShowCreateTable);
        LogicalShow showRel = (LogicalShow) showCreateTablePlan.getPlan();

        String schemaName =
            StringUtils.isEmpty(showRel.getSchemaName()) ? executionContext.getSchemaName() :
                showRel.getSchemaName();

        Cursor showCreateResultCursor =
            ExecutorContext.getContext(schemaName).getTopologyExecutor()
                .execByExecPlanNode(showRel, executionContext);

        String createTableDdl = null;
        Row showCreateResult = showCreateResultCursor.next();
        if (showCreateResult != null && showCreateResult.getString(1) != null) {
            createTableDdl = showCreateResult.getString(1);
        } else {
            GeneralUtil.nestedException("Get reference table architecture failed.");
        }

        //if (((SqlCreateTable) logicalCreateTable.sqlDdl).isIfNotExists()) {
        //    createTableDdl = createTableDdl.replace(CREATE_TABLE, CREATE_TABLE_IF_NOT_EXISTS);
        //}

        return createTableDdl;
    }
}
