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

package com.alibaba.polardbx.executor.ddl.job.factory;

import com.alibaba.polardbx.executor.ddl.job.converter.PhysicalPlanData;
import com.alibaba.polardbx.executor.ddl.job.task.basic.InsertIntoTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.SubJobTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlExceptionAction;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlJobFactory;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.ExecutableDdlJob;
import com.alibaba.polardbx.executor.ddl.newengine.job.wrapper.ExecutableDdlJob4CreateSelect;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.google.common.collect.Lists;
import org.apache.calcite.sql.dialect.MysqlSqlDialect;
import org.apache.calcite.sql.pretty.SqlPrettyWriter;

import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

public class CreateTableSelectJobFactory extends DdlJobFactory {
    protected final String schemaName;
    protected final String logicalTableName;
    protected final ExecutionContext executionContext;
    protected final String createTableSql;
    protected final String selectSql;

    public CreateTableSelectJobFactory(String schemaName,
                                       String logicalTableName,
                                       ExecutionContext executionContext,
                                       String createTableSql,
                                       String selectSql) {
        this.schemaName = schemaName;
        this.logicalTableName = logicalTableName;
        this.executionContext = executionContext;
        this.createTableSql = createTableSql;
        this.selectSql = selectSql;
    }

    @Override
    protected void validate() {
    }

    @Override
    protected ExecutableDdlJob doCreate() {
        String dropTableStmt = null;
        SqlPrettyWriter writer1 = new SqlPrettyWriter(MysqlSqlDialect.DEFAULT);
        writer1.setAlwaysUseParentheses(true);
        writer1.setSelectListItemsOnSeparateLines(false);
        writer1.setIndentation(0);
        writer1.keyword("DROP");
        writer1.keyword("TABLE");
        writer1.keyword(logicalTableName);
        dropTableStmt = writer1.toSqlString().getSql();
        SubJobTask createTableSubJob = new SubJobTask(schemaName, createTableSql, null);
        createTableSubJob.setParentAcquireResource(true);

        InsertIntoTask insertIntoTask = new InsertIntoTask(schemaName, logicalTableName, selectSql, null, 0);
        //insert 只能rollback，无法重试
        insertIntoTask.setExceptionAction(DdlExceptionAction.ROLLBACK);

        ExecutableDdlJob4CreateSelect result = new ExecutableDdlJob4CreateSelect();

        List<DdlTask> taskList = Lists.newArrayList(
            createTableSubJob,
            insertIntoTask);
        result.addSequentialTasks(taskList.stream().filter(Objects::nonNull).collect(Collectors.toList()));
        result.setInsertTask(insertIntoTask);
        return result;
    }

    @Override
    protected void excludeResources(Set<String> resources) {
        resources.add(concatWithDot(schemaName, logicalTableName));
    }

    @Override
    protected void sharedResources(Set<String> resources) {

    }
}
