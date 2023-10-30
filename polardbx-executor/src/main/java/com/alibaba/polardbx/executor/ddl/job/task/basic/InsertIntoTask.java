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

package com.alibaba.polardbx.executor.ddl.job.task.basic;

import com.alibaba.fastjson.annotation.JSONCreator;
import com.alibaba.polardbx.common.exception.TddlNestableRuntimeException;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.Parameters;
import com.alibaba.polardbx.common.properties.ConnectionProperties;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.executor.ExecutorHelper;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.ddl.job.builder.DdlPhyPlanBuilder;
import com.alibaba.polardbx.executor.ddl.job.builder.DropPartitionTableBuilder;
import com.alibaba.polardbx.executor.ddl.job.builder.DropTableBuilder;
import com.alibaba.polardbx.executor.ddl.job.builder.TruncatePartitionTableBuilder;
import com.alibaba.polardbx.executor.ddl.job.builder.TruncateTableBuilder;
import com.alibaba.polardbx.executor.ddl.job.converter.DdlJobDataConverter;
import com.alibaba.polardbx.executor.ddl.job.converter.PhysicalPlanData;
import com.alibaba.polardbx.executor.ddl.job.task.BaseDdlTask;
import com.alibaba.polardbx.executor.ddl.job.task.BasePhyDdlTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.executor.utils.ExecUtils;
import com.alibaba.polardbx.executor.utils.failpoint.FailPoint;
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.planner.ExecutionPlan;
import com.alibaba.polardbx.optimizer.core.planner.Planner;
import com.alibaba.polardbx.optimizer.core.rel.PhyDdlTableOperation;
import com.alibaba.polardbx.optimizer.core.rel.ReplaceTableNameWithSomethingVisitor;
import com.alibaba.polardbx.optimizer.parse.FastsqlParser;
import com.alibaba.polardbx.statistics.SQLRecorderLogger;
import com.google.common.collect.ImmutableList;
import lombok.Getter;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlDmlKeyword;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlInsert;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.commons.lang3.StringUtils;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@TaskName(name = "InsertIntoTask")
@Getter
public class InsertIntoTask extends BaseDdlTask {

    String logicalTableName;
    String insertSql;
    Map<Integer, ParameterContext> currentParameter;
    int affectRows;

    @JSONCreator
    public InsertIntoTask(String schemaName,
                          String logicalTableName,
                          String insertSql,
                          Map<Integer, ParameterContext> currentParameter, int affectRows) {
        super(schemaName);
        this.logicalTableName = logicalTableName;
        this.insertSql = insertSql;
        this.currentParameter = currentParameter;
        this.affectRows = affectRows;
        onExceptionTryRollback();
    }

    @Override
    protected void beforeTransaction(ExecutionContext executionContext) {
        updateSupportedCommands(false, true, null);
        executeImpl(executionContext);
    }

    @Override
    protected void beforeRollbackTransaction(ExecutionContext executionContext) {
        super.beforeRollbackTransaction(executionContext);
    }

    public void executeImpl(ExecutionContext executionContext) {
        executionContext = executionContext.copy();

        FailPoint.injectRandomExceptionFromHint(executionContext);
        FailPoint.injectRandomSuspendFromHint(executionContext);

        executionContext.setPhySqlId(0L);
        boolean isBroadcast = OptimizerContext.getContext(schemaName).getRuleManager().isBroadCast(logicalTableName);
        if (isBroadcast) {
            //广播表策略改为FIRST_THEN_CONCURRENT_POLICY，为了兼容affectRow，理论上可以用GROUP_CONCURRENT_BLOCK
            executionContext.getExtraCmds().put(ConnectionProperties.FIRST_THEN_CONCURRENT_POLICY, true);
        }

        List<?> params = null;
        if (executionContext.getParams() == null) {
            Parameters parameters = new Parameters();
            if (currentParameter != null) {
                //不为空意味着是执行PreparedStatement，newInsert已经参数化，需要附带数值运行
                parameters.setParams(currentParameter);
                params =
                    currentParameter.values().stream().map(ParameterContext::getValue).collect(Collectors.toList());
            }
            executionContext.setParams(parameters);
        }

//        String CountNum = "select count(1) from " + '`' + logicalTableName + '`';
//        SqlNode count = new FastsqlParser().parse(CountNum, params, executionContext).get(0);
//        PlannerContext plannerContext1 = PlannerContext.fromExecutionContext(executionContext);
//        ExecutionPlan countPlan = Planner.getInstance().getPlan(count, plannerContext1);
//        Cursor countCursor = ExecutorHelper.execute(countPlan.getPlan(), executionContext);
//        long numOfLine = (long)countCursor.next().getValues().get(0);
//        if(numOfLine == 0) {
//            throw new TddlNestableRuntimeException(
//                String.format("There are some records int the table %s, you must roll back the job.", logicalTableName));
//        }

        SqlInsert insert = (SqlInsert) new FastsqlParser().parse(insertSql, params, executionContext).get(0);
        //执行
        SQLRecorderLogger.ddlEngineLogger.info(String.format("[Job:%d Task:%d] Execute Logical Insert: %s", this.jobId,
            this.taskId, StringUtils.substring(TStringUtil.quoteString(insert.toString()), 0, 5000)));

        PlannerContext plannerContext = PlannerContext.fromExecutionContext(executionContext);
        ExecutionPlan insertPlan = Planner.getInstance().getPlan(insert, plannerContext);
        Cursor cursor = null;
        try {
            cursor = ExecutorHelper.execute(insertPlan.getPlan(), executionContext);
            this.affectRows = ExecUtils.getAffectRowsByCursor(cursor);
        } finally {
            if (cursor != null) {
                cursor.close(new ArrayList<>());
            }
        }
    }

    // 目前插入数据失败不回滚
    protected List<RelNode> genRollbackPhysicalPlans(ExecutionContext executionContext) {
        boolean isNewPartDb = DbInfoManager.getInstance().isNewPartitionDb(schemaName);
        DdlPhyPlanBuilder builder = isNewPartDb ?
            TruncatePartitionTableBuilder.createBuilder(schemaName, logicalTableName, false, executionContext).build() :
            TruncateTableBuilder.createBuilder(schemaName, logicalTableName, false, executionContext).build();

        Map<String, List<List<String>>> tableTopology = builder.getTableTopology();
        List<PhyDdlTableOperation> physicalPlans = builder.getPhysicalPlans();
        //generate a "truncate table" physical plan
        List<RelNode> relNodes = new ArrayList<>();
        for (PhyDdlTableOperation phyDdlTableOperation : physicalPlans) {
            relNodes.add(phyDdlTableOperation);
        }
        return relNodes;
    }

}
