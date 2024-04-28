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
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.Parameters;
import com.alibaba.polardbx.common.properties.ConnectionProperties;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.executor.ExecutorHelper;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.ddl.job.task.BaseDdlTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.executor.utils.ExecUtils;
import com.alibaba.polardbx.executor.utils.failpoint.FailPoint;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.planner.ExecutionPlan;
import com.alibaba.polardbx.optimizer.core.planner.Planner;
import com.alibaba.polardbx.optimizer.core.rel.ReplaceTableNameWithSomethingVisitor;
import com.alibaba.polardbx.optimizer.parse.FastsqlParser;
import com.alibaba.polardbx.statistics.SQLRecorderLogger;
import com.google.common.collect.ImmutableList;
import lombok.Getter;
import org.apache.calcite.sql.SqlDmlKeyword;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlInsert;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * @author lijiu.lzw
 */
@TaskName(name = "LogicalInsertTask")
@Getter
public class LogicalInsertTask extends BaseDdlTask {
    String logicalTableName;
    String tmpTableName;
    String insertSql;
    Map<Integer, ParameterContext> currentParameter;
    int affectRows;

    @JSONCreator
    public LogicalInsertTask(String schemaName,
                             String logicalTableName,
                             String tmpTableName,
                             String insertSql,
                             Map<Integer, ParameterContext> currentParameter, int affectRows) {
        super(schemaName);
        this.logicalTableName = logicalTableName;
        this.tmpTableName = tmpTableName;
        this.insertSql = insertSql;
        this.currentParameter = currentParameter;
        this.affectRows = affectRows;
        onExceptionTryRollback();
    }

    @Override
    protected void beforeTransaction(ExecutionContext executionContext) {
        executeImpl(executionContext);
    }

    protected void executeImpl(ExecutionContext executionContext) {
        executionContext = executionContext.copy();

        FailPoint.injectRandomExceptionFromHint(executionContext);
        FailPoint.injectRandomSuspendFromHint(executionContext);

        //只打印ddl的trace，暂时不打印dml的trace
//        if (executionContext.isEnableDdlTrace()) {
//            executionContext.setEnableTrace(true);
//        }
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

        SqlInsert insert = (SqlInsert) new FastsqlParser().parse(insertSql, params, executionContext).get(0);
        // 去除overwrite关键字
        List<String> keywords = SqlDmlKeyword.convertFromSqlNodeToString(insert.getKeywords());
        List<String> newKeywords =
            keywords.stream().filter(s -> !"OVERWRITE".equalsIgnoreCase(s)).collect(Collectors.toList());
        insert.setOperand(0, SqlDmlKeyword.convertFromStringToSqlNode(newKeywords));
        //替换表名
        ReplaceTableNameWithSomethingVisitor visitor =
            new ReplaceTableNameWithSomethingVisitor(schemaName, executionContext) {
                @Override
                protected SqlNode buildSth(SqlNode sqlNode) {
                    if (!(sqlNode instanceof SqlIdentifier)) {
                        return sqlNode;
                    }
                    SqlIdentifier oldSqlNode = (SqlIdentifier) sqlNode;
                    //一个表名
                    if (oldSqlNode.names.size() == 1 && oldSqlNode.getLastName().equalsIgnoreCase(logicalTableName)) {
                        return new SqlIdentifier(tmpTableName, SqlParserPos.ZERO);
                    } else if (oldSqlNode.names.size() == 2
                        && oldSqlNode.getLastName().equalsIgnoreCase(logicalTableName)
                        && oldSqlNode.names.get(0).equalsIgnoreCase(schemaName)) {
                        //schema.tableName ，跨库时，匹配两个名字
                        return new SqlIdentifier(ImmutableList.of(oldSqlNode.names.get(0), tmpTableName),
                            SqlParserPos.ZERO);
                    }
                    return sqlNode;
                }
            };
        SqlNode newInsert = insert.accept(visitor);

        //执行
        SQLRecorderLogger.ddlEngineLogger.info(String.format("[Job:%d Task:%d] Execute Logical Insert: %s", this.jobId,
            this.taskId, StringUtils.substring(TStringUtil.quoteString(newInsert.toString()), 0, 5000)));

        PlannerContext plannerContext = PlannerContext.fromExecutionContext(executionContext);
        ExecutionPlan insertPlan = Planner.getInstance().getPlan(newInsert, plannerContext);
        Cursor cursor = ExecutorHelper.execute(insertPlan.getPlan(), executionContext);
        this.affectRows = ExecUtils.getAffectRowsByCursor(cursor);
    }

}
