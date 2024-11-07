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

package com.alibaba.polardbx.executor.ddl.job.task.backfill;

import com.alibaba.fastjson.annotation.JSONCreator;
import com.alibaba.polardbx.executor.ExecutorHelper;
import com.alibaba.polardbx.executor.ddl.job.task.BaseBackfillTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.executor.utils.failpoint.FailPoint;
import com.alibaba.polardbx.optimizer.config.table.GeneratedColumnUtil;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.ColumnBackFill;
import lombok.Getter;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;

@TaskName(name = "ColumnBackFillTask")
@Getter
public class ColumnBackFillTask extends BaseBackfillTask {

    final String tableName;
    final List<String> sourceExprs;
    final List<String> targetColumns;
    final boolean onlineModifyColumn;
    final boolean forceCnEval;

    @JSONCreator
    public ColumnBackFillTask(String schemaName, String tableName, List<String> sourceExprs, List<String> targetColumns,
                              boolean onlineModifyColumn, boolean forceCnEval) {
        super(schemaName);
        this.tableName = tableName;
        this.sourceExprs = sourceExprs;
        this.targetColumns = targetColumns;
        this.onlineModifyColumn = onlineModifyColumn;
        this.forceCnEval = forceCnEval;
    }

    @Override
    protected void executeImpl(ExecutionContext executionContext) {
        executionContext = executionContext.copy();
        executionContext.setBackfillId(getTaskId());
        executionContext.setTaskId(getTaskId());

        List<SqlCall> sourceNodes = new ArrayList<>();
        List<String> newTargetColumns;
        if (onlineModifyColumn) {
            for (String sourceExpr : sourceExprs) {
                sourceNodes.add(new SqlBasicCall(SqlStdOperatorTable.ALTER_TYPE,
                    new SqlNode[] {new SqlIdentifier(sourceExpr, SqlParserPos.ZERO)}, SqlParserPos.ZERO));
            }
            newTargetColumns = targetColumns;
        } else {
            // Add generated column
            Map<String, Set<String>> refColMap = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
            for (int i = 0; i < sourceExprs.size(); i++) {
                refColMap.put(targetColumns.get(i), GeneratedColumnUtil.getReferencedColumns(sourceExprs.get(i)));
            }

            // Reorder column based on dependency
            newTargetColumns =
                GeneratedColumnUtil.getGeneratedColumnEvaluationOrder(refColMap).stream().flatMap(Collection::stream)
                    .collect(Collectors.toList());
            for (String column : newTargetColumns) {
                for (int i = 0; i < targetColumns.size(); i++) {
                    if (targetColumns.get(i).equalsIgnoreCase(column)) {
                        sourceNodes.add(
                            GeneratedColumnUtil.getSqlCallAndValidateFromExprWithoutTableName(schemaName, tableName,
                                sourceExprs.get(i), executionContext));
                    }
                }
            }
        }

        ColumnBackFill columnBackFill =
            ColumnBackFill.createColumnBackfill(schemaName, tableName, sourceNodes, newTargetColumns, forceCnEval,
                executionContext);

        FailPoint.injectRandomExceptionFromHint(executionContext);
        FailPoint.injectRandomSuspendFromHint(executionContext);
        ExecutorHelper.execute(columnBackFill, executionContext);
    }

    @Override
    public String remark() {
        StringBuilder result = new StringBuilder("|Backfill columns in " + tableName);
        for (int i = 0; i < sourceExprs.size(); i++) {
            result.append(String.format(" from %s to %s; ", sourceExprs.get(i), targetColumns.get(i)));
        }
        return result.toString();
    }

}
