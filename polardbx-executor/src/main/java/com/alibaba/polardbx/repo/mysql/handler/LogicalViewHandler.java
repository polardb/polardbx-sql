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

package com.alibaba.polardbx.repo.mysql.handler;

import com.google.common.collect.Lists;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.common.ExecutorContext;
import com.alibaba.polardbx.executor.cursor.AbstractCursor;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.cursor.impl.LogicalViewResultCursor;
import com.alibaba.polardbx.executor.cursor.impl.MultiCursorAdapter;
import com.alibaba.polardbx.executor.handler.HandlerCommon;
import com.alibaba.polardbx.executor.utils.ExecUtils;
import com.alibaba.polardbx.executor.utils.SubqueryUtils;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.LogicalView;
import com.alibaba.polardbx.optimizer.core.rel.ReplaceCallWithLiteralVisitor;
import com.alibaba.polardbx.optimizer.utils.CalciteUtils;
import com.alibaba.polardbx.optimizer.utils.QueryConcurrencyPolicy;
import com.alibaba.polardbx.optimizer.utils.RexUtils;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.sql.SqlSelect;
import org.apache.commons.lang.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @author chuanqin
 */
public class LogicalViewHandler extends HandlerCommon {

    private static final Logger logger = LoggerFactory.getLogger(LogicalViewHandler.class);

    public LogicalViewHandler() {
        super(null);
    }

    @Override
    public Cursor handle(RelNode logicalPlan, ExecutionContext executionContext) {
        final LogicalView logicalView = (LogicalView) logicalPlan;

        if (logicalView.isMGetEnabled()) {
            throw new RuntimeException("multi-get is not supported in Cursor executor");
        }

        QueryConcurrencyPolicy queryConcurrencyPolicy =
            ExecUtils.getQueryConcurrencyPolicy(executionContext, logicalView);

        List<Cursor> inputCursors = new ArrayList<>();

        List<RexDynamicParam> scalarList = logicalView.getScalarList();
        SubqueryUtils.buildScalarSubqueryValue(scalarList, executionContext);// handle scalar subquery

        List<RelNode> inputs;
        if (executionContext.isModifyCrossDb()) {
            Map<Integer, ParameterContext> params = executionContext.getParams().getCurrentParameter();
            ReplaceCallWithLiteralVisitor visitor = new ReplaceCallWithLiteralVisitor(Lists.newArrayList(),
                params, RexUtils.getEvalFunc(executionContext), true);

            // Dynamic functions will be calculated in buildSqlTemplate()
            final SqlSelect sqlTemplate = (SqlSelect) logicalView.getSqlTemplate(visitor);
            inputs = logicalView.getInnerInput(
                sqlTemplate, ExecUtils.getUnionOptHelper(logicalView, executionContext), executionContext);
        } else {
            inputs = ExecUtils.getInputs(logicalView, executionContext, false);
        }

        String schemaName = StringUtils.isEmpty(
            logicalView.getSchemaName()) ? executionContext.getSchemaName() : logicalView.getSchemaName();

        if (inputs.size() == 1) {
            return ExecutorContext.getContext(schemaName).getTopologyExecutor().execByExecPlanNode
                (inputs.get(0), executionContext);
        } else {
            executeWithConcurrentPolicy(executionContext, inputs, queryConcurrencyPolicy, inputCursors, schemaName);

            /*
             * 记录全表扫描，当前判断条件为访问分片数大于1，用于后续sql.log输出
             */
            if (inputs.size() > 1) {
                executionContext.setHasScanWholeTable(true);
            }

            List<Cursor> newInputCursors = new ArrayList<>();

            for (int i = 0; i < inputCursors.size(); i++) {
                LogicalViewResultCursor tsrc =
                    new LogicalViewResultCursor((AbstractCursor) inputCursors.get(i), executionContext);
                newInputCursors.add(tsrc);
            }

            if (newInputCursors.size() == 0 && logicalView.isNewPartDbTbl()) {
                /**
                 * LogicalView has NOT any paritions after sharding, then return an empty cursor
                 */
                String tbName = logicalView.getLogicalTableName();
                List<ColumnMeta> returnCm = CalciteUtils.buildColumnMeta(logicalView, tbName);
                ArrayResultCursor resultCursor = new ArrayResultCursor(tbName);
                for (int i = 0; i < returnCm.size(); i++) {
                    resultCursor.addColumn(returnCm.get(i));
                }
                LogicalViewResultCursor lvRs = new LogicalViewResultCursor(resultCursor, executionContext);
                newInputCursors.add(lvRs);
            }
            return MultiCursorAdapter.wrap(newInputCursors);
        }

    }
}
