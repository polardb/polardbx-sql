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

package com.alibaba.polardbx.executor.utils;

import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.utils.CalciteUtils;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ListenableFuture;
import com.alibaba.polardbx.common.properties.ConnectionProperties;
import com.alibaba.polardbx.common.utils.ExecutorMode;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.executor.ExecutorHelper;
import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.GatherCursor;
import com.alibaba.polardbx.executor.cursor.impl.MultiCursorAdapter;
import com.alibaba.polardbx.executor.mpp.deploy.ServiceProvider;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.expression.calc.IExpression;
import com.alibaba.polardbx.optimizer.core.rel.LogicalView;
import com.alibaba.polardbx.optimizer.core.row.Row;
import com.alibaba.polardbx.optimizer.utils.FunctionUtils;
import com.alibaba.polardbx.optimizer.utils.RexUtils;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelVisitor;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCorrelVariable;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SemiJoinType;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.alibaba.polardbx.executor.ExecutorHelper.useCursorExecutorMode;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class SubqueryApply {

    private final String subqueryId;
    protected final RelNode plan;
    protected final Chunk.ChunkRow inputRow;
    protected ExecutionContext parentEc;
    protected ExecutionContext subQueryEc;
    private final CorrelationId correlateId;
    protected SemiJoinType semiJoinType;

    protected boolean emptyResult = true;
    protected Cursor cursor;
    private AtomicBoolean isClosed = new AtomicBoolean(false);
    protected boolean isFinish = false;
    protected boolean localMode;
    private Object resultValue;
    private boolean hasApplyInLogicalView = false;
    private IExpression iExpression;
    private List<RexNode> leftConditions;
    private SqlKind opKind;
    private RelDataType correlateDataRowType;
    private boolean hasNull;
    private boolean maxOnerow = true;

    public SubqueryApply(String subqueryId,
                         Chunk.ChunkRow inputRow,
                         ExecutionContext parentEc,
                         CorrelationId correlateId,
                         SemiJoinType semiJoinType,
                         RelNode plan,
                         List<RexNode> leftConditions,
                         SqlKind opKind,
                         RelDataType correlateDataRowType,
                         boolean maxOnerow) {
        this.subqueryId = subqueryId;
        this.plan = plan;
        this.inputRow = inputRow;
        this.parentEc = parentEc;
        this.correlateId = correlateId;
        this.semiJoinType = semiJoinType;
        this.leftConditions = leftConditions;
        this.opKind = opKind;
        this.correlateDataRowType = correlateDataRowType;
        this.maxOnerow = maxOnerow;
    }

    public void prepare() {
        subQueryEc = SubqueryUtils.prepareSubqueryContext(parentEc, subqueryId);
        //FIXME 断点
        subQueryEc.getCorrelateRowMap().remove(correlateId);
        // find all correlateVariableScalars in LogicalView
        CorrelateInLogicalViewFinder correlateInLogicalViewFinder = new CorrelateInLogicalViewFinder();
        correlateInLogicalViewFinder.go(plan);

        // replace rexfieldAccess with real value in logicalview
        List<RexFieldAccess> rexFieldAccesses = correlateInLogicalViewFinder.getCorrelateVariableScalar();
        for (RexFieldAccess rexFieldAccess : rexFieldAccesses) {
            hasApplyInLogicalView = true;
            RexCorrelVariable rexCorrelVariable = (RexCorrelVariable) (rexFieldAccess).getReferenceExpr();
            if (correlateId.equals(rexCorrelVariable.getId())) {
                DataType dataType = CalciteUtils.getType(rexFieldAccess.getField());
                Object value = dataType.convertFrom(inputRow.getObject(rexFieldAccess.getField().getIndex()));
                RexNode literal = plan
                    .getCluster()
                    .getRexBuilder()
                    .makeLiteral(value, rexFieldAccess.getType(), true);
                rexFieldAccess.setReplace(literal);
                subQueryEc.getCorrelateFieldInViewMap().put(rexFieldAccess, literal);
            }
        }

        subQueryEc.registCorrelateRow(correlateId, inputRow);
        if (leftConditions != null && leftConditions.size() > 0) {
            List<RexNode> conditions = Lists.newArrayList();
            RexBuilder rexBuilder = plan.getCluster().getRexBuilder();
            for (int i = 0; i < leftConditions.size(); i++) {
                conditions
                    .add(rexBuilder
                        .makeCall(RexUtil.op(opKind), RexUtil
                                .replaceConditionToCorrelate(leftConditions.get(i), correlateDataRowType, correlateId),
                            rexBuilder.makeInputRef(plan, i)));
            }
            this.iExpression =
                RexUtils.buildRexNode(conditions.size() > 1 ? rexBuilder.makeCall(SqlStdOperatorTable.AND, conditions) :
                    conditions.get(0), subQueryEc);
        }
        // build needed chunk executor for apply
        if (!isClosed.get()) {
            //for thread-safe
            if (!ExecUtils.isTpMode(subQueryEc)) {
                //子查询强制走Local，避免走到MPP模式
                subQueryEc.setExecuteMode(ExecutorMode.AP_LOCAL);
            }
            subQueryEc.setStartTime(System.nanoTime());
            Map<String, Object> extraCmd = new HashMap<>();
            extraCmd.putAll(subQueryEc.getExtraCmds());
            extraCmd.put(ConnectionProperties.PARALLELISM, 1);
            subQueryEc.setExtraCmds(extraCmd);

            if (useCursorExecutorMode(plan) && !hasApplyInLogicalView) {
                //不是apply子查询尽量用cursor
                cursor = ExecutorHelper.executeByCursor(plan, subQueryEc, false);
                if (cursor instanceof MultiCursorAdapter) {
                    cursor = new GatherCursor(((MultiCursorAdapter) cursor).getSubCursors(), subQueryEc);
                }
            } else {
                localMode = true;
                cursor = ExecutorHelper.executeLocal(plan, subQueryEc, false, false);
            }
            if (isClosed.get()) {
                try {
                    cursor.close(Lists.newArrayList());
                } finally {
                    subQueryEc.clearAllMemoryPool();
                }
            }
        }
    }

    //FIXME 这个方法是阻塞式的，只有数据被消费完，才让出线程。如果当前线程是Task-Executor，那么这里是有死锁风险的。
    public void processUntilFinish() {
        while (!isFinish) {
            process();
            if (!isFinish) {
                try {
                    if (!cursor.isBlocked().isDone()) {
                        try {
                            cursor.isBlocked().get(100, MILLISECONDS);
                        } catch (TimeoutException e) {
                            //ignore
                        }
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }

    public void process() {
        if (isFinish) {
            return;
        }
        Row row = cursor.next();

        // exit code for time slicing executor
        if (row == null) {
            if (localMode && !cursor.isFinished()) {
                return;
            }
        }
        Object o;
        switch (semiJoinType) {
        // left type represent scalar subquery
        case LEFT:
            // exit code for time slicing executor
            if (row == null) {
                isFinish = true;
                if (emptyResult) {
                    resultValue = RexDynamicParam.DYNAMIC_SPECIAL_VALUE.EMPTY;
                }
                return;
            }
            Object value = row.getObject(0);
            // multi value
            if (!emptyResult && !(ConfigDataMode.isFastMock()) && maxOnerow) {
                GeneralUtil.nestedException("Subquery returns more than 1 row");
            }
            if (!emptyResult) {
                if (resultValue instanceof List) {
                    ((List) resultValue).add(value);
                } else {
                    List tmpResult = Lists.newLinkedList();
                    tmpResult.add(resultValue);
                    tmpResult.add(value);
                    resultValue = tmpResult;
                }
                return;
            }

            // single value
            emptyResult = false;
            resultValue = value;
            return;
        case SEMI:
            // exit code for time slicing executor
            if (row == null) {
                if (hasNull) {
                    resultValue = null;
                    isFinish = true;
                    return;
                } else {
                    resultValue = 0;
                    isFinish = true;
                    return;
                }
            }

            // semi type has no condition only care if the first row is null
            if (iExpression == null) {
                resultValue = 1;
                isFinish = true;
                return;
            }

            o = iExpression.eval(row);
            if (o == null) {
                // hasNull would affect result when there is no row matching the condition
                hasNull = true;
            } else if (FunctionUtils.convertConditionToBoolean(o)) {
                // semi type first match first out
                resultValue = 1;
                isFinish = true;
                return;
            }
            return;
        case ANTI:
            // exit code for time slicing executor
            if (row == null) {
                if (hasNull) {
                    resultValue = null;
                    isFinish = true;
                    return;
                } else {
                    resultValue = 1;
                    isFinish = true;
                    return;
                }
            }

            // anty type has no condition only care if the first row is not null
            if (iExpression == null) {
                resultValue = 0;
                isFinish = true;
                return;
            }

            o = iExpression.eval(row);
            if (o == null) {
                // hasNull would affect anti result when there is no row not matching the condition
                hasNull = true;
            } else if (!FunctionUtils.convertConditionToBoolean(o)) {
                // anti type first not match and out
                resultValue = 0;
                isFinish = true;
                return;
            }
            return;
        default:
            isFinish = true;
            throw GeneralUtil.nestedException("Unsupported Subquery type " + semiJoinType);
        }
    }

    public boolean isFinished() {
        return isFinish;
    }

    public ListenableFuture<?> isBlocked() {
        return cursor.isBlocked();
    }

    public void close() {
        if (isClosed.compareAndSet(false, true)) {
            this.isFinish = true;
            try {
                if (cursor != null) {
                    cursor.close(Lists.newArrayList());
                }
            } finally {
                ServiceProvider.getInstance().getServer().getQueryManager().cancelQuery(
                    parentEc.getTraceId() + "_" + subqueryId);
                if (subQueryEc != null) {
                    subQueryEc.clearAllMemoryPool();
                }
            }
        }
    }

    public Object getResultValue() {
        return resultValue;
    }

    /**
     * Visitor that finds all Correlate variables in logicalview.
     */
    public static class CorrelateInLogicalViewFinder extends RelVisitor {

        private List<RexFieldAccess> correlateVariableScalar = Lists.newArrayList();

        // implement RelVisitor
        @Override
        public void visit(RelNode p, int ordinal, RelNode parent) {
            super.visit(p, ordinal, parent);
            if (p instanceof LogicalView) {
                correlateVariableScalar.addAll(((LogicalView) p).getCorrelateVariableScalar());
            }
        }

        public List<RexFieldAccess> getCorrelateVariableScalar() {
            return correlateVariableScalar;
        }
    }
}
