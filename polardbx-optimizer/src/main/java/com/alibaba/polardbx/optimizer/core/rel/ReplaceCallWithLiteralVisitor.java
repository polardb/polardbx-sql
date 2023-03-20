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

package com.alibaba.polardbx.optimizer.core.rel;

import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.google.common.collect.ImmutableList;
import com.alibaba.polardbx.common.charset.CharsetName;
import com.alibaba.polardbx.common.exception.NotSupportException;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.optimizer.core.TddlOperatorTable;
import com.alibaba.polardbx.optimizer.core.TddlRelDataTypeSystemImpl;
import com.alibaba.polardbx.optimizer.core.TddlTypeFactoryImpl;
import io.airlift.slice.Slice;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexCallParam;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.DateTimeStringUtils;
import org.apache.calcite.util.NlsString;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * 1. Some functions can't be pushed down, like LAST_INSERT_ID().
 * <p>
 * 2. Sharding keys of base table and secondary tables can only be literal.
 * <p>
 * 3. In transactions or in gsi, unique keys can only be literal.
 * <p>
 * 4. For broadcast tables or gsi tables, all nondeterministic functions, like NOW(),
 * should be transformed to a constant. For now, this class is only used in DML.
 *
 * @author minggong.zm 2018/3/13
 */
public class ReplaceCallWithLiteralVisitor extends RelShuttleImpl {

    private ReplaceCallRexVisitor rexVisitor;
    private Function<RexNode, Object> calcValueFunc;
    private RexBuilder builder;
    // columns to be calculated
    private List<Integer> calcColumnIndexes;

    private boolean replaceRexCallParam;

    public ReplaceCallWithLiteralVisitor(List<Integer> calcColumnIndexes, Map<Integer, ParameterContext> params,
                                         Function<RexNode, Object> calcValueFunc, boolean needConsistency) {
        this.calcColumnIndexes = calcColumnIndexes;
        this.calcValueFunc = calcValueFunc;
        this.rexVisitor = new ReplaceCallRexVisitor(params, needConsistency);
    }

    private RexBuilder getRexBuilder() {
        if (builder == null) {
            RelDataTypeFactory typeFactory = new TddlTypeFactoryImpl(TddlRelDataTypeSystemImpl.getInstance());
            builder = new RexBuilder(typeFactory);
        }
        return builder;
    }

    public LogicalInsert visit(LogicalInsert logicalInsert) {
        RelNode input = logicalInsert.getInput();

        // Batch insert won't include RexCalls, so skip it.
        if (input != null && logicalInsert.getBatchSize() == 0) {
            // VALUES clause
            input = input.accept(this);
        }

        List<RexNode> duplicateKeyUpdateList = logicalInsert.getDuplicateKeyUpdateList();
        if (duplicateKeyUpdateList != null && !duplicateKeyUpdateList.isEmpty()) {
            duplicateKeyUpdateList = visitDuplicateKeyUpdateList(duplicateKeyUpdateList);
        }

        // Always create a new one, because PhyTableInsertSharder is a member
        // and can't be shared between two threads.
        LogicalInsert newInsert = new LogicalInsert(logicalInsert.getCluster(),
            logicalInsert.getTraitSet(),
            logicalInsert.getTable(),
            logicalInsert.getCatalogReader(),
            input,
            logicalInsert.getOperation(),
            logicalInsert.isFlattened(),
            logicalInsert.getInsertRowType(),
            logicalInsert.getKeywords(),
            duplicateKeyUpdateList,
            logicalInsert.getBatchSize(),
            logicalInsert.getAppendedColumnIndex(),
            logicalInsert.getHints(),
            logicalInsert.getTableInfo(),
            logicalInsert.getPrimaryInsertWriter(),
            logicalInsert.getGsiInsertWriters(),
            logicalInsert.getAutoIncParamIndex(),
            logicalInsert.getUnOptimizedLogicalDynamicValues(),
            logicalInsert.getUnOptimizedDuplicateKeyUpdateList());
        return newInsert;
    }

    public LogicalModify visit(LogicalModify logicalModify) {
        RelNode input = logicalModify.getInput();
        // WHERE clause
        input = input.accept(this);

        List<RexNode> updateList = logicalModify.getSourceExpressionList();
        if (updateList != null && !updateList.isEmpty()) {
            updateList = visitUpdateList(updateList);
        }

        return new LogicalModify(logicalModify.getCluster(),
            logicalModify.getTraitSet(),
            logicalModify.getTable(),
            logicalModify.getCatalogReader(),
            input,
            logicalModify.getOperation(),
            logicalModify.getUpdateColumnList(),
            updateList,
            logicalModify.isFlattened(),
            logicalModify.getKeywords(),
            logicalModify.getHints(),
            logicalModify.getHintContext(),
            logicalModify.getTableInfo(),
            logicalModify.getExtraTargetTables(),
            logicalModify.getExtraTargetColumns(),
            logicalModify.getPrimaryModifyWriters(),
            logicalModify.getGsiModifyWriters(),
            logicalModify.isWithoutPk());
    }

    @Override
    public RelNode visit(RelNode other) {
        if (other instanceof LogicalDynamicValues) {
            return visit((LogicalDynamicValues) other);
        }
        return super.visit(other);
    }

    public RelNode visit(LogicalDynamicValues values) {
        ImmutableList<ImmutableList<RexNode>> tuples = values.getTuples();
        List<RelDataTypeField> fieldList = values.getRowType().getFieldList();
        List<ImmutableList<RexNode>> newTuples = new ArrayList<>(tuples.size());
        for (List<RexNode> tuple : tuples) {
            List<RexNode> rexList = new ArrayList<>(tuple.size());
            for (int i = 0; i < tuple.size(); i++) {
                RexNode node = tuple.get(i);

                if (replaceRexCallParam && node instanceof RexCallParam) {
                    node = ((RexCallParam) node).getRexCall();
                }

                // must compute
                boolean shouldCalc = false;
                // may compute
                boolean shouldCheck = false;
                if (node instanceof RexCall) {
                    RexCall call = (RexCall) node;

                    if (calcColumnIndexes.contains(i)) {
                        if (call.getKind() == SqlKind.DEFAULT) {
                            String columnName = fieldList.get(i).getName();
                            throw new NotSupportException("Using DEFAULT for column " + columnName);
                        } else if (call.getOperator() == TddlOperatorTable.NEXTVAL) {
                            // will be calculated in
                            // ReplaceSequenceWithLiteralVisitor
                            shouldCalc = false;
                        } else {
                            shouldCalc = true;
                        }
                    }

                    shouldCheck = true;
                }

                // Only DynamicParam inside RexCall is calculated.
                if (shouldCalc) {
                    RexNode newNode = rexVisitor.compute(node);
                    rexList.add(newNode);
                } else if (shouldCheck) {
                    RexNode newNode = rexVisitor.mayCompute(node);
                    rexList.add(newNode);
                } else {
                    rexList.add(node);
                }
            }

            newTuples.add(ImmutableList.copyOf(rexList));
        }
        return LogicalDynamicValues.createDrdsValues(values.getCluster(),
            values.getTraitSet(),
            values.getRowType(),
            ImmutableList.copyOf(newTuples));
    }

    /**
     * insert ... values ... ON DUPLICATE KEY UPDATE ...
     */
    private List<RexNode> visitDuplicateKeyUpdateList(List<RexNode> updateList) {
        List<RexNode> newUpdateList = new ArrayList<>(updateList.size());
        for (RexNode updateNode : updateList) {
            // format of each node: identifier = expression
            if (!(updateNode instanceof RexCall)) {
                newUpdateList.add(updateNode);
                continue;
            }

            RexCall oldCall = (RexCall) updateNode;
            RexNode expr = oldCall.getOperands().get(1);
            RexNode newNode = oldCall;
            if (expr instanceof RexCall) {
                RexNode newExpr = rexVisitor.mayCompute(expr);
                if (expr != newExpr) {
                    List<RexNode> clonedOperands = ImmutableList.of(oldCall.getOperands().get(0), newExpr);
                    newNode = getRexBuilder().makeCall(oldCall.getType(), oldCall.getOperator(), clonedOperands);
                }
            }
            newUpdateList.add(newNode);
        }
        return newUpdateList;
    }

    /**
     * update ... SET ... where ...
     */
    private List<RexNode> visitUpdateList(List<RexNode> updateList) {
        List<RexNode> newUpdateList = new ArrayList<>(updateList.size());
        for (RexNode updateNode : updateList) {
            // format of each node: expression
            if (!(updateNode instanceof RexCall)) {
                newUpdateList.add(updateNode);
                continue;
            }

            RexNode newNode = rexVisitor.mayCompute(updateNode);
            newUpdateList.add(newNode);
        }
        return newUpdateList;
    }

    /**
     * Filter in WHERE clause
     */
    @Override
    public RelNode visit(LogicalFilter filter) {
        final LogicalFilter visited = (LogicalFilter) super.visit(filter);

        RexNode condition = visited.getCondition();
        if (!(condition instanceof RexCall)) {
            return visited;
        }

        RexNode newCondition = rexVisitor.mayCompute(condition);
        if (newCondition == condition) {
            return visited;
        }

        return visited.copy(visited.getTraitSet(), visited.getInput(), newCondition);
    }

    @Override
    public RelNode visit(LogicalProject project) {
        final LogicalProject visited = (LogicalProject) super.visit(project);

        final List<RexNode> projects = visited.getProjects();

        boolean updated = false;
        final List<RexNode> newProjects = new ArrayList<>(projects.size());
        for (RexNode rex : projects) {
            if (rex instanceof RexCall && !notReplaceWhenProject((RexCall) rex)) {
                final RexNode newRex = rexVisitor.mayCompute(rex);

                newProjects.add(newRex);
                updated |= (newRex != rex);
            } else {
                newProjects.add(rex);
            }
        }

        if (updated) {
            return visited.copy(visited.getTraitSet(), visited.getInput(), newProjects, visited.getRowType());
        } else {
            return visited;
        }

    }

    private boolean notReplaceWhenProject(RexCall call) {
        String functionName = call.getOperator().getName();
        return TddlOperatorTable.UUID.getName().equalsIgnoreCase(functionName)
            || TddlOperatorTable.UUID_SHORT.getName().equalsIgnoreCase(functionName);
    }

    /**
     * Check type and compute functions. If the function is a sharding key,
     * compute it. If the function can not be pushed down ( like
     * LAST_INSERT_ID() ), compute it. If the function can be pushed down, check
     * its operands, which may be functions can't be pushed down. If the
     * function is not deterministic, compute it. If a parent node need to be
     * computed, its child node must also be computed. If a child node is
     * cloned, its parent node must also be cloned.
     */
    private class ReplaceCallRexVisitor extends RexShuttle {

        private Map<Integer, ParameterContext> params;
        // if the function is not deterministic, it should be calculated.
        private boolean needConsistency;
        // Whether check its type or must compute the function.
        private boolean mustCompute;

        ReplaceCallRexVisitor(Map<Integer, ParameterContext> params, boolean needConsistency) {
            this.params = params;
            this.needConsistency = needConsistency;
            mustCompute = false;
        }

        /**
         * It's a sharding key or unique key, and must be computed.
         */
        public RexNode compute(RexNode node) {
            if (!needComputeNow(node)) {
                return node;
            }
            mustCompute = true;
            node = node.accept(this);
            mustCompute = false;
            return node;
        }

        /**
         * It's not a sharding key, but it may include some functions that
         * can'be pushed down, or not deterministic.
         */
        public RexNode mayCompute(RexNode node) {
            if (!needComputeNow(node)) {
                return node;
            }
            mustCompute = false;
            node = node.accept(this);
            mustCompute = false;
            return node;
        }

        /**
         * If it's NEXTVAL, compute later in ReplaceSequenceWithLiteralVisitor
         */
        private boolean needComputeNow(RexNode node) {
            if (node instanceof RexCall) {
                if (((RexCall) node).getOperator() == TddlOperatorTable.NEXTVAL) {
                    return false;
                }
            }
            return true;
        }

        /**
         * It can't be computed if parameters not function or literal
         */
        private boolean verifyOperands(final RexCall call) {
            for (RexNode node : call.operands) {
                if (node instanceof RexCall) {
                    if (!verifyOperands((RexCall) node)) {
                        return false;
                    }
                } else if (node instanceof RexDynamicParam) {
                } else if (!(node instanceof RexLiteral)) {
                    return false;
                }
            }
            return true;
        }

        @Override
        public RexNode visitCall(final RexCall call) {
            // If the function can't be pushed down, all its operands must be
            // computed too.
            if (!mustCompute) {
                if (!call.getOperator().canPushDown()) {
                    mustCompute = true;
                } else if (needConsistency && call.getOperator().isDynamicFunction()) {
                    mustCompute = true;
                }
            }

            // If it's a nested NEXTVAL, we can't compute.
            if (mustCompute && call.getOperator() == TddlOperatorTable.NEXTVAL) {
                throw new TddlRuntimeException(ErrorCode.ERR_FUNCTION, "'" + call + "'");
            }

            if (!mustCompute) {
                // Save current state to avoid being overridden by children.
                boolean curMustCompute = mustCompute;
                boolean update = false;
                ImmutableList.Builder<RexNode> operandBuilder = ImmutableList.builder();
                for (RexNode operand : call.operands) {
                    RexNode clonedOperand = operand.accept(this);
                    mustCompute = curMustCompute;
                    if (clonedOperand != operand) {
                        update = true;
                    }
                    operandBuilder.add(clonedOperand);
                }

                // its children were not even changed
                if (!update) {
                    return call;
                } else {
                    return getRexBuilder().makeCall(call.getType(), call.getOperator(), operandBuilder.build());
                }
            }

            // It can't be computed if parameters not function or literal
            if (!verifyOperands(call)) {
                return call;
            }

            // If must compute, apply RexCall directly
            Object value = calcValueFunc.apply(call);

            if (value instanceof Timestamp || value instanceof Date || value instanceof Time) {

                // For all now-based func (e.g.
                // now/current_timestamp/unix_timestamp...)
                // Should use the String Literal to store its pre-compute
                // timestampe value
                // during doing insert sql.
                value = DateTimeStringUtils.convertDateTimeToString(value, call.getType().getSqlTypeName());
                NlsString timeStr = new NlsString(DataTypes.StringType.convertFrom(value), null, null);
                RexNode literal = getRexBuilder().makeCharLiteral(timeStr);
                return literal;

            } else if (value instanceof Number) {
                if (call.getType().getSqlTypeName() == SqlTypeName.BOOLEAN) {
                    value = ((Number) value).longValue() != 0;
                }
            } else if (value instanceof Slice) {
                value = ((Slice) value).toString(CharsetName.DEFAULT_STORAGE_CHARSET_IN_CHUNK);
            }
            // Only RexLiteral can be operand of function
            RexNode literal = getRexBuilder().makeLiteral(value, call.getType(), true);
            return literal;
        }

        /**
         * If it's an argument of a function, it must be a literal.
         */
        @Override
        public RexNode visitDynamicParam(RexDynamicParam dynamicParam) {
            if (!mustCompute) {
                return dynamicParam;
            }

            Object value = params.get(dynamicParam.getIndex() + 1).getValue();
            RexNode node = getRexBuilder().makeLiteral(value, dynamicParam.getType(), true);
            // If it's a CAST function
            if (node instanceof RexCall && node.getKind() == SqlKind.CAST) {
                node = ((RexCall) node).getOperands().get(0);
            }
            return node;
        }
    }

    public boolean isReplaceRexCallParam() {
        return replaceRexCallParam;
    }

    public void setReplaceRexCallParam(boolean replaceRexCallParam) {
        this.replaceRexCallParam = replaceRexCallParam;
    }

}
