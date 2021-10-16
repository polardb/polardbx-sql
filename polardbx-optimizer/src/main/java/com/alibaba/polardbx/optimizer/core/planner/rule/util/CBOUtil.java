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

package com.alibaba.polardbx.optimizer.core.planner.rule.util;

import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.config.meta.DrdsRelOptCostImpl;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.IndexMeta;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.DrdsConvention;
import com.alibaba.polardbx.optimizer.core.MppConvention;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypeUtil;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.planner.ExecutionPlan;
import com.alibaba.polardbx.optimizer.core.planner.rule.MysqlAggRule;
import com.alibaba.polardbx.optimizer.core.planner.rule.MysqlCorrelateRule;
import com.alibaba.polardbx.optimizer.core.planner.rule.MysqlJoinRule;
import com.alibaba.polardbx.optimizer.core.planner.rule.MysqlMultiJoinToLogicalJoinRule;
import com.alibaba.polardbx.optimizer.core.planner.rule.MysqlSemiJoinRule;
import com.alibaba.polardbx.optimizer.core.planner.rule.MysqlSortRule;
import com.alibaba.polardbx.optimizer.core.planner.rule.MysqlTableScanRule;
import com.alibaba.polardbx.optimizer.core.rel.CheckBkaJoinRelVisitor;
import com.alibaba.polardbx.optimizer.core.rel.Gather;
import com.alibaba.polardbx.optimizer.core.rel.LogicalIndexScan;
import com.alibaba.polardbx.optimizer.core.rel.LogicalView;
import com.alibaba.polardbx.optimizer.index.Index;
import com.alibaba.polardbx.optimizer.index.IndexUtil;
import com.alibaba.polardbx.optimizer.utils.RexUtils;
import com.alibaba.polardbx.optimizer.view.DrdsViewTable;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTrait;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.hep.HepMatchOrder;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.plan.hep.HepRelVertex;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.prepare.RelOptTableImpl;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollationImpl;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelDistributionTraitDef;
import org.apache.calcite.rel.RelDistributions;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalSemiJoin;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rel.logical.LogicalTableLookup;
import org.apache.calcite.rel.metadata.RelColumnOrigin;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.rules.JoinToMultiJoinRule;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexCallBinding;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.runtime.PredicateImpl;
import org.apache.calcite.schema.Table;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.validate.SqlMonotonicity;
import org.apache.calcite.util.mapping.MappingType;
import org.apache.calcite.util.mapping.Mappings;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

public class CBOUtil {

    private static final Logger logger = LoggerFactory.getLogger(CBOUtil.class);

    public static LogicalView sortLimitSingleGroupLogicalView(LogicalView logicalView, final Sort originalSort) {
        RelCollation relCollation = originalSort.getCollation();
        if (originalSort.fetch == null && originalSort.offset == null) {
            if (traitSetSatisfyCollation(logicalView.getTraitSet(), relCollation)) {
                return logicalView;
            }
        }

        RexNode offset = originalSort.offset;
        if (offset instanceof RexLiteral) {
            if (DataTypes.ULongType.convertFrom(((RexLiteral) offset).getValue()).longValue() == 0) {
                offset = null;
            }
        }

        LogicalSort sort;
        if (traitSetSatisfyCollation(logicalView.getTraitSet(), relCollation)) {
            sort = LogicalSort
                .create(logicalView.getPushedRelNode(), RelCollations.EMPTY, offset, originalSort.fetch);
        } else {
            sort = LogicalSort
                .create(logicalView.getPushedRelNode(), relCollation, offset, originalSort.fetch);
        }

        LogicalView newLogicalView = logicalView.copy(originalSort.getTraitSet().replace(relCollation));
        newLogicalView.push(sort);
        return newLogicalView;
    }

    public static LogicalView sortLimitLogicalView(LogicalView logicalView, final Sort originalSort) {
        RelCollation relCollation = originalSort.getCollation();

        if (originalSort.fetch == null && originalSort.offset == null) {
            if (traitSetSatisfyCollation(logicalView.getTraitSet(), relCollation)) {
                return logicalView;
            }
        }

        RexNode fetch = calPushDownFetch(originalSort);

        LogicalSort sort;
        if (traitSetSatisfyCollation(logicalView.getTraitSet(), relCollation)) {
            sort = LogicalSort.create(logicalView.getPushedRelNode(), RelCollations.EMPTY, null, fetch);
        } else {
            sort = LogicalSort.create(logicalView.getPushedRelNode(), relCollation, null, fetch);
        }

        LogicalView newLogicalView = logicalView.copy(logicalView.getTraitSet().replace(relCollation).replace(
            DrdsConvention.INSTANCE));
        newLogicalView.push(sort);
        return newLogicalView;
    }

    public static RexNode calPushDownFetch(Sort originalSort) {
        RexBuilder builder = originalSort.getCluster().getRexBuilder();
        RexNode fetch = originalSort.fetch;
        if (originalSort.offset != null && originalSort.fetch != null) {
            Map<Integer, ParameterContext> parameterContextMap = PlannerContext.getPlannerContext(
                originalSort).getParams().getCurrentParameter();

            if (originalSort.fetch instanceof RexDynamicParam || originalSort.offset instanceof RexDynamicParam) {
                /**
                 * fetch or offset be parameterized.
                 */
                fetch = builder.makeCall(SqlStdOperatorTable.PLUS, fetch, originalSort.offset);

            } else {
                long fetchVal = getRexParam(originalSort.fetch, parameterContextMap);
                long offsetVal = getRexParam(originalSort.offset, parameterContextMap);
                /**
                 * fetch or offset be parameterized.
                 */
                fetch = builder.makeBigIntLiteral(offsetVal + fetchVal);
            }
        }
        return fetch;
    }

    private static boolean traitSetSatisfyCollation(RelTraitSet relTraits, RelCollation relCollation) {
        for (int i = 0; i < relTraits.size(); i++) {
            RelTrait trait = relTraits.get(i);
            if (trait.getTraitDef() == RelCollationTraitDef.INSTANCE) {
                if (trait.satisfies(relCollation)) {
                    return true;
                }
            }
        }
        return false;
    }

    public static boolean checkHashJoinCondition(
        Join join, RexNode condition, int leftBound,
        RexNodeHolder equalConditionHolder, RexNodeHolder otherConditionHolder) {
        boolean canHashJoin = false;

        if (condition == null) {
            return false;
        }

        if (RexUtils.forceNLJoin(join)) {
            return false;
        }

        if (condition instanceof RexCall) {
            final RexCall currentCondition = (RexCall) condition;
            switch (currentCondition.getKind()) {
            case EQUALS: {
                if (currentCondition.getOperands().size() == 2) {
                    RexNode operand1 = currentCondition.getOperands().get(0);
                    RexNode operand2 = currentCondition.getOperands().get(1);
                    if (operand1 instanceof RexInputRef && operand2 instanceof RexInputRef) {
                        int indexOp1 = ((RexInputRef) operand1).getIndex();
                        int indexOp2 = ((RexInputRef) operand2).getIndex();
                        RelDataType relDataTypeLeft;
                        RelDataType relDataTypeRight;
                        if ((indexOp1 < leftBound && indexOp2 < leftBound) || (indexOp1 >= leftBound
                            && indexOp2 >= leftBound)) {
                            otherConditionHolder.setRexNode(currentCondition);
                            return false;
                        } else if (indexOp1 < leftBound) {
                            relDataTypeLeft = join.getLeft().getRowType().getFieldList().get(indexOp1).getType();
                            relDataTypeRight =
                                join.getRight().getRowType().getFieldList().get(indexOp2 - leftBound).getType();
                        } else {
                            relDataTypeLeft = join.getLeft().getRowType().getFieldList().get(indexOp2).getType();
                            relDataTypeRight =
                                join.getRight().getRowType().getFieldList().get(indexOp1 - leftBound).getType();
                        }
                        canHashJoin = hashJoinTypeCheck(Pair.of(relDataTypeLeft, relDataTypeRight));
                    }
                }
                if (!canHashJoin) {
                    otherConditionHolder.setRexNode(currentCondition);
                } else {
                    equalConditionHolder.setRexNode(currentCondition);
                }
                break;
            }
            case AND: {
                RexNode otherCondition = null;
                RexNode equalCondition = null;
                for (int i = 0; i < currentCondition.getOperands().size(); i++) {
                    canHashJoin |= checkHashJoinCondition(join, currentCondition.getOperands().get(i), leftBound,
                        equalConditionHolder, otherConditionHolder);
                    if (otherConditionHolder.getRexNode() != null) {
                        if (otherCondition == null) {
                            otherCondition = otherConditionHolder.getRexNode();
                        } else {
                            otherCondition = join.getCluster().getRexBuilder().makeCall(
                                SqlStdOperatorTable.AND,
                                Arrays.asList(otherCondition, otherConditionHolder.getRexNode()));
                        }
                        otherConditionHolder.setRexNode(null);
                    }

                    if (equalConditionHolder.getRexNode() != null) {
                        if (equalCondition == null) {
                            equalCondition = equalConditionHolder.getRexNode();
                        } else {
                            equalCondition = join.getCluster().getRexBuilder().makeCall(
                                SqlStdOperatorTable.AND,
                                Arrays.asList(equalCondition, equalConditionHolder.getRexNode()));
                        }
                        equalConditionHolder.setRexNode(null);
                    }

                }
                otherConditionHolder.setRexNode(otherCondition);
                equalConditionHolder.setRexNode(equalCondition);
                break;
            }
            default: {
                canHashJoin = false;
                otherConditionHolder.setRexNode(condition);
            }
            }
        } else {
            otherConditionHolder.setRexNode(condition);
        }
        return canHashJoin;
    }

    public static boolean hashJoinTypeCheck(Pair<RelDataType, RelDataType> relDataTypePair) {
        RelDataType relDataType1 = relDataTypePair.getKey();
        RelDataType relDataType2 = relDataTypePair.getValue();

        DataType dt1 = DataTypeUtil.calciteToDrdsType(relDataType1);
        DataType dt2 = DataTypeUtil.calciteToDrdsType(relDataType2);

        if (dt1 == null || dt2 == null) {
            return false;
        }

        if (DataTypeUtil.equalsSemantically(dt1, dt2)) {
            return true;
        } else if ((DataTypeUtil.isNumberSqlType(dt1) || DataTypeUtil.isStringSqlType(dt1)) &&
            (DataTypeUtil.isNumberSqlType(dt2) || DataTypeUtil.isStringSqlType(dt2))) {
            return true;
        } else {
            return false;
        }
    }

    public static boolean bkaTypeCheck(Pair<RelDataType, RelDataType> relDataTypePair) {
        RelDataType relDataType1 = relDataTypePair.getKey();
        RelDataType relDataType2 = relDataTypePair.getValue();

        DataType dt1 = DataTypeUtil.calciteToDrdsType(relDataType1);
        DataType dt2 = DataTypeUtil.calciteToDrdsType(relDataType2);

        if (dt1 == null || dt2 == null) {
            return false;
        }
        if (DataTypeUtil.isFloatSqlType(dt2)) {
            // float number can not be materialized
            return false;
        } else if (DataTypeUtil.isNumberSqlType(dt1)) {
            if (!DataTypeUtil.isStringSqlType(dt2) && !DataTypeUtil.isNumberSqlType(dt2)) {
                // if dt2 neither of number or string may mis-match with dt1
                return false;
            } else {
                return true;
            }
        } else if (DataTypeUtil.equalsSemantically(dt1, DataTypes.BooleanType)) {
            // BooleanType may come from tinyint(1) which will mis-match
            if (!DataTypeUtil.isStringSqlType(dt2)
                && !DataTypeUtil.isNumberSqlType(dt2)
                && !DataTypeUtil.equalsSemantically(dt2, DataTypes.BooleanType)) {
                return false;
            } else {
                return true;
            }
        } else if (DataTypeUtil.equalsSemantically(dt1, dt2)) {
            return true;
        } else if (DataTypeUtil.isDateType(dt1) || DataTypeUtil.isDateType(dt2)) {
            return false;
        } else {
            return true;
        }
    }

    public static boolean canBKAJoin(LogicalJoin join) {
        if (join.getJoinType().equals(JoinRelType.RIGHT)) {
            RelNode left = join.getLeft();
            if (left instanceof RelSubset) {
                left = ((RelSubset) left).getOriginal();
            }
            if (left instanceof HepRelVertex) {
                left = ((HepRelVertex) left).getCurrentRel();
            }
            if (checkBkaJoinForLogicalView(left)) {
                return true;
            }
        } else {
            RelNode right = join.getRight();
            if (right instanceof RelSubset) {
                right = ((RelSubset) right).getOriginal();
            }
            if (right instanceof HepRelVertex) {
                right = ((HepRelVertex) right).getCurrentRel();
            }
            if (checkBkaJoinForLogicalView(right)) {
                return true;
            }
        }
        return false;
    }

    public static boolean checkBkaJoinForLogicalView(RelNode relNode) {
        boolean useBkaJoin = false;
        if (relNode instanceof LogicalView) {
            CheckBkaJoinRelVisitor checkBkaJoinRelVisitor = new CheckBkaJoinRelVisitor();
            relNode.accept(checkBkaJoinRelVisitor);
            useBkaJoin = checkBkaJoinRelVisitor.isSupportUseBkaJoin();
        } else if (relNode instanceof Gather) {
            for (RelNode subNode : relNode.getInputs()) {
                if (subNode instanceof RelSubset) {
                    subNode = ((RelSubset) subNode).getOriginal();
                }
                if (!checkBkaJoinForLogicalView(subNode)) {
                    useBkaJoin = false;
                    break;
                } else {
                    useBkaJoin = true;
                }
            }
        } else if (relNode instanceof LogicalTableLookup) {
            LogicalTableLookup logicalTableLookup = (LogicalTableLookup) relNode;
            RelNode input = logicalTableLookup.getInput();
            if (input instanceof RelSubset) {
                input = ((RelSubset) input).getOriginal();
            }
            if (input instanceof HepRelVertex) {
                input = ((HepRelVertex) input).getCurrentRel();
            }
            LogicalIndexScan logicalIndexScan = (LogicalIndexScan) input;
            return checkBkaJoinForLogicalView(logicalIndexScan);
        }
        return useBkaJoin;
    }

    public static class RexNodeHolder {

        public RexNode rexNode = null;

        public RexNode getRexNode() {
            return rexNode;
        }

        public void setRexNode(RexNode rexNode) {
            this.rexNode = rexNode;
        }
    }

    public static RelCollation createRelCollation(List<Integer> sortColumns) {
        return createRelCollation(sortColumns, null);
    }

    public static RelCollation createRelCollation(List<Integer> sortColumns, List<RelFieldCollation> other) {
        List<RelFieldCollation> toAdd = new ArrayList<>();
        for (Integer index : sortColumns) {
            RelFieldCollation relFieldCollationTemp = new RelFieldCollation(index,
                RelFieldCollation.Direction.ASCENDING,
                RelFieldCollation.NullDirection.FIRST);
            toAdd.add(relFieldCollationTemp);
        }
        if (other != null) {
            toAdd.addAll(other);
        }
        RelCollation relCollation = RelCollationImpl.of(toAdd);
        return relCollation;
    }

    public static boolean checkSortMergeCondition(Join join, RexNode condition, int leftBound,
                                                  List<Integer> leftColumns, List<Integer> rightColumns,
                                                  CBOUtil.RexNodeHolder otherConditionHolder) {
        boolean ok =
            checkSortMergeConditionUnOrder(join, condition, leftBound, leftColumns, rightColumns, otherConditionHolder);
        if (ok == false) {
            return false;
        }

        if (RexUtils.forceNLJoin(join)) {
            return false;
        }

        List<Integer> orderLeftColumns = new ArrayList<>();
        List<Integer> orderRightColumns = new ArrayList<>();
        while (leftColumns.size() > 0) {
            // find the index of min value of leftColumns
            int index = -1;
            int min = Integer.MAX_VALUE;
            for (int i = 0; i < leftColumns.size(); i++) {
                if (min > leftColumns.get(i)) {
                    min = leftColumns.get(i);
                    index = i;
                }
            }
            orderLeftColumns.add(leftColumns.remove(index));
            orderRightColumns.add(rightColumns.remove(index));
        }
        leftColumns.addAll(orderLeftColumns);
        rightColumns.addAll(orderRightColumns);
        return true;
    }

    public static boolean checkSortMergeConditionUnOrder(Join join, RexNode condition, int leftBound,
                                                         List<Integer> leftColumns, List<Integer> rightColumns,
                                                         CBOUtil.RexNodeHolder otherConditionHolder) {
        RelMetadataQuery relMetadataQuery = join.getCluster().getMetadataQuery();
        boolean canSortMerge = false;

        if (condition == null) {
            return false;
        }

        if (condition instanceof RexCall) {
            final RexCall currentCondition = (RexCall) condition;
            switch (currentCondition.getKind()) {
            case EQUALS: {
                if (currentCondition.getOperands().size() == 2) {
                    RexNode operand1 = currentCondition.getOperands().get(0);
                    RexNode operand2 = currentCondition.getOperands().get(1);
                    if (operand1 instanceof RexInputRef && operand2 instanceof RexInputRef) {
                        int indexOp1 = ((RexInputRef) operand1).getIndex();
                        int indexOp2 = ((RexInputRef) operand2).getIndex();
                        RelColumnOrigin relColumnOrigin1 = null;
                        RelColumnOrigin relColumnOrigin2 = null;
                        if ((indexOp1 < leftBound && indexOp2 < leftBound) || (indexOp1 >= leftBound
                            && indexOp2 >= leftBound)) {
                            canSortMerge = false;
                        } else if (indexOp1 < leftBound) {
                            relColumnOrigin1 =
                                relMetadataQuery.getColumnOrigin(join.getLeft(), ((RexInputRef) operand1).getIndex());
                            relColumnOrigin2 = relMetadataQuery
                                .getColumnOrigin(join.getRight(), ((RexInputRef) operand2).getIndex() - leftBound);
                        } else {
                            relColumnOrigin2 =
                                relMetadataQuery.getColumnOrigin(join.getLeft(), ((RexInputRef) operand2).getIndex());
                            relColumnOrigin1 = relMetadataQuery
                                .getColumnOrigin(join.getRight(), ((RexInputRef) operand1).getIndex() - leftBound);
                        }

                        if (relColumnOrigin1 != null && relColumnOrigin2 != null) {
                            canSortMerge = true;
                            if (indexOp1 < leftBound) {
                                leftColumns.add(indexOp1);
                                rightColumns.add(indexOp2 - leftBound);
                            } else {
                                leftColumns.add(indexOp2);
                                rightColumns.add(indexOp1 - leftBound);
                            }

                            RelDataTypeField field1 = relColumnOrigin1.getOriginTable().getRowType().getFieldList()
                                .get(relColumnOrigin1.getOriginColumnOrdinal());
                            RelDataTypeField field2 = relColumnOrigin2.getOriginTable().getRowType().getFieldList()
                                .get(relColumnOrigin2.getOriginColumnOrdinal());

                            DataType dataType1 = DataTypeUtil.calciteToDrdsType(field1.getType());
                            DataType dataType2 = DataTypeUtil.calciteToDrdsType(field2.getType());

                            if (dataType1 == null || dataType2 == null) {
                                canSortMerge = false;
                            } else if (!DataTypeUtil.equalsSemantically(dataType1, dataType2)) {
                                if (DataTypeUtil.isStringType(dataType1) || DataTypeUtil.isStringType(dataType2)) {
                                    canSortMerge = false;
                                }
                            }
                        }
                    }
                }
                if (!canSortMerge) {
                    otherConditionHolder.setRexNode(currentCondition);
                }
                break;
            }
            case AND: {
                RexNode otherCondition = null;
                for (int i = 0; i < currentCondition.getOperands().size(); i++) {
                    canSortMerge |=
                        checkSortMergeConditionUnOrder(join, currentCondition.getOperands().get(i), leftBound,
                            leftColumns, rightColumns, otherConditionHolder);
                    if (otherConditionHolder.getRexNode() == null) {
                        continue;
                    } else {
                        if (otherCondition == null) {
                            otherCondition = otherConditionHolder.getRexNode();
                        } else {
                            otherCondition = join.getCluster().getRexBuilder().makeCall(
                                SqlStdOperatorTable.AND,
                                Arrays.asList(otherCondition, otherConditionHolder.getRexNode()));
                        }
                        otherConditionHolder.setRexNode(null);
                    }
                }
                otherConditionHolder.setRexNode(otherCondition);
                break;
            }
            default: {
                canSortMerge = false;
                otherConditionHolder.setRexNode(condition);
            }
            }
        } else {
            otherConditionHolder.setRexNode(condition);
        }
        if (otherConditionHolder.getRexNode() != null && join.getJoinType() != JoinRelType.INNER) {
            canSortMerge = false;
        }
        return canSortMerge;
    }

    public static long getRexParam(RexNode rex, Map<Integer, ParameterContext> params) {
        long rs = 0L;
        if (rex instanceof RexDynamicParam) {
            rs =
                Long.valueOf(String.valueOf(params.get(((RexDynamicParam) rex).getIndex() + 1).getValue())).longValue();
        } else if (rex instanceof RexCall) {
            RexCall rexcall = (RexCall) rex;
            if (rexcall.isA(SqlKind.PLUS) && rexcall.operands.size() == 2) {
                long l = getRexParam(rexcall.operands.get(0), params);
                long r = getRexParam(rexcall.operands.get(1), params);
                if (l > -1 && r > -1) {
                    rs = l + r;
                }
            } else {
                throw new IllegalArgumentException("Invalid RexNode " + rex);
            }
        } else if (rex instanceof RexLiteral) {
            rs = DataTypes.ULongType.convertFrom(((RexLiteral) rex).getValue()).longValue();
        } else {
            throw new IllegalArgumentException("Invalid RexNode " + rex);
        }
        return rs;
    }

    public static TableMeta getTableMeta(RelOptTable relOptTable) {
        if (!(relOptTable instanceof RelOptTableImpl)) {
            return null;
        }
        Table table = ((RelOptTableImpl) relOptTable).getImplTable();
        return table instanceof TableMeta ? (TableMeta) table : null;
    }

    public static DrdsViewTable getDrdsViewTable(RelOptTable relOptTable) {
        if (!(relOptTable instanceof RelOptTableImpl)) {
            return null;
        }
        Table table = ((RelOptTableImpl) relOptTable).getImplTable();
        return table instanceof DrdsViewTable ? (DrdsViewTable) table : null;
    }

    static class SortFinder extends RelShuttleImpl {

        private Sort sort;

        public SortFinder() {
        }

        public Sort getSort() {
            return sort;
        }

        @Override
        public RelNode visit(RelNode other) {
            // other type
            if (other instanceof LogicalSemiJoin) {
                return this.visit((LogicalSemiJoin) other);
            } else {
                return visitChildren(other);
            }
        }

        @Override
        public RelNode visit(LogicalJoin join) {
            return join;
        }

        public RelNode visit(LogicalSemiJoin join) {
            return join;
        }

        @Override
        public RelNode visit(LogicalAggregate agg) {
            return agg;
        }

        @Override
        public RelNode visit(LogicalSort logicalSort) {
            sort = logicalSort;
            visitChildren(logicalSort);
            return logicalSort;
        }
    }

    private static RelOptTable recommendTableForMysqlJoinReorder(RelNode rel) {
        // find the first TopN over join, we need to consider TopN while join reorder
        SortFinder sortFinder = new SortFinder();
        rel.accept(sortFinder);
        Sort sort = sortFinder.getSort();

        if (sort == null) {
            return null;
        }

        // we only consider TopN
        if (!(sort.withLimit() && sort.withOrderBy())) {
            return null;
        }

        RelMetadataQuery mq = rel.getCluster().getMetadataQuery();

        RelOptTable table = null;

        Set<Integer> orderKeyColumnSet = new HashSet<>();
        List<Integer> orderByColumn = new ArrayList<>();
        int ascCount = 0;
        int descCount = 0;
        for (RelFieldCollation relFieldCollation : sort.getCollation().getFieldCollations()) {
            RelColumnOrigin relColumnOrigin = mq.getColumnOrigin(sort.getInput(),
                relFieldCollation.getFieldIndex());
            if (relColumnOrigin == null) {
                return null;
            }

            // FIXME: Is that condition strict enough ?
            if (table == null) {
                table = relColumnOrigin.getOriginTable();
            } else if (relColumnOrigin.getOriginTable() != table) {
                return null;
            }

            if (orderKeyColumnSet.add(relColumnOrigin.getOriginColumnOrdinal())) {
                orderByColumn.add(relColumnOrigin.getOriginColumnOrdinal());
                if (relFieldCollation.direction.isDescending()) {
                    descCount++;
                } else {
                    ascCount++;
                }
            }
        }

        if (ascCount != 0 && descCount != 0) {
            // not the same direction;
            return null;
        }

        TableMeta tableMeta = CBOUtil.getTableMeta(table);
        Set<String> canUseIndexSet =
            tableMeta.getIndexes().stream().map(x -> x.getPhysicalIndexName()).collect(Collectors.toSet());

        Index orderByIndex = IndexUtil.selectIndexForOrderBy(tableMeta, orderByColumn, canUseIndexSet);

        if (orderByIndex != null) {
            return table;
        } else {
            return null;
        }
    }

    public static RelNode optimizeByMysqlImpl(RelNode rel) {
        HepProgramBuilder builder = new HepProgramBuilder();
        builder.addMatchOrder(HepMatchOrder.BOTTOM_UP);
        builder.addGroupBegin();
        builder.addRuleInstance(MysqlTableScanRule.FILTER_TABLESCAN);
        builder.addGroupEnd();
        builder.addMatchOrder(HepMatchOrder.BOTTOM_UP);
        builder.addGroupBegin();
        builder.addRuleInstance(MysqlTableScanRule.TABLESCAN);
        builder.addGroupEnd();
        builder.addMatchOrder(HepMatchOrder.BOTTOM_UP);
        builder.addGroupBegin();
        builder.addRuleInstance(JoinToMultiJoinRule.INSTANCE);
        builder.addGroupEnd();
        builder.addMatchOrder(HepMatchOrder.BOTTOM_UP);
        builder.addGroupBegin();
        builder.addRuleInstance(MysqlMultiJoinToLogicalJoinRule.INSTANCE);
        builder.addGroupEnd();
        builder.addMatchOrder(HepMatchOrder.BOTTOM_UP);
        builder.addGroupBegin();
        builder.addRuleInstance(MysqlJoinRule.INSTANCE);
        builder.addRuleInstance(MysqlSemiJoinRule.INSTANCE);
        builder.addRuleInstance(MysqlSemiJoinRule.INSTANCE);
        builder.addRuleInstance(MysqlSortRule.INSTANCE);
        builder.addRuleInstance(MysqlAggRule.INSTANCE);
        builder.addRuleInstance(MysqlCorrelateRule.INSTANCE);
        builder.addGroupEnd();
        HepPlanner planner = new HepPlanner(builder.build());
        planner.stopOptimizerTrace();
        planner.setRoot(rel);

        PlannerContext plannerContext = PlannerContext.getPlannerContext(rel);
        plannerContext.setMysqlJoinReorderFirstTable(recommendTableForMysqlJoinReorder(rel));
        RelNode output = planner.findBestExp();
        // clear
        plannerContext.setMysqlJoinReorderFirstTable(null);
        return output;
    }

    public static RelOptCost getCost(ExecutionContext executionContext) {
        final RelOptCost zero = DrdsRelOptCostImpl.FACTORY.makeZeroCost();
        if (executionContext.getExplain() != null) {
            return zero;
        }
        ExecutionPlan executionPlan = executionContext.getFinalPlan();
        if (executionPlan == null) {
            return zero;
        }
        RelNode plan = executionPlan.getPlan();
        if (plan == null) {
            return zero;
        }

        // get from plannerContext cache
        RelOptCost cost = PlannerContext.getPlannerContext(plan).getCost();
        if (cost == null) {
            RelMetadataQuery mq = plan.getCluster().getMetadataQuery();
            synchronized (mq) {
                cost = mq.getCumulativeCost(plan);
                PlannerContext.getPlannerContext(plan).setCost(cost);
            }
        }
        return cost;
    }

    public static boolean isGroupSets(LogicalAggregate agg) {
        for (AggregateCall aggCall : agg.getAggCallList()) {
            SqlKind sqlKind = aggCall.getAggregation().getKind();
            if (sqlKind == SqlKind.GROUP_ID || sqlKind == SqlKind.GROUPING || sqlKind == SqlKind.GROUPING_ID) {
                return true;
            }
        }
        return agg.getGroupSets().size() > 1;
    }

    public static org.apache.calcite.util.Pair<RelTraitSet, List<RelTraitSet>> passThroughTraitsForJoin(
        RelTraitSet required, Join join, JoinRelType joinType,
        int leftInputFieldCount, RelTraitSet joinTraitSet) {

        if (required.getConvention() != joinTraitSet.getConvention()) {
            return null;
        }

        if (required.getConvention() == MppConvention.INSTANCE) {
            return null;
        }

        final RelDistribution leftDistribution = RelDistributions.ANY;
        final RelDistribution rightDistribution = RelDistributions.ANY;

        RelCollation collation = required.getCollation();
        if (collation == null
            || collation == RelCollations.EMPTY
            || joinType == JoinRelType.FULL
            || joinType == JoinRelType.RIGHT) {
            return null;
        }

        for (RelFieldCollation fc : collation.getFieldCollations()) {
            // If field collation belongs to right input: cannot push down collation.
            if (fc.getFieldIndex() >= leftInputFieldCount) {
                return null;
            }
        }

        RelTraitSet passthroughTraitSet =
            joinTraitSet.replace(collation).replace(required.getTrait(RelDistributionTraitDef.INSTANCE));
        return org.apache.calcite.util.Pair.of(passthroughTraitSet,
            ImmutableList.of(
                passthroughTraitSet.replace(leftDistribution),
                passthroughTraitSet.replace(RelCollations.EMPTY).replace(rightDistribution)));
    }

    public static org.apache.calcite.util.Pair<RelTraitSet, List<RelTraitSet>> deriveTraitsForJoin(
        RelTraitSet childTraits, int childId, JoinRelType joinType,
        RelTraitSet joinTraitSet, RelTraitSet rightTraitSet) {
        // should only derive traits (limited to collation for now) from left join input.
        assert childId == 0;

        RelCollation collation = childTraits.getCollation();
        if (collation == null
            || collation == RelCollations.EMPTY
            || joinType == JoinRelType.FULL
            || joinType == JoinRelType.RIGHT) {
            return null;
        }

        RelTraitSet derivedTraits = joinTraitSet.replace(collation);
        return org.apache.calcite.util.Pair.of(
            derivedTraits,
            ImmutableList.of(derivedTraits, rightTraitSet));
    }

    public static org.apache.calcite.util.Pair<RelTraitSet, List<RelTraitSet>> passThroughTraitsForProject(
        RelTraitSet required,
        List<RexNode> exps,
        RelDataType inputRowType,
        RelDataTypeFactory typeFactory,
        RelTraitSet currentTraits) {
        final RelCollation collation = required.getCollation();
        final RelDistribution distribution = required.getDistribution();
        final Convention convention = required.getConvention();
        if (collation == null || collation == RelCollations.EMPTY) {
            if (convention == MppConvention.INSTANCE) {
                if (distribution == null || distribution == RelDistributions.ANY) {
                    return null;
                }
            } else {
                return null;
            }
        }

        final Mappings.TargetMapping map =
            RelOptUtil.permutationIgnoreCast(
                exps, inputRowType);

        if (collation.getFieldCollations().stream().anyMatch(
            rc -> !isCollationOnTrivialExpr(exps, typeFactory,
                map, rc, true))) {
            return null;
        }

        if (distribution.getKeys().stream().anyMatch(
            key -> !isDistributionKeyOnTrivialExpr(exps, typeFactory,
                map, key, true))) {
            return null;
        }

        final RelCollation newCollation = collation.apply(map);
        final RelDistribution newDistribution = distribution.apply(map);

        return org.apache.calcite.util.Pair.of(currentTraits.replace(collation).replace(distribution),
            ImmutableList.of(currentTraits.replace(newCollation).replace(newDistribution)));
    }

    public static org.apache.calcite.util.Pair<RelTraitSet, List<RelTraitSet>> deriveTraitsForProject(
        RelTraitSet childTraits, int childId, List<RexNode> exps,
        RelDataType inputRowType, RelDataTypeFactory typeFactory, RelTraitSet currentTraits) {
        final RelCollation collation = childTraits.getCollation();
        final RelDistribution distribution = childTraits.getDistribution();
        final Convention convention = childTraits.getConvention();
        if (collation == null || collation == RelCollations.EMPTY) {
            if (convention == MppConvention.INSTANCE) {
                if (distribution == null || distribution == RelDistributions.ANY) {
                    return null;
                }
            } else {
                return null;
            }
        }

        final int maxField = Math.max(exps.size(),
            inputRowType.getFieldCount());
        Mappings.TargetMapping mapping = Mappings
            .create(MappingType.FUNCTION, maxField, maxField);
        for (Ord<RexNode> node : Ord.zip(exps)) {
            if (node.e instanceof RexInputRef) {
                mapping.set(((RexInputRef) node.e).getIndex(), node.i);
            } else if (node.e.isA(SqlKind.CAST)) {
                final RexNode operand = ((RexCall) node.e).getOperands().get(0);
                if (operand instanceof RexInputRef) {
                    mapping.set(((RexInputRef) operand).getIndex(), node.i);
                }
            }
        }

        List<RelFieldCollation> collationFieldsToDerive = new ArrayList<>();
        for (RelFieldCollation rc : collation.getFieldCollations()) {
            if (isCollationOnTrivialExpr(exps, typeFactory, mapping, rc, false)) {
                collationFieldsToDerive.add(rc);
            } else {
                break;
            }
        }

        if (distribution.getKeys().stream().anyMatch(
            key -> !isDistributionKeyOnTrivialExpr(exps, typeFactory,
                mapping, key, false))) {
            return null;
        }

        if (collationFieldsToDerive.size() > 0 || !distribution.isTop()) {
            final RelCollation newCollation = RelCollations
                .of(collationFieldsToDerive).apply(mapping);
            final RelDistribution newDistribution = distribution.apply(mapping);
            return org.apache.calcite.util.Pair.of(currentTraits.replace(newCollation).replace(newDistribution),
                ImmutableList.of(currentTraits.replace(collation).replace(distribution)));
        } else {
            return null;
        }
    }

    private static boolean isCollationOnTrivialExpr(
        List<RexNode> projects, RelDataTypeFactory typeFactory,
        Mappings.TargetMapping map, RelFieldCollation fc, boolean passDown) {
        final int index = fc.getFieldIndex();
        int target = map.getTargetOpt(index);
        if (target < 0) {
            return false;
        }

        final RexNode node = passDown ? projects.get(index) : projects.get(target);
        if (node.isA(SqlKind.CAST)) {
            // Check whether it is a monotonic preserving cast
            final RexCall cast = (RexCall) node;
            RelFieldCollation newFieldCollation = Objects.requireNonNull(RexUtil.apply(map, fc));
            final RexCallBinding binding =
                RexCallBinding.create(typeFactory, cast,
                    ImmutableList.of(RelCollations.of(newFieldCollation)));
            if (cast.getOperator().getMonotonicity(binding)
                == SqlMonotonicity.NOT_MONOTONIC) {
                return false;
            }
        }

        return true;
    }

    private static boolean isDistributionKeyOnTrivialExpr(List<RexNode> projects, RelDataTypeFactory typeFactory,
                                                          Mappings.TargetMapping map, Integer key, boolean passDown) {
        final int index = key;
        int target = map.getTargetOpt(index);
        if (target < 0) {
            return false;
        }

        final RexNode node = passDown ? projects.get(index) : projects.get(target);
        if (node.isA(SqlKind.CAST)) {
            return false;
        }

        return true;
    }

    public static boolean isIndexColumn(TableMeta tableMeta, ColumnMeta columnMeta) {
        for (IndexMeta indexMeta : tableMeta.getIndexes()) {
            for (ColumnMeta keyColumn : indexMeta.getKeyColumns()) {
                if (keyColumn.equals(columnMeta)) {
                    return true;
                }
            }
        }
        return false;
    }

    public static final Predicate<RelNode> DRDS_CONVENTION = new PredicateImpl<RelNode>() {
        @Override
        public boolean test(RelNode relNode) {
            return relNode.getTraitSet().simplify().getConvention() == DrdsConvention.INSTANCE;
        }
    };
}
