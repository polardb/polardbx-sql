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

import com.alibaba.polardbx.common.Engine;
import com.alibaba.polardbx.common.exception.TddlNestableRuntimeException;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.type.MySQLStandardFieldType;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.gms.metadb.table.TablesExtAccessor;
import com.alibaba.polardbx.gms.metadb.table.TablesExtRecord;
import com.alibaba.polardbx.gms.partition.TablePartitionAccessor;
import com.alibaba.polardbx.gms.partition.TablePartitionRecord;
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.config.meta.DrdsRelOptCostImpl;
import com.alibaba.polardbx.optimizer.config.meta.TableScanIOEstimator;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.GlobalIndexMeta;
import com.alibaba.polardbx.optimizer.config.table.GsiMetaManager;
import com.alibaba.polardbx.optimizer.config.table.IndexMeta;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.DrdsConvention;
import com.alibaba.polardbx.optimizer.core.MppConvention;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypeUtil;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.planner.ExecutionPlan;
import com.alibaba.polardbx.optimizer.core.planner.rule.AccessPathRule;
import com.alibaba.polardbx.optimizer.core.planner.rule.ConstantFoldRule;
import com.alibaba.polardbx.optimizer.core.planner.rule.FilterMergeRule;
import com.alibaba.polardbx.optimizer.core.planner.rule.MysqlAggRule;
import com.alibaba.polardbx.optimizer.core.planner.rule.MysqlCorrelateRule;
import com.alibaba.polardbx.optimizer.core.planner.rule.MysqlJoinRule;
import com.alibaba.polardbx.optimizer.core.planner.rule.MysqlMultiJoinToLogicalJoinRule;
import com.alibaba.polardbx.optimizer.core.planner.rule.MysqlSemiJoinRule;
import com.alibaba.polardbx.optimizer.core.planner.rule.MysqlSortRule;
import com.alibaba.polardbx.optimizer.core.planner.rule.MysqlTableScanRule;
import com.alibaba.polardbx.optimizer.core.planner.rule.OrcTableScanRule;
import com.alibaba.polardbx.optimizer.core.planner.rule.ProjectFoldRule;
import com.alibaba.polardbx.optimizer.core.planner.rule.PushFilterRule;
import com.alibaba.polardbx.optimizer.core.planner.rule.PushProjectRule;
import com.alibaba.polardbx.optimizer.core.planner.rule.mpp.RuleUtils;
import com.alibaba.polardbx.optimizer.core.rel.BaseTableOperation;
import com.alibaba.polardbx.optimizer.core.rel.CheckBkaJoinRelVisitor;
import com.alibaba.polardbx.optimizer.core.rel.Gather;
import com.alibaba.polardbx.optimizer.core.rel.LogicalIndexScan;
import com.alibaba.polardbx.optimizer.core.rel.LogicalView;
import com.alibaba.polardbx.optimizer.core.rel.OSSTableScan;
import com.alibaba.polardbx.optimizer.index.Index;
import com.alibaba.polardbx.optimizer.index.IndexUtil;
import com.alibaba.polardbx.optimizer.planmanager.LogicalViewWithSubqueryFinder;
import com.alibaba.polardbx.optimizer.utils.CalciteUtils;
import com.alibaba.polardbx.optimizer.utils.PlannerUtils;
import com.alibaba.polardbx.optimizer.utils.RelUtils;
import com.alibaba.polardbx.optimizer.utils.RexUtils;
import com.alibaba.polardbx.optimizer.utils.TableTopologyUtil;
import com.alibaba.polardbx.optimizer.view.DrdsViewTable;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptSchema;
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
import org.apache.calcite.rel.RelVisitor;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Correlate;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalSemiJoin;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rel.logical.LogicalTableLookup;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.metadata.RelColumnOrigin;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.rules.FilterAggregateTransposeRule;
import org.apache.calcite.rel.rules.FilterProjectTransposeRule;
import org.apache.calcite.rel.rules.FilterWindowTransposeRule;
import org.apache.calcite.rel.rules.JoinProjectTransposeRule;
import org.apache.calcite.rel.rules.JoinToMultiJoinRule;
import org.apache.calcite.rel.rules.ProjectFilterTransposeRule;
import org.apache.calcite.rel.rules.ProjectMergeRule;
import org.apache.calcite.rel.rules.ProjectRemoveRule;
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
import org.apache.calcite.rex.RexSubQuery;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.runtime.PredicateImpl;
import org.apache.calcite.schema.Table;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.validate.SqlMonotonicity;
import org.apache.calcite.util.Util;
import org.apache.calcite.util.mapping.MappingType;
import org.apache.calcite.util.mapping.Mappings;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import static com.alibaba.polardbx.gms.partition.TablePartitionRecord.PARTITION_TABLE_TYPE_COLUMNAR_TABLE;
import static com.alibaba.polardbx.gms.partition.TablePartitionRecord.PARTITION_TABLE_TYPE_GSI_TABLE;

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
            sort = LogicalSort.create(logicalView.getPushedRelNode(), RelCollations.EMPTY, offset, originalSort.fetch);
        } else {
            sort = LogicalSort.create(logicalView.getPushedRelNode(), relCollation, offset, originalSort.fetch);
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

        LogicalView newLogicalView =
            logicalView.copy(logicalView.getTraitSet().replace(relCollation).replace(DrdsConvention.INSTANCE));
        newLogicalView.push(sort);
        return newLogicalView;
    }

    public static RexNode calPushDownFetch(Sort originalSort) {
        RexBuilder builder = originalSort.getCluster().getRexBuilder();
        RexNode fetch = originalSort.fetch;
        if (originalSort.offset != null && originalSort.fetch != null) {
            Map<Integer, ParameterContext> parameterContextMap =
                PlannerContext.getPlannerContext(originalSort).getParams().getCurrentParameter();

            if (originalSort.fetch instanceof RexDynamicParam || originalSort.offset instanceof RexDynamicParam) {
                /**
                 * fetch or offset be parameterized.
                 */
                fetch = builder.makeCall(SqlStdOperatorTable.PLUS, fetch, originalSort.offset);
            } else {
                long fetchVal = getRexParam(originalSort.fetch, parameterContextMap);
                long offsetVal = getRexParam(originalSort.offset, parameterContextMap);
                if (offsetVal == Long.MAX_VALUE || fetchVal == Long.MAX_VALUE) {
                    fetch = builder.makeBigIntLiteral(Long.MAX_VALUE);
                } else {
                    if (offsetVal < 0) {
                        throw new TddlRuntimeException(ErrorCode.ERR_OPTIMIZER, "get rex " + offsetVal);
                    }
                    if (fetchVal < 0) {
                        throw new TddlRuntimeException(ErrorCode.ERR_OPTIMIZER, "get rex " + fetchVal);
                    }
                    /**
                     * fetch or offset be parameterized.
                     */
                    fetch = builder.makeBigIntLiteral(offsetVal + fetchVal);
                }
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

    public static boolean checkHashJoinCondition(Join join, RexNode condition, int leftBound,
                                                 RexNodeHolder equalConditionHolder,
                                                 RexNodeHolder otherConditionHolder) {
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
            case IS_NOT_DISTINCT_FROM:
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
                            otherCondition = join.getCluster().getRexBuilder().makeCall(SqlStdOperatorTable.AND,
                                Arrays.asList(otherCondition, otherConditionHolder.getRexNode()));
                        }
                        otherConditionHolder.setRexNode(null);
                    }

                    if (equalConditionHolder.getRexNode() != null) {
                        if (equalCondition == null) {
                            equalCondition = equalConditionHolder.getRexNode();
                        } else {
                            equalCondition = join.getCluster().getRexBuilder().makeCall(SqlStdOperatorTable.AND,
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
        } else if ((DataTypeUtil.isNumberSqlType(dt1) || DataTypeUtil.isStringSqlType(dt1)) && (
            DataTypeUtil.isNumberSqlType(dt2) || DataTypeUtil.isStringSqlType(dt2))) {
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
            if (!DataTypeUtil.isStringSqlType(dt2) && !DataTypeUtil.isNumberSqlType(dt2)
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

    /**
     * the expected format is [project] -- filter -- [project] -- TS
     *
     * @param plan the tree to be formatted
     * @return the formatted tree
     */
    static public RelNode OssTableScanFormat(RelNode plan) {
        HepProgramBuilder builder = new HepProgramBuilder();
        builder.addMatchOrder(HepMatchOrder.TOP_DOWN);
        builder.addGroupBegin();
        builder.addRuleInstance(FilterMergeRule.INSTANCE);
        builder.addRuleInstance(ProjectFilterTransposeRule.INSTANCE);
        builder.addRuleInstance(ProjectMergeRule.INSTANCE);
        builder.addGroupEnd();
        builder.addMatchOrder(HepMatchOrder.BOTTOM_UP);
        HepPlanner planner = new HepPlanner(builder.build());
        planner.stopOptimizerTrace();
        planner.setRoot(plan);

        return planner.findBestExp();
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
            RelFieldCollation relFieldCollationTemp =
                new RelFieldCollation(index, RelFieldCollation.Direction.ASCENDING,
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
                            relColumnOrigin2 = relMetadataQuery.getColumnOrigin(join.getRight(),
                                ((RexInputRef) operand2).getIndex() - leftBound);
                        } else {
                            relColumnOrigin2 =
                                relMetadataQuery.getColumnOrigin(join.getLeft(), ((RexInputRef) operand2).getIndex());
                            relColumnOrigin1 = relMetadataQuery.getColumnOrigin(join.getRight(),
                                ((RexInputRef) operand1).getIndex() - leftBound);
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
                            otherCondition = join.getCluster().getRexBuilder().makeCall(SqlStdOperatorTable.AND,
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

    /**
     * get the long value of rex parameter, if big integer is bigger than Long.MAX_VALUE, return Long.MAX_VALUE
     */
    public static long getRexParam(RexNode rex, Map<Integer, ParameterContext> params) {
        long rs = 0L;
        if (rex instanceof RexDynamicParam) {
            if (params.size() == 0) {
                return rs;
            }
            try {
                return Long.valueOf(String.valueOf(params.get(((RexDynamicParam) rex).getIndex() + 1).getValue()));
            } catch (NumberFormatException e) {
                Object obj = params.get(((RexDynamicParam) rex).getIndex() + 1).getValue();
                if (obj instanceof BigInteger) {
                    if (((BigInteger) obj).signum() < 0) {
                        return -Long.MAX_VALUE;
                    } else {
                        return Long.MAX_VALUE;
                    }
                }
                if (obj instanceof BigDecimal) {
                    if (((BigDecimal) obj).scale() > 0) {
                        throw e;
                    }
                    if (((BigDecimal) obj).signum() < 0) {
                        return -Long.MAX_VALUE;
                    } else {
                        return Long.MAX_VALUE;
                    }
                }
                throw e;
            }
        } else if (rex instanceof RexCall) {
            RexCall rexcall = (RexCall) rex;
            if (rexcall.isA(SqlKind.PLUS) && rexcall.operands.size() == 2) {
                long l = getRexParam(rexcall.operands.get(0), params);
                long r = getRexParam(rexcall.operands.get(1), params);
                if (l > -1 && r > -1) {
                    rs = l + r;
                    if (rs < 0) {
                        rs = Long.MAX_VALUE;
                    }
                }
            } else {
                throw new IllegalStateException("Invalid RexNode " + rex);
            }
        } else if (rex instanceof RexLiteral) {
            rs = DataTypes.ULongType.convertFrom(((RexLiteral) rex).getValue()).longValue();
        } else {
            throw new IllegalStateException("Invalid RexNode " + rex);
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

    public static boolean useColPlanCache(ExecutionContext ec) {
        if (ec.isUseHint()) {
            return false;
        }
        return ec.isColumnarPlanCache();
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

    public static RelNode optimizeByColumnarPostRBO(RelNode input, PlannerContext plannerContext) {
        if (!plannerContext.getParamManager().getBoolean(ConnectionParams.ENABLE_COLUMNAR_PULL_UP_PROJECT)) {
            return input;
        }
        plannerContext.getCalcitePlanOptimizerTrace()
            .ifPresent(x -> x.addSnapshot("Columnar Post RBO", input, plannerContext));

        HepProgramBuilder builder = new HepProgramBuilder();

        builder.addGroupBegin();
        builder.addRuleInstance(ConstantFoldRule.INSTANCE);
        builder.addRuleInstance(ProjectFoldRule.INSTANCE);
        builder.addGroupEnd();
        // push filter
        builder.addGroupBegin();
        builder.addRuleInstance(PushFilterRule.LOGICALVIEW);
        builder.addGroupEnd();
        // push project
        builder.addGroupBegin();
        builder.addRuleInstance(PushProjectRule.INSTANCE);
        builder.addRuleInstance(ProjectMergeRule.INSTANCE);
        builder.addRuleInstance(ProjectRemoveRule.INSTANCE);
        builder.addGroupEnd();

        builder.addGroupBegin();
        builder.addRuleInstance(JoinProjectTransposeRule.BOTH_PROJECT);
        builder.addRuleInstance(JoinProjectTransposeRule.LEFT_PROJECT);
        builder.addRuleInstance(JoinProjectTransposeRule.RIGHT_PROJECT);
        builder.addRuleInstance(ProjectMergeRule.INSTANCE);

        builder.addGroupEnd();
        HepPlanner hepPlanner = new HepPlanner(builder.build(), plannerContext);
        hepPlanner.setRoot(input);
        return hepPlanner.findBestExp();
    }

    public static class ColumnarScanReplacer extends RelShuttleImpl {
        PlannerContext pc;

        ExecutionContext ec;

        public ColumnarScanReplacer(PlannerContext plannerContext) {
            this.pc = plannerContext;
            this.ec = plannerContext.getExecutionContext();
        }

        @Override
        public RelNode visit(LogicalFilter filter) {
            RexUtil.RexSubqueryListFinder finder = new RexUtil.RexSubqueryListFinder();
            filter.getCondition().accept(finder);
            for (RexSubQuery subQuery : finder.getSubQueries()) {
                subQuery.rel.accept(this);
            }
            return visitChild(filter, 0, filter.getInput());
        }

        @Override
        public RelNode visit(LogicalProject project) {
            RexUtil.RexSubqueryListFinder finder = new RexUtil.RexSubqueryListFinder();
            for (RexNode node : project.getProjects()) {
                node.accept(finder);
            }
            for (RexSubQuery subQuery : finder.getSubQueries()) {
                subQuery.rel.accept(this);
            }
            return visitChild(project, 0, project.getInput());
        }

        @Override
        public RelNode visit(TableScan scan) {
            if (scan instanceof LogicalView && !(scan instanceof OSSTableScan)) {
                LogicalView logicalView = (LogicalView) scan;
                final String schemaName = logicalView.getSchemaName();
                final RelOptTable primaryTable = logicalView.getTable();
                List<String> columnarNameList =
                    GlobalIndexMeta.getColumnarIndexNames(logicalView.getLogicalTableName(), schemaName, ec);
                if (columnarNameList.size() == 1) {
                    String columnarIndex = columnarNameList.get(0);
                    final RelOptSchema catalog = RelUtils.buildCatalogReader(schemaName, ec);
                    final RelOptTable indexTable =
                        catalog.getTableForMember(ImmutableList.of(schemaName, columnarIndex));
                    final CBOUtil.OSSTableScanVisitor ossTableScanVisitor =
                        new CBOUtil.OSSTableScanVisitor(primaryTable, indexTable, logicalView.getLockMode());
                    return logicalView.getPushedRelNode().accept(ossTableScanVisitor);
                }
            }
            return scan;
        }
    }

    public static class OSSTableScanVisitor extends RelShuttleImpl {
        private final RelOptTable primaryTable;
        private final RelOptTable indexTable;
        private final SqlSelect.LockMode lockMode;

        public OSSTableScanVisitor(RelOptTable primaryTable, RelOptTable indexTable, SqlSelect.LockMode lockMode) {
            this.primaryTable = primaryTable;
            this.indexTable = indexTable;
            this.lockMode = lockMode;
        }

        @Override
        public RelNode visit(TableScan scan) {
            if (!scan.getTable().equals(primaryTable)) {
                return super.visit(scan);
            }

            final LogicalTableScan columnarTableScan =
                LogicalTableScan.create(scan.getCluster(), this.indexTable, scan.getHints(), null, scan.getFlashback(),
                    scan.getFlashbackOperator(),
                    null);
            return new OSSTableScan(columnarTableScan, lockMode);
        }
    }

    public static void columnarHashDistribution(
        List<Pair<List<Integer>, List<Integer>>> keyPairList,
        Join join,
        RelNode left,
        RelNode right,
        Mappings.TargetMapping mapping,
        List<Pair<RelDistribution, Pair<RelNode, RelNode>>> implementationList) {

        // Local Hash Shuffle
        for (Pair<List<Integer>, List<Integer>> keyPair : keyPairList) {
            // local hash must have same datatype
            RelDataType keyDataType = CalciteUtils.getJoinKeyDataType(
                join.getCluster().getTypeFactory(), join, keyPair.getKey(), keyPair.getValue());
            boolean leftSameType = RuleUtils.sameDateType(left, keyDataType, keyPair.getKey());
            boolean rightSameType = RuleUtils.sameDateType(right, keyDataType, keyPair.getValue());
            if (!(leftSameType && rightSameType)) {
                continue;
            }
            boolean leftNotNull = !join.getJoinType().generatesNullsOnLeft();
            boolean rightNotNull = !join.getJoinType().generatesNullsOnRight();

            RelDistribution leftDistribution = RelDistributions.hashOss(keyPair.getKey(),
                PlannerContext.getPlannerContext(join).getColumnarMaxShardCnt());
            RelDistribution rightDistribution = RelDistributions.hashOss(keyPair.getValue(),
                PlannerContext.getPlannerContext(join).getColumnarMaxShardCnt());
            RelNode newLeft = RelOptRule.convert(left, left.getTraitSet().replace(leftDistribution));
            RelNode newRight = RelOptRule.convert(right, right.getTraitSet().replace(rightDistribution));
            implementationList.add(
                new Pair<>(leftNotNull ? leftDistribution : RelDistributions.ANY,
                    new Pair<>(newLeft, newRight)));

            implementationList.add(
                new Pair<>((mapping != null && rightNotNull) ? rightDistribution.apply(mapping) : RelDistributions.ANY,
                    new Pair<>(newLeft, newRight)));
        }
    }

    public static void columnarBroadcastDistribution(
        Join join,
        RelNode left,
        RelNode right,
        List<Pair<RelDistribution, Pair<RelNode, RelNode>>> implementationList) {
        RelMetadataQuery mq = join.getCluster().getMetadataQuery();
        Pair<Boolean, Boolean> pair = CBOUtil.canBroadcast(mq, join, left, right);
        CBOUtil.columnarBroadCastLeft(left, right, implementationList, pair.getKey());
        CBOUtil.columnarBroadCastRight(left, right, implementationList, pair.getValue());
    }

    /**
     * Decides whether the join can convert to BroadcastJoin.
     *
     * @param mq metadata query
     * @param join the original join node
     * @param left join left child
     * @param right join right child
     * @return an Tuple2 instance. The first element of tuple is true if left side can convert to
     * * broadcast, false else. The second element of tuple is true if right side can convert to
     * * broadcast, false else.
     */
    public static Pair<Boolean, Boolean> canBroadcast(RelMetadataQuery mq, Join join, RelNode left, RelNode right) {
        if (!PlannerContext.getPlannerContext(join).getParamManager()
            .getBoolean(ConnectionParams.ENABLE_BROADCAST_JOIN)) {
            return new Pair<>(false, false);
        }
        /* consider (A join B). (A join broadcast(B)) can be replaced by shuffle(shuffle(A) join shuffle(B))
        so if broadcast(B) is worse than shuffle(A) + shuffle(B) + shuffle(A join B), we can prune it
         */
        int parallelism = PlannerContext.getPlannerContext(join).getParamManager()
            .getInt(ConnectionParams.BROADCAST_SHUFFLE_PARALLELISM);
        double shuffleCost =
            CBOUtil.getShuffleCost(mq, left) + CBOUtil.getShuffleCost(mq, right) + CBOUtil.getShuffleCost(mq, join);

        switch (join.getJoinType()) {
        case LEFT:
        case SEMI:
        case ANTI:
            return new Pair<>(false, shuffleCost >= parallelism * CBOUtil.getShuffleCost(mq, right));
        case RIGHT:
            return new Pair<>(shuffleCost >= parallelism * CBOUtil.getShuffleCost(mq, left), false);
        case FULL:
            return new Pair<>(false, false);
        case INNER:
            if (PlannerContext.getPlannerContext(right).getParamManager()
                .getBoolean(ConnectionParams.ENABLE_BROADCAST_LEFT)) {
                return new Pair<>(shuffleCost >= parallelism * CBOUtil.getShuffleCost(mq, left),
                    shuffleCost >= parallelism * CBOUtil.getShuffleCost(mq, right));
            } else {
                return new Pair<>(false,
                    shuffleCost >= parallelism * CBOUtil.getShuffleCost(mq, right));
            }
        default:
            throw new RuntimeException("Don't invoke here!!!");
        }
    }

    public static double getShuffleCost(RelMetadataQuery mq, RelNode rel) {
        double rowCount = mq.getRowCount(rel);
        long rowSize = TableScanIOEstimator.estimateRowSize(rel.getRowType());
        return rowCount * rowSize;
    }

    public static void columnarBroadCastLeft(
        RelNode left,
        RelNode right,
        List<Pair<RelDistribution, Pair<RelNode, RelNode>>> implementationList,
        boolean leftBroadcast) {
        if (!leftBroadcast) {
            return;
        }
        RelDistribution rightDistribution = RelDistributions.ANY;
        RelNode broadcastLeft =
            RelOptRule.convert(left, left.getTraitSet().replace(RelDistributions.BROADCAST_DISTRIBUTED));
        RelNode newRight = RelOptRule.convert(right, right.getTraitSet().replace(rightDistribution));
        implementationList.add(
            new Pair<>(rightDistribution,
                new Pair<>(broadcastLeft, newRight)));
    }

    public static void columnarBroadCastRight(
        RelNode left,
        RelNode right,
        List<Pair<RelDistribution, Pair<RelNode, RelNode>>> implementationList,
        boolean rightBroadcast) {
        if (!rightBroadcast) {
            return;
        }
        RelDistribution leftDistribution = RelDistributions.ANY;
        RelNode newLeft = RelOptRule.convert(left, left.getTraitSet().replace(leftDistribution));
        RelNode broadcastRight =
            RelOptRule.convert(right, right.getTraitSet().replace(RelDistributions.BROADCAST_DISTRIBUTED));
        implementationList.add(new Pair<>(leftDistribution, new Pair<>(newLeft, broadcastRight)));
    }

    public static boolean containsCorrelate(RelNode node) {
        if (node instanceof HepRelVertex) {
            node = ((HepRelVertex) node).getCurrentRel();
        }
        try {
            new RelVisitor() {
                public void visit(RelNode node, int ordinal, RelNode parent) {
                    if (node instanceof HepRelVertex) {
                        node = ((HepRelVertex) node).getCurrentRel();
                    }
                    if (node instanceof Correlate) {
                        throw Util.FoundOne.NULL;
                    }
                    super.visit(node, ordinal, parent);
                }
                // CHECKSTYLE: IGNORE 1
            }.go(node);
            return false;
        } catch (Util.FoundOne e) {
            return true;
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
            RelColumnOrigin relColumnOrigin = mq.getColumnOrigin(sort.getInput(), relFieldCollation.getFieldIndex());
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

    public static RelNode optimizeByOrcImpl(RelNode rel) {
        HepProgramBuilder builder = new HepProgramBuilder();
        builder.addMatchOrder(HepMatchOrder.TOP_DOWN);
        builder.addGroupBegin();
        builder.addRuleInstance(FilterMergeRule.INSTANCE);
        builder.addRuleInstance(ProjectFilterTransposeRule.INSTANCE);
        builder.addRuleInstance(ProjectMergeRule.INSTANCE);
        builder.addGroupEnd();
        builder.addMatchOrder(HepMatchOrder.BOTTOM_UP);
        builder.addGroupBegin();
        builder.addRuleInstance(OrcTableScanRule.PROJECT_FILTER_PROJECT_TABLESCAN);
        builder.addGroupEnd();
        builder.addMatchOrder(HepMatchOrder.BOTTOM_UP);
        builder.addGroupBegin();
        builder.addRuleInstance(OrcTableScanRule.PROJECT_FILTER_TABLESCAN);
        builder.addGroupEnd();
        builder.addMatchOrder(HepMatchOrder.BOTTOM_UP);
        builder.addGroupBegin();
        builder.addRuleInstance(OrcTableScanRule.FILTER_PROJECT_TABLESCAN);
        builder.addGroupEnd();
        builder.addMatchOrder(HepMatchOrder.BOTTOM_UP);
        builder.addGroupBegin();
        builder.addRuleInstance(OrcTableScanRule.PROJECT_TABLESCAN);
        builder.addGroupEnd();
        builder.addMatchOrder(HepMatchOrder.BOTTOM_UP);
        builder.addGroupBegin();
        builder.addRuleInstance(OrcTableScanRule.FILTER_TABLESCAN);
        builder.addGroupEnd();
        builder.addMatchOrder(HepMatchOrder.BOTTOM_UP);
        builder.addGroupBegin();
        builder.addRuleInstance(OrcTableScanRule.TABLESCAN);
        builder.addGroupEnd();
        HepPlanner planner = new HepPlanner(builder.build());
        planner.stopOptimizerTrace();
        planner.setRoot(rel);

        RelNode output = planner.findBestExp();
        return output;
    }

    public static RelNode optimizeByPushFilterImpl(RelNode rel) {
        HepProgramBuilder builder = new HepProgramBuilder();

        // Push filter to TableScan and generate GetPlan.
        builder.addGroupBegin();
        // Just make filter closer to TableScan which helps generating Get in XPlan.
        builder.addRuleInstance(FilterAggregateTransposeRule.INSTANCE);
        builder.addRuleInstance(FilterWindowTransposeRule.INSTANCE);
        builder.addRuleInstance(FilterProjectTransposeRule.INSTANCE);
        builder.addRuleInstance(FilterMergeRule.INSTANCE);
        builder.addGroupEnd();
        HepPlanner planner = new HepPlanner(builder.build());
        planner.stopOptimizerTrace();
        planner.setRoot(rel);
        return planner.findBestExp();
    }

    public static RelOptCost getCost(ExecutionContext executionContext) {
        final RelOptCost zero = DrdsRelOptCostImpl.FACTORY.makeZeroCost();
        if (!executionContext.getParamManager().getBoolean(ConnectionParams.RECORD_SQL_COST)) {
            return zero;
        }
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
            if (mq != null) {
                synchronized (mq) {
                    try {
                        cost = mq.getCumulativeCost(plan);
                    } catch (Throwable t) {
                        cost = plan.getCluster().getPlanner().getCostFactory().makeTinyCost();
                    }
                }
            } else {
                cost = plan.getCluster().getPlanner().getCostFactory().makeTinyCost();
            }
            PlannerContext.getPlannerContext(plan).setCost(cost);
        }
        return cost;
    }

    public static boolean containUnpushableAgg(Aggregate agg) {
        if (agg == null) {
            return false;
        }
        for (AggregateCall call : agg.getAggCallList()) {
            if (call.getAggregation().getKind() == SqlKind.HYPER_LOGLOG) {
                return true;
            }
            if (call.getAggregation().getKind() == SqlKind.PARTIAL_HYPER_LOGLOG) {
                return true;
            }
            if (call.getAggregation().getKind() == SqlKind.FINAL_HYPER_LOGLOG) {
                return true;
            }
            if (call.getAggregation().getKind() == SqlKind.JSON_ARRAY_GLOBALAGG) {
                return true;
            }
            if (call.getAggregation().getKind() == SqlKind.JSON_OBJECT_GLOBALAGG) {
                return true;
            }
            if (call.getAggregation().getKind() == SqlKind.CHECK_SUM_V2) {
                return true;
            }
            if (call.hasFilter()) {
                return true;
            }
            if (call.getAggregation().getKind() == SqlKind.COUNT) {
                if (!call.isDistinct()) {
                    if (call.getArgList().size() > 1) {
                        return true;
                    }
                }
            }
        }
        return false;
    }

    public static boolean isSingleValue(Aggregate agg) {
        for (AggregateCall aggCall : agg.getAggCallList()) {
            if (aggCall.getAggregation().getKind() == SqlKind.SINGLE_VALUE) {
                return true;
            }
        }
        return false;
    }

    public static boolean isCheckSum(Aggregate agg) {
        for (AggregateCall aggCall : agg.getAggCallList()) {
            if (aggCall.getAggregation().getKind() == SqlKind.CHECK_SUM) {
                return true;
            }
        }
        return false;
    }

    public static boolean isCheckSumV2(Aggregate agg) {
        for (AggregateCall aggCall : agg.getAggCallList()) {
            if (aggCall.getAggregation().getKind() == SqlKind.CHECK_SUM_V2) {
                return true;
            }
        }
        return false;
    }

    public static boolean isGroupSets(Aggregate agg) {
        for (AggregateCall aggCall : agg.getAggCallList()) {
            SqlKind sqlKind = aggCall.getAggregation().getKind();
            if (sqlKind == SqlKind.GROUP_ID || sqlKind == SqlKind.GROUPING || sqlKind == SqlKind.GROUPING_ID) {
                return true;
            }
        }
        return agg.getGroupSets().size() > 1;
    }

    public static boolean isColumnarOptimizer(RelNode node) {
        return PlannerContext.getPlannerContext(node).isUseColumnar();
    }

    public static Convention getColConvention() {
        return DrdsConvention.INSTANCE;
    }

    public static org.apache.calcite.util.Pair<RelTraitSet, List<RelTraitSet>> passThroughTraitsForJoin(
        RelTraitSet required, Join join, JoinRelType joinType, int leftInputFieldCount, RelTraitSet joinTraitSet) {

        if (required.getConvention() != joinTraitSet.getConvention()) {
            return null;
        }

        if (required.getConvention() == MppConvention.INSTANCE) {
            return null;
        }
        if (isColumnarOptimizer(join)) {
            // pass through for broadcast hash join in columnar
            RelDistribution distribution = required.getTrait(RelDistributionTraitDef.INSTANCE);
            if (distribution == null || !distribution.isShardWise()) {
                return null;
            }
            if (required.getCollation() != RelCollations.EMPTY) {
                return null;
            }
            if (join.getRight().getTraitSet().getDistribution() == RelDistributions.BROADCAST_DISTRIBUTED) {
                for (int key : distribution.getKeys()) {
                    if (key >= leftInputFieldCount) {
                        return null;
                    }
                }
                return org.apache.calcite.util.Pair.of(joinTraitSet.replace(distribution),
                    ImmutableList.of(join.getLeft().getTraitSet().replace(distribution),
                        join.getRight().getTraitSet()));
            } else if (join.getLeft().getTraitSet().getDistribution() == RelDistributions.BROADCAST_DISTRIBUTED) {
                for (int key : distribution.getKeys()) {
                    if (key < leftInputFieldCount) {
                        return null;
                    }
                }
                Mappings.TargetMapping mapping =
                    Mappings.createShiftMapping(join.getRowType().getFieldCount(),
                        0,
                        leftInputFieldCount,
                        join.getRight().getRowType().getFieldCount());
                return org.apache.calcite.util.Pair.of(joinTraitSet.replace(distribution),
                    ImmutableList.of(join.getLeft().getTraitSet(),
                        join.getRight().getTraitSet().replace(distribution.apply(mapping))));
            }
            return null;
        }

        final RelDistribution leftDistribution = RelDistributions.ANY;
        final RelDistribution rightDistribution = RelDistributions.ANY;

        RelCollation collation = required.getCollation();
        if (collation == null || collation == RelCollations.EMPTY || joinType == JoinRelType.FULL
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
            ImmutableList.of(passthroughTraitSet.replace(leftDistribution),
                passthroughTraitSet.replace(RelCollations.EMPTY).replace(rightDistribution)));
    }

    public static org.apache.calcite.util.Pair<RelTraitSet, List<RelTraitSet>> deriveTraitsForJoin(
        RelTraitSet childTraits, int childId, JoinRelType joinType, RelTraitSet joinTraitSet,
        RelTraitSet rightTraitSet) {
        // should only derive traits (limited to collation for now) from left join input.
        assert childId == 0;

        RelCollation collation = childTraits.getCollation();
        if (collation == null || collation == RelCollations.EMPTY || joinType == JoinRelType.FULL
            || joinType == JoinRelType.RIGHT) {
            return null;
        }

        RelTraitSet derivedTraits = joinTraitSet.replace(collation);
        return org.apache.calcite.util.Pair.of(derivedTraits, ImmutableList.of(derivedTraits, rightTraitSet));
    }

    public static org.apache.calcite.util.Pair<RelTraitSet, List<RelTraitSet>> passThroughTraitsForProject(
        RelTraitSet required, List<RexNode> exps, RelDataType inputRowType, RelDataTypeFactory typeFactory,
        RelTraitSet currentTraits,
        RelNode project) {
        final RelCollation collation = required.getCollation();
        final RelDistribution distribution = required.getDistribution();
        final Convention convention = required.getConvention();

        if (CBOUtil.isColumnarOptimizer(project)) {
            if (required.getConvention() != currentTraits.getConvention()) {
                return null;
            }
            if (convention != DrdsConvention.INSTANCE) {
                return null;
            }
            // don't pass through sort
            if (collation != null && collation != RelCollations.EMPTY) {
                return null;
            }
            if (distribution == null || distribution == RelDistributions.ANY) {
                return null;
            }

            final Mappings.TargetMapping map = RelOptUtil.permutationIgnoreCast(exps, inputRowType);

            if (distribution.getKeys().stream().anyMatch(
                key -> !isDistributionKeyOnTrivialExpr(exps, typeFactory,
                    map, key, true))) {
                return null;
            }

            RelTraitSet traits = currentTraits.replace(collation).replace(distribution);
            RelTraitSet childTrait = currentTraits.replace(distribution.apply(map));
            return org.apache.calcite.util.Pair.of(traits, ImmutableList.of(childTrait));
        }

        if (collation == null || collation == RelCollations.EMPTY) {
            if (convention == MppConvention.INSTANCE) {
                if (distribution == null || distribution == RelDistributions.ANY) {
                    return null;
                }
            } else {
                return null;
            }
        }

        final Mappings.TargetMapping map = RelOptUtil.permutationIgnoreCast(exps, inputRowType);

        if (collation.getFieldCollations().stream()
            .anyMatch(rc -> !isCollationOnTrivialExpr(exps, typeFactory, map, rc, true))) {
            return null;
        }

        if (distribution.getKeys().stream()
            .anyMatch(key -> !isDistributionKeyOnTrivialExpr(exps, typeFactory, map, key, true))) {
            return null;
        }

        final RelCollation newCollation = collation.apply(map);
        final RelDistribution newDistribution = distribution.apply(map);

        return org.apache.calcite.util.Pair.of(currentTraits.replace(collation).replace(distribution),
            ImmutableList.of(currentTraits.replace(newCollation).replace(newDistribution)));
    }

    public static org.apache.calcite.util.Pair<RelTraitSet, List<RelTraitSet>> deriveTraitsForProject(
        RelTraitSet childTraits, int childId, List<RexNode> exps, RelDataType inputRowType,
        RelDataTypeFactory typeFactory, RelTraitSet currentTraits) {
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

        final int maxField = Math.max(exps.size(), inputRowType.getFieldCount());
        Mappings.TargetMapping mapping = Mappings.create(MappingType.FUNCTION, maxField, maxField);
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

        if (distribution.getKeys().stream()
            .anyMatch(key -> !isDistributionKeyOnTrivialExpr(exps, typeFactory, mapping, key, false))) {
            return null;
        }

        if (collationFieldsToDerive.size() > 0 || !distribution.isTop()) {
            final RelCollation newCollation = RelCollations.of(collationFieldsToDerive).apply(mapping);
            final RelDistribution newDistribution = distribution.apply(mapping);
            return org.apache.calcite.util.Pair.of(currentTraits.replace(newCollation).replace(newDistribution),
                ImmutableList.of(currentTraits.replace(collation).replace(distribution)));
        } else {
            return null;
        }
    }

    private static boolean isCollationOnTrivialExpr(List<RexNode> projects, RelDataTypeFactory typeFactory,
                                                    Mappings.TargetMapping map, RelFieldCollation fc,
                                                    boolean passDown) {
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
                RexCallBinding.create(typeFactory, cast, ImmutableList.of(RelCollations.of(newFieldCollation)));
            if (cast.getOperator().getMonotonicity(binding) == SqlMonotonicity.NOT_MONOTONIC) {
                return false;
            }
        }

        return true;
    }

    private static boolean isDistributionKeyOnTrivialExpr(List<RexNode> projects, RelDataTypeFactory
        typeFactory,
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

    public static boolean isOss(ExecutionContext ec, String table) {
        TableMeta tm = ec.getSchemaManager().getTableWithNull(table);
        return tm != null && Engine.isFileStore(tm.getEngine());
    }

    public static boolean isOss(String schema, String table) {
        TableMeta tm = OptimizerContext.getContext(schema).getLatestSchemaManager().getTableWithNull(table);
        return tm != null && (!tm.isColumnar()) && Engine.isFileStore(tm.getEngine());
    }


    public static boolean isArchiveCCi(String schema, String table) {
        TableMeta tm = OptimizerContext.getContext(schema).getLatestSchemaManager().getTableWithNull(table);
        if (tm == null || !tm.isColumnar()) {
            return false;
        }
        return tm.isColumnarArchive();
    }

    /**
     * check whether there is any scan on columnar-store
     *
     * @param input the root of plan
     * @return true if find a columnar logicalView
     */
    public static boolean planWithColumnar(RelNode input) {
        LogicalViewWithSubqueryFinder logicalViewWithSubqueryFinder = new LogicalViewWithSubqueryFinder();
        input.accept(logicalViewWithSubqueryFinder);
        for (LogicalView lv : logicalViewWithSubqueryFinder.getResult()) {
            if (lv instanceof OSSTableScan) {
                if (((OSSTableScan) lv).isColumnarIndex()) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * assign ColumnarMaxShardCnt
     *
     * @param input the root of plan
     * @param plannerContext context info
     */
    public static void assignColumnarMaxShardCnt(RelNode input, PlannerContext plannerContext) {
        LogicalViewWithSubqueryFinder logicalViewWithSubqueryFinder = new LogicalViewWithSubqueryFinder();
        input.accept(logicalViewWithSubqueryFinder);

        int maxShard = -1;
        for (LogicalView lv : logicalViewWithSubqueryFinder.getResult()) {
            if (lv instanceof OSSTableScan) {
                TableMeta tm = CBOUtil.getTableMeta(lv.getTable());
                if (tm.isColumnar() ||
                    (Engine.isFileStore(tm.getEngine()) && plannerContext.getParamManager()
                        .getBoolean(ConnectionParams.ENABLE_OSS_MOCK_COLUMNAR))) {
                    int shard = TableTopologyUtil.isShard(tm) ?
                        tm.getPartitionInfo().getPartitionBy().getPartitions().size()
                        : -1;
                    maxShard = Math.max(shard, maxShard);
                }
            }
        }
        plannerContext.setColumnarMaxShardCnt(maxShard);
    }

    /**
     * check whether there is no scan on row-store
     *
     * @param input the root of plan
     * @return false if find a non-columnar logicalView
     */
    public static boolean planAllColumnar(RelNode input) {
        if (input instanceof BaseTableOperation) {
            return false;
        }
        LogicalViewWithSubqueryFinder logicalViewWithSubqueryFinder = new LogicalViewWithSubqueryFinder();
        input.accept(logicalViewWithSubqueryFinder);

        for (LogicalView lv : logicalViewWithSubqueryFinder.getResult()) {
            if (lv instanceof OSSTableScan) {
                if (((OSSTableScan) lv).isColumnarIndex()) {
                    continue;
                }
            }
            return false;
        }
        return true;
    }

    public static boolean allTablesHaveColumnar(RelNode input, ExecutionContext context) {
        if (input instanceof BaseTableOperation) {
            return false;
        }
        // find columnar table, use columnar optimizer
        LogicalViewWithSubqueryFinder logicalViewFinder = new LogicalViewWithSubqueryFinder();
        input.accept(logicalViewFinder);
        if (CollectionUtils.isEmpty(logicalViewFinder.getResult())) {
            return false;
        }
        for (LogicalView lv : logicalViewFinder.getResult()) {
            if (lv.getTableNames().size() != 1) {
                return false;
            }
            String schema = lv.getSchemaName();
            String table = lv.getTableNames().get(0);

            if (!CBOUtil.hasCci(schema, table, context)) {
                return false;
            }
            // OSSTableScan will be generated from force index(columnar) or archive table
            if (lv instanceof OSSTableScan) {
                TableMeta tm = context.getSchemaManager(schema).getTableWithNull(table);
                if (tm == null) {
                    continue;
                }
                // archive table
                if (!tm.isColumnar()) {
                    // mock columnar
                    if (!context.getParamManager().getBoolean(ConnectionParams.ENABLE_OSS_MOCK_COLUMNAR)) {
                        return false;
                    }
                }
                continue;
            }

            List<String> columnarIndexNameList =
                GlobalIndexMeta.getColumnarIndexNames(table, schema, context);
            if (lv.getIndexNode() != null) {
                // force index logicalView means force local index, as force index(col) will be replaced as ossTableScan
                if (!CollectionUtils.isEmpty(IndexUtil.getForceIndex(lv.getIndexNode()))) {
                    return false;
                }
                columnarIndexNameList =
                    AccessPathRule.filterUseIgnoreIndex(schema, table, columnarIndexNameList, lv.getIndexNode(),
                        context);
            }
            if (lv.getPartitions() != null) {
                return false;
            }
            if (CollectionUtils.isEmpty(columnarIndexNameList)) {
                return false;
            }
        }
        return true;
    }

    public static boolean isGsi(String schemaName, String tableName) {
        if (StringUtils.isNotBlank(schemaName) && StringUtils.isNotBlank(tableName)) {
            try (Connection metaDbConn = MetaDbUtil.getConnection()) {
                if (DbInfoManager.getInstance().isNewPartitionDb(schemaName)) {
                    TablePartitionAccessor tablePartitionAccessor = new TablePartitionAccessor();
                    tablePartitionAccessor.setConnection(metaDbConn);
                    List<TablePartitionRecord> partitionRecords =
                        tablePartitionAccessor.getTablePartitionsByDbNameTbName(schemaName, tableName, false);
                    return !partitionRecords.isEmpty()
                        && partitionRecords.get(0).tblType == PARTITION_TABLE_TYPE_GSI_TABLE;
                } else {
                    TablesExtAccessor tablesExtAccessor = new TablesExtAccessor();
                    tablesExtAccessor.setConnection(metaDbConn);
                    TablesExtRecord tablesExtRecord = tablesExtAccessor.query(schemaName, tableName, false);
                    return tablesExtRecord != null && tablesExtRecord.tableType == GsiMetaManager.TableType.GSI
                        .getValue();
                }
            } catch (Exception e) {
                throw new TddlNestableRuntimeException("check table is gsi failed!", e);
            }
        }
        return false;
    }

    /**
     * check whether table has cci
     *
     * @param schema given schema name
     * @param table given table name
     * @param context ExecutionContext
     * @return true if 1.the table is archive table while ENABLE_OSS_MOCK_COLUMNAR; 2. the table is cci
     * 3. the table has cci
     */
    public static boolean hasCci(String schema, String table, ExecutionContext context) {
        // oss mock columnar
        if (CBOUtil.isOss(schema, table)) {
            return context.getParamManager().getBoolean(ConnectionParams.ENABLE_OSS_MOCK_COLUMNAR);
        }

        TableMeta tm = context.getSchemaManager(schema).getTableWithNull(table);
        if (tm == null) {
            return false;
        }
        if (tm.isColumnar()) {
            return true;
        }
        return CollectionUtils.isNotEmpty(GlobalIndexMeta.getColumnarIndexNames(table, schema, context));
    }

    public static boolean isCci(String schemaName, String tableName, TablePartitionAccessor tablePartitionAccessor,
                                TablesExtAccessor tablesExtAccessor) {
        if (StringUtils.isNotBlank(schemaName) && StringUtils.isNotBlank(tableName)) {
            try (Connection metaDbConn = MetaDbUtil.getConnection()) {
                if (DbInfoManager.getInstance().isNewPartitionDb(schemaName)) {
                    tablePartitionAccessor.setConnection(metaDbConn);
                    List<TablePartitionRecord> partitionRecords =
                        tablePartitionAccessor.getTablePartitionsByDbNameTbName(schemaName, tableName, false);
                    return !partitionRecords.isEmpty()
                        && partitionRecords.get(0).tblType == PARTITION_TABLE_TYPE_COLUMNAR_TABLE;
                } else {
                    tablesExtAccessor.setConnection(metaDbConn);
                    TablesExtRecord tablesExtRecord = tablesExtAccessor.query(schemaName, tableName, false);
                    return tablesExtRecord != null && tablesExtRecord.tableType == GsiMetaManager.TableType.COLUMNAR
                        .getValue();
                }
            } catch (Exception e) {
                throw new TddlNestableRuntimeException("check table is cci failed!", e);
            }
        }
        return false;
    }

    /**
     * check whether the agg can be pushed down or not
     *
     * @return true if the agg can be pushed down
     */
    public static boolean canPushAggToOss(LogicalAggregate aggregate, OSSTableScan ossTableScan) {
        // can't deal with distinct
        if (PlannerUtils.haveAggWithDistinct(aggregate.getAggCallList())) {
            return false;
        }
        // can't deal with group by
        if (!aggregate.getGroupSet().isEmpty()) {
            return false;
        }

        // if agg has only one check_sum, check whether contains all the columns in given order
        if (aggregate.getAggCallList().size() == 1) {
            AggregateCall aggCall = aggregate.getAggCallList().get(0);
            if (aggCall.getAggregation().getKind() == SqlKind.CHECK_SUM
                || aggCall.getAggregation().getKind() == SqlKind.CHECK_SUM_V2) {
                if (aggCall.getArgList().size() == 0) {
                    return false;
                }
                int cnt = 0;
                for (int i = 0; i < aggCall.getArgList().size(); i++) {
                    RelColumnOrigin columnOrigin = ossTableScan.getCluster().getMetadataQuery()
                        .getColumnOrigin(ossTableScan.getPushedRelNode(), aggCall.getArgList().get(i));
                    if (columnOrigin == null) {
                        return false;
                    }
                    if (columnOrigin.getOriginColumnOrdinal() != cnt) {
                        return false;
                    }
                    cnt++;
                }
                return CBOUtil.getTableMeta(ossTableScan.getTable()).getAllColumns().size() == cnt;
            }
        }
        for (AggregateCall aggCall : aggregate.getAggCallList()) {
            SqlKind kind = aggCall.getAggregation().getKind();
            if (kind != SqlKind.SUM && kind != SqlKind.SUM0 && kind != SqlKind.COUNT && kind != SqlKind.MIN
                && kind != SqlKind.MAX) {
                return false;
            }
            if (aggCall.getArgList().size() == 0) {
                continue;
            }
            if (aggCall.getArgList().size() > 1) {
                return false;
            }
            // now we have only one column in the agg call
            if (ossTableScan.getCluster().getMetadataQuery()
                .getColumnOrigin(ossTableScan.getPushedRelNode(), aggCall.getArgList().get(0)) == null) {
                return false;
            }

            RelColumnOrigin columnOrigin = ossTableScan.getCluster().getMetadataQuery()
                .getColumnOrigin(ossTableScan.getPushedRelNode(), aggCall.getArgList().get(0));
            DataType type = CBOUtil.getTableMeta(columnOrigin.getOriginTable()).getColumn(columnOrigin.getColumnName())
                .getDataType();
            final MySQLStandardFieldType fieldType = type.fieldType();

            switch (kind) {
            case SUM:
            case SUM0: {
                switch (fieldType) {
                case MYSQL_TYPE_LONGLONG:
                    if (type.isUnsigned()) {
                        // reject bigint unsigned.
                        return false;
                    }
                case MYSQL_TYPE_LONG:
                case MYSQL_TYPE_INT24:
                case MYSQL_TYPE_SHORT:
                case MYSQL_TYPE_TINY:
                case MYSQL_TYPE_DOUBLE:
                case MYSQL_TYPE_FLOAT:
                    // fall to next agg function
                    break;
                default:
                    // reject directly
                    return false;
                }
            }
            case MAX:
            case MIN: {
                switch (fieldType) {
                case MYSQL_TYPE_DATETIME:
                case MYSQL_TYPE_DATETIME2:
                case MYSQL_TYPE_TIMESTAMP:
                case MYSQL_TYPE_TIMESTAMP2:
                case MYSQL_TYPE_DATE:
                case MYSQL_TYPE_NEWDATE:
                case MYSQL_TYPE_TIME:
                case MYSQL_TYPE_YEAR:
                case MYSQL_TYPE_DECIMAL:
                case MYSQL_TYPE_NEWDECIMAL:
                case MYSQL_TYPE_LONGLONG:
                case MYSQL_TYPE_LONG:
                case MYSQL_TYPE_INT24:
                case MYSQL_TYPE_SHORT:
                case MYSQL_TYPE_TINY:
                case MYSQL_TYPE_DOUBLE:
                case MYSQL_TYPE_FLOAT:
                    // fall to next agg function
                    break;
                default:
                    // reject directly
                    return false;
                }
            }
            }
        }
        return true;
    }
}
