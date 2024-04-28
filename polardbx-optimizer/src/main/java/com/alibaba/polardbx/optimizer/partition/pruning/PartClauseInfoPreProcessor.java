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

package com.alibaba.polardbx.optimizer.partition.pruning;

import com.alibaba.polardbx.common.utils.CaseInsensitive;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.core.TddlOperatorTable;
import com.alibaba.polardbx.optimizer.partition.PartitionByDefinition;
import com.alibaba.polardbx.optimizer.partition.common.PartKeyLevel;
import com.alibaba.polardbx.optimizer.partition.common.PartitionStrategy;
import com.alibaba.polardbx.optimizer.utils.PlannerUtils;
import com.alibaba.polardbx.optimizer.utils.SubQueryDynamicParamUtils;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexSubQuery;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static org.apache.calcite.sql.SqlKind.GREATER_THAN;
import static org.apache.calcite.sql.SqlKind.IS_NULL;
import static org.apache.calcite.sql.SqlKind.LESS_THAN;

/**
 * @author chenghui.lch
 */
public class PartClauseInfoPreProcessor {

    public static PartClauseItem convertToPartClauseItem(PartitionByDefinition partByDef,
                                                         RelDataType relRowType,
                                                         RexNode partPred,
                                                         RexNode partPredParent,
                                                         PartPruneStepBuildingContext stepContext) {
        PartClauseItem item =
            convertToPartClauseItemInner(partByDef, relRowType, partPred, partPredParent, stepContext);
        return item;
    }

    protected static PartClauseItem convertToPartClauseItemInner(PartitionByDefinition partByDef,
                                                                 RelDataType relRowType,
                                                                 RexNode partPred,
                                                                 RexNode partPredParent,
                                                                 PartPruneStepBuildingContext stepContext) {

        if (partPred == null) {
            return null;
        }

        if (!(partPred instanceof RexCall)) {
            PartClauseItem item = checkIfAlwaysTrueOrFalseExpr(partPred);
            return item;
        }

        RexCall partPredInfo = (RexCall) partPred;
        PartClauseItem clauseItem;
        SqlKind kind = partPred.getKind();
        if (kind == SqlKind.OR) {
            clauseItem =
                convertOrExprToPartClauseItem(partByDef, relRowType, partPredInfo, partPredParent, stepContext);
        } else if (kind == SqlKind.AND) {
            clauseItem =
                convertAndExprToPartClauseItem(partByDef, relRowType, partPredInfo, partPredParent, stepContext);
        } else {
            clauseItem =
                convertBoolExprToPartClauseItem(partByDef, relRowType, partPredInfo, partPredParent, stepContext);
        }

        return clauseItem;
    }

    private static PartClauseItem convertOrExprToPartClauseItem(PartitionByDefinition partByDef,
                                                                RelDataType relRowType,
                                                                RexCall partPred,
                                                                RexNode partPredParent,
                                                                PartPruneStepBuildingContext stepContext) {
        List<PartClauseItem> subOpItemList = new ArrayList<>();
        PartPruneStepType finalStepType = null;
        finalStepType = PartPruneStepType.PARTPRUNE_COMBINE_UNION;

        int misMatchPartClauseItemCnt = 0;
        for (RexNode op : partPred.getOperands()) {

            /**
             * <pre>
             *     For any single boolExpr which parentExpr is OR, if its partition key of the predicate is not 
             *     any partition columns, it should be treated as ALWAYS-TRUE expr that should do full scan.
             *
             *     e.g.
             *      if pk1 and npk are the partition columns, the following predicate 
             *
             *         pk1=a or npk=b
             *
             *      should be converted into 
             *
             *         pk1=a or TRUE
             *
             *      , because a single predicate "npk=b" cannot do any partition pruning and should do full scan
             *       .
             * </pre>
             */
            PartClauseItem item = convertToPartClauseItemInner(partByDef, relRowType, op, partPred, stepContext);
            item = rewritePartClauseInfoItemIfNeed(partByDef, relRowType, op, partPred, stepContext, item);
            if (item == null) {
                /**
                 * In OR-Expr, 
                 * if item is null, then it is the predicate without any partition column,
                 * so break here and generate always-true item
                 */
                misMatchPartClauseItemCnt++;
                break;
            } else {
                /**
                 * The returned item also maybe always-true item or always-false item.
                 * But we handle them later.
                 */
            }

            /**
             * <pre>
             *     For any single boolExpr which parentExpr is OR, if its partition key of the predicate is not 
             *     the first partition column, it should be treated as ALWAYS-TRUE expr that should do full scan.
             *
             *     e.g.
             *      if pk1 and pk2 are the partition columns, the following predicate 
             *
             *         pk1=a or pk2=b
             *
             *      should be converted into 
             *
             *         pk1=a or TRUE
             *
             *      , because a single predicate "pk2=b" cannot do any partition pruning without any predicate of pk1
             *       .
             * </pre>
             */
            if (item.getType() == PartPruneStepType.PARTPRUNE_OP_MATCHED_PART_KEY) {
                if (item.getClauseInfo().getPartKeyIndex() > 0) {
                    if (item.getClauseInfo().getOpKind() == SqlKind.IN
                        && item.getClauseInfo().getStrategy() == PartitionStrategy.CO_HASH) {
                        /**
                         * In co-hash, all part_cols are equivalent, so the partKeyIndex is not start with 0,
                         * its col In(SubQuery) Pruning should also be effective,
                         * so here ignore the count of misMatchPartClauseItemCnt
                         */
                    } else {
                        misMatchPartClauseItemCnt++;
                        break;
                    }
                }
            }

            if (item.getType() == finalStepType) {
                // the stepType of item is the same as its parent
                subOpItemList.addAll(item.getItemList());
            } else {
                subOpItemList.add(item);
            }
        }

        /**
         * If OrExpr contains predicates that contain NOT any partition columns or contain non-first-part-column predicate,
         * then auto generate a ALWAYS-TRUE expr for the OrExpr
         */
        if (misMatchPartClauseItemCnt > 0) {
            PartClauseItem alwaysTrueItem = PartClauseItem.buildAlwaysTrueItem();
            return alwaysTrueItem;
        }

        /**
         * Remove unused Always-false items for OR-expr
         */
        List<PartClauseItem> newSubOpItemList = new ArrayList<>();
        boolean containAlwayTrueItem = false;
        PartClauseItem targetItem = null;
        for (int i = 0; i < subOpItemList.size(); i++) {
            PartClauseItem item = subOpItemList.get(i);
            if (item.isAlwaysTrue()) {
                containAlwayTrueItem = true;
                targetItem = item;
                break;
            }
            if (item.isAlwaysFalse()) {
                continue;
            }
            newSubOpItemList.add(item);
        }
        if (containAlwayTrueItem) {
            return targetItem;
        }
        subOpItemList = newSubOpItemList;

        if (subOpItemList.size() == 0) {
            return null;
        }
        if (subOpItemList.size() == 1) {
            return subOpItemList.get(0);
        }

        PartClauseItem item = PartClauseItem.buildPartClauseItem(finalStepType, null, subOpItemList, partPred);
        return item;

    }

    private static PartClauseItem convertAndExprToPartClauseItem(PartitionByDefinition partByDef,
                                                                 RelDataType relRowType,
                                                                 RexCall partPred,
                                                                 RexNode partPredParent,
                                                                 PartPruneStepBuildingContext stepContext) {
        List<PartClauseItem> subOpItemList = new ArrayList<>();
        PartPruneStepType finalStepType = PartPruneStepType.PARTPRUNE_COMBINE_INTERSECT;
        List<PartClauseItem> opItemList = new ArrayList<>();
        List<PartClauseItem> otherItemList = new ArrayList<>();

        for (RexNode op : partPred.getOperands()) {

            /**
             * <pre>
             *     For any single boolExpr which parentExpr is AND, if its partition key of the predicate is not 
             *     any partition columns, it should be treated as ALWAYS-TRUE expr that should do full scan.
             *
             *     e.g.
             *      if pk1 is the partition columns, npk is NOT partition columns , the following predicate 
             *
             *         pk1=a AND npk=b
             *
             *      should be converted into 
             *
             *         pk1=a AND TRUE
             *
             *      , because a single predicate "npk=b" cannot do any partition pruning and should do full scan
             *       .
             * </pre>
             */
            PartClauseItem item = convertToPartClauseItemInner(partByDef, relRowType, op, partPred, stepContext);
            if (item == null) {
                /**
                 * if item is null, then it is the predicate without any partition column
                 */
                continue;
            } else {
                /**
                 * The returned item also maybe always-true item or always-false item.
                 * But we handle them later.
                 */
            }
            if (item.getType() == finalStepType) {
                // the stepType of item is the same as its parent
                subOpItemList.addAll(item.getItemList());
            } else {
                subOpItemList.add(item);
            }
        }

        /**
         * Split subOpItemList opItem list and no-op item list(and/or/always-true/always-false)
         */
        for (int i = 0; i < subOpItemList.size(); i++) {
            PartClauseItem item = subOpItemList.get(i);
            if (item.getType() == PartPruneStepType.PARTPRUNE_OP_MATCHED_PART_KEY) {
                opItemList.add(item);
            } else {
                otherItemList.add(item);
            }
        }

        /**
         * Because partPred has been converted to DNF formula (Notice: partPred must be a DNF formula), 
         * so if a AND expr contains NOT any predicates of first partition columns,
         * the AND expr should treated as Always-True expr
         *
         *     e.g.
         *      if pk1,pk2,pk3 are the partition columns, the following predicate 
         *
         *         pk2=a AND pk3=b
         *
         *      should be converted into 
         *
         *         TRUE AND TRUE
         *
         */
        boolean containFirstPartitionColumn = false;
        for (int i = 0; i < opItemList.size(); i++) {
            PartClauseItem partItem = opItemList.get(i);
            if (partItem.getClauseInfo().getPartKeyIndex() == 0) {
                containFirstPartitionColumn = true;
                break;
            }
        }
        if (!containFirstPartitionColumn) {
            /**
             * Treat all part predicates that does NOT contains the first partition columns
             * as a Always-True expr 
             */
            PartClauseItem alwaysTrueItem = PartClauseItem.buildAlwaysTrueItem();
            subOpItemList = otherItemList;
            subOpItemList.add(alwaysTrueItem);
        }

        /**
         * Remove unused Always-true items for AND-expr
         */
        List<PartClauseItem> newSubOpItemList = new ArrayList<>();
        boolean containAlwayFalseItem = false;
        PartClauseItem targetItem = null;
        for (int i = 0; i < subOpItemList.size(); i++) {
            PartClauseItem item = subOpItemList.get(i);
            if (item.isAlwaysFalse()) {
                containAlwayFalseItem = true;
                targetItem = item;
                break;
            }
            if (item.isAlwaysTrue()) {
                continue;
            }
            newSubOpItemList.add(item);
        }

        if (containAlwayFalseItem) {
            return targetItem;
        }
        subOpItemList = newSubOpItemList;

        if (subOpItemList.size() == 0) {
            return null;
        }

        if (subOpItemList.size() == 1) {
            return subOpItemList.get(0);
        }

        PartClauseItem item = PartClauseItem.buildPartClauseItem(finalStepType, null, subOpItemList, partPred);

        PartClauseItem finalItem =
            rewritePartClauseInfoItemIfNeed(partByDef, relRowType, partPred, partPredParent, stepContext, item);
        return finalItem;
    }

    private static PartClauseItem convertBoolExprToPartClauseItem(PartitionByDefinition partByDef,
                                                                  RelDataType relRowType,
                                                                  RexCall partPred,
                                                                  RexNode partPredParent,
                                                                  PartPruneStepBuildingContext stepContext) {

        PartClauseItem item = null;
        SqlKind opKind = partPred.getKind();
        switch (opKind) {

        // simple comparsion
        case LESS_THAN:
        case GREATER_THAN:
        case EQUALS:
        case NOT_EQUALS:
        case GREATER_THAN_OR_EQUAL:
        case LESS_THAN_OR_EQUAL:
            item = convertComparisonExprToPartClauseItem(partByDef, relRowType, partPred, stepContext);
            break;

        case IS_NULL:
            item = convertIsNullExprToPartClauseItem(partByDef, relRowType, partPred, false, stepContext);
            break;

        case BETWEEN:
            /**
             * all expr of "part_col between const1 and const2" has been rewrote as 
             * (part_col > const1 or part_col = const1) and (part_col < const2 or part_col = const2)
             * in PartPredRewriter.rewritePartPredicate
             */
            // all BETWEEN expr should NOT come here!!
            break;
        case IN:
            /**
             * all expr of "part_col in (const1,const2)" has been rewrote as 
             * (part_col = const1) or (part_col = const2)
             * in PartPredRewriter.rewritePartPredicate,
             *
             * only allow part_col in (scalarSubQuery) come here
             */
            // only allowed IN expr with non-max-one-row scalar subquery come here!!
            item = convertScalarQueryInExprToPartClauseItem(partByDef, relRowType, partPred, stepContext);

            break;

        default:
            return null;
        }

        if (item == null) {
            return null;
        }

        if (item.getType() == PartPruneStepType.PARTPRUNE_COMBINE_UNION
            || item.getType() == PartPruneStepType.PARTPRUNE_COMBINE_INTERSECT) {
            if (item.getItemList().size() == 1) {
                return item.getItemList().get(0);
            }
        }

        PartClauseItem finalItem =
            rewritePartClauseInfoItemIfNeed(partByDef, relRowType, partPred, partPredParent, stepContext, item);
        return finalItem;
    }

    private static PartClauseItem convertScalarQueryInExprToPartClauseItem(PartitionByDefinition partByDef,
                                                                           RelDataType relRowType,
                                                                           RexCall partPred,
                                                                           PartPruneStepBuildingContext stepContext) {

        RexNode input = partPred.getOperands().get(0);
        RexNode subQuery = partPred.getOperands().get(1);
        if (!SubQueryDynamicParamUtils.isNonMaxOneRowScalarSubQueryConstant(subQuery)) {
            return null;
        }

        PartitionStrategy strategy = partByDef.getStrategy();
        String predColName = null;
        boolean findInPartNameList = false;
        List<String> relPartNameList = partByDef.getPartitionColumnNameList();
        List<ColumnMeta> partKeyFldList = partByDef.getPartitionFieldList();
        int partKeyIdx = -1;
        ColumnMeta cmFldInfo = null;
        RexInputRef columnRef = (RexInputRef) input;
        RelDataType relDataType = relRowType;

        RelDataTypeField predColRelFld = relDataType.getFieldList().get(columnRef.getIndex());
        predColName = predColRelFld.getName();

        for (int i = 0; i < relPartNameList.size(); i++) {
            if (predColName.equalsIgnoreCase(relPartNameList.get(i))) {
                findInPartNameList = true;
                partKeyIdx = i;
                cmFldInfo = partKeyFldList.get(i);
                break;
            }
        }
        if (!findInPartNameList) {
            return null;
        }

        if (partKeyIdx != 0) {
            /**
             * the partCol index of InSubQuery is not the first col of partition col
             */
            if (strategy != PartitionStrategy.CO_HASH) {
                return null;
            }
        }

        /**
         * prepare eq expr of "partCol = ?n"
         */
        RexBuilder rexBuilder = PartitionPrunerUtils.getRexBuilder();
        List<RexNode> eqOpList = new ArrayList<>();
        eqOpList.add(input);

        RexDynamicParam subQueryDynamicParam = (RexDynamicParam) subQuery;
        RexDynamicParam eqValDynamicParam =
            new RexDynamicParam(subQueryDynamicParam.getType(), subQueryDynamicParam.getIndex(),
                RexDynamicParam.DYNAMIC_TYPE_VALUE.SUBQUERY_TEMP_VAR, subQueryDynamicParam.getRel());

        eqValDynamicParam.setMaxOnerow(true);
        eqOpList.add(eqValDynamicParam);
        RexNode eqExprOfInPred = rexBuilder.makeCall(TddlOperatorTable.EQUALS, eqOpList);
        PartClauseInfo eqExprClauseInfo =
            matchPartPredToPartKey(partByDef, relRowType, eqExprOfInPred, input, eqValDynamicParam, false,
                TddlOperatorTable.EQUALS, stepContext);
        if (eqExprClauseInfo == null) {
            return null;
        }
        eqExprClauseInfo.setDynamicConstOnly(true);

        SubQueryInPartClauseInfo sbInPartClauseInfo = new SubQueryInPartClauseInfo();
        sbInPartClauseInfo.setSubQueryDynamicParam(subQueryDynamicParam);

        sbInPartClauseInfo.setOp(TddlOperatorTable.IN);
        sbInPartClauseInfo.setOpKind(SqlKind.IN);
        sbInPartClauseInfo.setInput(input);
        PredConstExprReferenceInfo exprReferenceInfo = stepContext.buildPredConstExprReferenceInfo(subQuery);
        sbInPartClauseInfo.setConstExprId(exprReferenceInfo.getConstExprId());
        sbInPartClauseInfo.setConstExpr(exprReferenceInfo.getConstExpr());
        sbInPartClauseInfo.setOriginalPredicate(partPred);
        sbInPartClauseInfo.setNull(false);
        sbInPartClauseInfo.setPartKeyLevel(stepContext.getPartLevel());
        sbInPartClauseInfo.setStrategy(strategy);
        sbInPartClauseInfo.setPartKeyIndex(partKeyIdx);
        sbInPartClauseInfo.setPartKeyDataType(cmFldInfo.getField().getRelType());
        sbInPartClauseInfo.setDynamicConstOnly(true);

        PartClauseItem eqExprClauseInfoItem = PartClauseItem
            .buildPartClauseItem(PartPruneStepType.PARTPRUNE_OP_MATCHED_PART_KEY, eqExprClauseInfo,
                null,
                eqExprOfInPred);

        if (strategy == PartitionStrategy.CO_HASH) {
            PartClauseItem newPartClauseItem =
                rewriteDnfPartClauseInfoItemIfNeed(partByDef, relRowType, eqExprOfInPred, stepContext,
                    eqExprClauseInfoItem, null);
            if (newPartClauseItem.getType() == PartPruneStepType.PARTPRUNE_COMBINE_INTERSECT) {
                sbInPartClauseInfo.getEqExprClauseItems().addAll(newPartClauseItem.getItemList());
            } else {
                sbInPartClauseInfo.getEqExprClauseItems().add(eqExprClauseInfoItem);
            }
            sbInPartClauseInfo.getEqExprClauseItems().add(eqExprClauseInfoItem);
        } else {
            sbInPartClauseInfo.getEqExprClauseItems().add(eqExprClauseInfoItem);
        }
        sbInPartClauseInfo.getPartColDynamicParams().add(eqValDynamicParam);

        PartClauseItem inSubQueryItem = PartClauseItem
            .buildPartClauseItem(PartPruneStepType.PARTPRUNE_OP_MATCHED_PART_KEY, sbInPartClauseInfo,
                null,
                partPred);

        return inSubQueryItem;

    }

    /**
     * Convert the partition comparsion predicate to the uniform representation PartClauseItem
     */
    private static PartClauseItem convertComparisonExprToPartClauseItem(PartitionByDefinition partByDef,
                                                                        RelDataType relRowType,
                                                                        RexCall partPred,
                                                                        PartPruneStepBuildingContext stepContext) {

        if (partPred instanceof RexSubQuery) {
            return null;
        }

        RexNode left = partPred.getOperands().get(0);
        RexNode right = partPred.getOperands().get(1);
        SqlKind kind = partPred.getKind();
        PartitionStrategy strategy = partByDef.getStrategy();
        boolean isMultiCols = partByDef.getPartitionFieldList().size() > 1;
        SqlOperator operator = partPred.getOperator();
        PartClauseInfo matchedPartClauseInfo =
            matchPartPredToPartKey(partByDef, relRowType, partPred, left, right, false, operator, stepContext);
        if (matchedPartClauseInfo != null) {
            switch (kind) {
            case LESS_THAN:
            case GREATER_THAN:
            case EQUALS:
            case GREATER_THAN_OR_EQUAL:
            case LESS_THAN_OR_EQUAL:
            case NOT_EQUALS: {

                if (strategy == PartitionStrategy.KEY && isMultiCols && (kind == LESS_THAN || kind == GREATER_THAN)) {
                    /**
                     * For multi-column partition with key strategy, if p1 < const or p1 > const and p1 is the first part col,
                     * then should treat it as always-true predicates and generate full sscan.
                     * Because key strategy is not support do range query by using first part col directly
                     *
                     */
                    PartClauseItem item = PartClauseItem.buildAlwaysTrueItem();
                    return item;
                } else {
                    PartClauseItem item = PartClauseItem
                        .buildPartClauseItem(PartPruneStepType.PARTPRUNE_OP_MATCHED_PART_KEY, matchedPartClauseInfo,
                            null,
                            partPred);
                    return item;
                }
            }
            default:
                return null;
            }
        } else {
            // The part clause does NOT contain any part keys
            return null;
        }
    }

    private static PartClauseItem convertIsNullExprToPartClauseItem(PartitionByDefinition partByDef,
                                                                    RelDataType relRowType,
                                                                    RexCall partPred,
                                                                    boolean isNegative,
                                                                    PartPruneStepBuildingContext stepContext) {

        if (isNegative) {
            return null;
        }

        assert partPred.isA(IS_NULL);
        List<RexNode> operands = partPred.getOperands();
        RexNode input = operands.get(0);
        PartClauseInfo clauseInfo =
            matchPartPredToPartKey(partByDef, relRowType, partPred, input, null, true, TddlOperatorTable.EQUALS,
                stepContext);
        if (clauseInfo == null) {
            return null;
        }
        PartClauseItem clauseItem = PartClauseItem
            .buildPartClauseItem(PartPruneStepType.PARTPRUNE_OP_MATCHED_PART_KEY, clauseInfo, null, partPred);
        return clauseItem;
    }

    private static PartClauseItem buildPartClauseItemByUsingAnyValueExpr(PartitionByDefinition partByDef,
                                                                         RelDataType relRowType,
                                                                         int rexInputRefIndex,
                                                                         PartPruneStepBuildingContext stepContext) {

        RexInputRef partColInput = RexInputRef.of(rexInputRefIndex, relRowType);
        RexNode nullExpr = PartitionPrunerUtils.getRexBuilder().makeNullLiteral(relRowType);
        List<RexNode> opList = new ArrayList<>();
        opList.add(partColInput);
        opList.add(nullExpr);
        SqlOperator isNullOp = RexUtil.op(SqlKind.IS_NULL);

        RexNode newPartColPred = PartitionPrunerUtils.getRexBuilder().makeCall(isNullOp, opList);

        PartClauseInfo clauseInfo =
            matchPartPredToPartKey(partByDef, relRowType, newPartColPred, partColInput, null, true,
                TddlOperatorTable.EQUALS,
                stepContext);
        clauseInfo.setAnyValueEqCond(true);

        PartClauseItem clauseItem = PartClauseItem
            .buildPartClauseItem(PartPruneStepType.PARTPRUNE_OP_MATCHED_PART_KEY, clauseInfo, null, newPartColPred);

        return clauseItem;
    }

    /**
     * Try to match part predicate to part key and check if the part predicate contains part key
     */
    protected static PartClauseInfo matchPartPredToPartKey(PartitionByDefinition partByDef,
                                                           RelDataType relRowType,
                                                           RexNode originalPred,
                                                           RexNode left,
                                                           RexNode right,
                                                           boolean isNull,
                                                           SqlOperator op,
                                                           PartPruneStepBuildingContext stepContext) {

        List<String> relPartNameList = null;
        boolean findInPartNameList = false;
        SqlOperator predOp = null;
        SqlKind predOpKind = null;
        RexNode input = null;
        RexNode constExpr = null;
        String predColName = null;

        if (!isNull) {
            if (RexUtil.isReferenceOrAccess(left, false) && (RexUtil.isConstant(right)
                || SubQueryDynamicParamUtils.isScalarSubQueryConstant(right))) {
                predOp = op;
                predOpKind = predOp.getKind();
                input = left;
                constExpr = right;
            } else if ((RexUtil.isConstant(right) || SubQueryDynamicParamUtils.isScalarSubQueryConstant(right))
                && RexUtil.isReferenceOrAccess(left, false)) {
                predOp = PartPredRewriter.inverseOp(op);
                predOpKind = predOp.getKind();
                input = right;
                constExpr = left;

            } else {
                // The expr is not support to doing partition pruning
                return null;
            }
        } else {

            if (RexUtil.isReferenceOrAccess(left, false) && right == null) {
                predOpKind = op.getKind();
                input = left;
                constExpr = right;
            } else {
                // The expr is not support to doing partition pruning
                return null;
            }
        }

        // Only support the dynamic pruning of expr with RexInputRef
        if (!(input instanceof RexInputRef)) {
            return null;
        }

        if (constExpr instanceof RexDynamicParam) {
            /**
             * When
             *  index >= 0, it means the content of RexDynamicParam is the params value can be fetched from ExecutionContext
             *  index = -1, it means the content of RexDynamicParam is phy table name;
             *  index = -2, it means the content of RexDynamicParam is scalar subquery;
             *  index = -3, it means the content of RexDynamicParam is apply subquery.
             */
            RexDynamicParam dynamicParam = (RexDynamicParam) constExpr;
            //if (dynamicParam.getIndex() == PlannerUtils.SCALAR_SUBQUERY_PARAM_INDEX || dynamicParam.getIndex() == PlannerUtils.APPLY_SUBQUERY_PARAM_INDEX ) {
            if (dynamicParam.getIndex() == PlannerUtils.APPLY_SUBQUERY_PARAM_INDEX) {
                /**
                 * Not support to do pruning with apply subquery,
                 * it will be optimized later
                 */
                return null;
            }
        }

        PartClauseInfo clauseInfo = new PartClauseInfo();
        RexInputRef columnRef = (RexInputRef) input;
        RelDataType relDataType = relRowType;

        RelDataTypeField predColRelFld = relDataType.getFieldList().get(columnRef.getIndex());
        predColName = predColRelFld.getName();

        relPartNameList = partByDef.getPartitionColumnNameList();
        List<ColumnMeta> partKeyFldList = partByDef.getPartitionFieldList();

        PartitionStrategy strategy = partByDef.getStrategy();

        int partKeyIdx = -1;
        ColumnMeta cmFldInfo = null;

        for (int i = 0; i < relPartNameList.size(); i++) {
            if (predColName.equalsIgnoreCase(relPartNameList.get(i))) {
                findInPartNameList = true;
                partKeyIdx = i;
                cmFldInfo = partKeyFldList.get(i);
                break;
            }
        }
        if (findInPartNameList) {

            clauseInfo.setPartKeyLevel(stepContext.getPartLevel());
            clauseInfo.setOp(op);
            clauseInfo.setOpKind(predOpKind);
            clauseInfo.setInput(input);

            Integer constExprId = null;
            if (stepContext != null && !isNull) {
                /**
                 * Check if new const expr does already exists in cache, 
                 * if exists, then reuse;
                 * if does NOT exists, the put it into cache
                 */
                PredConstExprReferenceInfo constExprInfo = stepContext.buildPredConstExprReferenceInfo(constExpr);
                constExprId = constExprInfo.getConstExprId();
                constExpr = constExprInfo.getConstExpr();
            }
            clauseInfo.setConstExpr(constExpr);
            clauseInfo.setConstExprId(constExprId);

            clauseInfo.setDynamicConstOnly(constExpr instanceof RexDynamicParam);
            clauseInfo.setOriginalPredicate(originalPred);
            clauseInfo.setNull(isNull);
            clauseInfo.setPartKeyIndex(partKeyIdx);
            clauseInfo.setPartKeyDataType(cmFldInfo.getField().getRelType());
            clauseInfo.setStrategy(strategy);

            return clauseInfo;
        }
        return null;
    }

    private static PartClauseItem checkIfAlwaysTrueOrFalseExpr(RexNode partPred) {
        PartClauseItem alwaysTrueOrFalseItem = null;
        if (partPred.isAlwaysFalse()) {
            alwaysTrueOrFalseItem = PartClauseItem.buildAlwaysFalseItem();
        } else if (partPred.isAlwaysTrue()) {
            alwaysTrueOrFalseItem = PartClauseItem.buildAlwaysTrueItem();
        }
        return alwaysTrueOrFalseItem;
    }

    private static PartClauseItem rewriteDnfPartClauseInfoItemIfNeed(
        PartitionByDefinition partByDef,
        RelDataType relRowType,
        RexNode partPred,
        PartPruneStepBuildingContext stepContext,
        PartClauseItem targetPartItem,
        PartClauseItem targetPartItemParent) {

        PartKeyLevel partKeyLevel = stepContext.getPartLevel();
        PartitionByDefinition targetPartByDef = partByDef;
        if (partKeyLevel == PartKeyLevel.SUBPARTITION_KEY) {
            targetPartByDef = partByDef.getSubPartitionBy();
        }
        PartitionStrategy partStrategy = targetPartByDef.getStrategy();
        if (partStrategy != PartitionStrategy.CO_HASH) {
            return targetPartItem;
        }

        PartPruneStepType stepType = targetPartItem.getType();

        boolean singleEqOpItemWithoutParent = false;
        boolean singleEqOpItemUnderOrItem = false;
        boolean opItemUnderUnderAndItem = false;

//        RexCall partPredParentCall = null;
//        if (partPredParent == null) {
//            hasParent = false;
//        }
//
//        if (!hasParent) {
//            if (kind == SqlKind.EQUALS) {
//                singleEqOpItemWithoutParent = true;
//            }
//        } else {
//            if (partPredParent instanceof RexCall) {
//                partPredParentCall = (RexCall) partPredParent;
//            }
//            if (partPredParentCall == null) {
//                return targetPartClauseItem;
//            }
//
//            parentKind = partPredParentCall.getKind();
//            if (parentKind == SqlKind.OR) {
//                if (kind == SqlKind.EQUALS) {
//                    singleEqOpItemUnderOrItem = true;
//                }
//            } else {
//                opItemUnderUnderAndItem = true;
//            }
//        }

        if (stepType == PartPruneStepType.PARTPRUNE_OP_MATCHED_PART_KEY && targetPartItemParent == null) {
            if (targetPartItem.getClauseInfo().getOpKind() == SqlKind.EQUALS) {
                singleEqOpItemWithoutParent = true;
            }
        } else if (stepType == PartPruneStepType.PARTPRUNE_COMBINE_INTERSECT) {
            opItemUnderUnderAndItem = true;
        } else if (stepType == PartPruneStepType.PARTPRUNE_COMBINE_UNION) {
            List<PartClauseItem> newSubItems = new ArrayList<>();
            List<PartClauseItem> curSubItems = targetPartItem.getItemList();
            for (int i = 0; i < curSubItems.size(); i++) {
                PartClauseItem item = curSubItems.get(i);
                PartClauseItem newItem =
                    rewriteDnfPartClauseInfoItemIfNeed(partByDef, relRowType, partPred, stepContext, item,
                        targetPartItem);
                newSubItems.add(newItem);
            }
            targetPartItem.setItemList(newSubItems);
            return targetPartItem;
        }

        List<String> partCols = targetPartByDef.getPartitionColumnNameList();
        Map<String, Integer> partCol2PartKeyIdxMapping = new TreeMap<>(CaseInsensitive.CASE_INSENSITIVE_ORDER);
        int[] partColEqConditionStats = new int[partCols.size()];
        for (int i = 0; i < partColEqConditionStats.length; i++) {
            partColEqConditionStats[i] = 0;
            partCol2PartKeyIdxMapping.put(partCols.get(i), i);
        }

        int[] partKeyIdx2RexInputIdxMapping = new int[partCols.size()];
        List<RelDataTypeField> relFlds = relRowType.getFieldList();
        for (int i = 0; i < relFlds.size(); i++) {
            RelDataTypeField fld = relFlds.get(i);
            String fldName = fld.getName();
            Integer partKeyIdx = partCol2PartKeyIdxMapping.get(fldName);
            if (partKeyIdx != null) {
                partKeyIdx2RexInputIdxMapping[partKeyIdx] = i;
            }
        }

        List<PartClauseItem> itemList = new ArrayList<>();
        if (singleEqOpItemWithoutParent) {
            itemList.add(targetPartItem);
        } else if (singleEqOpItemUnderOrItem) {

        } else if (opItemUnderUnderAndItem) {
            itemList.addAll(targetPartItem.getItemList());
        } else {
            return targetPartItem;
        }

        for (int i = 0; i < itemList.size(); i++) {
            PartClauseItem item = itemList.get(i);
            PartPruneStepType itemStepType = item.getType();
            if (itemStepType != PartPruneStepType.PARTPRUNE_OP_MATCHED_PART_KEY) {
                return targetPartItem;
            }
            PartClauseInfo clauseInfo = item.getClauseInfo();
            SqlKind opKind = clauseInfo.getOpKind();
            int partKeyIdx = clauseInfo.getPartKeyIndex();
            if (opKind == SqlKind.EQUALS) {
                partColEqConditionStats[partKeyIdx]++;
            }
        }

        boolean foundPartColEqConds = false;
        for (int i = 0; i < partColEqConditionStats.length; i++) {
            if (partColEqConditionStats[i] > 0) {
                foundPartColEqConds = true;
                break;
            }
        }

        if (foundPartColEqConds) {
            List<PartClauseItem> partColItemsWithAnyValue = new ArrayList<>();
            for (int i = 0; i < partColEqConditionStats.length; i++) {
                int eqCondCount = partColEqConditionStats[i];
                if (eqCondCount == 0) {

                    /**
                     * Auto Fill eq conds for other part cols by use ANY_VALUES
                     */
                    int relInputIdx = partKeyIdx2RexInputIdxMapping[i];
                    PartClauseItem newPartItem =
                        buildPartClauseItemByUsingAnyValueExpr(partByDef, relRowType, relInputIdx, stepContext);
                    partColItemsWithAnyValue.add(newPartItem);
                }
            }

            if (singleEqOpItemWithoutParent) {
                /**
                 * c1=expr
                 *
                 * ==>
                 *
                 * And
                 *  c1=expr
                 *  c2=any
                 *  c3=any
                 *  ...
                 */
                List<PartClauseItem> allItems = new ArrayList<>();
                allItems.add(targetPartItem);
                allItems.addAll(partColItemsWithAnyValue);
                PartClauseItem andItem =
                    PartClauseItem.buildPartClauseItem(PartPruneStepType.PARTPRUNE_COMBINE_INTERSECT, null, allItems,
                        null);
                return andItem;
            } else if (opItemUnderUnderAndItem) {
                /**
                 * And
                 *  c2=expr1
                 *  c3=expr2
                 *  ...
                 *
                 * ==>
                 *
                 * and
                 *  c1=any
                 *  c2=expr1
                 *  c3=expr2
                 *  ...
                 */
                targetPartItem.getItemList().addAll(partColItemsWithAnyValue);
            } else {
                /**
                 * Do nothing
                 */
            }

        }

        return targetPartItem;

    }

    private static PartClauseItem rewritePartClauseInfoItemIfNeed(
        PartitionByDefinition partByDef,
        RelDataType relRowType,
        RexNode partPred,
        RexNode partPredParent,
        PartPruneStepBuildingContext stepContext,
        PartClauseItem targetPartItem
    ) {

        PartKeyLevel partKeyLevel = stepContext.getPartLevel();
        PartitionByDefinition targetPartByDef = partByDef;
        PartitionStrategy partStrategy = targetPartByDef.getStrategy();
        if (partStrategy != PartitionStrategy.CO_HASH) {
            return targetPartItem;
        }

        if (targetPartItem == null) {
            return targetPartItem;
        }

        PartPruneStepType stepType = targetPartItem.getType();
        SqlKind kind = null;

        boolean containPredParent = false;
        boolean singleEqOpItemWithoutParent = false;
        boolean singleEqOpItemUnderOrItem = false;
        boolean opItemUnderUnderAndItem = false;

        if (partPredParent != null) {
            containPredParent = true;
        }

        if (stepType == PartPruneStepType.PARTPRUNE_OP_MATCHED_PART_KEY) {

            if (targetPartItem.getClauseInfo().getOpKind() == SqlKind.IN) {
                /**
                 * all expr of "part_col in (const1,const2)" has been rewrote as
                 * (part_col = const1) or (part_col = const2)
                 * in PartPredRewriter.rewritePartPredicate,
                 *
                 * only allow part_col in (scalarSubQuery) come here,
                 *
                 * so the ClauseInfo of targetPartItem must be the SubQueryInPartClauseInfo
                 */
                return targetPartItem;
            }
            if (!containPredParent) {
                if (targetPartItem.getClauseInfo().getOpKind() == SqlKind.EQUALS) {
                    singleEqOpItemWithoutParent = true;
                }
            } else {
                if (partPredParent.getKind() == SqlKind.OR) {
                    singleEqOpItemUnderOrItem = true;
                }
            }
        } else if (stepType == PartPruneStepType.PARTPRUNE_COMBINE_INTERSECT) {
            opItemUnderUnderAndItem = true;
        } else if (stepType == PartPruneStepType.PARTPRUNE_COMBINE_UNION) {
            return targetPartItem;
        }

        List<String> partCols = targetPartByDef.getPartitionColumnNameList();
        Map<String, Integer> partCol2PartKeyIdxMapping = new TreeMap<>(CaseInsensitive.CASE_INSENSITIVE_ORDER);
        int[] partColEqConditionStats = new int[partCols.size()];
        for (int i = 0; i < partColEqConditionStats.length; i++) {
            partColEqConditionStats[i] = 0;
            partCol2PartKeyIdxMapping.put(partCols.get(i), i);
        }

        int[] partKeyIdx2RexInputIdxMapping = new int[partCols.size()];
        List<RelDataTypeField> relFlds = relRowType.getFieldList();
        for (int i = 0; i < relFlds.size(); i++) {
            RelDataTypeField fld = relFlds.get(i);
            String fldName = fld.getName();
            Integer partKeyIdx = partCol2PartKeyIdxMapping.get(fldName);
            if (partKeyIdx != null) {
                partKeyIdx2RexInputIdxMapping[partKeyIdx] = i;
            }
        }

        List<PartClauseItem> itemList = new ArrayList<>();
        if (singleEqOpItemWithoutParent) {
            itemList.add(targetPartItem);
        } else if (singleEqOpItemUnderOrItem) {
            itemList.add(targetPartItem);
        } else if (opItemUnderUnderAndItem) {
            itemList.addAll(targetPartItem.getItemList());
        } else {
            return targetPartItem;
        }

        for (int i = 0; i < itemList.size(); i++) {
            PartClauseItem item = itemList.get(i);
            PartPruneStepType itemStepType = item.getType();
            if (itemStepType != PartPruneStepType.PARTPRUNE_OP_MATCHED_PART_KEY) {
                return targetPartItem;
            }
            PartClauseInfo clauseInfo = item.getClauseInfo();
            SqlKind opKind = clauseInfo.getOpKind();
            int partKeyIdx = clauseInfo.getPartKeyIndex();
            if (opKind == SqlKind.EQUALS) {
                partColEqConditionStats[partKeyIdx]++;
            }
        }

        boolean foundPartColEqConds = false;
        for (int i = 0; i < partColEqConditionStats.length; i++) {
            if (partColEqConditionStats[i] > 0) {
                foundPartColEqConds = true;
                break;
            }
        }

        if (foundPartColEqConds) {
            List<PartClauseItem> partColItemsWithAnyValue = new ArrayList<>();
            for (int i = 0; i < partColEqConditionStats.length; i++) {
                int eqCondCount = partColEqConditionStats[i];
                if (eqCondCount == 0) {

                    /**
                     * Auto Fill eq conds for other part cols by use ANY_VALUES
                     */
                    int relInputIdx = partKeyIdx2RexInputIdxMapping[i];
                    PartClauseItem newPartItem =
                        buildPartClauseItemByUsingAnyValueExpr(partByDef, relRowType, relInputIdx, stepContext);
                    partColItemsWithAnyValue.add(newPartItem);
                }
            }

            if (singleEqOpItemWithoutParent || singleEqOpItemUnderOrItem) {
                /**
                 * c1=expr
                 *
                 * ==>
                 *
                 * And
                 *  c1=expr
                 *  c2=any
                 *  c3=any
                 *  ...
                 */
                List<PartClauseItem> allItems = new ArrayList<>();
                allItems.add(targetPartItem);
                allItems.addAll(partColItemsWithAnyValue);
                PartClauseItem andItem =
                    PartClauseItem.buildPartClauseItem(PartPruneStepType.PARTPRUNE_COMBINE_INTERSECT, null, allItems,
                        null);
                return andItem;
            } else if (opItemUnderUnderAndItem) {
                /**
                 * And
                 *  c2=expr1
                 *  c3=expr2
                 *  ...
                 *
                 * ==>
                 *
                 * and
                 *  c1=any
                 *  c2=expr1
                 *  c3=expr2
                 *  ...
                 */
                targetPartItem.getItemList().addAll(partColItemsWithAnyValue);
            } else {
                /**
                 * Do nothing
                 */
            }

        }

        return targetPartItem;

    }
}
