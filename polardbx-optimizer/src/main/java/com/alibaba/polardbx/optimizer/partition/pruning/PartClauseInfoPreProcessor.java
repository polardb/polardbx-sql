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

import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.core.TddlOperatorTable;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.partition.PartitionStrategy;
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

import static org.apache.calcite.sql.SqlKind.GREATER_THAN;
import static org.apache.calcite.sql.SqlKind.IS_NULL;
import static org.apache.calcite.sql.SqlKind.LESS_THAN;

/**
 * @author chenghui.lch
 */
public class PartClauseInfoPreProcessor {

    public static PartClauseItem convertToPartClauseItem(PartitionInfo partInfo,
                                                         RelDataType relRowType,
                                                         RexNode partPred,
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
            clauseItem = convertOrExprToPartClauseItem(partInfo, relRowType, partPredInfo, stepContext);
        } else if (kind == SqlKind.AND) {
            clauseItem = convertAndExprToPartClauseItem(partInfo, relRowType, partPredInfo, stepContext);
        } else {
            clauseItem = convertBoolExprToPartClauseItem(partInfo, relRowType, partPredInfo, stepContext);
        }

        return clauseItem;
    }

    private static PartClauseItem convertOrExprToPartClauseItem(PartitionInfo partInfo,
                                                                RelDataType relRowType,
                                                                RexCall partPred,
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
            PartClauseItem item = convertToPartClauseItem(partInfo, relRowType, op, stepContext);
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
                    misMatchPartClauseItemCnt++;
                    break;
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

    private static PartClauseItem convertAndExprToPartClauseItem(PartitionInfo partInfo,
                                                                 RelDataType relRowType,
                                                                 RexCall partPred,
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
            PartClauseItem item = convertToPartClauseItem(partInfo, relRowType, op, stepContext);
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
        return item;
    }

    private static PartClauseItem convertBoolExprToPartClauseItem(PartitionInfo partInfo,
                                                                  RelDataType relRowType,
                                                                  RexCall partPred,
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
            item = convertComparisonExprToPartClauseItem(partInfo, relRowType, partPred, stepContext);
            break;

        case IS_NULL:
            item = convertIsNullExprToPartClauseItem(partInfo, relRowType, partPred, false, stepContext);
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
            item = convertScalarQueryInExprToPartClauseItem(partInfo, relRowType, partPred, stepContext);

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
        return item;
    }

    private static PartClauseItem convertScalarQueryInExprToPartClauseItem(PartitionInfo partInfo,
                                                                           RelDataType relRowType,
                                                                           RexCall partPred,
                                                                           PartPruneStepBuildingContext stepContext) {

        RexNode input = partPred.getOperands().get(0);
        RexNode subQuery = partPred.getOperands().get(1);
        if (!SubQueryDynamicParamUtils.isNonMaxOneRowScalarSubQueryConstant(subQuery)) {
            return null;
        }

        PartitionStrategy strategy = partInfo.getPartitionBy().getStrategy();
        String predColName = null;
        boolean findInPartNameList = false;
        List<String> relPartNameList = partInfo.getPartitionBy().getPartitionColumnNameList();
        List<ColumnMeta> partKeyFldList = partInfo.getPartitionBy().getPartitionFieldList();
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
        if (!findInPartNameList || partKeyIdx != 0) {
            return null;
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
            matchPartPredToPartKey(partInfo, relRowType, eqExprOfInPred, input, eqValDynamicParam, false,
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
        sbInPartClauseInfo.setPartKeyLevel(PartKeyLevel.PARTITION_KEY);
        sbInPartClauseInfo.setStrategy(strategy);
        sbInPartClauseInfo.setPartKeyIndex(partKeyIdx);
        sbInPartClauseInfo.setPartKeyDataType(cmFldInfo.getField().getRelType());
        sbInPartClauseInfo.setDynamicConstOnly(true);

        PartClauseItem eqExprClauseInfoItem = PartClauseItem
            .buildPartClauseItem(PartPruneStepType.PARTPRUNE_OP_MATCHED_PART_KEY, eqExprClauseInfo,
                null,
                eqExprOfInPred);
        sbInPartClauseInfo.getEqExprClauseItems().add(eqExprClauseInfoItem);
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
    private static PartClauseItem convertComparisonExprToPartClauseItem(PartitionInfo partInfo,
                                                                        RelDataType relRowType,
                                                                        RexCall partPred,
                                                                        PartPruneStepBuildingContext stepContext) {

        if (partPred instanceof RexSubQuery) {
            return null;
        }

        RexNode left = partPred.getOperands().get(0);
        RexNode right = partPred.getOperands().get(1);
        SqlKind kind = partPred.getKind();
        PartitionStrategy strategy = partInfo.getPartitionBy().getStrategy();
        boolean isMultiCols = partInfo.getPartitionBy().getPartitionFieldList().size() > 1;
        SqlOperator operator = partPred.getOperator();
        PartClauseInfo matchedPartClauseInfo =
            matchPartPredToPartKey(partInfo, relRowType, partPred, left, right, false, operator, stepContext);
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
                     * then should treat it as always-true predicates and generate full scan.
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

    private static PartClauseItem convertIsNullExprToPartClauseItem(PartitionInfo partInfo,
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
            matchPartPredToPartKey(partInfo, relRowType, partPred, input, null, true, TddlOperatorTable.EQUALS,
                stepContext);
        if (clauseInfo == null) {
            return null;
        }
        PartClauseItem clauseItem = PartClauseItem
            .buildPartClauseItem(PartPruneStepType.PARTPRUNE_OP_MATCHED_PART_KEY, clauseInfo, null, partPred);
        return clauseItem;
    }

    /**
     * Try to match part predicate to part key and check if the part predicate contains part key
     */
    protected static PartClauseInfo matchPartPredToPartKey(PartitionInfo partInfo,
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

        relPartNameList = partInfo.getPartitionBy().getPartitionColumnNameList();
        List<ColumnMeta> partKeyFldList = partInfo.getPartitionBy().getPartitionFieldList();

        PartitionStrategy strategy = partInfo.getPartitionBy().getStrategy();

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

            clauseInfo.setPartKeyLevel(PartKeyLevel.PARTITION_KEY);
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

}
