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

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.utils.CaseInsensitive;
import com.alibaba.polardbx.optimizer.core.TddlOperatorTable;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.utils.RexUtils;
import org.apache.calcite.plan.RelOptPredicateList;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexSimplify;
import org.apache.calcite.rex.RexSubQuery;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * The preprocessor for partition predicate
 *
 * @author chenghui.lch
 */
public class PartPredRewriter {

    protected static RexNode rewritePartPredicate(PartitionInfo partInfo,
                                                  RelDataType relRowType,
                                                  RexNode partPred,
                                                  PartPruneStepBuildingContext context) {
        RexNode newPartPred = null;
        RexNode finalPartPred = null;
        try {
            newPartPred = rewritePredExpr(context, partInfo, relRowType, partPred);
            if (newPartPred == null) {
                return null;
            }
            RexBuilder rexBuilder = PartitionPrunerUtils.getRexBuilder();
            finalPartPred = RexUtil.toDnf(rexBuilder, newPartPred);
        } catch (TddlRuntimeException e) {
            if (e.getErrorCodeType() == ErrorCode.ERR_TODNF_LIMIT_EXCEED ||
                e.getErrorCodeType() == ErrorCode.ERR_TOCNF_LIMIT_EXCEED) {
                // Maybe the rexnode is too many after toDnf/toCnf
                // Ignore exception
                finalPartPred = newPartPred;
                context.setDnfFormula(false);
            } else {
                throw e;
            }
        }
        return finalPartPred;
    }

    protected static RexNode rewritePredExpr(PartPruneStepBuildingContext context,
                                             PartitionInfo partInfo,
                                             RelDataType relRowType,
                                             RexNode partPred) {
        if (partPred == null) {
            return null;
        }

        if (!(partPred instanceof RexCall)) {

            if (partPred.isAlwaysTrue() || partPred.isAlwaysFalse()) {
                return partPred;
            }

            return null;
        }

        RexCall partPredInfo = (RexCall) partPred;
        RexNode rewritedPred = null;
        SqlKind kind = partPred.getKind();

        if (kind == SqlKind.OR || kind == SqlKind.AND) {
            rewritedPred = rewriteAndOrExpr(context, partInfo, relRowType, partPredInfo);
        } else {
            rewritedPred = rewriteOpExpr(context, partInfo, relRowType, partPredInfo);
        }
        return rewritedPred;
    }

    protected static RexNode rewriteAndOrExpr(PartPruneStepBuildingContext context,
                                              PartitionInfo partInfo,
                                              RelDataType relRowType,
                                              RexCall andOrExpr) {

        List<RexNode> subExprNodes = new ArrayList<>();
        int nonPartKeyPredCnt = 0;
        SqlKind parentExprKind = andOrExpr.getKind();
        for (RexNode op : andOrExpr.getOperands()) {
            RexNode expr = rewritePredExpr(context, partInfo, relRowType, op);
            if (expr == null) {
                nonPartKeyPredCnt++;
                if (andOrExpr.getKind() == SqlKind.OR) {
                    break;
                }
                continue;
            }

            SqlKind subExprKind = expr.getKind();
            if (parentExprKind == subExprKind) {
                // the sqlKind of subExpr is the same as parentExpr
                subExprNodes.addAll(((RexCall) expr).getOperands());
            } else {
                subExprNodes.add(expr);
            }
        }

        if (nonPartKeyPredCnt > 0 && andOrExpr.getKind() == SqlKind.OR) {
            RexBuilder rexBuilder = PartitionPrunerUtils.getRexBuilder();
            subExprNodes.clear();
            RexNode alwaysTruePred = rexBuilder.makeLiteral(true);
            subExprNodes.add(alwaysTruePred);
        }

        if (subExprNodes.size() == 0) {
            return null;
        }
        if (subExprNodes.size() == 1) {
            return subExprNodes.get(0);
        }
        RexCall newOrExpr = andOrExpr.clone(andOrExpr.getType(), subExprNodes);
        return newOrExpr;
    }

    protected static RexNode rewriteOpExpr(PartPruneStepBuildingContext context,
                                           PartitionInfo partInfo,
                                           RelDataType relRowType,
                                           RexNode partPredExpr) {

        if (!(partPredExpr instanceof RexCall)) {
            if (partPredExpr.isAlwaysFalse() || partPredExpr.isAlwaysTrue()) {
                return partPredExpr;
            }
        }

        RexCall partPred = (RexCall) partPredExpr;
        RexNode rewritedExpr = null;
        SqlKind opKind = partPred.getKind();
        switch (opKind) {

        // simple comparsion
        case LESS_THAN:
        case LESS_THAN_OR_EQUAL:
        case EQUALS:
        case NOT_EQUALS:
        case GREATER_THAN_OR_EQUAL:
        case GREATER_THAN:
            rewritedExpr = rewriteComparisonExpr(context, partInfo, relRowType, partPred);
            break;

        case IS_NULL:
            // ignore rewrite
            rewritedExpr = partPred;
            break;

        case BETWEEN:
            rewritedExpr = rewriteBetweenExpr(context, partInfo, relRowType, partPred);
            break;

        case IN:
            rewritedExpr = rewriteInExpr(context, partInfo, relRowType, partPred);
            break;

        default:
            return null;
        }

        if (rewritedExpr == null) {

            return null;
        }

        SqlKind rewritedExprSqlKind = rewritedExpr.getKind();
        if (rewritedExprSqlKind == SqlKind.OR
            || rewritedExprSqlKind == SqlKind.AND) {
            if (((RexCall) rewritedExpr).getOperands().size() == 1) {
                return ((RexCall) rewritedExpr).getOperands().get(0);
            }
        }
        return rewritedExpr;
    }

    protected static RexNode rewriteComparisonExpr(PartPruneStepBuildingContext context,
                                                   PartitionInfo partInfo,
                                                   RelDataType relRowType,
                                                   RexCall partCompPred) {

        if (partCompPred instanceof RexSubQuery) {
            return null;
        }

        RexNode left = partCompPred.getOperands().get(0);
        RexNode right = partCompPred.getOperands().get(1);

        if (left.getKind() == SqlKind.ROW && right.getKind() == SqlKind.ROW) {
            return rewriteRowCompareExpr(context, partInfo, relRowType, partCompPred);
        }

        RexBuilder rexBuilder = PartitionPrunerUtils.getRexBuilder();
        RexNode partPred = partCompPred;
        if (RexUtils.allLiteral(partCompPred.getOperands())) {
            final RexSimplify simplify = new RexSimplify(rexBuilder, RelOptPredicateList.EMPTY, true, RexUtil.EXECUTOR);
            RexNode simplifiedPartPred = simplify.simplify(partCompPred);

            if (!(simplifiedPartPred instanceof RexCall)) {
                if (simplifiedPartPred.isAlwaysTrue() || simplifiedPartPred.isAlwaysFalse()) {
                    /**
                     * Only accept the simplified result "AlwaysTrue" from RexSimplify
                     */
                    return simplifiedPartPred;
                } else {
                    /**
                     * Not support to pruning non-rexCall expr, treat it as always-true expr and return
                     */
                    return rexBuilder.makeLiteral(true);
                }
            } else {
                partPred = simplifiedPartPred;
            }
        }

        RexCall newPartPred = formatInputAndConst(context, partPred);
        if (newPartPred == null) {
            return null;
        }

        RexInputRef inputRef = (RexInputRef) newPartPred.getOperands().get(0);
        RexNode constExpr = newPartPred.getOperands().get(1);
        SqlOperator op = newPartPred.getOperator();
        SqlKind opKind = op.getKind();

        boolean isPartCol = tryMatchInputToPartCol(context, relRowType, inputRef);
        if (!isPartCol) {
            return null;
        }
        boolean isSingleCol = partInfo.getPartitionBy().getPartitionColumnNameList().size() == 1;

        switch (opKind) {
        case LESS_THAN:
        case GREATER_THAN:
        case EQUALS:
        case NOT_EQUALS: {
            return newPartPred;
        }

        case LESS_THAN_OR_EQUAL:
        case GREATER_THAN_OR_EQUAL: {
            if (isSingleCol) {
                return newPartPred;
            } else {
                // <= -->  < or =
                // >= -->  > or =
                SqlOperator nonEqualOp = opKind == SqlKind.LESS_THAN_OR_EQUAL ? TddlOperatorTable.LESS_THAN :
                    TddlOperatorTable.GREATER_THAN;
                RexNode newExpr = convertSemiClosedCmpExprToOrExpr(inputRef, constExpr, rexBuilder, nonEqualOp);
                return newExpr;
            }
        }
        default:
            break;
        }

        return null;
    }

    /**
     * <pre>
     *     In multi-partition-columns partFldAccessType, all compKind of "<=" and ">=" should do the following transform:
     *      (pk1 is the first part col, pk2 is the second part col)
     *
     *      pk1 <= a and pk2<b
     *     ==>
     *      ((pk1<a) or (pk1=a)) and pk2<b
     *
     *     ,
     *
     *     because during prefix predicate enumeration, the compKind of "<=" ( or ">=" ) need to split into "<" & "="
     *     (or  ">" & "=" ),
     *     for example:
     *
     *     AND
     *      pk1<=a
     *      pk2<b
     *     =>
     *     AND
     *      (pk1<a and pk1=a)
     *      pk2<b
     *     =>
     *     OR
     *      pk1<a and pk2<b ( as pk1 is the first col and pk1<a is not eq cmpKind, so the pk2<b can be ignored in the and expr  )
     *      pk1=a and pk2<b
     *     =>
     *     OR (enum prefix predicates)
     *      pk1<a
     *      pk1=a and pk2<b
     *     .
     *
     *     If the transform above is NOT done, then will result in a wrong result:
     *     for example(wrong result):
     *
     *     AND
     *      pk1<=a
     *      pk2<b
     *     =>
     *     AND  (enum prefix predicates)
     *      pk1<=a
     *      pk1=a AND pk2<b
     *     ,
     *     then the reulst is wrong because the interval pk1<a has been miss !!
     *
     * </pre>
     */
    private static RexNode convertSemiClosedCmpExprToOrExpr(RexInputRef inputRef, RexNode constExpr,
                                                            RexBuilder rexBuilder,
                                                            SqlOperator nonEqualOp) {
        RexNode newExpr;
        List<RexNode> eqOpList = new ArrayList<>();
        eqOpList.add(inputRef);
        eqOpList.add(constExpr);
        RexNode eqExpr = rexBuilder.makeCall(TddlOperatorTable.EQUALS, eqOpList);

        List<RexNode> neqOpList = new ArrayList<>();
        neqOpList.add(inputRef);
        neqOpList.add(constExpr);
        RexNode neqExpr = rexBuilder.makeCall(nonEqualOp, neqOpList);

        List<RexNode> orOpList = new ArrayList<>();
        orOpList.add(neqExpr);
        orOpList.add(eqExpr);
        RexNode orExpr = rexBuilder.makeCall(TddlOperatorTable.OR, orOpList);
        newExpr = orExpr;
        return newExpr;
    }

    protected static RexNode rewriteBetweenExpr(PartPruneStepBuildingContext context,
                                                PartitionInfo partInfo,
                                                RelDataType relRowType,
                                                RexCall partPred) {

        RexBuilder rexBuilder = PartitionPrunerUtils.getRexBuilder();
        RexNode column = partPred.getOperands().get(0);
        RexNode left = partPred.getOperands().get(1);
        RexNode right = partPred.getOperands().get(2);

        if (column.getKind() == SqlKind.ROW && left.getKind() == SqlKind.ROW && right.getKind() == SqlKind.ROW) {
            throw new TddlRuntimeException(ErrorCode.ERR_OPTIMIZER, "Between's Operand should contain 1 column(s)");
        }

        if (!(column instanceof RexInputRef)) {
            /**
             * Such as the following cases are not support to perform pruning:
             *  2 between col1 and col2,
             * （col1 + col2) between 1 and 2
             *  ...
             */
            return null;
        }

        RexInputRef input = (RexInputRef) column;
        boolean isPartCol = tryMatchInputToPartCol(context, relRowType, input);
        if (!isPartCol) {
            return null;
        }

        boolean isSingleCol = partInfo.getPartitionBy().getPartitionColumnNameList().size() == 1;

        //-----left-----
        RexNode geExpr = null;
        if (isSingleCol) {
            List<RexNode> gtOpList = new ArrayList<>();
            gtOpList.add(input);
            gtOpList.add(left);
            geExpr = rexBuilder.makeCall(TddlOperatorTable.GREATER_THAN_OR_EQUAL, gtOpList);
        } else {
            geExpr = convertSemiClosedCmpExprToOrExpr(input, left, rexBuilder, TddlOperatorTable.GREATER_THAN);
        }

        //-----right-----
        RexNode leExpr = null;
        if (isSingleCol) {
            List<RexNode> ltOpList = new ArrayList<>();
            ltOpList.add(input);
            ltOpList.add(right);
            leExpr = rexBuilder.makeCall(TddlOperatorTable.LESS_THAN_OR_EQUAL, ltOpList);
        } else {
            leExpr = convertSemiClosedCmpExprToOrExpr(input, right, rexBuilder, TddlOperatorTable.LESS_THAN);
        }

        List<RexNode> btOpExpr = new ArrayList<>();
        btOpExpr.add(leExpr);
        btOpExpr.add(geExpr);
        RexNode btExpr = rexBuilder.makeCall(TddlOperatorTable.AND, btOpExpr);

        return btExpr;
    }

    protected static RexNode rewriteRowCompareExpr(PartPruneStepBuildingContext context,
                                                   PartitionInfo partInfo,
                                                   RelDataType relRowType,
                                                   RexCall partPred) {

        RexCall newPartPred = formatInputAndConstForRowExpr(context, partPred);
        if (newPartPred == null) {
            /**
             * Maybe the partPred of ROW is NOT support to perform pruning
             */
            return newPartPred;
        }

        RexCall inputItem = (RexCall) newPartPred.getOperands().get(0);
        RexCall valueItem = (RexCall) newPartPred.getOperands().get(1);
        SqlOperator predOp = newPartPred.getOperator();
        SqlKind predOpKind = predOp.getKind();

        switch (predOpKind) {
        case LESS_THAN:
        case GREATER_THAN: {
            return rewriteRowLtGtExpr(context, partInfo, relRowType, inputItem, predOpKind, valueItem);
        }
        case LESS_THAN_OR_EQUAL:
        case GREATER_THAN_OR_EQUAL: {
            return rewriteRowLeGeExpr(context, partInfo, relRowType, inputItem, predOpKind, valueItem);
        }
        case EQUALS: {
            return rewriteRowEqExpr(context, partInfo, relRowType, inputItem, predOpKind, valueItem);
        }
        case NOT_EQUALS: {
            return rewriteRowNotEqExpr(context, partInfo, relRowType, inputItem, predOpKind, valueItem);
        }

        default:
            break;
        }

        return null;
    }

    protected static RexNode rewriteRowBetweenExpr(PartPruneStepBuildingContext context,
                                                   PartitionInfo partInfo,
                                                   RelDataType relRowType,
                                                   RexCall partPred) {

        RexNode column = partPred.getOperands().get(0);
        RexNode left = partPred.getOperands().get(1);
        RexNode right = partPred.getOperands().get(2);
        RexBuilder rexBuilder = PartitionPrunerUtils.getRexBuilder();

        /**
         * Build a tmp predicate and try to formulate predicate
         */
        List<RexNode> geOpList = new ArrayList<>();
        geOpList.add(column);
        geOpList.add(left);
        RexNode geExpr = rexBuilder.makeCall(TddlOperatorTable.GREATER_THAN_OR_EQUAL, geOpList);
        RexCall newGeExpr = formatInputAndConstForRowExpr(context, geExpr);
        if (newGeExpr == null) {
            /**
             * Not support to perform pruning for the following case :
             *  (c1,c2,const4) between (const1, const2, const3) and  (const6, const7, const8),
             *  (c1,c3 + c2,const4) between (const1, const2, const3) and  (const6, const7, const8)
             *  ....
             */
            return null;
        }

        List<RexNode> leOpList = new ArrayList<>();
        leOpList.add(column);
        leOpList.add(right);
        RexNode leExpr = rexBuilder.makeCall(TddlOperatorTable.LESS_THAN_OR_EQUAL, leOpList);
        RexCall newLeExpr = formatInputAndConstForRowExpr(context, leExpr);
        if (newLeExpr == null) {
            /**
             * Not support to perform pruning for the following case :
             *  (c1,c2,const4) between (const1, const2, const3) and  (const6, const7, const8),
             *  (c1,c3 + c2,const4) between (const1, const2, const3) and  (const6, const7, const8)
             *  ....
             */
            return null;
        }

        List<RexNode> allExprList = new ArrayList<>();
        RexNode newLeftExpr =
            rewriteRowLeGeExpr(context, partInfo, relRowType, column, TddlOperatorTable.GREATER_THAN_OR_EQUAL.getKind(),
                left);
        if (newLeftExpr != null) {
            allExprList.add(newLeftExpr);
        } else {
            /**
             * Maybe left expr contains not any partition columns, so treat it as always-true expr and ignore
             */
        }

        RexNode newRightExpr =
            rewriteRowLeGeExpr(context, partInfo, relRowType, column, TddlOperatorTable.LESS_THAN_OR_EQUAL.getKind(),
                right);
        if (newRightExpr != null) {
            allExprList.add(newRightExpr);
        } else {
            /**
             * Maybe left expr contains not any partition columns, so treat it as always-true expr and ignore
             */
        }

        if (allExprList.size() == 0) {
            return null;
        } else if (allExprList.size() == 1) {
            return allExprList.get(0);
        } else {
            RexNode orExpr = rexBuilder.makeCall(TddlOperatorTable.AND, allExprList);
            return orExpr;
        }

    }

    /**
     * Format the predicate to form: : col op const
     * <p>
     * e.g. convert 3 < col ->  col > 3
     */
    protected static RexCall formatInputAndConst(PartPruneStepBuildingContext context, RexNode predNode) {

        if (!(predNode instanceof RexCall)) {
            return null;
        }

        RexCall predCall = (RexCall) predNode;
        RexNode left = predCall.getOperands().get(0);
        RexNode right = predCall.getOperands().get(1);
        SqlOperator op = predCall.getOperator();

        boolean isRightIsNull = right == null;
        SqlOperator predOp = null;
        RexNode input = null;
        RexNode constExpr = null;

        if (!isRightIsNull) {
            if (isInputRef(left, context) && isConstant(right, context)) {
                input = left;
                constExpr = right;
                predOp = op;
            } else if (isConstant(left, context) && isInputRef(left, context)) {
                input = right;
                constExpr = left;
                predOp = inverseOp(op);
            } else {
                // The expr is not support to doing partition pruning
                return null;
            }
        } else {
            if (isInputRef(left, context)) {
                input = left;
                constExpr = right;
                predOp = op;
            } else {
                // The expr is not support to doing partition pruning
                return null;
            }
        }

        // Only support the dynamic pruning of expr with RexInputRef
        if (!(input instanceof RexInputRef)) {
            return null;
        }

        RexBuilder rexBuilder = PartitionPrunerUtils.getRexBuilder();
        List<RexNode> opList = new ArrayList<>();
        opList.add(input);
        opList.add(constExpr);
        RexNode newPred = rexBuilder.makeCall(predOp, opList);
        return (RexCall) newPred;
    }

    protected static RexCall formatInputAndConstForRowExpr(PartPruneStepBuildingContext context, RexNode predNode) {

        if (!(predNode instanceof RexCall)) {
            return null;
        }

        RexCall predCall = (RexCall) predNode;
        RexNode left = predCall.getOperands().get(0);
        RexNode right = predCall.getOperands().get(1);

        assert left.getKind() == SqlKind.ROW;
        assert right.getKind() == SqlKind.ROW;

        RexCall leftExpr = (RexCall) left;
        RexCall rightExpr = (RexCall) right;

        assert leftExpr.getOperands().size() == ((RexCall) right).getOperands().size();

        int opCnt = leftExpr.getOperands().size();
        RexCall inputItem;
        RexCall valueItem;
        SqlOperator predOp = predCall.getOperator();

        boolean allInputInLeft = true;
        for (int i = 0; i < opCnt; i++) {
            RexNode inputNode = leftExpr.getOperands().get(i);
            RexNode valueNode = rightExpr.getOperands().get(i);
            if (!(isInputRef(inputNode, context) && isConstant(valueNode, context))) {
                allInputInLeft = false;
                break;
            }
        }

        if (!allInputInLeft) {

            /**
             * Maybe inputs are all in right expr
             */
            boolean allInputInRight = true;
            for (int i = 0; i < opCnt; i++) {
                RexNode inputNode = rightExpr.getOperands().get(i);
                RexNode valueNode = leftExpr.getOperands().get(i);
                if (!(isInputRef(inputNode, context) && isConstant(valueNode, context))) {
                    allInputInRight = false;
                    break;
                }
            }

            if (!allInputInRight) {
                /**
                 *  Not support to perform pruning for the following case:
                 *
                 *   case1 : ROW(col1,col2+col3, col4) < ROW(1,2,3),
                 *   case2 : ROW(col1,col2+col3, const1) > ROW(col3,2,col4),
                 *
                 *   ....
                 */
                return null;
            } else {
                inputItem = rightExpr;
                valueItem = leftExpr;
                SqlOperator op = predCall.getOperator();
                predOp = inverseOp(op);
            }
        } else {
            inputItem = leftExpr;
            valueItem = rightExpr;
        }

        RexBuilder rexBuilder = PartitionPrunerUtils.getRexBuilder();
        List<RexNode> opList = new ArrayList<>();
        opList.add(inputItem);
        opList.add(valueItem);
        RexNode newPred = rexBuilder.makeCall(predOp, opList);
        return (RexCall) newPred;
    }

    protected static SqlOperator inverseOp(SqlOperator op) {
        switch (op.getKind()) {
        case LESS_THAN:
            return TddlOperatorTable.GREATER_THAN;
        case LESS_THAN_OR_EQUAL:
            return TddlOperatorTable.GREATER_THAN_OR_EQUAL;
        case GREATER_THAN:
            return TddlOperatorTable.LESS_THAN;
        case GREATER_THAN_OR_EQUAL:
            return TddlOperatorTable.LESS_THAN_OR_EQUAL;
        default:
            return op;
        }
    }

    protected static boolean tryMatchInputToPartCol(PartPruneStepBuildingContext context, RelDataType relDataType,
                                                    RexInputRef inputRef) {
        RelDataTypeField predColRelFld = relDataType.getFieldList().get(inputRef.getIndex());
        String predColName = predColRelFld.getName();
        boolean isPartCol = context.getPartColIdxMap().containsKey(predColName);
        return isPartCol;
    }

    /**
     * Handle row equal expr : (c1,c2,...,cn) = (v1,v2,...,vn) ,
     * <p>
     * convert (c1,c2,...,cn) = (v1,v2,...,vn)  to c1=v1 and c2=v2 and ... and cn=vn
     */
    protected static RexNode rewriteRowEqExpr(PartPruneStepBuildingContext context,
                                              PartitionInfo partInfo,
                                              RelDataType relRowType,
                                              RexNode predInput,
                                              SqlKind opKind,
                                              RexNode predValue) {

        RexCall rowInput = (RexCall) predInput;
        RexCall rowValue = (RexCall) predValue;

        List<RexNode> inputCols = rowInput.getOperands();
        int inputColCnt = inputCols.size();

        assert rowInput.getKind() == SqlKind.ROW;
        assert rowValue.getKind() == SqlKind.ROW;
        assert inputColCnt == rowValue.getOperands().size();
        assert opKind == SqlKind.EQUALS;

        RexBuilder rexBuilder = PartitionPrunerUtils.getRexBuilder();
        List<RexNode> singleColEqExprList = new ArrayList<>();
        for (int i = 0; i < inputCols.size(); i++) {
            RexNode inputNode = inputCols.get(i);
            RexNode valueNode = rowValue.getOperands().get(i);
            if (inputNode instanceof RexInputRef) {
                boolean isPartCol =
                    PartPredRewriter.tryMatchInputToPartCol(context, relRowType, (RexInputRef) inputNode);
                List<RexNode> eqOpList = new ArrayList<>();
                if (isPartCol) {
                    eqOpList.add(inputNode);
                    eqOpList.add(valueNode);
                    RexNode eqExpr = rexBuilder.makeCall(TddlOperatorTable.EQUALS, eqOpList);
                    singleColEqExprList.add(eqExpr);
                } else {
                    /**
                     * input Col is not a partition col, so
                     *
                     * treat the predicate as alwaysTrue and ignore
                     */
                }
            } else {
                /**
                 * treat the predicate as alwaysTrue and ignore
                 */
                continue;
            }
        }

        if (singleColEqExprList.size() == 0) {
            return null;
        } else if (singleColEqExprList.size() == 1) {
            return singleColEqExprList.get(0);
        } else {
            RexNode andExpr = rexBuilder.makeCall(TddlOperatorTable.AND, singleColEqExprList);
            return andExpr;
        }
    }

    protected static RexNode buildAlwaysTrueExpr() {
        RexBuilder rexBuilder = PartitionPrunerUtils.getRexBuilder();
        RexNode alwaysTruePred = rexBuilder.makeLiteral(true);
        return alwaysTruePred;
    }

    protected static RexNode buildAlwaysFalseExpr() {
        RexBuilder rexBuilder = PartitionPrunerUtils.getRexBuilder();
        RexNode alwaysFalsePred = rexBuilder.makeLiteral(false);
        return alwaysFalsePred;
    }

    /**
     * Handle expr (c1,c2,...,cn) op (v1,v2,...,vn), op : < or >
     */
    protected static RexNode rewriteRowLtGtExpr(PartPruneStepBuildingContext context,
                                                PartitionInfo partInfo,
                                                RelDataType relRowType,
                                                RexNode predInput,
                                                SqlKind opKind,
                                                RexNode predValue) {

        assert predInput.getKind() == SqlKind.ROW;
        assert predValue.getKind() == SqlKind.ROW;

        RexCall inputItem = (RexCall) predInput;
        RexCall valueItem = (RexCall) predValue;

        List<RexNode> inputs = inputItem.getOperands();
        List<RexNode> values = valueItem.getOperands();

        boolean containInputOnlyInLeft = true;
        for (int i = 0; i < inputs.size(); i++) {
            RexNode inputNode = inputs.get(i);
            if (!(inputNode instanceof RexInputRef)) {
                containInputOnlyInLeft = false;
                break;
            }
        }
        if (!containInputOnlyInLeft) {
            /**
             *  Handle the case: ROW(col1,col2+col3, col4) in ROW(ROW(1,2,3),ROW(4,5,6),....)
             */
            return null;
        }

        boolean containPartColInLeft = false;
        List<Boolean> partColFlags = new ArrayList<>();
        for (int i = 0; i < inputs.size(); i++) {
            RexInputRef inputNode = (RexInputRef) inputs.get(i);
            boolean isPartCol = PartPredRewriter.tryMatchInputToPartCol(context, relRowType, inputNode);
            partColFlags.add(isPartCol);
            if (isPartCol) {
                containPartColInLeft = true;
            }
        }

        if (!containPartColInLeft) {
            /**
             * Not found any partition column in left
             */
            return null;
        }

        if (inputs.size() > 3) {
            /**
             * Not support pruning for row(c1,c2,...,cn) cmp (v1,v2,...,vn) with col count is more than 3
             */
            return null;
        }

        /**
         *
         * Convert row(c1,c2,c3) < (v1,v2,v3) to
         * =>
         *          c1 < v1
         *     or
         *          c1 = v1 and c2 < v2
         *     or
         *          c1 = v1 and c2 = v2 or c3 < v3
         */
        List<RexNode> allExprList = new ArrayList<>();
        RexBuilder rexBuilder = PartitionPrunerUtils.getRexBuilder();
        for (int i = 0; i < inputItem.getOperands().size(); i++) {

            /**
             * Build eq expr list for all the col before the index i
             */
            List<RexNode> tmpExprList = new ArrayList<>();
            for (int j = 0; j < i; j++) {
                RexInputRef inputNode = (RexInputRef) inputs.get(j);
                RexNode valueNode = values.get(j);
                boolean isPartCol = partColFlags.get(j);
                if (isPartCol) {
                    List<RexNode> eqOpList = new ArrayList<>();
                    eqOpList.add(inputNode);
                    eqOpList.add(valueNode);
                    RexNode eqExpr = rexBuilder.makeCall(TddlOperatorTable.EQUALS, eqOpList);
                    tmpExprList.add(eqExpr);
                } else {
                    /**
                     *  inputNode is not a partition column, so treat it as always true and ignore it
                     */
                }
            }
            boolean isPartCol = partColFlags.get(i);
            RexInputRef inputNode = (RexInputRef) inputs.get(i);
            RexNode valueNode = values.get(i);
            if (isPartCol) {
                List<RexNode> opList = new ArrayList<>();
                opList.add(inputNode);
                opList.add(valueNode);
                SqlOperator op =
                    opKind == SqlKind.LESS_THAN ? TddlOperatorTable.LESS_THAN : TddlOperatorTable.GREATER_THAN;
                RexNode cmpExpr = rexBuilder.makeCall(op, opList);
                tmpExprList.add(cmpExpr);
            }

            if (tmpExprList.size() == 0) {
                /**
                 * Not contain any partition columns predicate
                 */
                RexNode alwaysTrueExpr = buildAlwaysTrueExpr();
                allExprList.add(alwaysTrueExpr);
            } else if (tmpExprList.size() == 1) {
                allExprList.add(tmpExprList.get(0));
            } else {
                RexNode andExpr = rexBuilder.makeCall(TddlOperatorTable.AND, tmpExprList);
                allExprList.add(andExpr);
            }
        }

        if (allExprList.size() == 0) {
            return null;
        } else if (allExprList.size() == 1) {
            return allExprList.get(0);
        } else {
            RexNode orExpr = rexBuilder.makeCall(TddlOperatorTable.OR, allExprList);
            return orExpr;
        }
    }

    protected static RexNode rewriteRowLeGeExpr(PartPruneStepBuildingContext context,
                                                PartitionInfo partInfo,
                                                RelDataType relRowType,
                                                RexNode predInput,
                                                SqlKind opKind,
                                                RexNode predValue) {

        assert predInput.getKind() == SqlKind.ROW;
        assert predValue.getKind() == SqlKind.ROW;

        List<RexNode> allExprList = new ArrayList<>();

        SqlKind neqKind = opKind == SqlKind.LESS_THAN_OR_EQUAL ? SqlKind.LESS_THAN : SqlKind.GREATER_THAN;
        SqlKind eqKind = SqlKind.EQUALS;

        RexNode neqExpr = rewriteRowLtGtExpr(context, partInfo, relRowType, predInput, neqKind, predValue);
        if (neqExpr != null) {
            allExprList.add(neqExpr);
        } else {
            /**
             * Maybe no find any partition column in prediate, treat is always-true predicate
             */
            RexNode alwaysTrueExpr = buildAlwaysTrueExpr();
            allExprList.add(alwaysTrueExpr);
        }

        RexNode eqExpr = rewriteRowEqExpr(context, partInfo, relRowType, predInput, eqKind, predValue);
        if (eqExpr != null) {
            allExprList.add(eqExpr);
        } else {
            /**
             * Maybe no find any partition column in predicate, treat is always-true predicate
             */
            RexNode alwaysTrueExpr = buildAlwaysTrueExpr();
            allExprList.add(alwaysTrueExpr);
        }

        if (allExprList.size() == 0) {
            return null;
        } else if (allExprList.size() == 1) {
            return allExprList.get(0);
        } else {
            RexBuilder rexBuilder = PartitionPrunerUtils.getRexBuilder();
            RexNode orExpr = rexBuilder.makeCall(TddlOperatorTable.OR, allExprList);
            return orExpr;
        }
    }

    protected static RexNode rewriteRowNotEqExpr(PartPruneStepBuildingContext context,
                                                 PartitionInfo partInfo,
                                                 RelDataType relRowType,
                                                 RexNode predInput,
                                                 SqlKind opKind,
                                                 RexNode predValue) {

        List<RexNode> allExprList = new ArrayList<>();

        RexNode ltExpr = rewriteRowLtGtExpr(context, partInfo, relRowType, predInput, SqlKind.LESS_THAN, predValue);
        if (ltExpr != null) {
            allExprList.add(ltExpr);
        } else {
            /**
             * Maybe no find any partition column in prediate, treat is always-true predicate
             */
            RexNode alwaysTrueExpr = buildAlwaysTrueExpr();
            allExprList.add(alwaysTrueExpr);
        }
        RexNode gtExpr = rewriteRowLtGtExpr(context, partInfo, relRowType, predInput, SqlKind.GREATER_THAN, predValue);
        if (gtExpr != null) {
            allExprList.add(gtExpr);
        } else {
            /**
             * Maybe no find any partition column in prediate, treat is always-true predicate
             */
            RexNode alwaysTrueExpr = buildAlwaysTrueExpr();
            allExprList.add(alwaysTrueExpr);
        }

        if (allExprList.size() == 0) {
            return null;
        } else if (allExprList.size() == 1) {
            return allExprList.get(0);
        } else {
            RexBuilder rexBuilder = PartitionPrunerUtils.getRexBuilder();
            RexNode orExpr = rexBuilder.makeCall(TddlOperatorTable.OR, allExprList);
            return orExpr;
        }
    }

    /**
     * Handle row in expr : (c1,c2,...,cn) in ( (y1,y2,...,yn) , (x1,x2,...,xn) ),
     */
    protected static RexNode rewriteRowInExpr(PartPruneStepBuildingContext context,
                                              PartitionInfo partInfo,
                                              RelDataType relRowType,
                                              RexCall partPred) {
        List<RexNode> operands = partPred.getOperands();
        RexNode left = operands.get(0);
        RexNode right = operands.get(1);
        RexCall leftItem = (RexCall) left;
        RexCall rightItem = (RexCall) right;
        assert partPred.getKind() == SqlKind.IN;
        assert leftItem.getKind() == SqlKind.ROW;
        assert rightItem.getKind() == SqlKind.ROW;

        List<RexNode> inValList = null;

        boolean containInputOnlyInLeft = true;
        for (int i = 0; i < leftItem.getOperands().size(); i++) {
            RexNode inputNode = leftItem.getOperands().get(i);
            if (!(isInputRef(inputNode, context))) {
                containInputOnlyInLeft = false;
                break;
            }
        }

        if (!containInputOnlyInLeft) {
            /**
             *  Not support to perform prunint for  the case:
             *  ROW(col1,col2+col3, col4) in ROW(ROW(1,2,3),ROW(4,5,6),....)
             */
            return null;
        }

        boolean containPartColInLeft = false;
        for (int i = 0; i < leftItem.getOperands().size(); i++) {
            RexInputRef inputNode = (RexInputRef) leftItem.getOperands().get(i);
            boolean isPartCol = PartPredRewriter.tryMatchInputToPartCol(context, relRowType, inputNode);
            if (isPartCol) {
                containPartColInLeft = true;
                break;
            }
        }

        if (!containPartColInLeft) {
            /**
             * Not found any partition column in left
             */
            return null;
        }

        boolean containConstExprOnlyInRight = true;
        for (int i = 0; i < rightItem.getOperands().size(); i++) {
            RexNode valueNode = rightItem.getOperands().get(i);
            if (valueNode.getKind() != SqlKind.ROW) {
                containConstExprOnlyInRight = false;
                break;
            }
            List<RexNode> constNodes = ((RexCall) valueNode).getOperands();
            for (int j = 0; j < constNodes.size(); j++) {
                if (!(isConstant(constNodes.get(j), context))) {
                    containConstExprOnlyInRight = false;
                    break;
                }
            }
        }
        if (!containConstExprOnlyInRight) {
            /**
             *  Not support to perform prunint for  the case:
             *  col1,col2, col4 in ( (1,3,4),(5,7,col2), (8,9,col3) )
             *  ...
             */
            return null;
        }

        inValList = rightItem.getOperands();
        List<RexNode> allExprList = new ArrayList<>();
        for (int i = 0; i < inValList.size(); i++) {
            RexNode oneVal = inValList.get(i);
            RexNode expr = rewriteRowEqExpr(context, partInfo, relRowType, leftItem, SqlKind.EQUALS, oneVal);
            if (expr != null) {
                allExprList.add(expr);
            } else {
                /**
                 * Maybe no find any partition column in prediate, treat is always-true predicate
                 */
                RexNode alwaysTrueExpr = buildAlwaysTrueExpr();
                allExprList.add(alwaysTrueExpr);
            }
        }

        if (allExprList.size() == 0) {
            return null;
        } else if (allExprList.size() == 1) {
            return allExprList.get(0);
        } else {
            RexBuilder rexBuilder = PartitionPrunerUtils.getRexBuilder();
            RexNode orExpr = rexBuilder.makeCall(TddlOperatorTable.OR, allExprList);
            return orExpr;
        }
    }

    protected static RexNode rewriteInExpr(PartPruneStepBuildingContext context,
                                           PartitionInfo partInfo,
                                           RelDataType relRowType,
                                           RexCall partPred) {

        RexBuilder rexBuilder = PartitionPrunerUtils.getRexBuilder();
        List<RexNode> operands = partPred.getOperands();
        RexNode left = operands.get(0);
        RexNode right = operands.get(1);

        Map<String, Integer> partColIdxMap = new TreeMap<>(CaseInsensitive.CASE_INSENSITIVE_ORDER);
        List<String> colList = partInfo.getPartitionBy().getPartitionColumnNameList();
        for (int i = 0; i < colList.size(); i++) {
            partColIdxMap.put(colList.get(i), i);
        }

        List<RexNode> inValueList = new ArrayList<>();
        RexNode inputSide = null;
        if (left.getKind() == SqlKind.ROW && right.getKind() == SqlKind.ROW) {
            return rewriteRowInExpr(context, partInfo, relRowType, partPred);
        } else if (right.getKind() == SqlKind.ROW) {

            /**
             *  Try to handle : col in ROW(1,2,3,...)
             */

            if (left instanceof RexInputRef) {
                /**
                 *  Try to handle : id in (1,2,3,...)
                 */
                inputSide = left;
                inValueList = ((RexCall) right).getOperands();
            } else {
                /**
                 *  Try to handle the case : 1 in (id)
                 */
                List<RexNode> opList = ((RexCall) right).getOperands();
                if (opList.size() == 1 && opList.get(0) instanceof RexInputRef) {
                    // handle the case  "2 in (id)"
                    inputSide = opList.get(0);
                    inValueList.add(left);
                } else {
                    /**
                     * but for the case "2 in (id, 1,3,4)", Not supported to do partition pruning
                     */
                    return null;
                }
            }

            RexInputRef input = (RexInputRef) inputSide;
            boolean isPartCol = PartPredRewriter.tryMatchInputToPartCol(context, relRowType, input);
            if (!isPartCol) {
                /**
                 * No find any partition col in the in predicate
                 */
                return null;
            }

        } else {
            // should not be here
            return null;
        }

        if (inValueList.size() <= 0) {
            // should not be here
            return null;
        }

        List<RexNode> allExprList = new ArrayList<>();
        for (int i = 0; i < inValueList.size(); i++) {
            /**
             * Handle pk in (v1, v2, ..., vn) and
             * => ( (pk=v1) or (pk=v2) or ... or (pk=vn) )
             */
            RexNode inValExpr = inValueList.get(i);
            List<RexNode> eqOpList = new ArrayList<>();
            eqOpList.add(inputSide);
            eqOpList.add(inValExpr);
            RexNode eqExpr = rexBuilder.makeCall(TddlOperatorTable.EQUALS, eqOpList);
            allExprList.add(eqExpr);
        }

        if (allExprList.size() == 0) {
            return null;
        } else if (allExprList.size() == 1) {
            return allExprList.get(0);
        } else {
            RexNode orExpr = rexBuilder.makeCall(TddlOperatorTable.OR, allExprList);
            return orExpr;
        }
    }

    private static boolean isInputRef(RexNode rexNode, PartPruneStepBuildingContext context) {
        return RexUtil.isReferenceOrAccess(rexNode, false);
    }

    private static boolean isConstant(RexNode rexNode, PartPruneStepBuildingContext context) {
        if (RexUtil.isLiteral(rexNode, true)) {
            return true;
        }

        if (rexNode instanceof RexDynamicParam) {
            if (((RexDynamicParam) rexNode).getIndex() >= 0) {
                return true;
            } else {
                return false;
            }
        }

        if (context.isEnableConstExpr()) {
            return RexUtil.isConstant(rexNode);
        }

        return false;
    }
}
