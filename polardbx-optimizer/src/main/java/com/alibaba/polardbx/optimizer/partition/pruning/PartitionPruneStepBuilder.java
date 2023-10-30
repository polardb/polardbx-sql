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

import com.alibaba.polardbx.common.exception.NotSupportException;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.utils.TreeMaps;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.common.utils.time.calculator.MySQLIntervalType;
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.TddlOperatorTable;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypeUtil;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.field.TypeConversionStatus;
import com.alibaba.polardbx.optimizer.partition.PartitionByDefinition;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.partition.PartitionInfoManager;
import com.alibaba.polardbx.optimizer.partition.common.PartKeyLevel;
import com.alibaba.polardbx.optimizer.partition.common.PartitionTableType;
import com.alibaba.polardbx.optimizer.partition.datatype.PartitionField;
import com.alibaba.polardbx.optimizer.partition.datatype.iterator.PartitionFieldIterator;
import com.alibaba.polardbx.optimizer.partition.datatype.iterator.PartitionFieldIterators;
import com.alibaba.polardbx.optimizer.utils.ExprContextProvider;
import org.apache.calcite.avatica.util.ByteString;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static com.alibaba.polardbx.common.utils.time.calculator.MySQLIntervalType.*;

/**
 * @author chenghui.lch
 */
public class PartitionPruneStepBuilder {

    private static final Logger log = LoggerFactory.getLogger(PartitionPruneStepBuilder.class);

    protected static final long DEFAULT_MAX_ENUMERABLE_INTERVAL_LENGTH = 32;
    protected static final long DEFAULT_MAX_IN_SUBQUERY_PRUNING_SIZE = 8192L;

    /**
     * generate full scan all physical partitions step info
     */
    public static PartitionPruneStep genFullScanAllPhyPartsStepInfoByDbNameAndTbName(String dbName,
                                                                                     String logTbName,
                                                                                     ExecutionContext executionContext) {
        PartitionInfoManager partitionInfoManager =
            executionContext.getSchemaManager(dbName).getTddlRuleManager().getPartitionInfoManager();
        PartitionInfo partInfo = partitionInfoManager.getPartitionInfo(logTbName);
        if (partInfo == null) {
            log.warn(String.format("db-%s logTbName %s %s", dbName, logTbName,
                partitionInfoManager.getPartInfoCtxCache().size()));
        }
        return genFullScanPruneStepInfoInner(partInfo, partInfo.getPartitionBy().getPhysicalPartLevel(), true);
    }

    /**
     * generate full scan by partKey level, included 3 cases:
     * no useSubPart:
     * partLevel=PARTITION_KEY: scan all partitions
     * useSubPart:
     * partLevel=PARTITION_KEY: scan all partitions, its result bitset is partition-level;
     * partLevel=SUBPARTITION_KEY:
     * fullScanSubPartsCrossAllParts=false:  scan subpartitions of just one partition
     * fullScanSubPartsCrossAllParts=true:  scan subpartitions of just all partitions
     */
    protected static PartitionPruneStep genFullScanPruneStepInfoInner(PartitionInfo partInfo,
                                                                      PartKeyLevel partLevel,
                                                                      boolean fullScanSubPartsCrossAllParts) {
        BuildStepOpParams buildParams = new BuildStepOpParams();
        buildParams.setCurrFullContext(null);
        buildParams.setPartInfo(partInfo);
        buildParams.setPartPredPathInfo(null);
        buildParams.setPredRouteFunc(null);
        buildParams.setPartKeyMatchLevel(partLevel);
        buildParams.setConflict(false);
        buildParams.setForceFullScan(true);
        buildParams.setFullScanSubPartsCrossAllParts(fullScanSubPartsCrossAllParts);
        buildParams.setScanFirstPartOnly(false);
        PartitionPruneStepOp finalStep = buildStepOp(buildParams);

        return finalStep;
    }

    protected static PartitionPruneStep genZeroScanPruneStepInfoInner(PartitionInfo partInfo, PartKeyLevel partLevel,
                                                                      boolean zeroScanSubPartsCrossAllParts) {
        /**
         * When conflict=true, fullScan step will became a zeroScan step
         */
        BuildStepOpParams buildParams = new BuildStepOpParams();
        buildParams.setCurrFullContext(null);
        buildParams.setPartInfo(partInfo);
        buildParams.setPartPredPathInfo(null);
        buildParams.setPredRouteFunc(null);
        buildParams.setPartKeyMatchLevel(partLevel);
        buildParams.setConflict(true);
        buildParams.setForceFullScan(true);
        buildParams.setFullScanSubPartsCrossAllParts(zeroScanSubPartsCrossAllParts);
        buildParams.setScanFirstPartOnly(false);
        PartitionPruneStepOp finalStep = buildStepOp(buildParams);

        return finalStep;
    }

    /**
     * Only use for build prune step for single/broadcast table
     */
    protected static PartitionPruneStep genFirstPartScanOnlyPruneStepInfoInner(PartitionInfo partInfo) {
        /**
         * When scanFirstPartOnly=true, forceFullScan will be ignored
         */
//        PartitionPruneStep finalStep =
//            buildStepOp(null, partInfo, null, null, partInfo.getPartitionBy().getPhysicalPartLevel(), false, false, true);

        /**
         * When scanFirstPartOnly=true, forceFullScan && isConflict will be ignored
         */
        BuildStepOpParams buildParams = new BuildStepOpParams();
        buildParams.setCurrFullContext(null);
        buildParams.setPartInfo(partInfo);
        buildParams.setPartPredPathInfo(null);
        buildParams.setPredRouteFunc(null);
        buildParams.setPartKeyMatchLevel(partInfo.getPartitionBy().getPhysicalPartLevel());
        buildParams.setConflict(false);
        buildParams.setForceFullScan(false);
        buildParams.setFullScanSubPartsCrossAllParts(false);
        buildParams.setScanFirstPartOnly(true);
        PartitionPruneStepOp finalStep = buildStepOp(buildParams);

        return finalStep;
    }

    public static PartitionPruneStep generatePointSelectPruneStepInfo(String dbName,
                                                                      String tbName,
                                                                      List<Object> pointValue,
                                                                      List<DataType> pointValueOpTypes,
                                                                      ExecutionContext ec,
                                                                      ExecutionContext[] newEcOutput) {
        OptimizerContext opCtx = OptimizerContext.getContext(dbName);
        if (opCtx == null) {
            throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR, String.format("No found database %s", dbName));
        }
        String targetDbName = opCtx.getSchemaName();
        if (!DbInfoManager.getInstance().isNewPartitionDb(targetDbName)) {
            throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                "Only support tables created by syntax 'partition by'", dbName);
        }

        TableMeta tableMeta = opCtx.getLatestSchemaManager().getTable(tbName);
        if (tableMeta == null) {
            throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                String.format("No found partition table %s", tbName));
        }

        PartitionInfo partInfo = tableMeta.getPartitionInfo();

        RelDataType tbRelRowType = tableMeta.getRowType(PartitionPrunerUtils.getTypeFactory());
        return generatePointSelectPruneStepInfo(pointValue, pointValueOpTypes, ec, newEcOutput, partInfo, tbRelRowType);
    }

    public static PartitionPruneStep generatePointSelectPruneStepInfo(List<Object> pointValue,
                                                                      List<DataType> pointValueOpTypes,
                                                                      ExecutionContext ec,
                                                                      ExecutionContext[] newEcOutput,
                                                                      PartitionInfo partInfo,
                                                                      RelDataType tbRelRowType) {
        RexBuilder rexBuilder = PartitionPrunerUtils.getRexBuilder();

        List<ColumnMeta> partColFldList = partInfo.getPartitionBy().getPartitionFieldList();
        List<RexNode> partColRexInputList = new ArrayList<>();
        final Map<String, Integer> indexColRefMap = tbRelRowType.getFieldList()
            .stream()
            .collect(Collectors.toMap(RelDataTypeField::getName,
                RelDataTypeField::getIndex,
                (x, y) -> y,
                TreeMaps::caseInsensitiveMap));
        partColFldList.stream().forEach(
            fld -> {
                if (indexColRefMap.containsKey(fld.getName())) {
                    final Integer ref = indexColRefMap.get(fld.getName());
                    final RelDataType type = fld.getField().getRelType();
                    partColRexInputList.add(rexBuilder.makeInputRef(type, ref));
                }
            }
        );

        List<Object> pointValStr = new ArrayList<>();
        List<Boolean> pointValBinStrFlags = new ArrayList<>();
        boolean containStringType = false;
        for (int i = 0; i < pointValue.size(); i++) {
            Object pointValueObj = pointValue.get(i);
            boolean isBytes = pointValueObj instanceof byte[];
            boolean isStringObj = false;
            if (DataTypeUtil.isStringSqlType(partColFldList.get(i).getField().getDataType())) {
                containStringType = true;
                isStringObj = true;
            }
            if (isStringObj) {

                if (!isBytes) {
                    // All string are convert to utf8Str,except byte[]
                    PartitionField pointValFld =
                        PartitionPrunerUtils.buildPartField(pointValueObj, pointValueOpTypes.get(i),
                            DataTypes.VarcharType,
                            null, ec, PartFieldAccessType.QUERY_PRUNING);
                    if (!pointValFld.isNull()) {
                        pointValStr.add(pointValFld.stringValue().toStringUtf8());
                    } else {
                        pointValStr.add(null);
                    }
                    pointValBinStrFlags.add(false);
                } else {
                    pointValStr.add(pointValueObj);
                    pointValBinStrFlags.add(true);
                }
            } else {
                if (pointValueObj != null) {
                    // All non-string are convert to utf8Str
                    pointValStr.add(
                        DataTypeUtil.convert(pointValueOpTypes.get(i), DataTypes.StringType, pointValueObj));
                } else {
                    pointValStr.add(null);
                }
                pointValBinStrFlags.add(false);
            }

        }
        String encoding = ec.getEncoding();
        ExecutionContext tmpEc = ec;
        if (containStringType
            && !("utf8".equalsIgnoreCase(encoding) || "utf8mb4".equalsIgnoreCase(encoding))) {
            tmpEc = ec.copy();
            tmpEc.setEncoding("utf8");
        }
        if (newEcOutput != null && newEcOutput.length == 1) {
            newEcOutput[0] = tmpEc;
        }

        PartitionPruneStep pointSelectStep = null;
        if (partInfo.getTableType() == PartitionTableType.PARTITION_TABLE
            || partInfo.getTableType() == PartitionTableType.GSI_TABLE) {
            // Build PartPred expr
            int partColValueCnt = pointValue.size();
            if (partColValueCnt > partColFldList.size()) {
                throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                    "Constant values columns are NOT allowed to be more than the partition columns");
            }
            List<RexNode> eqPredList = new ArrayList<>();
            for (int i = 0; i < partColValueCnt; i++) {
                RexNode colInput = partColRexInputList.get(i);
                Object val = pointValStr.get(i);
                Boolean binaryFlags = pointValBinStrFlags.get(i);
                RexNode eqExpr = null;
                if (val == null) {
                    eqExpr = rexBuilder.makeCall(TddlOperatorTable.IS_NULL, colInput);
                } else {
                    RexNode colValLiteral = null;
                    if (!binaryFlags) {
                        colValLiteral = rexBuilder.makeLiteral((String) val);
                    } else {
                        colValLiteral = rexBuilder.makeBinaryLiteral(new ByteString((byte[]) val));
                    }
                    eqExpr = rexBuilder.makeCall(TddlOperatorTable.EQUALS, colInput, colValLiteral);
                }
                eqPredList.add(eqExpr);
            }
            RexNode finalPredExpr;
            if (eqPredList.size() == 1) {
                finalPredExpr = eqPredList.get(0);
            } else {
                finalPredExpr = rexBuilder.makeCall(TddlOperatorTable.AND, eqPredList);
            }

            pointSelectStep = generatePartitionPruneStepInfo(partInfo, tbRelRowType, finalPredExpr, tmpEc);
        } else {
            pointSelectStep = genFirstPartScanOnlyPruneStepInfoInner(partInfo);
        }

        return pointSelectStep;
    }

    protected static PartitionPruneStep generatePartitionPruneStepInfo(PartitionInfo partInfo,
                                                                       RelDataType relRowType,
                                                                       RexNode partPredInfo,
                                                                       ExecutionContext ec) {
        if (partInfo.getTableType() == PartitionTableType.BROADCAST_TABLE
            || partInfo.getTableType() == PartitionTableType.SINGLE_TABLE) {
            return genFirstPartScanOnlyPruneStepInfoInner(partInfo);
        }
        PartitionPruneStep finalStep = genCompletedPartPruneSteps(partInfo, relRowType, partPredInfo, ec);
        return finalStep;

//        //========== old builder ========
//
//        AtomicInteger constExprIdGenerator = new AtomicInteger(0);
//        ExprContextProvider exprCtxProvider = new ExprContextProvider();
//        PartPruneStepBuildingContext stepContext =
//            PartPruneStepBuildingContext
//                .getNewPartPruneStepContext(partInfo, PartKeyLevel.PARTITION_KEY, constExprIdGenerator, exprCtxProvider, ec);
//
//        /**
//         * Rewrite partition predicate & toDnf
//         */
//        if (ec != null && ec.getParams() != null) {
//            partPredInfo = RexUtil.recoverInExpr(partPredInfo, ec.getParams().getCurrentParameter());
//        }
//        RexNode rewrotePartPred =
//            PartPredRewriter.rewritePartPredicate(partInfo, relRowType, partPredInfo, stepContext);
//
//        /**
//         * Check If the predicate expr is two complex and its OpSteps are too many,
//         * then it will lead to giving up pruning and return  full scan step
//         */
//        boolean needGiveUpPruning = checkIfNeedGiveUpPruning(partInfo, stepContext, rewrotePartPred);
//        if (needGiveUpPruning) {
//            return generateFullScanPruneStepInfo(partInfo,partInfo.getPartitionBy().getPhysicalPartLevel());
//        }
//
//        /**
//         *
//         * Simplify the predicates which have been finishing DNF conversion, include:
//         *
//         *      1. all the opExpr in OR/AND which contains NOT any partition columns will treated as Always-True expr;
//         *      2. all the opExpr in OR Expr which partitionKey is NOT the first partition columns is treated as Always-True expr;
//         *      3. all opExpr in a AND Expr which partitionKeys DOES NOT contains the first partition column are treated as Always-True expr;
//         *
//         * <pre>
//         *
//         * pre process the predicate and
//         * convert predicateInfo to the uniform PartClauseItem
//         * which PartClauseItem will be using as prefix predicate enumeration
//         *
//         *
//         * </pre>
//         */
//        PartClauseItem clauseItem =
//            PartClauseInfoPreProcessor.convertToPartClauseItem(partInfo.getPartitionBy(), relRowType, rewrotePartPred, stepContext);
//
//        /**
//         * Build the PartPruneStep by the PartClauseItem rewrited from rexnode
//         */
//        PartitionPruneStep pruneStep = genPartPruneStepsInner(partInfo, relRowType, clauseItem, stepContext);
//        if (pruneStep == null) {
//            pruneStep = generateFullScanPruneStepInfo(partInfo,partInfo.getPartitionBy().getPhysicalPartLevel());
//        }
//        return pruneStep;
    }

    /**
     * If the predicate expr is two complex and its OpSteps are too many, then it will lead to giving up pruning and return  full scan step
     */
    protected static boolean checkIfNeedGiveUpPruning(PartitionInfo partInfo,
                                                      PartPruneStepBuildingContext buildingContext,
                                                      RexNode partPred) {

        if (partPred == null) {
            return true;
        }
        PartOpPredCounter counter = new PartOpPredCounter();
        partPred.accept(counter);
        int opPredCnt = counter.getOpPredCnt();
        if (opPredCnt > buildingContext.getPruneStepOpCountLimit()) {
            return true;
        }
        return false;
    }

    /**
     * Generate the pruning step by sharding predicates
     */
    protected static PartitionPruneStep genPartPruneStepsInner(PartitionInfo partInfo,
                                                               RelDataType relRowType,
                                                               PartClauseItem clauseItem,
                                                               PartPruneStepBuildingContext stepContext) {

        PartitionPruneStep stepInfo = null;
        if (clauseItem == null) {
            stepInfo = genFullScanPruneStepInfoInner(partInfo, stepContext.getPartLevel(), false);
            return stepInfo;
        }

        /**
         * Generate pruning steps for the predicate of orExpr & andExpr
         */
        PartPruneStepType stepType = clauseItem.getType();
        if (stepType == PartPruneStepType.PARTPRUNE_COMBINE_UNION) {
            stepInfo = genPruneStepsFromOrExpr(partInfo, relRowType, clauseItem, stepContext);
            return stepInfo;
        } else if (stepType == PartPruneStepType.PARTPRUNE_COMBINE_INTERSECT) {
            stepInfo = genPruneStepsFromAndExpr(partInfo, relRowType, clauseItem, stepContext);
            return stepInfo;
        }

        /**
         * Generate pruning steps for the predicate of opExpr
         */
        stepInfo = genPruneStepsFromOpExpr(partInfo, relRowType, clauseItem, stepContext);
        return stepInfo;
    }

    protected static PartitionPruneStep genPruneStepsFromOpExpr(PartitionInfo partInfo,
                                                                RelDataType relRowType,
                                                                PartClauseItem clauseItem,
                                                                PartPruneStepBuildingContext stepContext) {

        if (clauseItem.getType() == PartPruneStepType.PARTPRUNE_OP_MISMATCHED_PART_KEY) {

            if (clauseItem.isAlwaysTrue()) {
                PartitionPruneStep alwaysTrueStep =
                    genFullScanPruneStepInfoInner(partInfo, stepContext.getPartLevel(), false);
                return alwaysTrueStep;
            }

            if (clauseItem.isAlwaysFalse()) {
                PartitionPruneStep alwaysFalseStep =
                    genZeroScanPruneStepInfoInner(partInfo, stepContext.getPartLevel(), false);
                return alwaysFalseStep;
            }
        }

        PartClauseInfo clauseInfo = clauseItem.getClauseInfo();
        boolean isSubQueryInClause = clauseInfo.getOpKind() == SqlKind.IN;
        List<PartitionPruneStep> steps = new ArrayList<>();
        if (!isSubQueryInClause) {
            stepContext.addPartPredIntoContext(clauseInfo);
            steps.addAll(stepContext.enumPrefixPredAndGenPruneSteps());
            stepContext.resetPrefixPartPredPathCtx();
        } else {
            /**
             *  (p1,p2) in (dynamicScalarSubQuery) will be converted to
             *      foreach val of dynamicScalarSubQuery
             *          1. set the values for tmpDynamicParams1, tmpDynamicParams2 by val of dynamicScalarSubQuery;
             *          2. perform pruning by p1=tmpDynamicParams1 and p2=tmpDynamicParams2
             * , which the tmpDynamicParams1,tmpDynamicParams2 is the list of the following partColDynamicParams
             */
            SubQueryInPartClauseInfo sbInPartClause = (SubQueryInPartClauseInfo) clauseInfo;
            PartKeyLevel partKeyLevel = clauseInfo.getPartKeyLevel();
            List<PartitionPruneStep> eqExprSteps = new ArrayList<>();
            List<PartClauseItem> eqExprClauseItems = sbInPartClause.getEqExprClauseItems();
            Map<Integer, PartClauseItem> partKeyIdxClauseItemMap = new TreeMap<>();
            for (int i = 0; i < eqExprClauseItems.size(); i++) {
                PartClauseItem oneEqExprClauseItem = eqExprClauseItems.get(i);
                PartClauseInfo oneEqExprClause = oneEqExprClauseItem.getClauseInfo();
                stepContext.addPartPredIntoContext(oneEqExprClause);
                eqExprSteps.addAll(stepContext.enumPrefixPredAndGenPruneSteps());
                stepContext.resetPrefixPartPredPathCtx();
                partKeyIdxClauseItemMap.put(oneEqExprClause.getPartKeyIndex(), oneEqExprClauseItem);
            }
            int partColCnt = stepContext.getPartByDef().getPartitionFieldList().size();
            int prefixPartColCnt = 0;
            for (int i = 0; i < partColCnt; i++) {
                if (partKeyIdxClauseItemMap.containsKey(i)) {
                    prefixPartColCnt++;
                } else {
                    break;
                }
            }

            PartitionPruneStep finalExprStep = null;
            if (eqExprSteps.size() > 1) {
                PartitionPruneStepCombine exprStepCombine =
                    new PartitionPruneStepCombine(PartPruneStepType.PARTPRUNE_COMBINE_INTERSECT);
                exprStepCombine.setSubSteps(eqExprSteps);
                exprStepCombine.setIntervalInfo(null);
                exprStepCombine.setIntervalMerger(null);
                finalExprStep = exprStepCombine;
            } else {
                finalExprStep = eqExprSteps.get(0);
            }
            PartitionPruneStepInSubQuery stepInSubQuery =
                new PartitionPruneStepInSubQuery(partInfo, partKeyLevel);
            stepInSubQuery.setEqExprDynamicParams(sbInPartClause.getPartColDynamicParams());
            stepInSubQuery.setEqPredPrefixPartColCount(prefixPartColCnt);
            stepInSubQuery.setEqExprFinalStep(finalExprStep);
            stepInSubQuery.setSubQueryRexDynamicParam(sbInPartClause.getSubQueryDynamicParam());

            steps.add(stepInSubQuery);
        }

        if (steps.size() == 0) {
            return null;
        } else if (steps.size() == 1) {
            return steps.get(0);
        } else {
            PartitionPruneStepCombine stepCombine =
                buildStepCombine(partInfo, PartPruneStepType.PARTPRUNE_COMBINE_INTERSECT, steps, true, null,
                    stepContext);
            return stepCombine;
        }
    }

    /**
     * Generate pruning combine steps for orExpr / andExpr
     */
    protected static PartitionPruneStep genPruneStepsFromOrExpr(PartitionInfo partInfo,
                                                                RelDataType relRowType,
                                                                PartClauseItem clauseItem,
                                                                PartPruneStepBuildingContext stepContext) {
        PartPruneStepType stepType = clauseItem.getType();
        List<PartitionPruneStep> subStepList = new ArrayList<>();
        List<PartClauseItem> subItemList = clauseItem.getItemList();
        Map<String, PartitionPruneStep> duplicateStepsCache = new HashMap<>();
        boolean enableRangeMerge = stepContext.isDnfFormula();

        for (int i = 0; i < subItemList.size(); i++) {
            PartClauseItem subItem = subItemList.get(i);
            PartitionPruneStep subStepInfo = genPartPruneStepsInner(partInfo, relRowType, subItem, stepContext);
            if (subStepInfo != null) {
                subStepList.add(subStepInfo);
            }
        }

        List<PartitionPruneStep> newSubStepList = new ArrayList<>();
        for (int i = 0; i < subStepList.size(); i++) {
            PartitionPruneStep step = subStepList.get(i);

            if (step.getStepType() != PartPruneStepType.PARTPRUNE_OP_MISMATCHED_PART_KEY) {
                newSubStepList.add(step);
            } else {
                if (!((PartitionPruneStepOp) step).isConflict()) {
                    /**
                     * Found a full scan step !!
                     * So just return a full scan step instead because (A OR TRUE = TRUE)
                     */
                    newSubStepList.clear();
                    newSubStepList.add(step);
                    break;
                } else {
                    /**
                     * Found a zero scan step,
                     * just ignore it because (A OR FALSE = A)
                     */
                    continue;
                }
            }
        }

        /**
         * Remove duplicated step from UnionStep
         */
        newSubStepList = removeDuplicatedSteps(duplicateStepsCache, newSubStepList);

        if (newSubStepList.size() == 0) {
            return null;
        }
        if (newSubStepList.size() == 1) {
            return newSubStepList.get(0);
        }

        PartitionPruneStepCombine stepCombine =
            buildStepCombine(partInfo, stepType, newSubStepList, enableRangeMerge, null, stepContext);
        return stepCombine;

    }

    /**
     * Generate pruning combine steps for orExpr / andExpr
     */
    protected static PartitionPruneStep genPruneStepsFromAndExpr(PartitionInfo partInfo,
                                                                 RelDataType relRowType,
                                                                 PartClauseItem clauseItem,
                                                                 PartPruneStepBuildingContext stepContext) {
        PartPruneStepType stepType = clauseItem.getType();
        List<PartitionPruneStep> subStepList = new ArrayList<>();
        List<PartClauseItem> subItemList = clauseItem.getItemList();
        Map<String, PartitionPruneStep> duplicateStepsCache = new HashMap<>();
        boolean isDnfFormula = stepContext.isDnfFormula();
        boolean enableRangeMerge = stepContext.isDnfFormula();
        List<PartClauseItem> partKeyOpItems = subItemList;
        if (!partKeyOpItems.isEmpty()) {
            boolean findAlwayFalseItem = false;
            List<PartClauseItem> andOrItems = new ArrayList<>();
            List<PartClauseItem> opItems = new ArrayList<>();
            if (!isDnfFormula) {
                for (int i = 0; i < subItemList.size(); i++) {
                    PartClauseItem subItem = subItemList.get(i);
                    if (subItem.getType() == PartPruneStepType.PARTPRUNE_COMBINE_INTERSECT
                        || subItem.getType() == PartPruneStepType.PARTPRUNE_COMBINE_UNION) {
                        andOrItems.add(subItem);
                    } else if (subItem.getType() == PartPruneStepType.PARTPRUNE_OP_MATCHED_PART_KEY) {
                        opItems.add(subItem);
                    } else {
                        if (subItem.isAlwaysTrue()) {
                            /**
                             * When find always-true item in AndExpr,
                             * should ignore it
                             */
                            continue;
                        }
                        if (subItem.isAlwaysFalse()) {
                            /**
                             * When find always-false item in AndExpr,
                             * should ignore it
                             */
                            findAlwayFalseItem = true;
                            break;
                        }
                    }
                }
                if (findAlwayFalseItem) {
                    /**
                     * When find always-false item in AndExpr,
                     * should just return a zero-scan step directly.
                     */
                    stepContext.resetPrefixPartPredPathCtx();
                    PartitionPruneStep zeroScanStep =
                        genZeroScanPruneStepInfoInner(partInfo, stepContext.getPartLevel(), false);
                    return zeroScanStep;
                }
                for (int i = 0; i < andOrItems.size(); i++) {
                    PartitionPruneStep subStepInfo =
                        genPartPruneStepsInner(partInfo, relRowType, andOrItems.get(i), stepContext);
                    if (subStepInfo != null) {
                        subStepList.add(subStepInfo);
                    }
                }
                partKeyOpItems = opItems;
            }

            /**
             * remove subQueryIn item from the all the opItems
             */
            List<PartClauseItem> nonSubQueryInOpItems = new ArrayList<>();
            List<PartClauseItem> subQueryInOpItems = new ArrayList<>();
            for (int i = 0; i < partKeyOpItems.size(); i++) {
                PartClauseItem item = partKeyOpItems.get(i);
                if (!item.getClauseInfo().isSubQueryInExpr()) {
                    nonSubQueryInOpItems.add(item);
                } else {
                    subQueryInOpItems.add(item);
                }
            }
            partKeyOpItems = nonSubQueryInOpItems;
            if (!subQueryInOpItems.isEmpty()) {
                for (int i = 0; i < subQueryInOpItems.size(); i++) {
                    PartitionPruneStep subStepInfo =
                        genPartPruneStepsInner(partInfo, relRowType, subQueryInOpItems.get(i), stepContext);
                    if (subStepInfo != null) {
                        subStepList.add(subStepInfo);
                    }
                }
            }

            /**
             * Prepare the new step context for the current  item of PARTPRUNE_COMBINE_INTERSECT
             */
            for (int i = 0; i < partKeyOpItems.size(); i++) {
                PartClauseItem item = partKeyOpItems.get(i);
                stepContext.addPartPredIntoContext(item.getClauseInfo());
            }

            /**
             * After all the predicates of a AND expr are added into context, then generate the pruneSteps by
             * enum the prefix partition predicate paths.
             *
             */
            subStepList.addAll(stepContext.enumPrefixPredAndGenPruneSteps());
            stepContext.resetPrefixPartPredPathCtx();

            /**
             * For all subStep generated by step of PARTPRUNE_COMBINE_INTERSECT,
             * remove all the full scan step with the PartPruneStepType.PARTPRUNE_OP_MISMATCHED_PART_KEY that maybe generated by HASH/KEY
             * when its predicate path info has not completed partCols.
             * such as (a,b,c) is a multi-columns key ,
             * query predicate: a=const1,
             * than it will return a opStep of PARTPRUNE_OP_MISMATCHED_PART_KEY.
             */
            List<PartitionPruneStep> newSubStepList = new ArrayList<>();
            for (int i = 0; i < subStepList.size(); i++) {
                PartitionPruneStep step = subStepList.get(i);
                // Ignore all step that mismatch partition key and need full scan
                if (step.getStepType() != PartPruneStepType.PARTPRUNE_OP_MISMATCHED_PART_KEY) {
                    newSubStepList.add(step);
                } else {
                    if (!((PartitionPruneStepOp) step).isConflict()) {
                        // Found a full scan step ,
                        // just ignore it because (A and TRUE = A)
                        continue;
                    } else {
                        // Found a zero scan step !!
                        // So just return a zero scan step instead because (A and FALSE = FALSE)
                        newSubStepList.clear();
                        newSubStepList.add(step);
                        break;
                    }
                }
            }

            /**
             * Remove duplicated step from IntersectStep
             */
            newSubStepList = removeDuplicatedSteps(duplicateStepsCache, newSubStepList);

            if (newSubStepList.size() == 0) {
                return null;
            }
            if (newSubStepList.size() == 1) {
                return newSubStepList.get(0);
            }
            PartitionPruneStepCombine stepCombine =
                buildStepCombine(partInfo, stepType, newSubStepList, enableRangeMerge, null, stepContext);
            return stepCombine;
        } else {
            /**
             * No find any partKeyOpItems
             */
            return null;
        }
    }

    private static List<PartitionPruneStep> removeDuplicatedSteps(Map<String, PartitionPruneStep> duplicateStepsCache,
                                                                  List<PartitionPruneStep> newSubStepList) {
        if (newSubStepList.size() <= 1) {
            return newSubStepList;
        }
        for (int i = 0; i < newSubStepList.size(); i++) {
            PartitionPruneStep step = newSubStepList.get(i);
            String digest = step.getStepDigest();
            duplicateStepsCache.putIfAbsent(digest, step);
        }
        List<PartitionPruneStep> removedDuplicatedStepList = new ArrayList<>();
        for (PartitionPruneStep step : duplicateStepsCache.values()) {
            removedDuplicatedStepList.add(step);
        }
        newSubStepList = removedDuplicatedStepList;
        return newSubStepList;
    }

    /**
     * Build a PartitionPruneStep according to one PartPredPathInfo such as ( p1=c1 and p2=c2 and p3<=c3 )
     */
    protected static List<PartitionPruneStep> genPartitionPruneStepByPartPredPathInfo(
        PartPruneStepBuildingContext currFullContext,
        PartPredPathInfo partPathInfo) {

        List<PartitionPruneStep> pruneSteps = new ArrayList<>();
        ExprContextProvider ctxProvider = currFullContext.getExprCtxHolder();
        PartitionInfo partInfo = currFullContext.getPartInfo();

        // Fetch the part columns count
        Integer partColCnt = currFullContext.getPartColIdxMap().size();

        PartClauseIntervalInfo[] partIntervalArr = new PartClauseIntervalInfo[partColCnt];
        partPathInfo.getPrefixPathItem().toPartClauseInfoArray(partIntervalArr);

        /**
         * Init each part clause info and its execution form for each partition columns
         */
        int partKeyEnd = partPathInfo.getPartKeyEnd();
        PartClauseExprExec[] partExprExecArr = new PartClauseExprExec[partColCnt];
        for (int i = 0; i <= partKeyEnd; i++) {
            partExprExecArr[i] = partIntervalArr[i].getPartClauseExec();
        }

        /**
         * The the partition function operator if it exits, or its value will be null
         */
        PartClauseInfo clauseInfo = partIntervalArr[0].getPartClause();
        SqlOperator partFuncOp =
            getPartFuncSqlOperation(clauseInfo.getPartKeyLevel(), clauseInfo.getPartKeyIndex(), partInfo);
        PartKeyLevel keyLevel = partIntervalArr[0].getPartClause().getPartKeyLevel();
        SqlKind opKindOfPartKeyEnd = partIntervalArr[partKeyEnd].getPartClause().getOpKind();
        ComparisonKind cmpKindOfSearchExpr = getComparisonBySqlKind(opKindOfPartKeyEnd);

        switch (cmpKindOfSearchExpr) {

        case GREATER_THAN:
        case LESS_THAN:
        case GREATER_THAN_OR_EQUAL:
        case LESS_THAN_OR_EQUAL: {

            if (partColCnt == 1) {
                /**
                 * Build pruneStep for single-partition-column
                 */
                PartitionPruneStep pruneStep =
                    buildStepByPartExprExecArr(currFullContext, partPathInfo, ctxProvider, partInfo, keyLevel,
                        partFuncOp,
                        partExprExecArr,
                        cmpKindOfSearchExpr);
                pruneSteps.add(pruneStep);
            } else {
                /**
                 * Build pruneSteps for multi-partition-columns
                 */
                buildVectorialRangeForMultiPartCols(currFullContext, ctxProvider, partInfo, keyLevel, partFuncOp,
                    partColCnt,
                    partExprExecArr, partPathInfo, partKeyEnd, cmpKindOfSearchExpr, pruneSteps);
            }

        }
        break;
        case EQUAL: {

            if (partKeyEnd < partColCnt - 1) {
                /**
                 *  Not contains all prefix predicates for all partition columns
                 */

                /**
                 *
                 * <pre>
                 * Handle the case as followed for range/range col/list/list cols :
                 *  (p1,p2,p3) is the multi-columns partition key
                 *  and p1=c1 is the partition predicate
                 *
                 * p1=c1
                 * =>
                 * (c1,min,min)<=(p1,p2,p3) and (p1,p2,p3)<=(c1,max,max)
                 *
                 * </pre>
                 */
                buildVectorialRangeForMultiPartCols(currFullContext, ctxProvider, partInfo, keyLevel, partFuncOp,
                    partColCnt,
                    partExprExecArr, partPathInfo, partKeyEnd, cmpKindOfSearchExpr, pruneSteps);
            } else {

                /**
                 *  Contains all prefix predicates for all partition columns
                 */

                /**
                 *
                 * <pre>
                 * Handle the case as followed :
                 *  (p1,p2,p3) is the multi-columns partition key
                 *  and the partition predicate is
                 *
                 * p1=c1 and p2=c2 and p3=c3
                 * =>
                 *  (p1,p2,p3) = (c1,c2,c3)
                 *
                 * </pre>
                 */
                PartitionPruneStep pruneStep =
                    buildStepByPartExprExecArr(currFullContext, partPathInfo, ctxProvider, partInfo, keyLevel,
                        partFuncOp,
                        partExprExecArr,
                        ComparisonKind.EQUAL);
                pruneSteps.add(pruneStep);
            }
        }
        break;
        }
        return pruneSteps;
    }

    private static void checkIfNeedDoFullScan() {

    }

    private static void buildVectorialRangeForMultiPartCols(PartPruneStepBuildingContext currFullContext,
                                                            ExprContextProvider ctxProvider,
                                                            PartitionInfo partInfo,
                                                            PartKeyLevel keyLevel,
                                                            SqlOperator partFuncOp,
                                                            Integer partColCnt,
                                                            PartClauseExprExec[] partExprExecArr,
                                                            PartPredPathInfo partPathInfo,
                                                            int partKeyEnd,
                                                            ComparisonKind targetCmpKind,
                                                            List<PartitionPruneStep> outputPruningSteps) {
        // Generate lower bound
        PartClauseExprExec[] gtLowerBndExec = new PartClauseExprExec[partColCnt];
        ComparisonKind[] qtLowerBndCmpKind = new ComparisonKind[1];
        buildVectorialRangeBoundExec(partExprExecArr, partKeyEnd, partColCnt, targetCmpKind, true,
            gtLowerBndExec, qtLowerBndCmpKind);
        if (!isNegativeInfinite(gtLowerBndExec)) {
            PartitionPruneStep gtLowerStep =
                buildStepByPartExprExecArr(currFullContext, partPathInfo, ctxProvider, partInfo, keyLevel, partFuncOp,
                    gtLowerBndExec,
                    qtLowerBndCmpKind[0]);
            outputPruningSteps.add(gtLowerStep);
        } else {
            /**
             *  if (p1,p2,...,pn) > (min,...,min), then ignore to build prune step, because it has not any useful routing info.
             */
        }

        // Generate upper bound
        PartClauseExprExec[] gtUpperBndExec = new PartClauseExprExec[partColCnt];
        ComparisonKind[] qtUpperBndCmpKind = new ComparisonKind[1];
        buildVectorialRangeBoundExec(partExprExecArr, partKeyEnd, partColCnt, targetCmpKind, false,
            gtUpperBndExec, qtUpperBndCmpKind);
        if (!isPositiveInfinite(gtUpperBndExec)) {
            PartitionPruneStep gtUpperStep =
                buildStepByPartExprExecArr(currFullContext, partPathInfo, ctxProvider, partInfo, keyLevel, partFuncOp,
                    gtUpperBndExec,
                    qtUpperBndCmpKind[0]);
            outputPruningSteps.add(gtUpperStep);
        } else {
            /**
             *  if (p1,p2,...,pn) < (max,...,max), then ignore to build prune step, because it has not any useful routing info.
             */
        }
    }

    private static PartitionPruneStep buildStepByPartExprExecArr(PartPruneStepBuildingContext currFullContext,
                                                                 PartPredPathInfo partPathInfo,
                                                                 ExprContextProvider ctxProvider,
                                                                 PartitionInfo partInfo,
                                                                 PartKeyLevel keyLevel,
                                                                 SqlOperator partFuncOp,
                                                                 PartClauseExprExec[] partExprExecArr,
                                                                 ComparisonKind cmpKindOfSearchExpr) {
        SearchExprInfo searchExprInfo = new SearchExprInfo(partExprExecArr, cmpKindOfSearchExpr);
        PartPredicateRouteFunction routeFunc =
            new PartPredicateRouteFunction(partInfo, partFuncOp, keyLevel, searchExprInfo, ctxProvider);

        BuildStepOpParams buildParams = new BuildStepOpParams();
        buildParams.setCurrFullContext(currFullContext);
        buildParams.setPartInfo(partInfo);
        buildParams.setPartPredPathInfo(partPathInfo);
        buildParams.setPredRouteFunc(routeFunc);
        buildParams.setPartKeyMatchLevel(keyLevel);
        buildParams.setConflict(false);
        buildParams.setForceFullScan(false);
        buildParams.setFullScanSubPartsCrossAllParts(false);
        buildParams.setScanFirstPartOnly(false);
        PartitionPruneStepOp newStepOp = buildStepOp(buildParams);

//        PartitionPruneStepOp newStepOp =
//            buildStepOp(currFullContext, partInfo, partPathInfo, routeFunc, keyLevel, false, false, false);
        return newStepOp;
    }

    protected static SqlCall getPartFuncCall(PartKeyLevel level, int partKeyIndex, PartitionInfo partInfo) {
        List<SqlNode> partColExprList = new ArrayList<>();
        if (level == PartKeyLevel.PARTITION_KEY) {
            partColExprList = partInfo.getPartitionBy().getPartitionExprList();
        } else if (level == PartKeyLevel.SUBPARTITION_KEY) {
            partColExprList = partInfo.getPartitionBy().getSubPartitionBy().getPartitionExprList();
        }
        SqlNode partKeyExpr = partColExprList.get(partKeyIndex);
        if (partKeyExpr instanceof SqlIdentifier) {
            // The part col is only
            // so ignore.
            return null;
        } else if (partKeyExpr instanceof SqlCall) {
            SqlCall partKeyExprSqlCall = (SqlCall) partKeyExpr;
            return partKeyExprSqlCall;
        } else {
            throw new NotSupportException("should not be here");
        }
    }

    protected static SqlOperator getPartFuncSqlOperation(PartKeyLevel level, int partKeyIndex, PartitionInfo partInfo) {
        List<SqlNode> partColExprList = new ArrayList<>();
        if (level == PartKeyLevel.PARTITION_KEY) {
            partColExprList = partInfo.getPartitionBy().getPartitionExprList();
        } else if (level == PartKeyLevel.SUBPARTITION_KEY) {
            partColExprList = partInfo.getPartitionBy().getSubPartitionBy().getPartitionExprList();
        }

        SqlNode partKeyExpr = partColExprList.get(partKeyIndex);
        if (partKeyExpr instanceof SqlIdentifier) {
            // The part col is only
            // so ignore.
            return null;
        } else if (partKeyExpr instanceof SqlCall) {
            SqlCall partKeyExprSqlCall = (SqlCall) partKeyExpr;
            SqlOperator op = partKeyExprSqlCall.getOperator();
            return op;
        } else {
            throw new NotSupportException("should not be here");
        }
    }

    protected static ComparisonKind getComparisonBySqlKind(SqlKind kind) {

        switch (kind) {
        case IN:
            return ComparisonKind.EQUAL;
        case LIKE:
        case NOT:
            throw new NotSupportException("Not support get comparsion from NOT/LIKE");
        case AND:
        case OR:
            throw new NotSupportException("Not support get comparsion from and/or");
            //return getComparativeAndOr((RexCall) rexNode, rowType, colName, new ComparativeOR(), param);
        case EQUALS:
            return ComparisonKind.EQUAL;
        case NOT_EQUALS:
            return ComparisonKind.NOT_EQUAL;
        case GREATER_THAN:
            return ComparisonKind.GREATER_THAN;
        case GREATER_THAN_OR_EQUAL:
            return ComparisonKind.GREATER_THAN_OR_EQUAL;
        case LESS_THAN:
            return ComparisonKind.LESS_THAN;
        case LESS_THAN_OR_EQUAL:
            return ComparisonKind.LESS_THAN_OR_EQUAL;
        case BETWEEN:
            //return getComparativeBetween((RexCall) rexNode, rowType, colName, param);
        case IS_NOT_FALSE:
        case IS_NOT_TRUE:
        case IS_NOT_NULL:
        case IS_FALSE:
        case IS_TRUE:
            // 这些运算符不参与下推判断
            return null;
        case IS_NULL:
            //return getComparativeIsNull((RexCall) rexNode, rowType, colName, param);
        case CAST:
            //return getComparative(((RexCall) rexNode).getOperands().get(0), rowType, colName, param);
        default:
            return null;
        } // end of switch
    }

    /**
     * Do the range merge and build new steps
     */
    protected static PartitionPruneStep mergePruneStepsForStepCombine(PartitionPruneStep pruneStep,
                                                                      ExecutionContext context,
                                                                      PartPruneStepPruningContext pruningCtx) {

        PartPruneStepType targetStepType = pruneStep.getStepType();
        if (targetStepType == PartPruneStepType.PARTPRUNE_OP_MISMATCHED_PART_KEY) {
            return pruneStep;
        }
        StepIntervalMerger rangeMerger = pruneStep.getIntervalMerger();
        PartitionInfo partInfo = rangeMerger.getPartInfo();

        /**
         * Merge range and get results
         */
        List<StepIntervalInfo> mergedRanges = rangeMerger.mergeIntervals(context, pruningCtx);

        if (mergedRanges.size() == 0) {

            /**
             * When pruneStep is union step, its merged results must be empty range if
             * all its sub ranges are the type CONFLICT_RANGE, so just return a zeroScanStep
             */
            PartitionPruneStep zeroScanStep =
                PartitionPruneStepBuilder.genZeroScanPruneStepInfoInner(partInfo, pruneStep.getPartLevel(), false);
            return zeroScanStep;

        } else if (mergedRanges.size() == 1) {

            /**
             * When pruneStep is intersect step, its merged range must be one at most
             */
            PartitionPruneStep newStep =
                buildPartPruneStepByStepRangeIntervalInfo(partInfo, context, pruningCtx, mergedRanges.get(0));
            return newStep;

        } else {

            /**
             * When pruneStep is union step, its merged ranges may be multi-ranges;
             * When pruneStep is intersect step, its merged ranges may be multi-ranges
             *      because of some unsupported merging exprs like " in (subQuery)".
             */
            List<PartitionPruneStep> subSteps = new ArrayList<>();
            for (int i = 0; i < mergedRanges.size(); i++) {
                StepIntervalInfo rng = mergedRanges.get(i);
                PartitionPruneStep subStep =
                    buildPartPruneStepByStepRangeIntervalInfo(partInfo, context, pruningCtx, rng);
                if (subStep != null) {
                    if (subStep instanceof PartitionPruneStepCombine && subStep.getStepType() == targetStepType) {
                        subSteps.addAll(((PartitionPruneStepCombine) subStep).getSubSteps());
                    } else {
                        subSteps.add(subStep);
                    }
                }
            }

            if (subSteps.size() == 1) {
                return subSteps.get(0);
            }

            PartitionPruneStepCombine stepCombine =
                buildStepCombine(partInfo, targetStepType, subSteps, false, null, null);
            return stepCombine;
        }
    }

    private static PartitionPruneStep buildPartPruneStepByStepRangeIntervalInfo(PartitionInfo partInfo,
                                                                                ExecutionContext context,
                                                                                PartPruneStepPruningContext pruningCtx,
                                                                                StepIntervalInfo rangeInfo) {
        if (rangeInfo.isForbidMerging()) {
            if (rangeInfo.isStepIntervalInfoCombine()) {
                List<PartitionPruneStep> subSteps = new ArrayList<>();
                List<StepIntervalInfo> subRngInfos = rangeInfo.getSubStepIntervalInfos();
                PartPruneStepType targetStepType = rangeInfo.getStepCombineType();
                for (int i = 0; i < subRngInfos.size(); i++) {
                    PartitionPruneStep step =
                        PartitionPruneStepBuilder.buildPartPruneStepByStepRangeIntervalInfo(partInfo, context,
                            pruningCtx, subRngInfos.get(i));
                    if (step.getStepType() == targetStepType && step instanceof PartitionPruneStepCombine) {
                        subSteps.addAll(((PartitionPruneStepCombine) step).getSubSteps());
                    } else {
                        subSteps.add(step);
                    }
                }
                PartitionPruneStepCombine stepCombine =
                    buildStepCombine(partInfo, rangeInfo.getStepCombineType(), subSteps, false,
                        rangeInfo, null);
                return stepCombine;
            } else {
                return rangeInfo.getFinalStep();
            }
        }

        RangeIntervalType rangeType = rangeInfo.getRangeType();
        if (rangeType == RangeIntervalType.CONFLICT_RANGE) {
            PartitionPruneStep zeroScanStep = PartitionPruneStepBuilder
                .genZeroScanPruneStepInfoInner(partInfo, rangeInfo.getPartLevel(), false);
            return zeroScanStep;
        } else if (rangeType == RangeIntervalType.TAUTOLOGY_RANGE) {
            PartitionPruneStep fullScanStep = PartitionPruneStepBuilder
                .genFullScanPruneStepInfoInner(partInfo, rangeInfo.getPartLevel(), false);
            return fullScanStep;
        } else {
            // rangeType is RangeIntervalType.SATISFIABLE_RANGE

            // For [-Inf, +Inf]
            if (rangeInfo.getMaxVal().isMaxInf() && rangeInfo.getMinVal().isMinInf()) {
                PartitionPruneStep fullScanStep = PartitionPruneStepBuilder
                    .genFullScanPruneStepInfoInner(partInfo, rangeInfo.getPartLevel(), false);
                return fullScanStep;
            }

            Boolean isBoundInclude[] = new Boolean[2];
            if (checkCanEnumRange(partInfo, pruningCtx, rangeInfo, isBoundInclude)) {
                PartKeyLevel keyLevel = PartKeyLevel.PARTITION_KEY;
                PartEnumRouteFunction routeFunc =
                    new PartEnumRouteFunction(partInfo, rangeInfo, isBoundInclude[0], isBoundInclude[1]);

                BuildStepOpParams buildParams = new BuildStepOpParams();
                buildParams.setCurrFullContext(null);
                buildParams.setPartInfo(partInfo);
                buildParams.setPartPredPathInfo(null);
                buildParams.setPredRouteFunc(routeFunc);
                buildParams.setPartKeyMatchLevel(keyLevel);
                buildParams.setConflict(false);
                buildParams.setForceFullScan(false);
                buildParams.setFullScanSubPartsCrossAllParts(false);
                buildParams.setScanFirstPartOnly(false);
                PartitionPruneStepOp newStepOp = buildStepOp(buildParams);

                //PartitionPruneStepOp newStepOp = buildStepOp(null, partInfo, null, routeFunc, keyLevel, false, false, false);
                return newStepOp;
            } else {

                PartitionPruneStep minValStep = rangeInfo.getMinValStep();
                PartitionPruneStep maxValStep = rangeInfo.getMaxValStep();

                if (rangeInfo.isBuildFromSinglePointInterval()) {
                    // For singlePointInterval, minValStep and maxValStep are the same.
                    return ((PartitionPruneStepOp) maxValStep).getOriginalStepOp();
                } else {
                    if (rangeInfo.isSinglePointInterval()) {
                        PartitionPruneStepOp tmpOpStep = (PartitionPruneStepOp) maxValStep;
                        PartitionPruneStepOp newSinglePointStep = tmpOpStep.copy();
                        newSinglePointStep.adjustComparisonKind(ComparisonKind.EQUAL);
                        newSinglePointStep.setOriginalStepOp(tmpOpStep);
                        return newSinglePointStep;
                    }
                }

                List<PartitionPruneStep> subStepList = new ArrayList<>();
                if (maxValStep != null) {
                    subStepList.add(maxValStep);
                }

                if (minValStep != null) {
                    subStepList.add(minValStep);
                }

                if (subStepList.size() == 1) {
                    return subStepList.get(0);
                }

                PartitionPruneStepCombine stepCombine =
                    buildStepCombine(partInfo, PartPruneStepType.PARTPRUNE_COMBINE_INTERSECT, subStepList, false,
                        rangeInfo, null);

                return stepCombine;
            }
        }
    }

    protected static boolean checkCanEnumRange(PartitionInfo partInfo,
                                               PartPruneStepPruningContext pruningCtx,
                                               StepIntervalInfo rangeInfo,
                                               Boolean isBoundInclude[]) {

        if (!pruningCtx.isEnableIntervalEnumeration()) {
            return false;
        }

        PartitionByDefinition partBy = partInfo.getPartitionBy();
        PartKeyLevel partLevel = rangeInfo.getPartLevel();
        if (partLevel == PartKeyLevel.SUBPARTITION_KEY) {
            partBy = partBy.getSubPartitionBy();
        }

        if (!partBy.isNeedEnumRange()) {
            return false;
        }

        if (rangeInfo.isBuildFromSinglePointInterval()) {
            return false;
        }

        if (rangeInfo.isSinglePointInterval()) {
            return false;
        }

        RangeInterval min = rangeInfo.getMinVal();
        RangeInterval max = rangeInfo.getMaxVal();
        if (min == null || max == null) {
            return false;
        }
        if (min.isMinInf() || max.isMaxInf()) {
            return false;
        }

        boolean inclMin = rangeInfo.getMinVal().isIncludedBndValue();
        boolean inclMax = rangeInfo.getMaxVal().isIncludedBndValue();

        assert min.getBndValue().getDatumInfo().length == 1;
        assert max.getBndValue().getDatumInfo().length == 1;

        if (min.getBndValue().getSingletonValue().isNullValue() || max.getBndValue().getSingletonValue()
            .isNullValue()) {
            return false;
        }

        PartitionField minPartitionField = min.getBndValue().getSingletonValue().getValue();
        PartitionField maxPartitionField = max.getBndValue().getSingletonValue().getValue();

        if (minPartitionField.lastStatus() != TypeConversionStatus.TYPE_OK
            || maxPartitionField.lastStatus() != TypeConversionStatus.TYPE_OK) {
            return false;
        }

        if (partBy.getPartIntFunc() != null) {
            /**
             * All the part int func is time func
             */
            MySQLIntervalType intervalType = partInfo.getPartitionBy().getIntervalType();
            if (intervalType != null) {
                if (!inclMin && (intervalType == INTERVAL_YEAR || intervalType == INTERVAL_MONTH
                    || intervalType == INTERVAL_WEEK || intervalType == INTERVAL_DAY)) {
                    inclMin = true;
                }
                if (!inclMax && (intervalType == INTERVAL_YEAR || intervalType == INTERVAL_MONTH
                    || intervalType == INTERVAL_WEEK || intervalType == INTERVAL_DAY)) {
                    inclMax = true;
                }
            } else {
                inclMin = true;
                inclMax = true;
            }

        }

        isBoundInclude[0] = inclMin;
        isBoundInclude[1] = inclMax;

        PartitionFieldIterator iterator = PartitionFieldIterators
            .getIterator(minPartitionField.dataType(),
                partInfo.getPartitionBy().getIntervalType(),
                partInfo.getPartitionBy().getPartIntFunc()
            );

        boolean isValidRange = iterator.range(minPartitionField, maxPartitionField, inclMin, inclMax);
        if (!isValidRange) {
            return false;
        }

        long maxEnumLength = PartitionPruneStepBuilder.DEFAULT_MAX_ENUMERABLE_INTERVAL_LENGTH;
        if (pruningCtx != null) {
            maxEnumLength = pruningCtx.getMaxEnumerableIntervalLength();
        }

        long cntVal = iterator.count();
        if (cntVal <= 0 || cntVal > maxEnumLength) {
            return false;
        }

        return true;
    }

    /**
     * Build a vectorial range bound by partition prefix predicates
     */
    protected static void buildVectorialRangeBoundExec(PartClauseExprExec[] partIntervalArr,
                                                       int partKeyEnd,
                                                       int partColCnt,
                                                       ComparisonKind cmpKind,
                                                       boolean lowerOrUpper,
                                                       PartClauseExprExec[] bndExecArrOutput,
                                                       ComparisonKind[] newCmpKindOutput) {

        PartClauseExprExec[] bndExecArr = bndExecArrOutput;
        boolean isForLowerBnd = lowerOrUpper;

        if (cmpKind == ComparisonKind.GREATER_THAN) {

            /**
             * <pre>
             *  If n is the partCol count , if k <= n ,
             *  (p_1=c_1) AND (p_2=c_2) AND... ADN (p_{k-1}=c_{k-1}) AND (p_k>c_k)
             *
             *  =>
             *
             *  (c_1,c_2,...,c_{k-1},c_k, max_{k+1},...,max_n) < (p1,p2,...,pn) < (c1,c2,..., c_{k-1},max_k,max_{k+1},...,max_n)
             *
             *  </pre>
             */

            // prepare vector range bound for c_1,c_2,...,c_{k-1}
            for (int i = 0; i <= partKeyEnd - 1; i++) {
                bndExecArr[i] = partIntervalArr[i];
            }

            if (isForLowerBnd) {
                /**
                 * Build lower bound of range : > (c_1,c_2,...,c_{k-1}, c_k, max_{k+1},...,max_n)
                 */

                // prepare vector range bound for c_k, max_{k+1},...,max_n
                bndExecArr[partKeyEnd] = partIntervalArr[partKeyEnd];
                for (int i = partKeyEnd + 1; i < partColCnt; i++) {
                    bndExecArr[i] = PartClauseExprExec.MAX_VAL;
                }
                newCmpKindOutput[0] = ComparisonKind.GREATER_THAN;

            } else {
                /**
                 * Build lower bound of range : < (c_1,c_2,...,c_{k-1}, c_k, max_{k+1},...,max_n)
                 */

                // prepare vector range bound for max_k, max_{k+1},...,max_n
                for (int i = partKeyEnd; i < partColCnt; i++) {
                    bndExecArr[i] = PartClauseExprExec.MAX_VAL;
                }
                newCmpKindOutput[0] = ComparisonKind.LESS_THAN;
            }

        } else if (cmpKind == ComparisonKind.GREATER_THAN_OR_EQUAL) {

            /**
             * <pre>
             *  If n is the partCol count , if k <= n,
             *  (p_1=c_1) AND (p_2=c_2) AND... ADN (p_{k-1}=c_{k-1}) AND (p_k>=c_k)
             *
             *  =>
             *
             *  (c_1,c_2,...,c_{k-1},c_k, min_{k+1},...,min_n) <= (p1,p2,...,pn) < (c1,c2,..., c_{k-1},max_k,max_{k+1},...,max_n)
             *
             *  </pre>
             */

            // prepare vector range bound for c_1,c_2,...,c_{k-1}
            for (int i = 0; i <= partKeyEnd - 1; i++) {
                bndExecArr[i] = partIntervalArr[i];
            }

            if (isForLowerBnd) {
                /**
                 * Build lower bound of range : >= (c_1,c_2,...,c_{k-1},c_k, min_{k+1},...,min_n)
                 */

                // prepare vector range bound for c_k, min_{k+1},...,min_n
                bndExecArr[partKeyEnd] = partIntervalArr[partKeyEnd];
                for (int i = partKeyEnd + 1; i < partColCnt; i++) {
                    bndExecArr[i] = PartClauseExprExec.MIN_VAL;
                }
                newCmpKindOutput[0] = ComparisonKind.GREATER_THAN_OR_EQUAL;

            } else {
                /**
                 * Build upper bound of range :  < (c1,c2,..., c_{k-1},max_k,max_{k+1},...,max_n)
                 */

                // prepare vector range bound for max_k,max_{k+1},...,max_n
                for (int i = partKeyEnd; i < partColCnt; i++) {
                    bndExecArr[i] = PartClauseExprExec.MAX_VAL;
                }
                newCmpKindOutput[0] = ComparisonKind.LESS_THAN;
            }

        } else if (cmpKind == ComparisonKind.EQUAL) {

            /**
             * <pre>
             *  If n is the partCol count , if k <= n,
             *  (p_1=c_1) AND (p_2=c_2) AND... ADN (p_{k-1}=c_{k-1}) AND (p_k=c_k)
             *
             *  =>
             *
             *  (c_1,c_2,...,c_{k-1},c_k, min_{k+1},...,min_n) <= (p1,p2,...,pn) <= (c1,c2,..., c_{k-1},c_k,max_{k+1},...,max_n)
             *
             *  </pre>
             */

            // prepare vector range bound for (c_1,c_2,...,c_{k-1},c_k
            for (int i = 0; i <= partKeyEnd; i++) {
                bndExecArr[i] = partIntervalArr[i];
            }

            if (isForLowerBnd) {
                /**
                 * Build lower bound of range : >= (c_1,c_2,...,c_{k-1},c_k, min_{k+1},...,min_n)
                 */

                // prepare vector range bound for min_{k+1},...,min_n
                for (int i = partKeyEnd + 1; i < partColCnt; i++) {
                    bndExecArr[i] = PartClauseExprExec.MIN_VAL;
                }
                newCmpKindOutput[0] = ComparisonKind.GREATER_THAN_OR_EQUAL;

            } else {
                /**
                 * Build upper bound of range :  <= (c1,c2,..., c_{k-1},c_k,max_{k+1},...,max_n)
                 */

                // prepare vector range bound for max_{k+1},...,max_n
                for (int i = partKeyEnd + 1; i < partColCnt; i++) {
                    bndExecArr[i] = PartClauseExprExec.MAX_VAL;
                }
                newCmpKindOutput[0] = ComparisonKind.LESS_THAN_OR_EQUAL;
            }

        } else if (cmpKind == ComparisonKind.LESS_THAN_OR_EQUAL) {

            /**
             * <pre>
             *  If n is the partCol count , if k <= n,
             *  (p_1=c_1) AND (p_2=c_2) AND... ADN (p_{k-1}=c_{k-1}) AND (p_k<=c_k)
             *
             *  =>
             *
             *  (c_1,c_2,...,c_{k-1},min_k, min_{k+1},...,min_n) < (p1,p2,...,pn) <= (c1,c2,..., c_{k-1},c_k,max_{k+1},...,max_n)
             *
             *  </pre>
             */

            // prepare vector range bound for c_1,c_2,...,c_{k-1}
            for (int i = 0; i <= partKeyEnd - 1; i++) {
                bndExecArr[i] = partIntervalArr[i];
            }

            if (isForLowerBnd) {
                /**
                 * Build lower bound of range :  > (c_1,c_2,...,c_{k-1},min_k, min_{k+1},...,min_n)
                 */

                // prepare vector range bound for min_k, min_{k+1},...,min_n
                for (int i = partKeyEnd; i < partColCnt; i++) {
                    bndExecArr[i] = PartClauseExprExec.MIN_VAL;
                }
                newCmpKindOutput[0] = ComparisonKind.GREATER_THAN;

            } else {
                /**
                 * Build upper bound of range :  <= (c1,c2,..., c_{k-1},c_k,max_{k+1},...,max_n)
                 */

                // prepare vector range bound for c_k,max_{k+1},...,max_n
                bndExecArr[partKeyEnd] = partIntervalArr[partKeyEnd];
                for (int i = partKeyEnd + 1; i < partColCnt; i++) {
                    bndExecArr[i] = PartClauseExprExec.MAX_VAL;
                }
                newCmpKindOutput[0] = ComparisonKind.LESS_THAN_OR_EQUAL;
            }

        } else if (cmpKind == ComparisonKind.LESS_THAN) {
            /**
             * <pre>
             *  If n is the partCol count , if k <= n ,
             *  (p_1=c_1) AND (p_2=c_2) AND... ADN (p_{k-1}=c_{k-1}) AND (p_k<c_k)
             *
             *  =>
             *
             *  (c_1,c_2,...,c_{k-1},min_k, min_{k+1},...,min_n) < (p1,p2,...,pn) < (c1,c2,..., c_{k-1},c_k,min_{k+1},...,min_n)
             *
             *  </pre>
             */

            // prepare vector range bound for c_1,c_2,...,c_{k-1}
            for (int i = 0; i <= partKeyEnd - 1; i++) {
                bndExecArr[i] = partIntervalArr[i];
            }

            if (isForLowerBnd) {
                /**
                 * Build lower bound of range : > (c_1,c_2,...,c_{k-1},min_k, min_{k+1},...,min_n)
                 */

                // prepare vector range bound for min_k, min_{k+1},...,min_n
                for (int i = partKeyEnd; i < partColCnt; i++) {
                    bndExecArr[i] = PartClauseExprExec.MIN_VAL;
                }
                newCmpKindOutput[0] = ComparisonKind.GREATER_THAN;

            } else {
                /**
                 * Build lower bound of range : < (c1,c2,..., c_{k-1},c_k, min_{k+1},...,min_n)
                 */

                // prepare vector range bound for c_k,min_{k+1},...,min_n
                bndExecArr[partKeyEnd] = partIntervalArr[partKeyEnd];
                for (int i = partKeyEnd + 1; i < partColCnt; i++) {
                    bndExecArr[i] = PartClauseExprExec.MIN_VAL;
                }
                newCmpKindOutput[0] = ComparisonKind.LESS_THAN;
            }
        }
    }

    protected static boolean isPositiveInfinite(PartClauseExprExec[] bndExec) {
        for (int i = 0; i < bndExec.length; i++) {
            if (bndExec[i] != PartClauseExprExec.MAX_VAL) {
                return false;
            }
        }
        return true;
    }

    protected static boolean isNegativeInfinite(PartClauseExprExec[] bndExec) {
        for (int i = 0; i < bndExec.length; i++) {
            if (bndExec[i] != PartClauseExprExec.MIN_VAL) {
                return false;
            }
        }
        return true;
    }

//    protected static PartitionPruneStepOp buildStepOp(PartPruneStepBuildingContext currFullContext,
//                                                      PartitionInfo partInfo,
//                                                      PartPredPathInfo partPredPathInfo,
//                                                      PartRouteFunction predRouteFunc,
//                                                      PartKeyLevel partKeyMatchLevel,
//                                                      boolean isConflict,
//                                                      boolean forceFullScan,
//                                                      boolean isScanFirstPartOnly) {
//        PartitionPruneStepOp stepOp = null;
//        if (currFullContext != null) {
//            boolean enableIntervalMerging = currFullContext.isEnableIntervalMerging() && currFullContext.isDnfFormula();
//            stepOp =
//                new PartitionPruneStepOp(partInfo, partPredPathInfo, predRouteFunc, partKeyMatchLevel,
//                    enableIntervalMerging, isConflict, forceFullScan, isScanFirstPartOnly);
//            PartPruneStepReferenceInfo stepReferenceInfo = currFullContext.buildPruneStepReferenceInfo(stepOp);
//            stepOp = (PartitionPruneStepOp) stepReferenceInfo.getStep();
//        } else {
//            stepOp =
//                new PartitionPruneStepOp(partInfo, partPredPathInfo, predRouteFunc, partKeyMatchLevel, true,
//                    isConflict,
//                    forceFullScan,
//                    isScanFirstPartOnly);
//
//        }
//        return stepOp;
//    }

    protected static PartitionPruneStepOp buildStepOp(BuildStepOpParams buildParams) {
        PartitionPruneStepOp stepOp = null;
        PartPruneStepBuildingContext currFullContext = buildParams.getCurrFullContext();
        if (currFullContext != null) {
            boolean enableIntervalMerging = currFullContext.isEnableIntervalMerging() && currFullContext.isDnfFormula();
            buildParams.setEnableRangeMerge(enableIntervalMerging);
            stepOp = new PartitionPruneStepOp(buildParams);
            PartPruneStepReferenceInfo stepReferenceInfo = currFullContext.buildPruneStepReferenceInfo(stepOp);
            stepOp = (PartitionPruneStepOp) stepReferenceInfo.getStep();
        } else {
            buildParams.setEnableRangeMerge(true);
            stepOp = new PartitionPruneStepOp(buildParams);
        }
        return stepOp;
    }

    protected static PartitionPruneStepCombine buildStepCombine(PartitionInfo partInfo,
                                                                PartPruneStepType stepType,
                                                                List<PartitionPruneStep> subSteps,
                                                                boolean enableRangeMerge,
                                                                StepIntervalInfo rangeInfo,
                                                                PartPruneStepBuildingContext buildContext) {

        PartitionPruneStepCombine stepCombine = new PartitionPruneStepCombine(stepType);
        stepCombine.setSubSteps(subSteps);
        if (enableRangeMerge) {
            StepIntervalMerger rangeMerger = null;
            if (stepType == PartPruneStepType.PARTPRUNE_COMBINE_UNION) {
                rangeMerger =
                    PartitionPruneStepIntervalAnalyzer.buildUnionStepRangeMerger(partInfo, stepCombine, buildContext);
            } else if (stepType == PartPruneStepType.PARTPRUNE_COMBINE_INTERSECT) {
                rangeMerger = PartitionPruneStepIntervalAnalyzer
                    .buildIntersectStepRangeMerger(partInfo, stepCombine, buildContext);
            }
            stepCombine.setIntervalMerger(rangeMerger);
        }
        if (rangeInfo != null) {
            stepCombine.setIntervalInfo(rangeInfo);
        }
        return stepCombine;
    }

    /**
     * ##################### build prune step for subpart  ##############################
     */
    protected static PartitionPruneStep genCompletedPartPruneSteps(PartitionInfo partInfo,
                                                                   RelDataType relRowType,
                                                                   RexNode partPredInfo,
                                                                   ExecutionContext ec) {

        if (partPredInfo == null) {
            return genFullScanPruneStepInfoInner(partInfo, partInfo.getPartitionBy().getPhysicalPartLevel(), true);
        }

        AtomicInteger constExprIdGenerator = new AtomicInteger(0);
        ExprContextProvider exprCtxProvider = new ExprContextProvider();

        PartitionPruneStep subPartPruneStep = null;
        if (partInfo.getPartitionBy().getSubPartitionBy() != null) {
            subPartPruneStep =
                genPartPruneStepsForPartLevel(partInfo, PartKeyLevel.SUBPARTITION_KEY, relRowType, partPredInfo,
                    constExprIdGenerator, exprCtxProvider, ec);
            if (subPartPruneStep == null) {
                subPartPruneStep = genFullScanPruneStepInfoInner(partInfo, PartKeyLevel.SUBPARTITION_KEY, false);
            }
        }

        /**
         * Generate pruneStep for 1st-level-partition
         */
        PartitionPruneStep partPruneStep = genPartPruneStepsForPartLevel(partInfo,
            PartKeyLevel.PARTITION_KEY, relRowType, partPredInfo, constExprIdGenerator, exprCtxProvider, ec);
        if (partPruneStep == null) {
            partPruneStep = genFullScanPruneStepInfoInner(partInfo, PartKeyLevel.PARTITION_KEY, false);
        }

        if (subPartPruneStep != null) {
            PartitionPruneSubPartStepOr subPartStepOr =
                new PartitionPruneSubPartStepOr(partInfo.getPartitionBy(), subPartPruneStep);
            PartitionPruneSubPartStepAnd subPartStepAnd =
                new PartitionPruneSubPartStepAnd(partPruneStep, subPartStepOr);
            return subPartStepAnd;
        } else {
            return partPruneStep;
        }
    }

    /**
     * Generate the pruning step by sharding predicates
     */
    protected static PartitionPruneStep genPartPruneStepsForPartLevel(PartitionInfo partInfo,
                                                                      PartKeyLevel partLevel,
                                                                      RelDataType relRowType,
                                                                      RexNode partPredInfo,
                                                                      AtomicInteger constExprIdGenerator,
                                                                      ExprContextProvider exprCtxProvider,
                                                                      ExecutionContext ec) {

        PartPruneStepBuildingContext stepContext =
            PartPruneStepBuildingContext
                .getNewPartPruneStepContext(partInfo, partLevel, constExprIdGenerator, exprCtxProvider, ec);

        /**
         * Rewrite partition predicate & toDnf
         */
        if (ec != null && ec.getParams() != null) {
            partPredInfo = RexUtil.recoverInExpr(partPredInfo, ec.getParams().getCurrentParameter());
        }
        RexNode rewrotePartPred =
            PartPredRewriter.rewritePartPredicate(partInfo, relRowType, partPredInfo, stepContext);

        /**
         * Check If the predicate expr is two complex and its OpSteps are too many,
         * then it will lead to giving up pruning and return  full scan step
         */
        boolean needGiveUpPruning = checkIfNeedGiveUpPruning(partInfo, stepContext, rewrotePartPred);
        if (needGiveUpPruning) {
            return genFullScanPruneStepInfoInner(partInfo, stepContext.getPartLevel(), false);
        }

        /**
         *
         * Simplify the predicates which have been finishing DNF conversion, include:
         *
         *      1. all the opExpr in OR/AND which contains NOT any partition columns will treated as Always-True expr;
         *      2. all the opExpr in OR Expr which partitionKey is NOT the first partition columns is treated as Always-True expr;
         *      3. all opExpr in a AND Expr which partitionKeys DOES NOT contains the first partition column are treated as Always-True expr;
         *
         * <pre>
         *
         * pre process the predicate and
         * convert predicateInfo to the uniform PartClauseItem
         * which PartClauseItem will be using as prefix predicate enumeration
         *
         *
         * </pre>
         */
        PartClauseItem clauseItem =
            PartClauseInfoPreProcessor.convertToPartClauseItem(stepContext.getPartByDef(), relRowType, rewrotePartPred,
                stepContext);

        /**
         * Build the PartPruneStep by the PartClauseItem rewrited from rexnode
         */
        PartitionPruneStep pruneStep = genPartPruneStepsInner(partInfo, relRowType, clauseItem, stepContext);
        if (pruneStep == null) {
            pruneStep = genFullScanPruneStepInfoInner(partInfo, stepContext.getPartLevel(), false);
        }

        return pruneStep;
    }

}
