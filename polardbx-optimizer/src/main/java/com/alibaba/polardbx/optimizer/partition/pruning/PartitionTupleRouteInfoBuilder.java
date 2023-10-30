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
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypeUtil;
import com.alibaba.polardbx.optimizer.core.expression.calc.DynamicParamExpression;
import com.alibaba.polardbx.optimizer.core.expression.calc.IExpression;
import com.alibaba.polardbx.optimizer.core.function.SqlSubStrFunction;
import com.alibaba.polardbx.optimizer.core.rel.LogicalInsert;
import com.alibaba.polardbx.optimizer.partition.PartitionByDefinition;
import com.alibaba.polardbx.optimizer.partition.boundspec.PartitionBoundValueKind;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.partition.PartitionSpec;
import com.alibaba.polardbx.optimizer.partition.common.PartKeyLevel;
import com.alibaba.polardbx.optimizer.partition.common.PartitionStrategy;
import com.alibaba.polardbx.optimizer.partition.datatype.function.PartitionFunctionBuilder;
import com.alibaba.polardbx.optimizer.partition.datatype.function.PartitionIntFunction;
import com.alibaba.polardbx.optimizer.utils.ExprContextProvider;
import com.alibaba.polardbx.optimizer.utils.RexUtils;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.hep.HepRelVertex;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.DynamicValues;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

/**
 * @author chenghui.lch
 */
public class PartitionTupleRouteInfoBuilder {

    /**
     * Build PartitionTupleRouteInfo by LogicalInsert plan
     */
    public static PartitionTupleRouteInfo genPartTupleRoutingInfo(LogicalInsert insert,
                                                                  PartitionInfo partitionInfo) {

        String schemaName = insert.getSchemaName();
        String logTbName = insert.getLogicalTableName();
        boolean isSourceSelect = insert.isSourceSelect();
        RelDataType valueRowType = insert.getInsertRowType();
        RelNode inputRel = insert.getInput();
        DynamicValues insertValues = null;
        if (inputRel instanceof DynamicValues) {
            insertValues = (DynamicValues) inputRel;
        } else if (inputRel instanceof HepRelVertex) {
            RelNode curRel = ((HepRelVertex) inputRel).getCurrentRel();
            insertValues = ((DynamicValues) curRel);
        }

        List<List<RexNode>> allTuples = new ArrayList<>();
        ImmutableList<ImmutableList<RexNode>> tuplesOfValues = insertValues.getTuples();
        for (int i = 0; i < tuplesOfValues.size(); i++) {
            ImmutableList<RexNode> oneTuple = tuplesOfValues.get(i);
            List<RexNode> oneTupleVal = new ArrayList<>();
            oneTupleVal.addAll(oneTuple);
            allTuples.add(oneTupleVal);
        }

        if (!isSourceSelect) {
            return genPartTupleRoutingInfo(schemaName, logTbName, valueRowType, allTuples, partitionInfo);
        } else {
            throw GeneralUtil.nestedException(new NotSupportException("insert select with partition table"));
        }
    }

//    public static PartitionTupleRouteInfo generateTupleRoutingInfoFromPruneStepOp(PartitionPruneStepOp op) {
//        PartitionInfo partInfo = op.getPartInfo();
//        PartPredicateRouteFunction partPredRouteFunc = (PartPredicateRouteFunction) op.getPredRouteFunc();
//        ComparisonKind comparisonKind = partPredRouteFunc.getCmpKind();
//        if (comparisonKind != ComparisonKind.EQUAL) {
//            return null;
//        }
//        PartClauseExprExec[] epxrExecArr = partPredRouteFunc.getSearchExprInfo().getExprExecArr();
//        List<PartClauseInfo> partClauseInfos = new ArrayList<>();
//        for (int i = 0; i < epxrExecArr.length; i++) {
//            if (epxrExecArr[i].getValueKind() != PartitionBoundValueKind.DATUM_NORMAL_VALUE) {
//                return null;
//            }
//            partClauseInfos.add(epxrExecArr[i].getClauseInfo());
//        }
//        String schemaName = partInfo.getTableSchema();
//        String logTbName = partInfo.getTableName();
//        List<List<PartClauseInfo>> partClauseInfosOfAllTuples = new ArrayList<>();
//        partClauseInfosOfAllTuples.add(partClauseInfos);
//        PartitionTupleRouteInfo tupleRouteInfo =
//            genPartTupleRoutingInfoByPartClauseInfos(schemaName, logTbName, partClauseInfosOfAllTuples, partInfo);
//        return tupleRouteInfo;
//    }

    /**
     * Generate a TupleRouteInfo by the insert value astNodes that are all SqlLiteral or SqlDynamic, NOT any SqlCall.
     */
    public static PartitionTupleRouteInfo genPartTupleRoutingInfo(String schemaName,
                                                                  String logTbName,
                                                                  PartitionInfo specificPartInfo,
                                                                  RelDataType valueRowType,
                                                                  List<List<SqlNode>> astValues,
                                                                  ExecutionContext ec) {

        PartitionTupleRouteInfo tupleRouteInfo = new PartitionTupleRouteInfo();

        PartitionInfo partInfo = null;
        if (specificPartInfo != null) {
            partInfo = specificPartInfo;
        } else {
            partInfo =
                ec.getSchemaManager(schemaName).getTable(logTbName).getPartitionInfo();
        }

        tupleRouteInfo.setSchemaName(schemaName);
        tupleRouteInfo.setTableName(logTbName);
        tupleRouteInfo.setPartInfo(partInfo);

        List<List<SqlNode>> astTuples = astValues;
        List<List<RexNode>> rexTuples = new ArrayList<>();

        // Build rexTuple for each astTuple
        for (int i = 0; i < astValues.size(); i++) {
            List<SqlNode> astTuple = astTuples.get(i);
            List<RexNode> rexTuple = new ArrayList<>();
            for (int j = 0; j < astTuple.size(); j++) {
                RelDataType fldType = valueRowType.getFieldList().get(j).getType();
                RexNode rexDynamicParam = new RexDynamicParam(fldType, j);
                rexTuple.add(rexDynamicParam);
            }
            rexTuples.add(rexTuple);
        }

//        List<PartTupleDispatchInfo> dispatchFuncInfos = new ArrayList<>();
//        for (int t = 0; t < rexTuples.size(); t++) {
//            PartTupleDispatchInfo dispatchFuncInfo = new PartTupleDispatchInfo(tupleRouteInfo, t);
//            List<RexNode> tupleRexInfo = rexTuples.get(t);
//            List<PartClauseInfo> partClauseInfos =
//                matchInsertValuesToPartKey(partInfo, valueRowType, tupleRexInfo,
//                    PartKeyLevel.PARTITION_KEY);
//
//            PartTupleRouteFunction tupleRouteFunction =
//                buildTupleRouteFunction(partInfo, PartKeyLevel.PARTITION_KEY, partClauseInfos);
//            dispatchFuncInfo.setPartDispatchFunc(tupleRouteFunction);
//
//            if (partInfo.getPartitionBy().getSubPartitionBy() != null) {
//                List<PartClauseInfo> subPartClauseInfos =
//                    matchInsertValuesToPartKey(partInfo, valueRowType, tupleRexInfo,
//                        PartKeyLevel.SUBPARTITION_KEY);
//                PartTupleRouteFunction subTupleRouteFunction =
//                    buildTupleRouteFunction(partInfo, PartKeyLevel.SUBPARTITION_KEY, subPartClauseInfos);
//                tupleRouteFunction.setSubPartDispatchFunc(subTupleRouteFunction);
//            }
//            dispatchFuncInfos.add(dispatchFuncInfo);
//        }
//        tupleRouteInfo.setTupleDispatchFuncInfos(dispatchFuncInfos);

        tupleRouteInfo = genPartTupleRoutingInfo(schemaName, logTbName, valueRowType, rexTuples, partInfo);
        return tupleRouteInfo;
    }

    private static List<List<PartClauseInfo>> tryFetchPartClauseInfoFromSinglePointStep(PartitionPruneStep step) {
        if (!(step instanceof PartitionPruneStepOp)) {
            return null;
        }
        PartitionPruneStepOp op = (PartitionPruneStepOp) step;
        PartPredicateRouteFunction partPredRouteFunc = (PartPredicateRouteFunction) op.getPredRouteFunc();
        ComparisonKind comparisonKind = partPredRouteFunc.getCmpKind();
        if (comparisonKind != ComparisonKind.EQUAL) {
            return null;
        }
        PartClauseExprExec[] epxrExecArr = partPredRouteFunc.getSearchExprInfo().getExprExecArr();
        List<PartClauseInfo> partClauseInfos = new ArrayList<>();
        for (int i = 0; i < epxrExecArr.length; i++) {
            if (epxrExecArr[i].getValueKind() != PartitionBoundValueKind.DATUM_NORMAL_VALUE) {
                return null;
            }
            partClauseInfos.add(epxrExecArr[i].getClauseInfo());
        }
        List<List<PartClauseInfo>> partClauseInfosOfAllTuples = new ArrayList<>();
        partClauseInfosOfAllTuples.add(partClauseInfos);
        return partClauseInfosOfAllTuples;
    }

    public static PartitionTupleRouteInfo genTupleRoutingInfoFromPruneStep(PartitionPruneStep step) {
        if (step instanceof PartitionPruneStepOp) {
            PartitionPruneStepOp op = (PartitionPruneStepOp) step;
            List<List<PartClauseInfo>> partClauseInfosOfAllTuples = tryFetchPartClauseInfoFromSinglePointStep(op);
            if (partClauseInfosOfAllTuples == null) {
                return null;
            }
            PartitionInfo opPartInfo = op.getPartInfo();
            String schemaName = opPartInfo.getTableSchema();
            String logTbName = opPartInfo.getTableName();
            PartitionTupleRouteInfo tupleRouteInfo =
                genPartTupleRoutingInfoByPartClauseInfos(schemaName, logTbName, partClauseInfosOfAllTuples, null,
                    opPartInfo);
            return tupleRouteInfo;
        } else if (step instanceof PartitionPruneSubPartStepAnd) {
            PartitionPruneSubPartStepAnd subStepAnd = (PartitionPruneSubPartStepAnd) step;
            PartitionPruneStep partLevelStep = subStepAnd.getSubStepByPartLevel(PartKeyLevel.PARTITION_KEY);
            PartitionPruneStep subPartLevelStep = subStepAnd.getSubStepByPartLevel(PartKeyLevel.SUBPARTITION_KEY);
            if (partLevelStep.getStepType() != PartPruneStepType.PARTPRUNE_OP_MATCHED_PART_KEY) {
                return null;
            }
            if (subPartLevelStep.getStepType() != PartPruneStepType.PARTPRUNE_OP_MATCHED_PART_KEY) {
                return null;
            }

            List<List<PartClauseInfo>> partClauseInfosOfAllTuples =
                tryFetchPartClauseInfoFromSinglePointStep(partLevelStep);
            if (partClauseInfosOfAllTuples == null) {
                return null;
            }

            List<List<PartClauseInfo>> subPartClauseInfosOfAllTuples =
                tryFetchPartClauseInfoFromSinglePointStep(subPartLevelStep);
            if (subPartClauseInfosOfAllTuples == null) {
                return null;
            }

            PartitionInfo partInfo = subStepAnd.getPartitionInfo();
            String schemaName = partInfo.getTableSchema();
            String logTbName = partInfo.getTableName();
            PartitionTupleRouteInfo tupleRouteInfo =
                genPartTupleRoutingInfoByPartClauseInfos(schemaName, logTbName, partClauseInfosOfAllTuples,
                    subPartClauseInfosOfAllTuples, partInfo);
            return tupleRouteInfo;
        } else {
            return null;
        }
    }

//    private static List<PartClauseInfo> tryFetchPartClauseInfoFromSinglePointStep(PartitionPruneStep step) {
//        if (step instanceof PartitionPruneStepOp) {
//            PartitionPruneStepOp op = (PartitionPruneStepOp) step;
//            PartitionInfo partInfo = op.getPartInfo();
//            PartPredicateRouteFunction partPredRouteFunc = (PartPredicateRouteFunction) op.getPredRouteFunc();
//            ComparisonKind comparisonKind = partPredRouteFunc.getCmpKind();
//            if (comparisonKind != ComparisonKind.EQUAL) {
//                return null;
//            }
//            PartClauseExprExec[] epxrExecArr = partPredRouteFunc.getSearchExprInfo().getExprExecArr();
//            List<PartClauseInfo> partClauseInfos = new ArrayList<>();
//            for (int i = 0; i < epxrExecArr.length; i++) {
//                if (epxrExecArr[i].getValueKind() != PartitionBoundValueKind.DATUM_NORMAL_VALUE ) {
//                    return null;
//                }
//                partClauseInfos.add(epxrExecArr[i].getClauseInfo());
//            }
//            return partClauseInfos;
//        }
//        return null;
//    }

    public static PartitionTupleRouteInfo genTupleRoutingInfoFromPruneStepOp(PartitionPruneStepOp op) {
        PartitionInfo partInfo = op.getPartInfo();
        PartPredicateRouteFunction partPredRouteFunc = (PartPredicateRouteFunction) op.getPredRouteFunc();
        ComparisonKind comparisonKind = partPredRouteFunc.getCmpKind();
        if (comparisonKind != ComparisonKind.EQUAL) {
            return null;
        }
        PartClauseExprExec[] epxrExecArr = partPredRouteFunc.getSearchExprInfo().getExprExecArr();
        List<PartClauseInfo> partClauseInfos = new ArrayList<>();
        for (int i = 0; i < epxrExecArr.length; i++) {
            if (epxrExecArr[i].getValueKind() != PartitionBoundValueKind.DATUM_NORMAL_VALUE) {
                return null;
            }
            partClauseInfos.add(epxrExecArr[i].getClauseInfo());
        }
        String schemaName = partInfo.getTableSchema();
        String logTbName = partInfo.getTableName();
        List<List<PartClauseInfo>> partClauseInfosOfAllTuples = new ArrayList<>();
        partClauseInfosOfAllTuples.add(partClauseInfos);
        PartitionTupleRouteInfo tupleRouteInfo =
            genPartTupleRoutingInfoByPartClauseInfos(schemaName, logTbName, partClauseInfosOfAllTuples, null, partInfo);
        return tupleRouteInfo;
    }

    /**
     * Get the PartitionSpec by specifying a const expressions of all partition columns.
     * If return null means no found any partitions
     */
    public static PartitionSpec getPartitionSpecByExprValues(PartitionInfo partInfo,
                                                             List<RexNode> exprValRexNodes,
                                                             ExecutionContext executionContext) {

        PartitionTupleRouteInfo tupleRouteInfo =
            genTupleRouteInfoByRexExprNodes(partInfo, exprValRexNodes);

        PartPrunedResult partPrunedResult =
            PartitionPruner.doPruningByTupleRouteInfo(tupleRouteInfo, 0, executionContext);
        List<PhysicalPartitionInfo> prunedPartInfos = partPrunedResult.getPrunedParttions();

        PartitionSpec partitionSpec = null;
        if (prunedPartInfos.size() == 0) {
            return partitionSpec;
        }

        PhysicalPartitionInfo prunedPartInfo = prunedPartInfos.get(0);
        String partName = prunedPartInfo.getPartName();
        partitionSpec = partInfo.getPartitionBy().getPhysicalPartitionByPartName(partName);

        return partitionSpec;
    }

    /**
     * Compute the hashcode of a const expressions
     */
    public static List<Long[]> computeExprValuesHashCode(PartitionInfo partInfo,
                                                         List<RexNode> exprValRexNodes,
                                                         ExecutionContext executionContext) {

        assert partInfo.getPartitionBy().getStrategy() == PartitionStrategy.HASH
            || partInfo.getPartitionBy().getStrategy() == PartitionStrategy.KEY;
        PartitionTupleRouteInfo tupleRouteInfo =
            genTupleRouteInfoByRexExprNodes(partInfo, exprValRexNodes);
        List<SearchDatumInfo> dataums = tupleRouteInfo.calcSearchDatum(0, executionContext, null);
        List<Long[]> hashResults = new ArrayList<>();
        for (int k = 0; k < dataums.size(); k++) {
            SearchDatumInfo dataum = dataums.get(k);
            int len = dataum.getDatumInfo().length;
            Long[] hashVals = new Long[len];
            for (int i = 0; i < len; i++) {
                hashVals[i] = dataum.getDatumInfo()[i].getValue().longValue();
            }
            hashResults.add(hashVals);
        }
        return hashResults;
    }

    public static PartitionSpec getPartitionSpecByHashCode(Long[] hashCode,
                                                           PartKeyLevel partLevel,
                                                           Integer parentPartPosi,
                                                           PartitionInfo partInfo,
                                                           ExecutionContext ec) {
        assert partInfo.getPartitionBy().getStrategy() == PartitionStrategy.KEY || (
            partInfo.getPartitionBy().getSubPartitionBy() != null
                && partInfo.getPartitionBy().getSubPartitionBy().getStrategy() == PartitionStrategy.KEY);

        PartitionRouter routerVal = PartRouteFunction.getRouterByPartInfo(partLevel, parentPartPosi, partInfo);
        KeyPartRouter router = (KeyPartRouter) routerVal;
        BitSet partBitSet = PartitionPrunerUtils.buildEmptyPartitionsBitSetByPartRouter(router);

        int partitionColumnSize;
        boolean useFullSubPartBitSet;
        if (partLevel == PartKeyLevel.SUBPARTITION_KEY) {
            partitionColumnSize = partInfo.getPartitionBy().getSubPartitionBy().getPartitionColumnNameList().size();
            useFullSubPartBitSet = true;
        } else {
            partitionColumnSize = partInfo.getPartitionBy().getPartitionColumnNameList().size();
            useFullSubPartBitSet = false;
        }

        PartitionRouter.RouterResult result = router
            .routePartitionsFromHashCode(ec, ComparisonKind.EQUAL, partitionColumnSize, hashCode);
        // Put the pruned result into the partition bitset
        if (result.strategy != PartitionStrategy.LIST && result.strategy != PartitionStrategy.LIST_COLUMNS) {
            PartitionPrunerUtils
                .setPartBitSetByStartEnd(partBitSet, result.partStartPosi, result.pasrEndPosi, true);
        } else {
            PartitionPrunerUtils
                .setPartBitSetForPartList(partBitSet, result.partPosiSet, true);
        }
        PartPrunedResult partPrunedResult =
            PartPrunedResult.buildPartPrunedResult(partInfo, partBitSet, partLevel, parentPartPosi,
                useFullSubPartBitSet);
        List<PhysicalPartitionInfo> prunedPartInfos = partPrunedResult.getPrunedParttions();
        PartitionSpec partitionSpec = null;
        if (prunedPartInfos.size() == 0) {
            return partitionSpec;
        }

        assert partBitSet.size() == 1;
        int pos = partBitSet.nextSetBit(0);
        if (partLevel == PartKeyLevel.SUBPARTITION_KEY) {
            PartitionSpec parentPartitionSpec = partInfo.getPartitionBy().getNthPartition(parentPartPosi);
            partitionSpec =
                parentPartitionSpec.getSubPartitions().stream().filter(o -> o.getPosition().intValue() == (pos + 1))
                    .findFirst().orElse(null);
        } else {
            partitionSpec = partInfo.getPartitionBy().getNthPartition(pos + 1);
        }

        return partitionSpec;
    }

    /**
     * Generate the a TupleRouteInfo by the DynamicValues(relNode) of insert.getInput() relNode that all value nodes
     * in DynamicValues(relNode) is a rexNode
     */
    protected static PartitionTupleRouteInfo genPartTupleRoutingInfo(String schemaName,
                                                                     String logTbName,
                                                                     RelDataType valueRowType,
                                                                     List<List<RexNode>> tuples,
                                                                     PartitionInfo partInfo) {
        List<List<PartClauseInfo>> partClauseInfoOfAllTuple = new ArrayList<>();
        List<List<PartClauseInfo>> subPartClauseInfoOfAllTuple = new ArrayList<>();
        for (int t = 0; t < tuples.size(); t++) {
            List<RexNode> tupleRexInfo = tuples.get(t);
            List<PartClauseInfo> partClauseInfos =
                matchInsertValuesToPartKey(partInfo, valueRowType, tupleRexInfo,
                    PartKeyLevel.PARTITION_KEY);
            partClauseInfoOfAllTuple.add(partClauseInfos);

            List<PartClauseInfo> subPartClauseInfos =
                matchInsertValuesToPartKey(partInfo, valueRowType, tupleRexInfo,
                    PartKeyLevel.SUBPARTITION_KEY);
            if (subPartClauseInfos != null && !subPartClauseInfos.isEmpty()) {
                subPartClauseInfoOfAllTuple.add(subPartClauseInfos);
            }
        }
        return genPartTupleRoutingInfoByPartClauseInfos(schemaName, logTbName, partClauseInfoOfAllTuple,
            subPartClauseInfoOfAllTuple, partInfo);
    }

    protected static PartitionTupleRouteInfo genPartTupleRoutingInfoByPartClauseInfos(String schemaName,
                                                                                      String logTbName,
                                                                                      List<List<PartClauseInfo>> partConstExprOfAllTuples,
                                                                                      List<List<PartClauseInfo>> subPartConstExprOfAllTuples,
                                                                                      PartitionInfo partInfo) {
        PartitionTupleRouteInfo tupleRouteInfo = new PartitionTupleRouteInfo();
        tupleRouteInfo.setSchemaName(schemaName);
        tupleRouteInfo.setTableName(logTbName);
        tupleRouteInfo.setPartInfo(partInfo);
        List<PartTupleDispatchInfo> dispatchFuncInfos = new ArrayList<>();
        boolean useSubPartBy = subPartConstExprOfAllTuples != null && !subPartConstExprOfAllTuples.isEmpty();
        for (int t = 0; t < partConstExprOfAllTuples.size(); t++) {
            List<PartClauseInfo> partClauseInfos = partConstExprOfAllTuples.get(t);
            PartTupleRouteFunction tupleRouteFunction =
                buildTupleRouteFunction(partInfo, PartKeyLevel.PARTITION_KEY, partClauseInfos);
            if (useSubPartBy) {
                List<PartClauseInfo> subPartClauseInfos = subPartConstExprOfAllTuples.get(t);
                PartTupleRouteFunction subTupleRouteFunction =
                    buildTupleRouteFunction(partInfo, PartKeyLevel.SUBPARTITION_KEY, subPartClauseInfos);
                tupleRouteFunction.setSubPartDispatchFunc(subTupleRouteFunction);
            }
            PartTupleDispatchInfo dispatchFuncInfo = new PartTupleDispatchInfo(tupleRouteInfo, 0);
            dispatchFuncInfo.setPartDispatchFunc(tupleRouteFunction);
            dispatchFuncInfos.add(dispatchFuncInfo);
        }
        tupleRouteInfo.setTupleDispatchFuncInfos(dispatchFuncInfos);
        return tupleRouteInfo;
    }

    protected static PartitionTupleRouteInfo genTupleRouteInfoByRexExprNodes(PartitionInfo partInfo,
                                                                             List<RexNode> exprValRexNodes) {
        String schemaName = partInfo.getTableSchema();
        String logTbName = partInfo.getTableName();

        List<List<PartClauseInfo>> partClauseInfoOfAllTuple = new ArrayList<>();
        List<PartClauseInfo> partClauseInfos =
            matchPartClauseInfoByRexInfos(partInfo, exprValRexNodes, PartKeyLevel.PARTITION_KEY);
        partClauseInfoOfAllTuple.add(partClauseInfos);

        List<List<PartClauseInfo>> subPartClauseInfoOfAllTuple = new ArrayList<>();
        List<PartClauseInfo> subPartClauseInfos =
            matchPartClauseInfoByRexInfos(partInfo, exprValRexNodes, PartKeyLevel.SUBPARTITION_KEY);
        if (!subPartClauseInfos.isEmpty()) {
            subPartClauseInfoOfAllTuple.add(subPartClauseInfos);
        }
        PartitionTupleRouteInfo tupleRouteInfo = genPartTupleRoutingInfoByPartClauseInfos(schemaName,
            logTbName,
            partClauseInfoOfAllTuple,
            subPartClauseInfoOfAllTuple,
            partInfo);
        return tupleRouteInfo;
    }

    protected static List<PartClauseInfo> matchPartClauseInfoByRexInfos(PartitionInfo partInfo,
                                                                        List<RexNode> constValRexInfo,
                                                                        PartKeyLevel matchLevel) {

        List<ColumnMeta> partColMetaList = new ArrayList<>();
        if (matchLevel == PartKeyLevel.PARTITION_KEY && partInfo.getPartitionBy() != null) {
            partColMetaList = partInfo.getPartitionBy().getPartitionFieldList();
        }
        if (matchLevel == PartKeyLevel.SUBPARTITION_KEY && partInfo.getPartitionBy().getSubPartitionBy() != null) {
            partColMetaList = partInfo.getPartitionBy().getSubPartitionBy().getPartitionFieldList();
        }

        List<PartClauseInfo> partClauseInfos = new ArrayList<>();
        int partColCnt = partColMetaList.size();
        for (int i = 0; i < partColCnt; i++) {

            ColumnMeta colMeta = partColMetaList.get(i);
            RelDataType constValType = colMeta.getField().getRelType();
            RexNode constValRex = constValRexInfo.get(i);

            PartClauseInfo partClauseInfo = new PartClauseInfo();
            partClauseInfo.setId(i);
            partClauseInfo.setPartKeyDataType(constValType);
            partClauseInfo.setOpKind(SqlKind.EQUALS);
            partClauseInfo.setPartKeyLevel(matchLevel);
            partClauseInfo.setPartKeyIndex(i);
            partClauseInfo.setInput(null);
            partClauseInfo.setConstExpr(constValRex);
            partClauseInfo.setDynamicConstOnly(constValRex instanceof RexDynamicParam);
            partClauseInfo.setIndexInTuple(i);
            partClauseInfos.add(partClauseInfo);
        }

        return partClauseInfos;

    }

    protected static List<PartClauseInfo> matchInsertValuesToPartKey(PartitionInfo partInfo,
                                                                     RelDataType valueRowType,
                                                                     List<RexNode> tupleRexInfo,
                                                                     PartKeyLevel matchLevel) {

        List<String> partColList = new ArrayList<>();
        if (matchLevel == PartKeyLevel.PARTITION_KEY && partInfo.getPartitionBy() != null) {
            partColList = partInfo.getPartitionBy().getPartitionColumnNameList();
        }
        if (matchLevel == PartKeyLevel.SUBPARTITION_KEY && partInfo.getPartitionBy().getSubPartitionBy() != null) {
            partColList = partInfo.getPartitionBy().getSubPartitionBy().getPartitionColumnNameList();
        }

        List<PartClauseInfo> partClauseInfos = new ArrayList<>();
        RelDataType rowType = valueRowType;
        List<RelDataTypeField> fldList = rowType.getFieldList();
        for (int k = 0; k < partColList.size(); k++) {
            String partColName = partColList.get(k);
            for (int i = 0; i < fldList.size(); i++) {
                RelDataTypeField fld = fldList.get(i);
                String fldName = fld.getName();
                if (!partColName.equalsIgnoreCase(fldName)) {
                    continue;
                }

                PartClauseInfo partClauseInfo = new PartClauseInfo();
                partClauseInfo.setPartKeyDataType(fld.getType());
                partClauseInfo.setOpKind(SqlKind.EQUALS);
                partClauseInfo.setPartKeyLevel(matchLevel);
                partClauseInfo.setPartKeyIndex(k);
                partClauseInfo.setInput(null);
                partClauseInfo.setConstExpr(tupleRexInfo.get(i));
                partClauseInfo.setDynamicConstOnly(tupleRexInfo.get(i) instanceof RexDynamicParam);
                partClauseInfo.setIndexInTuple(i);
                partClauseInfos.add(partClauseInfo);
            }

        }
        return partClauseInfos;
    }

    protected static PartTupleRouteFunction buildTupleRouteFunction(PartitionInfo partInfo,
                                                                    PartKeyLevel level,
                                                                    List<PartClauseInfo> partClauseInfoList) {

        PartitionByDefinition partByDef = null;

        if (level == PartKeyLevel.SUBPARTITION_KEY) {
            partByDef = partInfo.getPartitionBy().getSubPartitionBy();
        } else {
            partByDef = partInfo.getPartitionBy();
        }

        int partColNum = partByDef.getPartitionColumnNameList().size();
        SearchDatumComparator querySpaceComp = partByDef.getQuerySpaceComparator();

        int[] partValIndexInTupleArr = new int[partColNum];
        PartClauseExprExec[] partClauseExprExecArr = new PartClauseExprExec[partColNum];
        for (int i = 0; i < partClauseInfoList.size(); i++) {
            PartClauseInfo partClauseInfo = partClauseInfoList.get(i);
            int keyIdx = partClauseInfo.getPartKeyIndex();

            // Get the drds return type for partColumn
            DataType partColDataType = querySpaceComp.getDatumDrdsDataTypes()[keyIdx];
            ColumnMeta partColMeta = null;

            // Build the rexnode by wapping the tuple value with part expr
            // Get the sql operator of part expr
//            SqlOperator sqlOperator = null;
//            SqlNode partExpr = partByDef.getPartitionExprList().get(keyIdx);
//            partColMeta = partByDef.getPartitionFieldList().get(keyIdx);
//            if (partExpr instanceof SqlCall) {
//                SqlCall sqlCall = (SqlCall) partExpr;
//                sqlOperator = ((SqlCall) partExpr).getOperator();
//                List<SqlNode> operands = ((SqlCall) partExpr).getOperandList();
//                if (sqlOperator instanceof SqlSubStrFunction
//                    && sqlCall.getOperandList().size() >= 2) {
//                    SqlSubStrFunction realOp = (SqlSubStrFunction) ((SqlSubStrFunction) sqlOperator).clone();
//                    int position = ((SqlLiteral) sqlCall.operand(1)).intValue(true);
//                    int length = sqlCall.getOperandList().size() < 3 ? Integer.MIN_VALUE :
//                        ((SqlLiteral) sqlCall.operand(2)).intValue(true);
//                    realOp.setPosition(position);
//                    realOp.setLength(length);
//                    sqlOperator = realOp;
//                }
//            }

            partColMeta = partByDef.getPartitionFieldList().get(keyIdx);
            RexNode constVal = partClauseInfo.getConstExpr();
            boolean isAlwaysNull = partClauseInfo.isNull();
            ExprContextProvider exprCxtProvider = new ExprContextProvider();
            IExpression evalFuncExec = RexUtils.getEvalFuncExec(constVal, exprCxtProvider);
            DataType tupleExprReturnDataType = null;
            if (!isAlwaysNull) {
                tupleExprReturnDataType = DataTypeUtil.calciteToDrdsType(constVal.getType());
            }

//            PartitionIntFunction partIntFunc = null;
//            if (sqlOperator != null) {
//                partIntFunc = PartitionPrunerUtils.getPartitionIntFunction(sqlOperator, level, partInfo);
//            }

            PartitionIntFunction partIntFunc = null;
            SqlCall partFuncCall = PartitionFunctionBuilder.getPartFuncCall(level, keyIdx, partInfo);
            if (partFuncCall != null) {
                partIntFunc = PartitionFunctionBuilder.createPartFuncByPartFuncCal(partFuncCall);
            }

            PartClauseExprExec targetExprExecInfo = new PartClauseExprExec(PartitionBoundValueKind.DATUM_NORMAL_VALUE);
            targetExprExecInfo.setExprExec(evalFuncExec);
            targetExprExecInfo.setDynamicConstExprOnly(evalFuncExec instanceof DynamicParamExpression);
            targetExprExecInfo.setNeedOpenEvalResultCache(false);
            targetExprExecInfo.setPartIntFunc(partIntFunc);
            targetExprExecInfo.setClauseInfo(partClauseInfo);
            targetExprExecInfo.setPartColDataType(partColDataType);
            targetExprExecInfo.setPartColMeta(partColMeta);
            targetExprExecInfo.setPredExprReturnType(tupleExprReturnDataType);
            targetExprExecInfo.setPartFldAccessType(PartFieldAccessType.DML_PRUNING);
            targetExprExecInfo.setAlwaysNullValue(isAlwaysNull);
            partValIndexInTupleArr[i] = keyIdx;
            partClauseExprExecArr[i] = targetExprExecInfo;
        }

        PartTupleRouteFunction partTupleRouteFunction = new PartTupleRouteFunction();
        partTupleRouteFunction.setPartClauseExprExecArr(partClauseExprExecArr);
        partTupleRouteFunction.setPartInfo(partInfo);
        partTupleRouteFunction.setCmpKind(ComparisonKind.EQUAL);
        partTupleRouteFunction.setMatchLevel(level);
        partTupleRouteFunction.setStrategy(partByDef.getStrategy());

        return partTupleRouteFunction;
    }

}
