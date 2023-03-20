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
import com.alibaba.polardbx.optimizer.core.rel.LogicalInsert;
import com.alibaba.polardbx.optimizer.partition.PartitionBoundSpec;
import com.alibaba.polardbx.optimizer.partition.PartitionBoundValueKind;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.partition.PartitionSpec;
import com.alibaba.polardbx.optimizer.partition.PartitionStrategy;
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
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

/**
 * @author chenghui.lch
 */
public class PartitionTupleRouteInfoBuilder {

    protected static PartitionTupleRouteInfo generatePartitionTupleRoutingInfo(LogicalInsert insert,
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

        if (!isSourceSelect) {
            return generatePartitionTupleRoutingInfo(schemaName, logTbName, valueRowType, insertValues, partitionInfo);
        } else {
            throw GeneralUtil.nestedException(new NotSupportException("insert select with partition table"));
        }
    }

    public static PartitionTupleRouteInfo generateTupleRoutingInfoFromPruneStepOp(PartitionPruneStepOp op) {
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
            genPartTupleRoutingInfoByPartClauseInfos(schemaName, logTbName, partClauseInfosOfAllTuples, partInfo);
        return tupleRouteInfo;
    }

    /**
     * Generate a TupleRouteInfo by the insert value astNodes that are all SqlLiteral or SqlDynamic, NOT any SqlCall.
     */
    protected static PartitionTupleRouteInfo generatePartitionTupleRoutingInfo(String schemaName,
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

        List<PartTupleDispatchInfo> dispatchFuncInfos = new ArrayList<>();
        for (int t = 0; t < astTuples.size(); t++) {
            PartTupleDispatchInfo dispatchFuncInfo = new PartTupleDispatchInfo(tupleRouteInfo, t);
            List<RexNode> tupleRexInfo = rexTuples.get(t);
            List<PartClauseInfo> partClauseInfos =
                matchInsertValuesToPartKey(partInfo, valueRowType, tupleRexInfo,
                    PartKeyLevel.PARTITION_KEY);

            PartTupleRouteFunction tupleRouteFunction =
                buildTupleRouteFunction(partInfo, PartKeyLevel.PARTITION_KEY, partClauseInfos);
            dispatchFuncInfo.setPartDispatchFunc(tupleRouteFunction);

            if (partInfo.getSubPartitionBy() != null) {
                List<PartClauseInfo> subPartClauseInfos =
                    matchInsertValuesToPartKey(partInfo, valueRowType, tupleRexInfo,
                        PartKeyLevel.SUBPARTITION_KEY);
                PartTupleRouteFunction subTupleRouteFunction =
                    buildTupleRouteFunction(partInfo, PartKeyLevel.SUBPARTITION_KEY, subPartClauseInfos);
                dispatchFuncInfo.setSubPartDispatchFunc(subTupleRouteFunction);
            }
            dispatchFuncInfos.add(dispatchFuncInfo);
        }
        tupleRouteInfo.setTupleDispatchFuncInfos(dispatchFuncInfos);
        return tupleRouteInfo;
    }

    /**
     * Generate the a TupleRouteInfo by the DynamicValues(relNode) of insert.getInput() relNode that all value nodes
     * in DynamicValues(relNode) is a rexNode
     */
    protected static PartitionTupleRouteInfo generatePartitionTupleRoutingInfo(String schemaName,
                                                                               String logTbName,
                                                                               RelDataType valueRowType,
                                                                               DynamicValues values,
                                                                               PartitionInfo partInfo) {
        ImmutableList<ImmutableList<RexNode>> tuples = values.getTuples();
        List<List<PartClauseInfo>> partClauseInfoOfAllTuple = new ArrayList<>();
        for (int t = 0; t < tuples.size(); t++) {
            List<RexNode> tupleRexInfo = tuples.get(t);
            List<PartClauseInfo> partClauseInfos =
                matchInsertValuesToPartKey(partInfo, valueRowType, tupleRexInfo,
                    PartKeyLevel.PARTITION_KEY);
            partClauseInfoOfAllTuple.add(partClauseInfos);
        }
        return genPartTupleRoutingInfoByPartClauseInfos(schemaName, logTbName, partClauseInfoOfAllTuple, partInfo);
    }

    protected static PartitionTupleRouteInfo genPartTupleRoutingInfoByPartClauseInfos(String schemaName,
                                                                                      String logTbName,
                                                                                      List<List<PartClauseInfo>> partClauseInfoOfAllTuple,
                                                                                      PartitionInfo partInfo) {
        PartitionTupleRouteInfo tupleRouteInfo = new PartitionTupleRouteInfo();
        tupleRouteInfo.setSchemaName(schemaName);
        tupleRouteInfo.setTableName(logTbName);
        tupleRouteInfo.setPartInfo(partInfo);
        List<PartTupleDispatchInfo> dispatchFuncInfos = new ArrayList<>();
        for (int t = 0; t < partClauseInfoOfAllTuple.size(); t++) {
            List<PartClauseInfo> partClauseInfos = partClauseInfoOfAllTuple.get(t);
            PartTupleRouteFunction tupleRouteFunction =
                buildTupleRouteFunction(partInfo, PartKeyLevel.PARTITION_KEY, partClauseInfos);
            PartTupleDispatchInfo dispatchFuncInfo = new PartTupleDispatchInfo(tupleRouteInfo, 0);
            dispatchFuncInfo.setPartDispatchFunc(tupleRouteFunction);
            dispatchFuncInfos.add(dispatchFuncInfo);
        }
        tupleRouteInfo.setTupleDispatchFuncInfos(dispatchFuncInfos);
        return tupleRouteInfo;
    }

    /**
     * Get the PartitionSpec by specifying a const expressions of all partition columns.
     * If return null means no found any partitions
     */
    public static PartitionSpec getPartitionSpecByExprValues(PartitionInfo partInfo,
                                                             List<RexNode> exprValRexNodes,
                                                             ExecutionContext executionContext) {

        String schemaName = partInfo.getTableSchema();
        String logTbName = partInfo.getTableName();

        List<PartClauseInfo> partClauseInfos =
            matchPartClauseInfoByRexInfos(partInfo, exprValRexNodes, PartKeyLevel.PARTITION_KEY);
        List<List<PartClauseInfo>> partClauseInfoOfAllTuple = new ArrayList<>();
        partClauseInfoOfAllTuple.add(partClauseInfos);
        PartitionTupleRouteInfo tupleRouteInfo = genPartTupleRoutingInfoByPartClauseInfos(schemaName,
            logTbName,
            partClauseInfoOfAllTuple,
            partInfo);

        PartPrunedResult partPrunedResult =
            PartitionPruner.doPruningByTupleRouteInfo(tupleRouteInfo, 0, executionContext);
        List<PhysicalPartitionInfo> prunedPartInfos = partPrunedResult.getPrunedPartitions();

        PartitionSpec partitionSpec = null;
        if (prunedPartInfos.size() == 0) {
            return partitionSpec;
        }

        PhysicalPartitionInfo prunedPartInfo = prunedPartInfos.get(0);
        String partName = prunedPartInfo.getPartName();
        partitionSpec = partInfo.getPartitionBy().getPartitionByPartName(partName);

        return partitionSpec;
    }

    /**
     * Compare the const expressions of partition columns to the specified partition bound spec, and return compared result
     * return :
     * 0: partSpecBoundVal == exprValRexNodes
     * -1: partSpecBoundVal < exprValRexNodes
     * 1: partSpecBoundVal > exprValRexNodes
     */
    public static int comparePartSpecBound(List<RexNode> exprValRexNodes,
                                           PartitionStrategy strategy,
                                           PartitionBoundSpec boundSpec,
                                           PartitionInfo partInfo,
                                           ExecutionContext executionContext) {
        String schemaName = partInfo.getTableSchema();
        String logTbName = partInfo.getTableName();

        if (strategy == PartitionStrategy.LIST || strategy == PartitionStrategy.LIST_COLUMNS) {
            throw GeneralUtil.nestedException("list/list columns is NOT supported");
        }

        List<PartClauseInfo> partClauseInfos =
            matchPartClauseInfoByRexInfos(partInfo, exprValRexNodes, PartKeyLevel.PARTITION_KEY);
        List<List<PartClauseInfo>> partClauseInfoOfAllTuple = new ArrayList<>();
        partClauseInfoOfAllTuple.add(partClauseInfos);
        PartitionTupleRouteInfo tupleRouteInfo = genPartTupleRoutingInfoByPartClauseInfos(schemaName,
            logTbName,
            partClauseInfoOfAllTuple,
            partInfo);

        SearchDatumInfo searchDatumInfo = tupleRouteInfo.getTupleDispatchFuncInfos().get(0).getPartDispatchFunc()
            .buildTupleSearchDatumInfo(executionContext, null);
        List<SearchDatumInfo> searchDatumInfos = boundSpec.getMultiDatums();
        SearchDatumInfo bonuValDatumInfo = searchDatumInfos.get(0);

        SearchDatumComparator comparator = partInfo.getPartitionBy().getPruningSpaceComparator();
        int compResult = comparator.compareSearchDatum(bonuValDatumInfo, searchDatumInfo);

        return compResult;
    }

    /**
     * Compute the hashcode of a const expressions
     */
    public static Long[] computeExprValuesHashCode(PartitionInfo partInfo,
                                                   List<RexNode> exprValRexNodes,
                                                   ExecutionContext executionContext) {

        String schemaName = partInfo.getTableSchema();
        String logTbName = partInfo.getTableName();
        assert partInfo.getPartitionBy().getStrategy() == PartitionStrategy.HASH
            || partInfo.getPartitionBy().getStrategy() == PartitionStrategy.KEY;
        List<PartClauseInfo> partClauseInfos =
            matchPartClauseInfoByRexInfos(partInfo, exprValRexNodes, PartKeyLevel.PARTITION_KEY);

        List<List<PartClauseInfo>> partClauseInfoOfAllTuple = new ArrayList<>();
        partClauseInfoOfAllTuple.add(partClauseInfos);
        PartitionTupleRouteInfo tupleRouteInfo = genPartTupleRoutingInfoByPartClauseInfos(schemaName,
            logTbName,
            partClauseInfoOfAllTuple,
            partInfo);

        SearchDatumInfo searchDatumInfo = tupleRouteInfo.getTupleDispatchFuncInfos().get(0).getPartDispatchFunc()
            .buildTupleSearchDatumInfo(executionContext, null);
        SearchDatumHasher hasher = partInfo.getPartitionBy().getHasher();
        Long[] hashCodes = null;
        if (partInfo.getPartitionBy().getStrategy() == PartitionStrategy.HASH) {
            long hashCode = hasher.calcHashCodeForHashStrategy(executionContext, searchDatumInfo);
            hashCodes = new Long[1];
            hashCodes[0] = hashCode;
            return hashCodes;
        } else if (partInfo.getPartitionBy().getStrategy() == PartitionStrategy.KEY) {
            hashCodes = hasher.calcHashCodeForKeyStrategy(searchDatumInfo);
        }
        return hashCodes;
    }

    protected static List<PartClauseInfo> matchPartClauseInfoByRexInfos(PartitionInfo partInfo,
                                                                        List<RexNode> constValRexInfo,
                                                                        PartKeyLevel matchLevel) {

        List<ColumnMeta> partColMetaList = new ArrayList<>();
        if (matchLevel == PartKeyLevel.PARTITION_KEY && partInfo.getPartitionBy() != null) {
            partColMetaList = partInfo.getPartitionBy().getPartitionFieldList();
        }
        if (matchLevel == PartKeyLevel.SUBPARTITION_KEY && partInfo.getSubPartitionBy() != null) {
            partColMetaList = partInfo.getSubPartitionBy().getSubPartitionFieldList();
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
        if (matchLevel == PartKeyLevel.SUBPARTITION_KEY && partInfo.getSubPartitionBy() != null) {
            partColList = partInfo.getSubPartitionBy().getSubPartitionColumnNameList();
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

        int partCount = partInfo.getPartitionBy().getPartitions().size();
        int subPartCount = -1;
        if (partInfo.getSubPartitionBy() != null) {
            subPartCount = partInfo.getSubPartitionBy().getSubPartitionsTemplate().size();
        }

        int partColNum = partInfo.getPartitionBy().getPartitionColumnNameList().size();
        SearchDatumComparator querySpaceComp = partInfo.getPartitionBy().getQuerySpaceComparator();

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
            SqlOperator sqlOperator = null;
            if (level == PartKeyLevel.PARTITION_KEY) {
                SqlNode partExpr = partInfo.getPartitionBy().getPartitionExprList().get(keyIdx);
                partColMeta = partInfo.getPartitionBy().getPartitionFieldList().get(keyIdx);
                if (partExpr instanceof SqlCall) {
                    sqlOperator = ((SqlCall) partExpr).getOperator();
                }
            }

            RexNode constVal = partClauseInfo.getConstExpr();
            boolean isAlwaysNull = partClauseInfo.isNull();
            ExprContextProvider exprCxtProvider = new ExprContextProvider();
            IExpression evalFuncExec = RexUtils.getEvalFuncExec(constVal, exprCxtProvider);
            DataType tupleExprReturnDataType = null;
            if (!isAlwaysNull) {
                tupleExprReturnDataType = DataTypeUtil.calciteToDrdsType(constVal.getType());
            }

            PartitionIntFunction partIntFunc = null;
            if (sqlOperator != null) {
                partIntFunc = PartitionPrunerUtils.getPartitionIntFunction(sqlOperator.getName());
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

        PartitionRouter router = partInfo.getPartitionBy().getRouter();
        PartTupleRouteFunction partTupleRouteFunction = new PartTupleRouteFunction(partClauseExprExecArr);
        partTupleRouteFunction.setRouter(router);
        partTupleRouteFunction.setPartInfo(partInfo);
        partTupleRouteFunction.setCmpKind(ComparisonKind.EQUAL);
        partTupleRouteFunction.setMatchLevel(level);
        partTupleRouteFunction.setPartCount(partCount);
        partTupleRouteFunction.setSubPartCount(subPartCount);
        return partTupleRouteFunction;
    }

    public static PartitionSpec getPartitionSpecByHashCode(Long[] hashCode,
                                                           PartitionInfo partInfo,
                                                           ExecutionContext ec) {
        assert partInfo.getPartitionBy().getStrategy() == PartitionStrategy.KEY;
        // Route and build bitset by tuple value
        BitSet partBitSet = PartitionPrunerUtils.buildEmptyPartitionsBitSet(partInfo);
        KeyPartRouter router = (KeyPartRouter) partInfo.getPartitionBy().getRouter();

        PartitionRouter.RouterResult result = router
            .routePartitionsFromHashCode(ec, ComparisonKind.EQUAL, partInfo.getPartitionColumns().size(), hashCode);

        PartKeyLevel matchLevel = PartKeyLevel.PARTITION_KEY;
        Integer partCount = partInfo.getPartitionBy().getPartitions().size();
        Integer subPartCount = 0;
        // Put the pruned result into the partition bitset
        if (result.strategy != PartitionStrategy.LIST && result.strategy != PartitionStrategy.LIST_COLUMNS) {
            PartitionPrunerUtils
                .setPartBitSetByStartEnd(partBitSet, result.partStartPosi, result.pasrEndPosi, matchLevel, partCount,
                    subPartCount, true);
        } else {
            PartitionPrunerUtils
                .setPartBitSetForPartList(partBitSet, result.partPosiSet, matchLevel, partCount, subPartCount, true);
        }
        PartPrunedResult partPrunedResult = new PartPrunedResult();
        partPrunedResult.partBitSet = partBitSet;
        partPrunedResult.partInfo = partInfo;
        List<PhysicalPartitionInfo> prunedPartInfos = partPrunedResult.getPrunedPartitions();
        PartitionSpec partitionSpec = null;
        if (prunedPartInfos.size() == 0) {
            return partitionSpec;
        }

        PhysicalPartitionInfo prunedPartInfo = prunedPartInfos.get(0);
        String partName = prunedPartInfo.getPartName();
        partitionSpec = partInfo.getPartitionBy().getPartitionByPartName(partName);

        return partitionSpec;
    }

}
