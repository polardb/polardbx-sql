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
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.RawString;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.TddlOperatorTable;
import com.alibaba.polardbx.optimizer.core.TddlRelDataTypeSystemImpl;
import com.alibaba.polardbx.optimizer.core.TddlTypeFactoryImpl;
import com.alibaba.polardbx.optimizer.core.datatype.BinaryType;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypeUtil;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.expression.calc.IExpression;
import com.alibaba.polardbx.optimizer.core.field.FieldCheckLevel;
import com.alibaba.polardbx.optimizer.core.field.SessionProperties;
import com.alibaba.polardbx.optimizer.core.field.TypeConversionStatus;
import com.alibaba.polardbx.optimizer.partition.PartitionBoundSpec;
import com.alibaba.polardbx.optimizer.partition.PartitionBoundVal;
import com.alibaba.polardbx.optimizer.partition.PartitionBoundValueKind;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.partition.PartitionSpec;
import com.alibaba.polardbx.optimizer.partition.SubPartitionSpec;
import com.alibaba.polardbx.optimizer.partition.datatype.PartitionField;
import com.alibaba.polardbx.optimizer.partition.datatype.PartitionFieldBuilder;
import com.alibaba.polardbx.optimizer.partition.datatype.function.PartitionIntFunction;
import com.alibaba.polardbx.optimizer.partition.exception.InvalidTypeConversionException;
import com.alibaba.polardbx.optimizer.partition.util.StepExplainItem;
import com.alibaba.polardbx.optimizer.utils.ExprContextProvider;
import com.alibaba.polardbx.optimizer.utils.RexUtils;
import com.alibaba.polardbx.rule.model.Field;
import com.alibaba.polardbx.rule.model.TargetDB;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author chenghui.lch
 */
public class PartitionPrunerUtils {

    /**
     * The log for storage check ha log
     */
    public static final Logger PRUNER_LOG = LoggerFactory.getLogger("PRUNER_LOG");

    protected static final RelDataTypeFactory typeFactory =
        new TddlTypeFactoryImpl(TddlRelDataTypeSystemImpl.getInstance());
    protected static final RexBuilder rexBuilder = new RexBuilder(typeFactory);

    protected static Map<String, PartitionIntFunction> partFuncInfo = new HashMap<>();

    static {
        partFuncInfo.put(TddlOperatorTable.YEAR.getName(), PartitionIntFunction.create(TddlOperatorTable.YEAR));
        partFuncInfo.put(TddlOperatorTable.TO_DAYS.getName(), PartitionIntFunction.create(TddlOperatorTable.TO_DAYS));
        partFuncInfo
            .put(TddlOperatorTable.TO_SECONDS.getName(), PartitionIntFunction.create(TddlOperatorTable.TO_SECONDS));
        partFuncInfo.put(TddlOperatorTable.UNIX_TIMESTAMP.getName(),
            PartitionIntFunction.create(TddlOperatorTable.UNIX_TIMESTAMP));
        partFuncInfo.put(TddlOperatorTable.MONTH.getName(), PartitionIntFunction.create(TddlOperatorTable.MONTH));
    }

    //=========== Some tool methods for TargetDB ============
    /**
     * Full topology of one tbl : PartitionInfo.getPhysicalPartitionTopology() & PartitionInfo.getTopology()
     */

    /**
     * Convert one PartPrunedResult to TargetDB
     */
    public static List<TargetDB> buildTargetDbsByPartPrunedResults(PartPrunedResult result) {

        List<TargetDB> targetDbList = new ArrayList<>();
        Map<String, Map<String, Field>> targetDbInfo = new HashMap<>();

        List<PhysicalPartitionInfo> prunedParts = result.getPrunedPartitions();
        for (int j = 0; j < prunedParts.size(); j++) {
            PhysicalPartitionInfo prunedPart = prunedParts.get(j);

            String grpKey = prunedPart.getGroupKey();
            String phyTb = prunedPart.getPhyTable();
            Map<String, Field> phyTables = targetDbInfo.get(grpKey);
            if (phyTables == null) {
                phyTables = new HashMap<>();
                targetDbInfo.put(grpKey, phyTables);
            }
            phyTables.put(phyTb, null);
        }
        targetDbInfo.forEach((k, v) -> {
            TargetDB targetDB = new TargetDB();
            targetDB.setDbIndex(k);
            targetDB.setTableNames(v);
            targetDB.setLogTblName(result.getLogicalTableName());
            targetDbList.add(targetDB);
        });
        return targetDbList;
    }

    /**
     * Convert the list of PartPrunedResult to TargetDB
     */
    public static Map<String, List<List<String>>> buildTargetTablesByPartPrunedResults(List<PartPrunedResult> results) {

        /**
         * key: grpKey
         * val:
         *      List of phyTblList to be join that has different partition idx
         *          List of phyTbl of the same partition idx of each logTbl
         */
        Map<String, List<List<String>>> phyGrpInfoMap = new HashMap<>();

        /**
         * key: grpKey
         * val:
         *     partIdxPhyTbListMap:
         *          key: partIdx
         *          val: phyTbList that has same phy idx
         */
        Map<String, Map<Integer, List<String>>> allPhyInfos = new HashMap<>();

        List<Map<String, Set<String>>> broadcastTopologyList = new ArrayList<>();

        for (int i = 0; i < results.size(); i++) {
            PartPrunedResult result = results.get(i);
            if (result.getPrunedPartitions().isEmpty()) {
                return phyGrpInfoMap;
            }
        }

        for (int i = 0; i < results.size(); i++) {
            PartPrunedResult result = results.get(i);
            if (result.getPartInfo().isBroadcastTable()) {
                broadcastTopologyList.add(result.getPartInfo().getTopology());
                continue;
            }
            List<PhysicalPartitionInfo> prunedParts = result.getPrunedPartitions();
            for (int j = 0; j < prunedParts.size(); j++) {
                PhysicalPartitionInfo prunedPart = prunedParts.get(j);
                String grpKey = prunedPart.getGroupKey();
                String phyTb = prunedPart.getPhyTable();
                int partBitSetIdx = prunedPart.getPartBitSetIdx();

                // Get phyInfos of one Group
                Map<Integer, List<String>> phyInfosOfOneGrp = allPhyInfos.get(grpKey);
                if (phyInfosOfOneGrp == null) {
                    phyInfosOfOneGrp = new HashMap<>();
                    allPhyInfos.put(grpKey, phyInfosOfOneGrp);
                }

                // Get phyInfos that is the same partIdx
                List<String> phyTbListHasSameIdx = phyInfosOfOneGrp.get(partBitSetIdx);
                if (phyTbListHasSameIdx == null) {
                    phyTbListHasSameIdx = new ArrayList<>();
                    phyInfosOfOneGrp.put(partBitSetIdx, phyTbListHasSameIdx);
                }
                for (int k = 0; k < broadcastTopologyList.size(); k++) {
                    String phyTable = broadcastTopologyList.get(k).get(grpKey).iterator().next();
                    phyTbListHasSameIdx.add(phyTable);
                }
                phyTbListHasSameIdx.add(phyTb);
            }
            broadcastTopologyList = new ArrayList<>();
        }

        if (!broadcastTopologyList.isEmpty()) {
            if (allPhyInfos.isEmpty()) {
                // only broadcast
                List<String> phyInfosOfSameIdx = new ArrayList<>();
                List<List<String>> phyInfosOfOneGrp = new ArrayList<>();
                phyInfosOfOneGrp.add(phyInfosOfSameIdx);
                // TODO: broadcast table random access each group
                String groupKey = broadcastTopologyList.get(0).keySet().stream().findFirst().get();
                for (int i = 0; i < broadcastTopologyList.size(); i++) {
                    String phyTable = broadcastTopologyList.get(i).get(groupKey).iterator().next();
                    phyInfosOfSameIdx.add(phyTable);
                }
                phyGrpInfoMap.put(groupKey, phyInfosOfOneGrp);
                return phyGrpInfoMap;
            }
        }
        // not only broadcast
        for (Map.Entry<String, Map<Integer, List<String>>> phyInfosOfOneGrpItem : allPhyInfos.entrySet()) {
            String grpKey = phyInfosOfOneGrpItem.getKey();
            Map<Integer, List<String>> phyInfosOfOneGrpMap = phyInfosOfOneGrpItem.getValue();
            List<List<String>> phyInfosOfOneGrp = new ArrayList<>();
            for (Map.Entry<Integer, List<String>> phyInfosOfSameIdxItem : phyInfosOfOneGrpMap.entrySet()) {
                List<String> phyInfosOfSameIdx = phyInfosOfSameIdxItem.getValue();
                for (int i = 0; i < broadcastTopologyList.size(); i++) {
                    String phyTable = broadcastTopologyList.get(i).get(grpKey).iterator().next();
                    phyInfosOfSameIdx.add(phyTable);
                }
                phyInfosOfOneGrp.add(phyInfosOfSameIdx);
            }
            phyGrpInfoMap.put(grpKey, phyInfosOfOneGrp);
        }
        return phyGrpInfoMap;
    }

    /**
     * Convert the list of topologyInfo to TargetDB， used by cdc only
     */
    public static List<TargetDB> buildTargetDbsByTopologyInfos(String logTbl,
                                                               Map<String, List<PhysicalPartitionInfo>> topologyInfo) {

        List<TargetDB> targetDbList = new ArrayList<>();
        Map<String, Map<String, Field>> targetDbInfo = new HashMap<>();

        final List<PhysicalPartitionInfo> allPhyPartInfo = new ArrayList<>();
        topologyInfo.entrySet().stream().forEach(e -> allPhyPartInfo.addAll(e.getValue()));
        for (int j = 0; j < allPhyPartInfo.size(); j++) {
            PhysicalPartitionInfo prunedPart = allPhyPartInfo.get(j);

            String grpKey = prunedPart.getGroupKey();
            String phyTb = prunedPart.getPhyTable();
            Map<String, Field> phyTables = targetDbInfo.get(grpKey);
            if (phyTables == null) {
                phyTables = new HashMap<>();
                targetDbInfo.put(grpKey, phyTables);
            }
            phyTables.put(phyTb, null);
        }
        targetDbInfo.forEach((k, v) -> {
            TargetDB targetDB = new TargetDB();
            targetDB.setDbIndex(k);
            targetDB.setTableNames(v);
            targetDB.setLogTblName(logTbl);
            targetDbList.add(targetDB);
        });
        return targetDbList;
    }

    /*======= Methods for get PartitionIntFunction ========*/
    public static PartitionIntFunction getPartitionIntFunction(String funcName) {
        return partFuncInfo.get(funcName);
    }

    public static Set<String> getAllSupportedPartitionIntFunctions() {
        return partFuncInfo.keySet();
    }

    /*======== Methods for building partition bitset ========*/

    public static BitSet buildPartitionsBitSetByPartPostSet(PartitionInfo partInfo, Set<Integer> postSet) {
        BitSet partBitSet = buildEmptyPartitionsBitSet(partInfo);
        int partCnt = partInfo.getPartitionBy().getPartitions().size();
        setPartBitSetForPartList(partBitSet, postSet, PartKeyLevel.PARTITION_KEY, partCnt, -1, true);
        return partBitSet;
    }

    public static BitSet buildEmptyPartitionsBitSet(PartitionInfo partInfo) {

        boolean hasSubpartition = partInfo.getSubPartitionBy() != null;
        BitSet partBitSet;
        if (!hasSubpartition) {
            int allPartCount = partInfo.getPartitionBy().getPartitions().size();
            partBitSet = new BitSet(allPartCount);
        } else {
            throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                String.format("Not support subpartition, table name is %s", partInfo.getTableName()));
        }
        return partBitSet;
    }

    public static BitSet buildFullScanPartitionsBitSet(PartitionInfo partInfo) {

        boolean hasSubpartition = partInfo.getSubPartitionBy() != null;
        BitSet partBitSet = null;
        if (!hasSubpartition) {
            int allPartCount = partInfo.getPartitionBy().getPartitions().size();
            partBitSet = new BitSet(allPartCount);
            partBitSet.set(0, allPartCount, true);
            return partBitSet;
        } else {
            throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                String.format("Not support subpartition, table name is %s", partInfo.getTableName()));
        }
    }

    /**
     * Set Partition BitSet
     * <pre>
     *
     * some def:
     *      ps: partition start position
     *      pe: partition end position
     *      sps: subpartition template start position of all partition
     *      spe: subpartition template end position of all partition
     *      spCnt: the subparition count of each patition
     *
     *  part-only:
     *      input:(start from 1)
     *          ps, pe
     *      BitSet Input: (start from 0)
     *          set ps-1, pe-1
     *
     *
     * part with sub-part:
     *      input:(start from 1)
     *          ps, pe
     *      BitSet Input: (start from 0)
     *          set (ps-1)*spCnt+0, (pe-1)*spCnt+(spCnt-1)
     *
     * sub-part:
     *      input:(start from 1)
     *          sps, spe
     *      BitSet Input: (start from 0)
     *          for each part p
     *              set (p-1)*spCnt+(sps-1), (p-1)*spCnt+(spe-1)
     *
     * sub-part with part:
     *      input:(start from 1)
     *          p, sps, spe
     *      BitSet Input: (start from 0)
     *          for each part p
     *              set (p-1)*spCnt+(sps-1), (p-1)*spCnt+(spe-1)
     *
     * </pre>
     */
    protected static BitSet setPartBitSetByStartEnd(BitSet partBitSet,
                                                    Integer startPartPosi,
                                                    Integer endPartPosi,
                                                    PartKeyLevel level,
                                                    Integer partCnt,
                                                    Integer subPartCntEachPart,
                                                    boolean bitSetVal) {

        int subPartCnt = subPartCntEachPart;

        int bitSetStart = 0;
        int bitSetEnd = 0;
        if (startPartPosi.equals(PartitionRouter.RouterResult.NO_FOUND_PARTITION_IDX) || endPartPosi
            .equals(PartitionRouter.RouterResult.NO_FOUND_PARTITION_IDX)) {
            return partBitSet;
        }
        if (level == PartKeyLevel.PARTITION_KEY) {
            if (subPartCnt <= 0) {
                bitSetStart = startPartPosi - 1;
                bitSetEnd = endPartPosi - 1;
            } else {
                bitSetStart = (startPartPosi - 1) * subPartCnt;
                bitSetEnd = (endPartPosi - 1) * subPartCnt + (subPartCnt - 1);
            }

            partBitSet.set(bitSetStart, bitSetEnd + 1, bitSetVal);
        } else if (level == PartKeyLevel.SUBPARTITION_KEY) {

            for (int i = 1; i <= partCnt; i++) {
                bitSetStart = (i - 1) * subPartCnt + (startPartPosi - 1);
                bitSetEnd = (i - 1) * subPartCnt + (endPartPosi - 1);
                partBitSet.set(bitSetStart, bitSetEnd + 1, bitSetVal);
            }
        }

        return partBitSet;
    }

    /**
     * //--------------
     * <pre>
     * List:
     * part-only:
     *      input:(start from 1)
     *          list pl of part posi
     *      BitSet Input: (start from 0)
     *          for each element pp of pl
     *              set pp-1
     * part with sub-part:
     *      input:(start from 1)
     *          list pl of part posi
     *      BitSet Input: (start from 0)
     *          for each element p of pl
     *              set (p-1)*spCnt, (p-1)*spCnt+(spCnt-1)
     * sub-part:
     *      input:(start from 1)
     *          list spl of sub-part posi
     *      BitSet Input: (start from 0)
     *          for each part p
     *              for each element sp of spl
     *                  set (p-1)*spCnt+(sp-1)
     * sub-part with part:
     *      input:(start from 1)
     *          part posi p, list spl of sub-part posi
     *      BitSet Input: (start from 0)
     *          for each element sp of spl
     *              set (p-1)*spCnt+(sp-1)
     * </pre>
     */
    protected static BitSet setPartBitSetForPartList(BitSet partBitSet,
                                                     Set<Integer> partPostSet,
                                                     PartKeyLevel level,
                                                     Integer partCnt,
                                                     Integer subPartCntEachPart,
                                                     boolean bitSetVal) {

        int subPartCnt = subPartCntEachPart;
        int bitSetStart = 0;
        int bitSetEnd = 0;
        if (level == PartKeyLevel.PARTITION_KEY) {

            for (Integer posi : partPostSet) {
                if (subPartCnt > 0) {
                    bitSetStart = (posi - 1) * subPartCnt;
                    bitSetEnd = bitSetStart + (subPartCnt - 1);
                    partBitSet.set(bitSetStart, bitSetEnd, bitSetVal);
                } else {
                    partBitSet.set(posi - 1, bitSetVal);
                }
            }

        } else if (level == PartKeyLevel.SUBPARTITION_KEY) {
            for (int k = 1; k <= partCnt; k++) {
                for (Integer posi : partPostSet) {
                    bitSetStart = (k - 1) * subPartCnt + posi;
                    partBitSet.set(bitSetStart, bitSetVal);
                }
            }
        }
        return partBitSet;
    }

    public static Map<String, List<String>> getPartNameInfosFromBitSet(PartitionInfo partInfo, BitSet partBitSet) {

        Map<String, List<String>> partNameInfo = new HashMap<>();
        boolean hasSubPart = partInfo.containSubPartitions();
        List<PartitionSpec> partitions = partInfo.getPartitionBy().getPartitions();
        int partCnt = partitions.size();
        if (!hasSubPart) {
            for (int i = 0; i < partCnt; i++) {
                PartitionSpec ps = partitions.get(i);
                if (partBitSet.get(i)) {
                    partNameInfo.put(ps.getName(), new ArrayList<>());
                }
            }
        } else {
            PartitionSpec part0 = partitions.get(0);
            List<SubPartitionSpec> subpartitions = part0.getSubPartitions();
            int subPartCnt = subpartitions.size();
            for (int i = 0; i < partCnt; i++) {
                PartitionSpec ps = partitions.get(i);
                List<String> subPartNames = new ArrayList<>();
                for (int j = 0; j < subPartCnt; j++) {
                    int bsIndex = i * subPartCnt + j;
                    if (partBitSet.get(bsIndex)) {
                        SubPartitionSpec spec = subpartitions.get(j);
                        subPartNames.add(spec.getName());
                    }
                }
                if (subPartNames.size() > 0) {
                    partNameInfo.put(ps.getName(), subPartNames);
                }
            }
        }

        return partNameInfo;
    }

    /*======== Methods for eval expression of all partitioned table ddl and partitioned routing ========*/
    public static RexBuilder getRexBuilder() {
        return rexBuilder;
    }

    public static RelDataTypeFactory getTypeFactory() {
        return typeFactory;
    }

    /**
     * This method is called when need to build partInfo from metadb/create table ast/alter partition ast
     */
    public static PartitionBoundVal getBoundValByRexExpr(RexNode oneBndExpr,
                                                         RelDataType bndColRelDataType,
                                                         PartFieldAccessType specifiedAccessType,
                                                         ExecutionContext context) {

        try {
            ExprContextProvider exprContextProvider = new ExprContextProvider();
            IExpression boundValExpr = RexUtils.getEvalFuncExec(oneBndExpr, exprContextProvider);

            RelDataType oneBndExprReturnType = oneBndExpr.getType();
            DataType tmpBndExprDataType = DataTypeUtil.calciteToDrdsType(oneBndExprReturnType);
            DataType bndExprDataType = tmpBndExprDataType;
            DataType bndColDataType = DataTypeUtil.calciteToDrdsType(bndColRelDataType);

            if (DataTypeUtil.isStringType(bndColDataType) && DataTypeUtil.isStringType(tmpBndExprDataType)) {
                String connCharset = context.getEncoding();
                String colCharset = oneBndExprReturnType.getCharset().name();
                if (connCharset != null && colCharset != null && !connCharset.equalsIgnoreCase(colCharset)) {
                    oneBndExprReturnType =
                        DataTypeUtil.getCharacterTypeWithCharsetAndCollation(oneBndExprReturnType, connCharset, null);
                    bndExprDataType = DataTypeUtil.calciteToDrdsType(oneBndExprReturnType);
                }
            }

            ConstExprEvalParams exprEvalParams = new ConstExprEvalParams();
            exprEvalParams.calcExpr = boundValExpr;
            exprEvalParams.needGetTypeFromDynamicExpr = false;
            exprEvalParams.exprReturnType = bndExprDataType;
            exprEvalParams.partColType = bndColDataType;
            exprEvalParams.executionContext = context;
            exprEvalParams.fldEndpoints = null;
            exprEvalParams.accessType =
                specifiedAccessType != null ? specifiedAccessType : PartFieldAccessType.DDL_EXECUTION;
            exprEvalParams.constExprId = null;
            exprEvalParams.needCacheEvalResult = false;
            PartitionField partField = evalExprValAndCache(exprEvalParams);
            PartitionBoundVal bndVal = PartitionBoundVal
                .createPartitionBoundVal(partField, PartitionBoundValueKind.DATUM_NORMAL_VALUE);
            return bndVal;
        } catch (InvalidTypeConversionException ex) {
            throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_INVALID_PARAMS, ex,
                String.format("Partition column values of incorrect data type conversion[%s] in DDL", ex.getStatus()));
        }
    }

    /**
     * Calc the predicate expr value by IExpression and convert the data type of result to the specified exprReturnType
     */
    protected static PartitionField evalExprValAndCache(ConstExprEvalParams evalParams) {

        IExpression calcExpr = evalParams.calcExpr;
        boolean needGetTypeFromDynamicExpr = evalParams.needGetTypeFromDynamicExpr;
        DataType exprReturnType = evalParams.exprReturnType;
        DataType partColType = evalParams.partColType;
        ColumnMeta partColMeta = evalParams.partColMeta;
        boolean[] fldEndpoints = evalParams.fldEndpoints;
        ExecutionContext executionContext = evalParams.executionContext;
        PartPruneStepPruningContext pruningCtx = evalParams.pruningCtx;
        PartFieldAccessType accessType = evalParams.accessType;
        Integer constExprId = evalParams.constExprId;
        Boolean needCacheEvalResult = evalParams.needCacheEvalResult;

        /**
         * When constExpr is null expr, its constExprId will be null
         */
        PartPruneStepPruningContext execContext = pruningCtx;
        Object evalValObj = null;
        boolean isNullVal = false;
        needCacheEvalResult &= execContext != null && constExprId != null;
        if (needCacheEvalResult) {
            PartPruneStepPruningContext.ExprEvalResult evalResult = execContext.getEvalResult(constExprId);
            if (evalResult != null) {
                evalValObj = evalResult.rawVal;
                isNullVal = evalResult.isNull;
            }
        }

        /**
         * If fail to fetch eval result from cache, then eval expr directly
         */
        if (evalValObj == null && !isNullVal) {
            // Eval expr and get result
            evalValObj = calcExpr.eval(null, executionContext);
        }

//        // list meaning this value come from IN expr.
//        if(evalValObj instanceof List){
//            evalValObj = ((List<?>) evalValObj).get(0);
//        }

        /**
         * Try to fetch the data type from evalValObj
         */
        DataType exprDataType = exprReturnType;
        if (needGetTypeFromDynamicExpr && evalValObj != null) {
            exprDataType = DataTypeUtil.getTypeOfObject(evalValObj);
        }

        /**
         * Build the part field by the eval result
         */
        PartitionField partField =
            PartitionPrunerUtils
                .buildPartFieldForPartCol(evalValObj, exprDataType, partColType, partColMeta, fldEndpoints,
                    executionContext, accessType);

        if (needCacheEvalResult) {
            /**
             * If PartitionField exists data truncated, should not catch its result
             */
            if (partField.lastStatus() == TypeConversionStatus.TYPE_OK) {
                /**
                 * Try to cache eval result
                 */
                execContext
                    .putEvalResult(constExprId, new PartPruneStepPruningContext.ExprEvalResult(evalValObj, partField));
            }
        }
        return partField;
    }

    /**
     * Calc the partition int function value
     */
    protected static PartitionField evalPartFuncVal(PartitionField partField,
                                                    PartitionIntFunction partFunc,
                                                    ExecutionContext context,
                                                    boolean[] endpoints,
                                                    PartFieldAccessType scenario) {

        Object evalObj;
        // get a session properties from context
        SessionProperties sessionProperties = SessionProperties.fromExecutionContext(context);
        if (endpoints != null) {
            evalObj = partFunc.evalIntEndpoint(partField, sessionProperties, endpoints);
        } else {
            evalObj = partFunc.evalInt(partField, sessionProperties);
        }
        // evalObj must be Long because it is from partFunc.evalIntEndpoint or partFunc.evalInt
        PartitionField newPartField = PartitionPrunerUtils.buildPartField(evalObj,
            DataTypes.LongType, partFunc.getReturnType(), endpoints, context, scenario);

        return newPartField;
    }

    public static PartitionField buildPartField(Object predExprVal,
                                                DataType predExprDataType,
                                                DataType partFldDataType,
                                                boolean[] endpoints,
                                                ExecutionContext context,
                                                PartFieldAccessType accessType) {
        return buildPartFieldInner(predExprVal, predExprDataType, partFldDataType, 0, false, endpoints, context,
            accessType);
    }

    public static PartitionField buildPartFieldForPartCol(Object predExprVal,
                                                          DataType predExprDataType,
                                                          DataType partFldDataType,
                                                          ColumnMeta partColMeta,
                                                          boolean[] endpoints,
                                                          ExecutionContext context,
                                                          PartFieldAccessType accessType) {
        int binaryLengthDef = 0;
        boolean useVarbinary = false;
        if (partFldDataType instanceof BinaryType && partColMeta != null) {
            binaryLengthDef = partColMeta.getField().getPrecision();
            if (partColMeta.getField().getRelType().getSqlTypeName() == SqlTypeName.VARBINARY) {
                useVarbinary = true;
            }
        }
        return buildPartFieldInner(predExprVal, predExprDataType, partFldDataType, binaryLengthDef, useVarbinary,
            endpoints, context, accessType);
    }

    protected static PartitionField buildPartFieldInner(Object predExprVal,
                                                        DataType predExprDataType,
                                                        DataType partFldDataType,
                                                        int partFldBinaryTypeLen,
                                                        boolean useVarbinary,
                                                        boolean[] endpoints,
                                                        ExecutionContext context,
                                                        PartFieldAccessType accessType) {

        // make field of partition key by its data type definition
        PartitionField field = null;
        if (partFldDataType instanceof BinaryType) {
            if (useVarbinary) {
                field = PartitionFieldBuilder.createVarBinaryField(partFldBinaryTypeLen);
            } else {
                field = PartitionFieldBuilder.createBinaryField(partFldBinaryTypeLen);
            }
        } else {
            field = PartitionFieldBuilder.createField(partFldDataType);
        }

        SessionProperties sessionProperties;
        if (context == null) {
            if (endpoints == null) {
                // store value from query.
                field.store(predExprVal, predExprDataType);
            } else {
                // store value from query.
                field.store(predExprVal, predExprDataType, null, endpoints);
            }
        } else {
            // so we abstract a session properties:
            sessionProperties = SessionProperties.fromExecutionContext(context);
            if (accessType == PartFieldAccessType.DML_PRUNING || accessType == PartFieldAccessType.DDL_EXECUTION) {
                sessionProperties.setCheckLevel(FieldCheckLevel.CHECK_FIELD_WARN);
            }

            // store value from query.
            if (endpoints != null) {
                /**
                 * Store for Range Query of "<" or ">" or ">=" or "<="
                 */
                field.store(predExprVal, predExprDataType, sessionProperties, endpoints);
            } else {
                /**
                 * Store for Insert and Point Query of "="
                 */
                field.store(predExprVal, predExprDataType, sessionProperties);
            }
        }
        // Process the TypeConversionStatus for pruning
        processTypeConversionStatus(accessType, predExprDataType, field, endpoints);
        return field;
    }

    protected static void processTypeConversionStatus(PartFieldAccessType accessType,
                                                      DataType srcDataType, PartitionField storedField,
                                                      boolean[] endpoints) {
        PartFieldTypeConversionProcessor.processTypeConversionStatus(accessType, srcDataType, storedField, endpoints);
    }

    protected static SearchExprEvalResult evalExprValsAndBuildOneDatum(ExecutionContext context,
                                                                       PartPruneStepPruningContext pruningCtx,
                                                                       SearchExprInfo exprInfo) {
        PartClauseExprExec[] predExprExecArr = exprInfo.getExprExecArr();
        int partColNum = predExprExecArr.length;
        PartitionBoundVal[] searchValArr = new PartitionBoundVal[partColNum];

        boolean[] epInfo = null;
        ComparisonKind newCmpKind = null;
        epInfo = PartFuncMonotonicityUtil.buildIntervalEndPointInfo(exprInfo.getCmpKind());
        if (partColNum == 1) {
            searchValArr[0] =
                PartitionPrunerUtils.evalExecAndBuildBoundValue(context, pruningCtx, predExprExecArr[0], epInfo);
            newCmpKind = PartFuncMonotonicityUtil.buildComparisonKind(epInfo);
        } else {
            int invalidTypeCastPartColInddex = -1;
            for (int j = 0; j < partColNum; j++) {
                searchValArr[j] =
                    PartitionPrunerUtils.evalExecAndBuildBoundValue(context, pruningCtx, predExprExecArr[j], epInfo);
                if (searchValArr[j].isNormalValue() && !searchValArr[j].isNullValue()) {
                    if (searchValArr[j].getValue().lastStatus() != TypeConversionStatus.TYPE_OK) {
                        invalidTypeCastPartColInddex = j;
                        break;
                    }
                }
            }

            /**
             * When find a partition expr exists invalid type cast,
             * should auto ignore all partition expr after it and auto fill
             * max/min value to enlarge the range..
             */
            if (invalidTypeCastPartColInddex > -1) {
                ComparisonKind exprInfoCmpKind = exprInfo.getCmpKind();
                PartitionBoundVal autoFillVal = null;
                if (exprInfoCmpKind == ComparisonKind.GREATER_THAN_OR_EQUAL
                    || exprInfoCmpKind == ComparisonKind.GREATER_THAN) {
                    autoFillVal = PartitionBoundVal.createMinValue();
                    newCmpKind = exprInfoCmpKind;
                } else if (exprInfoCmpKind == ComparisonKind.LESS_THAN_OR_EQUAL
                    || exprInfoCmpKind == ComparisonKind.LESS_THAN) {
                    autoFillVal = PartitionBoundVal.createMaxValue();
                    newCmpKind = exprInfoCmpKind;
                } else {
                    // exprInfoCmpKind == ComparisonKind.EQUAL
                    newCmpKind = PartFuncMonotonicityUtil.buildComparisonKind(epInfo);
                }
                if (exprInfoCmpKind != ComparisonKind.EQUAL) {
                    for (int i = invalidTypeCastPartColInddex; i < partColNum; i++) {
                        searchValArr[i] = autoFillVal;
                    }
                }
            } else {
                newCmpKind = PartFuncMonotonicityUtil.buildComparisonKind(epInfo);
            }
        }

        SearchDatumInfo searchDatumInfo = new SearchDatumInfo(searchValArr);
        SearchExprEvalResult exprEvalResult = new SearchExprEvalResult(searchDatumInfo, newCmpKind);
        return exprEvalResult;
    }

    protected static PartitionBoundVal evalExecAndBuildBoundValue(ExecutionContext context,
                                                                  PartPruneStepPruningContext pruningCtx,
                                                                  PartClauseExprExec exprExec,
                                                                  boolean[] fldEndpoints) {
        PartitionBoundValueKind valueKind = exprExec.getValueKind();
        PartitionField partField = null;
        if (exprExec.getValueKind() == PartitionBoundValueKind.DATUM_NORMAL_VALUE && !exprExec.isAlwaysNullValue()) {
            partField = exprExec.evalPredExprVal(context, pruningCtx, fldEndpoints);
        }
        PartitionBoundVal searchVal =
            PartitionBoundVal.createPartitionBoundVal(partField, valueKind);

        return searchVal;
    }

    /**
     * i.e. p=[1,9), if atVal=1, flag = -1, [1,2),[2,9)
     * if atval=3, flag = 0, [1,3),[3,4),[4,9)
     * if atVal=8, flag = 1, [1,8),[8,9)
     * if p=[1,2) atVal=1, flag=-2 not need to split any more
     */
    public static int getExtractPosition(PartitionSpec curSpec, PartitionSpec prevSpec, Long[] atVal) {
        PartitionBoundSpec boundSpec = curSpec.getBoundSpec();
        int flag = 0;
        // TODO: simplify
        // FIXME: support key partition
        SearchDatumInfo curSpecUpperBound = boundSpec.getSingleDatum();
        Long[] lowBoundHashCode = new Long[atVal.length];
        for (int i = 0; i < atVal.length; i++) {
            lowBoundHashCode[i] = Long.MIN_VALUE;
        }
        SearchDatumInfo prevSpecUpperBound = SearchDatumInfo.createFromHashCodes(lowBoundHashCode);
        if (prevSpec != null) {
            prevSpecUpperBound = prevSpec.getBoundSpec().getSingleDatum();
        }
        SearchDatumInfo searchDatumInfo = SearchDatumInfo.createFromHashCodes(atVal);
        int distWithPrevPart = compareDistance(prevSpecUpperBound, searchDatumInfo);
        int distWithCurPart = compareDistance(searchDatumInfo, curSpecUpperBound);
        if (distWithCurPart == 0 && distWithPrevPart == -1) {
            flag = -2;
        } else if (distWithPrevPart == -1) {
            flag = -1;
        } else if (distWithCurPart == 0) {
            flag = 1;
        } else {
            flag = 0;
        }
        return flag;
    }

    // 1: distance greater than 1
    // 0: distance equal to 1
    // -1: one == two
    private static int compareDistance(SearchDatumInfo one, SearchDatumInfo two) {
        assert one.getDatumInfo().length == two.getDatumInfo().length;
        int i = 0;
        SearchDatumInfo small = one;
        SearchDatumInfo big = two;
        boolean equal = true;
        do {
            long v1 = small.getDatumInfo()[i].getValue().longValue();
            long v2 = big.getDatumInfo()[i].getValue().longValue();
            if (v1 != v2) {
                if (i + 1 < one.getDatumInfo().length) {
                    return 1;
                } else {
                    boolean needSwitch = (v1 > v2);
                    if (needSwitch) {
                        long temp = v1;
                        v1 = v2;
                        v2 = temp;
                    }
                    if ((v1 + 1) == v2) {
                        return 0;
                    } else {
                        return 1;
                    }
                }
            } else if (i + 1 == one.getDatumInfo().length) {
                return -1;
            }
            i++;
        } while (i < one.getDatumInfo().length);
        //can't reach here
        throw new RuntimeException("compare SearchDatumInfo error");
    }

    public static boolean checkIfPointSelect(PartitionPruneStep step, ExecutionContext ec) {
        if (ec.getParams() != null) {
            Map<Integer, ParameterContext> map = ec.getParams().getCurrentParameter();
            for (ParameterContext parameterContext : map.values()) {
                if (parameterContext.getValue() instanceof RawString) {
                    return false;
                }
            }
        }

        PartPruneStepType stepType = step.getStepType();
        if (stepType == PartPruneStepType.PARTPRUNE_OP_MATCHED_PART_KEY) {
            PartitionPruneStepOp stepOp = (PartitionPruneStepOp) step;
            if (stepOp.isDynamicSubQueryInStep()) {
                return false;
            }
            if (stepOp.getComparisonKind() == ComparisonKind.EQUAL) {
                return true;
            }
        }
        return false;
    }

    public static void logStepExplainInfo(ExecutionContext context,
                                          PartitionInfo partInfo,
                                          PartPruneStepPruningContext pruningContext) {
        if (!pruningContext.isEnableLogPruning()) {
            return;
        }
        try {
            String traceId = context.getTraceId();
            String dbName = partInfo.getTableSchema();
            String tblName = partInfo.getTableName();
            StringBuilder explainBuilder = new StringBuilder();
            explainBuilder.append("\nTraceId=").append(traceId);
            explainBuilder.append(",").append("Table=").append(dbName).append(".").append(tblName);
            if (!pruningContext.isPruningByTuple()) {
                logStepExplainInfoInner(pruningContext.getRootStep(), 2, pruningContext.getStepExplainInfo(),
                    explainBuilder);
            } else {
                PartitionTupleRouteInfo tupleRouteInfo = pruningContext.getRootTuple();
                List<PartTupleDispatchInfo> dispatchInfos = tupleRouteInfo.getTupleDispatchFuncInfos();
                for (int i = 0; i < dispatchInfos.size(); i++) {
                    logStepExplainInfoInner(dispatchInfos.get(i), 2, pruningContext.getStepExplainInfo(),
                        explainBuilder);
                }
            }

            explainBuilder.append("\n");
            PRUNER_LOG.info(explainBuilder.toString());
        } catch (Throwable ex) {
            // ignore
            PRUNER_LOG.error(ex);
        }
    }

    private static void logStepExplainInfoInner(PartitionPruneBase current,
                                                int currentLevel,
                                                Map<Object, StepExplainItem> stepExplainInfo,
                                                StringBuilder explainBuilder) {

        boolean isTuple = current instanceof PartTupleDispatchInfo;
        StepExplainItem item = stepExplainInfo.get(current);
        if (item == null) {
            return;
        }
        explainBuilder.append("\n");
        for (int i = 0; i < currentLevel; i++) {
            explainBuilder.append(" ");
        }
        explainBuilder.append(isTuple ? "Tuple=" : "Step=");
        explainBuilder.append(item.stepDesc);
        explainBuilder.append(",");
        explainBuilder.append("PartSet={").append(item.prunedResult.toString()).append("}");
        if (current instanceof PartitionPruneStepCombine) {
            PartitionPruneStepCombine stepCombine = (PartitionPruneStepCombine) current;
            List<PartitionPruneStep> steps = stepCombine.getSubSteps();
            for (int i = 0; i < steps.size(); i++) {
                logStepExplainInfoInner(steps.get(i), currentLevel + 1, stepExplainInfo, explainBuilder);
            }
        }
    }

    public static void collateTupleRouteExplainInfo(PartTupleDispatchInfo tupleDispatchInfo,
                                                    ExecutionContext context,
                                                    PartPrunedResult result,
                                                    PartPruneStepPruningContext pruningContext) {
        if (!pruningContext.isEnableLogPruning()) {
            return;
        }
        try {
            StepExplainItem item = new StepExplainItem();
            item.prunedResult = result;
            item.stepDesc = tupleDispatchInfo.buildStepDigest(context);
            Map<Object, StepExplainItem> explainInfo = pruningContext.getStepExplainInfo();
            explainInfo.put(tupleDispatchInfo, item);
        } catch (Throwable ex) {
            // ignore all exception
            PRUNER_LOG.error(ex);
        }
    }

    public static void collateStepExplainInfo(PartitionPruneStep step,
                                              ExecutionContext context,
                                              PartPrunedResult result,
                                              PartPruneStepPruningContext pruningContext) {
        if (!pruningContext.isEnableLogPruning()) {
            return;
        }
        try {
            StepExplainItem item = new StepExplainItem();
            item.prunedResult = result;
            if (step instanceof PartitionPruneStepOp) {
                item.stepDesc = ((PartitionPruneStepOp) step).buildStepDigest(context);
            } else {
                item.stepDesc = step.getStepType().getSymbol();
            }
            Map<Object, StepExplainItem> explainInfo = pruningContext.getStepExplainInfo();
            explainInfo.put(step, item);
        } catch (Throwable ex) {
            // ignore all exception
            PRUNER_LOG.error(ex);
        }
    }
}
