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

package com.alibaba.polardbx.optimizer.partition;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.druid.sql.SQLUtils;
import com.alibaba.polardbx.gms.locality.LocalityDesc;
import com.alibaba.polardbx.gms.metadb.table.TableInfoManager;
import com.alibaba.polardbx.gms.partition.ExtraFieldJSON;
import com.alibaba.polardbx.gms.partition.TablePartitionConfig;
import com.alibaba.polardbx.gms.partition.TablePartitionRecord;
import com.alibaba.polardbx.gms.partition.TablePartitionSpecConfig;
import com.alibaba.polardbx.gms.tablegroup.PartitionGroupRecord;
import com.alibaba.polardbx.gms.tablegroup.TableGroupConfig;
import com.alibaba.polardbx.gms.tablegroup.TableGroupRecord;
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.gms.util.GroupInfoUtil;
import com.alibaba.polardbx.gms.util.PartitionNameUtil;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.TddlOperatorTable;
import com.alibaba.polardbx.optimizer.core.TddlRelDataTypeSystemImpl;
import com.alibaba.polardbx.optimizer.core.TddlTypeFactoryImpl;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypeUtil;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.hint.util.HintUtil;
import com.alibaba.polardbx.optimizer.partition.boundspec.HashPartBoundValBuilder;
import com.alibaba.polardbx.optimizer.partition.boundspec.KeyPartBoundValBuilder;
import com.alibaba.polardbx.optimizer.partition.boundspec.MultiValuePartitionBoundSpec;
import com.alibaba.polardbx.optimizer.partition.boundspec.PartBoundValBuilder;
import com.alibaba.polardbx.optimizer.partition.boundspec.PartitionBoundSpec;
import com.alibaba.polardbx.optimizer.partition.boundspec.PartitionBoundVal;
import com.alibaba.polardbx.optimizer.partition.common.BuildAllPartSpecsFromAstParams;
import com.alibaba.polardbx.optimizer.partition.common.BuildAllPartSpecsFromMetaDbParams;
import com.alibaba.polardbx.optimizer.partition.common.BuildPartByDefFromAstParams;
import com.alibaba.polardbx.optimizer.partition.common.BuildPartByDefFromMetaDbParams;
import com.alibaba.polardbx.optimizer.partition.common.BuildPartInfoFromAstParams;
import com.alibaba.polardbx.optimizer.partition.common.BuildPartInfoFromMetaDbParams;
import com.alibaba.polardbx.optimizer.partition.common.BuildPartSpecFromAstParams;
import com.alibaba.polardbx.optimizer.partition.common.PartInfoSessionVars;
import com.alibaba.polardbx.optimizer.partition.common.PartKeyLevel;
import com.alibaba.polardbx.optimizer.partition.common.PartitionLocation;
import com.alibaba.polardbx.optimizer.partition.common.PartitionStrategy;
import com.alibaba.polardbx.optimizer.partition.common.PartitionTableType;
import com.alibaba.polardbx.optimizer.partition.datatype.PartitionField;
import com.alibaba.polardbx.optimizer.partition.datatype.PartitionFieldBuilder;
import com.alibaba.polardbx.optimizer.partition.datatype.function.Monotonicity;
import com.alibaba.polardbx.optimizer.partition.datatype.function.PartitionFunctionBuilder;
import com.alibaba.polardbx.optimizer.partition.datatype.function.PartitionIntFunction;
import com.alibaba.polardbx.optimizer.partition.pruning.PartFieldAccessType;
import com.alibaba.polardbx.optimizer.partition.pruning.PartitionPrunerUtils;
import com.alibaba.polardbx.optimizer.partition.pruning.PartitionRouter;
import com.alibaba.polardbx.optimizer.partition.pruning.SearchDatumComparator;
import com.alibaba.polardbx.optimizer.partition.pruning.SearchDatumHasher;
import com.alibaba.polardbx.optimizer.partition.pruning.SearchDatumInfo;
import com.alibaba.polardbx.optimizer.tablegroup.TableGroupInfoManager;
import com.alibaba.polardbx.optimizer.utils.SqlIdentifierUtil;
import com.amazonaws.services.dynamodbv2.xspec.L;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlAlterTableAddPartition;
import org.apache.calcite.sql.SqlAlterTableDropPartition;
import org.apache.calcite.sql.SqlAlterTableModifyPartitionValues;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNumericLiteral;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlPartition;
import org.apache.calcite.sql.SqlPartitionBy;
import org.apache.calcite.sql.SqlPartitionByCoHash;
import org.apache.calcite.sql.SqlPartitionByHash;
import org.apache.calcite.sql.SqlPartitionByList;
import org.apache.calcite.sql.SqlPartitionByRange;
import org.apache.calcite.sql.SqlPartitionByUdfHash;
import org.apache.calcite.sql.SqlPartitionValue;
import org.apache.calcite.sql.SqlPartitionValueItem;
import org.apache.calcite.sql.SqlSubPartition;
import org.apache.calcite.sql.SqlSubPartitionBy;
import org.apache.calcite.sql.SqlSubPartitionByCoHash;
import org.apache.calcite.sql.SqlSubPartitionByHash;
import org.apache.calcite.sql.SqlSubPartitionByList;
import org.apache.calcite.sql.SqlSubPartitionByRange;
import org.apache.calcite.sql.SqlSubPartitionByUdfHash;
import org.apache.calcite.sql.fun.SqlSubstringFunction;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.commons.lang.StringUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static com.alibaba.polardbx.gms.partition.TablePartitionRecord.PARTITION_LEVEL_PARTITION;
import static com.alibaba.polardbx.gms.partition.TablePartitionRecord.PARTITION_LEVEL_SUBPARTITION;

/**
 * @author chenghui.lch
 */
public class PartitionInfoBuilder {

    public static void prepareOrderNumForPartitions(PartitionTableType tblType, List<PartitionSpec> partitions) {
//        if (tblType != PartitionTableType.PARTITION_TABLE && tblType != PartitionTableType.GSI_TABLE) {
        if (!tblType.isA(PartitionTableType.PARTITIONED_TABLE)) {
            for (int i = 0; i < partitions.size(); i++) {
                partitions.get(i).setIntraGroupConnKey(null);
            }
            return;
        }
        Map<String, PartSpecSortedQueue> grpPartQueues = new HashMap<>();
        for (int i = 0; i < partitions.size(); i++) {
            PartitionSpec p = partitions.get(i);
            PartitionLocation location = p.getLocation();
            if (location != null) {
                String grpKey = location.getGroupKey();
                PartSpecSortedQueue queue = grpPartQueues.get(grpKey);
                if (queue == null) {
                    queue = new PartSpecSortedQueue();
                    grpPartQueues.put(grpKey, queue);
                }
                queue.add(p);
            }
        }
        for (Map.Entry<String, PartSpecSortedQueue> queueItem : grpPartQueues.entrySet()) {
            PartSpecSortedQueue queue = queueItem.getValue();
            Iterator<PartitionSpec> itor = queue.iterator();
            long orderNum = -1;
            while (itor.hasNext()) {
                PartitionSpec p = itor.next();
                orderNum++;
                p.setIntraGroupConnKey(orderNum);
            }
        }
    }

    public static boolean isSupportedPartitionDataType(DataType dataType) {
        boolean isSupportedDataType = true;
        try {
            PartitionField partFld = PartitionFieldBuilder.createField(dataType);
            if (partFld == null) {
                isSupportedDataType = false;
            }
        } catch (UnsupportedOperationException ex) {
            isSupportedDataType = false;
        } catch (IllegalArgumentException ex) {
            isSupportedDataType = false;
        } catch (Throwable ex) {
            isSupportedDataType = false;
        }
        return isSupportedDataType;
    }

    public static PartitionInfo buildPartitionInfoByMetaDbConfig(TablePartitionConfig tbPartConf,
                                                                 List<ColumnMeta> allColumnMetas,
                                                                 Map<Long, PartitionGroupRecord> partitionGroupRecordsMap) {
        BuildPartInfoFromMetaDbParams buildParams = new BuildPartInfoFromMetaDbParams();
        buildParams.setTbPartConf(tbPartConf);
        buildParams.setAllColumnMetas(allColumnMetas);
        buildParams.setPartitionGroupRecordsMap(partitionGroupRecordsMap);
        PartitionInfo partitionInfo = buildPartInfoByMetaDbParams(buildParams);
        return partitionInfo;
    }

    public static PartitionInfo buildPartitionInfoByPartDefAst(String schemaName,
                                                               String tableName,
                                                               String tableGroupName,
                                                               boolean withImplicitTableGroup,
                                                               String joinGroupName,
                                                               SqlPartitionBy sqlPartitionBy,
                                                               Map<SqlNode, RexNode> boundExprInfo,
                                                               List<ColumnMeta> pkColMetas,
                                                               List<ColumnMeta> allColMetas,
                                                               PartitionTableType tblType,
                                                               ExecutionContext ec) {
        return buildPartitionInfoByPartDefAst(schemaName, tableName, tableGroupName, withImplicitTableGroup,
            joinGroupName, sqlPartitionBy, boundExprInfo, pkColMetas, allColMetas, tblType, ec, new LocalityDesc(),
            false);
    }

//    public static PartitionInfo buildPartitionInfoByMetaDbConfigBackup(TablePartitionConfig tbPartConf,
//                                                                       List<ColumnMeta> allColumnMetas,
//                                                                       Map<Long, PartitionGroupRecord> partitionGroupRecordsMap) {
//
//        PartitionInfo partitionInfo = new PartitionInfo();
//
//        TablePartitionRecord logTableConfig = tbPartConf.getTableConfig();
//
//        String tableSchema = logTableConfig.tableSchema;
//        String tableName = logTableConfig.tableName;
//        Long tblGroupId = logTableConfig.groupId;
//        Long metaDbVersion = logTableConfig.metaVersion;
//        Integer spTempFlag = logTableConfig.spTempFlag;
//        Integer logTbStatus = logTableConfig.partStatus;
//        Integer autoFlag = logTableConfig.autoFlag;
//        Long partFlags = logTableConfig.partFlags;
//        Integer tableType = logTableConfig.tblType;
//        ExtraFieldJSON partExtras = logTableConfig.partExtras;
//
//        partitionInfo.tableSchema = tableSchema;
//        partitionInfo.tableName = tableName;
//        partitionInfo.tableGroupId = tblGroupId;
//        partitionInfo.metaVersion = metaDbVersion;
//        partitionInfo.spTemplateFlag = spTempFlag;
//        partitionInfo.status = logTbStatus;
//        partitionInfo.autoFlag = autoFlag;
//        partitionInfo.partFlags = partFlags;
//        partitionInfo.tableType = PartitionTableType.getTypeByIntVal(tableType);
//        partitionInfo.locality = logTableConfig.partExtras.locality;
//
//        PartInfoSessionVars sessionVars = new PartInfoSessionVars();
//        if (partExtras != null) {
//            String tablePattern = partExtras.getPartitionPattern();
//            if (StringUtils.isEmpty(tablePattern)) {
//                partitionInfo.setRandomTableNamePatternEnabled(false);
//            } else {
//                partitionInfo.setTableNamePattern(tablePattern);
//            }
//            if (partExtras.getTimeZone() != null) {
//                sessionVars.setTimeZone(partExtras.getTimeZone());
//            }
//            if (partExtras.getCharset() != null) {
//                sessionVars.setCharset(partExtras.getCharset());
//            }
//            if (partExtras.getCollation() != null) {
//                sessionVars.setCollation(partExtras.getCollation());
//            }
//        } else {
//            partitionInfo.setRandomTableNamePatternEnabled(false);
//        }
//        partitionInfo.setSessionVars(sessionVars);
//
//        List<TablePartitionSpecConfig> partSpecConfigs = tbPartConf.getPartitionSpecConfigs();
//
//        PartitionByDefinition partitionBy =
//            buildPartitionByDef(partSpecConfigs, logTableConfig, allColumnMetas, partitionGroupRecordsMap);
//        partitionInfo.setPartitionBy(partitionBy);
//
//        if (partitionInfo.spTemplateFlag == TablePartitionRecord.SUBPARTITION_TEMPLATE_NOT_EXISTED) {
//            partitionInfo.getPartitionBy().setSubPartitionBy(null);
//        }
//
//        partitionInfo.initPartSpecSearcher();
//
//        return partitionInfo;
//    }

    public static PartitionInfo buildPartitionInfoByPartDefAst(String schemaName,
                                                               String tableName,
                                                               String tableGroupName,
                                                               boolean withImplicitTableGroup,
                                                               String joinGroupName,
                                                               SqlPartitionBy sqlPartitionBy,
                                                               Map<SqlNode, RexNode> boundExprInfo,
                                                               List<ColumnMeta> pkColMetas,
                                                               List<ColumnMeta> allColMetas,
                                                               PartitionTableType tblType,
                                                               ExecutionContext ec,
                                                               LocalityDesc locality,
                                                               boolean ttlTemporary) {
        BuildPartInfoFromAstParams buildParams = new BuildPartInfoFromAstParams();
        buildParams.setSchemaName(schemaName);
        buildParams.setTableName(tableName);
        buildParams.setTableGroupName(tableGroupName);
        buildParams.setJoinGroupName(joinGroupName);
        buildParams.setSqlPartitionBy(sqlPartitionBy);
        buildParams.setBoundExprInfo(boundExprInfo);
        buildParams.setPkColMetas(pkColMetas);
        buildParams.setAllColMetas(allColMetas);
        buildParams.setTblType(tblType);
        buildParams.setEc(ec);
        buildParams.setLocality(locality);
        buildParams.setWithImplicitTableGroup(withImplicitTableGroup);
        buildParams.setTtlTemporary(ttlTemporary);
        PartitionInfo partitionInfo = buildPartInfoByAstParams(buildParams);
        return partitionInfo;
    }

    private static PartitionIntFunction[] buildPartFuncArr(
        PartitionStrategy strategy,
        List<SqlNode> partExprs,
        List<ColumnMeta> partColMetaList) {

        if (partColMetaList.size() != partExprs.size()) {
            throw new TddlRuntimeException(ErrorCode.ERR_NOT_SUPPORT);
        }

        List<DataType> partColDataTypes = new ArrayList<>();
        for (int i = 0; i < partColMetaList.size(); i++) {
            partColDataTypes.add(partColMetaList.get(i).getDataType());
        }

        int partFnCount = 0;
        PartitionIntFunction[] partFnArr = new PartitionIntFunction[partColMetaList.size()];
        for (int i = 0; i < partColMetaList.size(); i++) {
            SqlNode partFnExpr = partExprs.get(i);
            SqlOperator partFnOp = getPartFuncSqlOperator(strategy, partFnExpr);

            if (partFnOp == null) {
                continue;
            }

            if (partFnOp instanceof SqlSubstringFunction) {
                // SUBSTR is the same as SUBSTRING
                partFnOp = TddlOperatorTable.SUBSTR;
            }

            /**
             * Validate the partFunction name
             */
            PartitionFunctionBuilder.validatePartitionFunction(partFnOp.getName());

            SqlCall call = (SqlCall) partFnExpr;
            List<SqlNode> opList = call.getOperandList();
            PartitionFunctionBuilder.checkIfPartColDataTypesMatchUdfInputDataTypes(call, strategy, partColMetaList);

            /**
             * Create PartitionIntFunction by partFuncOp and operands
             */
            PartitionIntFunction partFn = PartitionFunctionBuilder.create(partFnOp, opList, partColMetaList);
            if (partFn != null) {
                partFnArr[i] = partFn;
                partFnCount++;
            }
        }

        if (strategy != PartitionStrategy.CO_HASH) {
            if (partFnCount > 1) {
                throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_INVALID_PARAMS,
                    String.format("multi columns with using partition functions are not allowed"));
            }
        }

//        if (partFuncOp instanceof SqlSubstringFunction) {
//            // SUBSTR is the same as SUBSTRING
//            partFuncOp = TddlOperatorTable.SUBSTR;
//        }
//
//        PartitionFunctionBuilder.validatePartitionFunction(partFuncOp.getName());
//        if (partExprs.size() > 1) {
//            throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_INVALID_PARAMS,
//                String.format("multi columns with using partition functions are not allowed"));
//        }
//
//        SqlNode partFnExpr = partExprs.get(0);
//        SqlCall call = (SqlCall) partFnExpr;
//        List<SqlNode> opList = call.getOperandList();
//        List<DataType> partColDataTypes = new ArrayList<>();
//        for (int i = 0; i < partColMetaList.size(); i++) {
//            partColDataTypes.add(partColMetaList.get(i).getDataType());
//        }
//        PartitionFunctionBuilder.checkIfPartColDataTypesMatchUdfInputDataTypes(call, partColDataTypes);
//
//        /**
//         * Create PartitionIntFunction by partFuncOp and operands
//         */
//        PartitionIntFunction partFn = PartitionFunctionBuilder.create(partFuncOp, opList);
//        partitionByDef.setPartIntFuncOperator(partFuncOp);
//        partitionByDef.setPartIntFunc(partFn);
//        partitionByDef.setPartIntFuncMonotonicity(PartFuncMonotonicityUtil
//            .getPartFuncMonotonicity(partFn, partColMetaList.get(0).getField().getRelType()));

        return partFnArr;
    }

//    private static void initPartFunc(
//        List<SqlNode> partExprs,
//        List<ColumnMeta> partColMetaList,
//        SqlOperator partFuncOp,
//        PartitionByDefinition partitionByDef) {
//        if (partFuncOp instanceof SqlSubstringFunction) {
//            // SUBSTR is the same as SUBSTRING
//            partFuncOp = TddlOperatorTable.SUBSTR;
//        }
//
//        PartitionFunctionBuilder.validatePartitionFunction(partFuncOp.getName());
//        if (partExprs.size() > 1) {
//            throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_INVALID_PARAMS,
//                String.format("multi columns with using partition functions are not allowed"));
//        }
//
//        SqlNode partFnExpr = partExprs.get(0);
//        SqlCall call = (SqlCall) partFnExpr;
//        List<SqlNode> opList = call.getOperandList();
//        List<DataType> partColDataTypes = new ArrayList<>();
//        for (int i = 0; i < partColMetaList.size(); i++) {
//            partColDataTypes.add(partColMetaList.get(i).getDataType());
//        }
//        PartitionFunctionBuilder.checkIfPartColDataTypesMatchUdfInputDataTypes(call, partColDataTypes);
//
//        /**
//         * Create PartitionIntFunction by partFuncOp and operands
//         */
//        PartitionIntFunction partFn = PartitionFunctionBuilder.create(partFuncOp, opList);
//        partitionByDef.setPartIntFuncOperator(partFuncOp);
//        partitionByDef.setPartIntFunc(partFn);
//        partitionByDef.setPartIntFuncMonotonicity(PartFuncMonotonicityUtil
//            .getPartFuncMonotonicity(partFn, partColMetaList.get(0).getField().getRelType()));
//
//    }

    private static int getActPartFunctionCount(PartitionIntFunction[] partFunArr) {
        int partFnCount = 0;
        for (int i = 0; i < partFunArr.length; i++) {
            if (partFunArr[i] != null) {
                partFnCount++;
            }
        }
        return partFnCount;
    }

    private static void initPartColMetasByPkColMetas(List<ColumnMeta> pkColMetas,
                                                     List<SqlNode> partExprList,
                                                     List<String> partColList,
                                                     List<ColumnMeta> partColMetaList) {
        for (int i = 0; i < pkColMetas.size(); i++) {
            ColumnMeta pkColMeta = pkColMetas.get(i);
            partColMetaList.add(pkColMeta);
            partColList.add(pkColMeta.getOriginColumnName());
        }
        String partExprStr = "";
        for (int i = 0; i < partColList.size(); i++) {
            if (i > 0) {
                partExprStr += ",";
            }

            /**
             *  Convert a unescapedIdString of partCol to an escapedIdString;
             */
            String partColNameStr = partColList.get(i);
            String partColNameEscapedStr = SqlIdentifierUtil.escapeIdentifierString(partColNameStr);
            partExprStr += partColNameEscapedStr;
        }
        List<SqlPartitionValueItem> partitionExprList = PartitionInfoUtil.buildPartitionExprByString(partExprStr);
        for (int i = 0; i < partitionExprList.size(); i++) {
            partExprList.add(partitionExprList.get(i).getValue());
        }
    }

    private static PartInfoSessionVars saveSessionVars(ExecutionContext context) {
        PartInfoSessionVars sessionVars = new PartInfoSessionVars();
        if (context == null) {
            return sessionVars;
        }
        String encoding = context.getEncoding();
        if (encoding != null) {
            sessionVars.setCharset(encoding);
        }

        if (context.getTimeZone() != null) {
            String timeZone = context.getTimeZone().getMySqlTimeZoneName();
            sessionVars.setTimeZone(timeZone);
        }

        return sessionVars;
    }

//    /**
//     * Build a new PartitionSpec from new astNode(partSpecAst) by specifying prefix partition columns
//     * <pre>
//     *     When prefixPartColCnt is specified ( prefixPartColCnt > 0 ),
//     *     this method will only check and validate the data-type and bound-value of the prefix partition columns
//     * </pre>
//     */
//    public static PartitionSpec buildPartitionSpecByPartSpecAst(ExecutionContext context,
//                                                                List<ColumnMeta> partColMetaList,
//                                                                PartitionIntFunction partIntFunc,
//                                                                SearchDatumComparator pruningComparator,
//                                                                Map<SqlNode, RexNode> partBoundExprInfo,
//                                                                PartBoundValBuilder partBoundValBuilder,
//                                                                SqlPartition partSpecAst,
//                                                                PartitionStrategy strategy,
//                                                                long partPosition,
//                                                                int prefixPartColCnt) {
//        PartitionSpec partSpec = new PartitionSpec();
//        String partName = null;
//        SqlPartitionValue value = null;
//        List<SqlPartitionValueItem> itemsOfVal = null;
//        int partColCnt = partColMetaList.size();
//        boolean isMultiCols = partColCnt > 1;
//        RelDataTypeFactory typeFactory = new TddlTypeFactoryImpl(TddlRelDataTypeSystemImpl.getInstance());
//        List<SearchDatumInfo> partBoundValues = new ArrayList<>();
//

//        if (partSpecAst != null) {
//            SqlIdentifier partNameId = (SqlIdentifier) partSpecAst.getName();
//            partName = partNameId.toString();
//
//            // all part name should convert to lower case
//            partName = PartitionNameUtil.toLowerCase(partName);
//
//            value = partSpecAst.getValues();
//            itemsOfVal = value.getItems();
//
//            PartitionInfoUtil.validatePartitionValueFormats(strategy, partColCnt, prefixPartColCnt, partName,
//                partSpecAst.getValues());
//            if (strategy == PartitionStrategy.LIST_COLUMNS && isMultiCols) {
//
//                // each item is SqlCall of ROW, such "p1 values in ( (2+1,'a','1999-01-01'), (4, 'b', '2000-01-01') )"
//                // which each operand of the SqlCall
//                // will be SqlCall (such as "2+1" ) or SqlLiteral(such as '2000-01-01' )
//                for (int i = 0; i < itemsOfVal.size(); i++) {
//                    SqlCall item = (SqlCall) itemsOfVal.get(i).getValue();
//                    List<PartitionBoundVal> oneBndVal = new ArrayList<>();
//                    // The item must be SqlCall of ROW,
//                    // So bndExprRex also must be RexCall of ROW
//                    if (item.getKind() != SqlKind.DEFAULT) {
//                        RexCall bndExprRex = (RexCall) partBoundExprInfo.get(item);
//                        List<RexNode> bndRexValsOfOneItem = bndExprRex.getOperands();
//                        for (int j = 0; j < bndRexValsOfOneItem.size(); j++) {
//                            RexNode oneBndExpr = bndRexValsOfOneItem.get(j);
//                            RelDataType bndValDt = pruningComparator.getDatumRelDataTypes()[j];
//                            PartitionInfoUtil.validateBoundValueExpr(oneBndExpr, bndValDt, partIntFunc, strategy);
//                            PartitionBoundVal bndVal =
//                                PartitionPrunerUtils.getBoundValByRexExpr(oneBndExpr, bndValDt,
//                                    PartFieldAccessType.DDL_EXECUTION, context);
//                            oneBndVal.add(bndVal);
//                        }
//                    } else {
//                        partSpec.setDefaultPartition(true);
//                        PartitionBoundVal bndVal = PartitionBoundVal.createDefaultValue();
//                        oneBndVal.add(bndVal);
//                    }
//
//                    SearchDatumInfo datum = new SearchDatumInfo(oneBndVal);
//                    partBoundValues.add(datum);
//                }
//
//            } else {
//                // each item is SqlNode may be the SqlCall like Func(const) or the SqlLiteral of const
//
//                if (strategy == PartitionStrategy.LIST || strategy == PartitionStrategy.LIST_COLUMNS) {
//                    RelDataType bndValDt = pruningComparator.getDatumRelDataTypes()[0];
//                    for (int i = 0; i < itemsOfVal.size(); i++) {
//                        SqlNode item = itemsOfVal.get(i).getValue();
//                        RexNode bndExprRex = partBoundExprInfo.get(item);
//                        PartitionBoundVal bndVal;
//                        if (item != null && item.getKind() == SqlKind.DEFAULT) {
//                            partSpec.setDefaultPartition(true);
//                            bndVal = PartitionBoundVal.createDefaultValue();
//                        } else {
//                            PartitionInfoUtil.validateBoundValueExpr(bndExprRex, bndValDt, partIntFunc, strategy);
//                            bndVal = PartitionPrunerUtils.getBoundValByRexExpr(bndExprRex, bndValDt,
//                                PartFieldAccessType.DDL_EXECUTION, context);
//                        }
//                        List<PartitionBoundVal> oneBndVal = Collections.singletonList(bndVal);
//                        SearchDatumInfo datum = new SearchDatumInfo(oneBndVal);
//                        partBoundValues.add(datum);
//                    }
//
//                } else {
//                    List<PartitionBoundVal> oneBndVal = new ArrayList<>();
//                    int itemsValCnt = itemsOfVal.size();
//                    for (int i = 0; i < itemsValCnt; i++) {
//                        SqlNode item = itemsOfVal.get(i).getValue();
//                        RexNode bndExprRex = partBoundExprInfo.get(item);
//                        RelDataType cmpValDt = pruningComparator.getDatumRelDataTypes()[i];
//                        RelDataType bndValDt = getDataTypeForBoundVal(typeFactory, strategy, cmpValDt);
//
//                        PartitionBoundVal bndVal;
//                        if (!itemsOfVal.get(i).isMaxValue()) {
//                            PartitionInfoUtil.validateBoundValueExpr(bndExprRex, bndValDt, partIntFunc, strategy);
//                            bndVal =
//                                PartitionPrunerUtils.getBoundValByRexExpr(bndExprRex, bndValDt,
//                                    PartFieldAccessType.DDL_EXECUTION, context);
//                        } else {
//                            bndVal = PartitionBoundVal.createMaxValue();
//                        }
//                        oneBndVal.add(bndVal);
//                    }
//                    if (itemsValCnt < partColCnt && prefixPartColCnt != PartitionInfoUtil.FULL_PART_COL_COUNT
//                        && partColCnt > 1) {
//                        /**
//                         * Auto make up maxvalue for the columns after prefix part columns
//                         */
//                        for (int i = itemsValCnt; i < partColCnt; i++) {
//                            PartitionBoundVal maxVal = PartitionBoundVal.createMaxValue();
//                            oneBndVal.add(maxVal);
//                        }
//                    }
//
//                    SearchDatumInfo datum = new SearchDatumInfo(oneBndVal);
//                    partBoundValues.add(datum);
//                }
//            }
//            if (!StringUtils.isEmpty(partSpecAst.getLocality())) {
//                partSpec.setLocality(partSpecAst.getLocality());
//            } else {
//                partSpec.setLocality("");
//            }
//        } else {
//
//            // auto build hash partition name
//            partName = PartitionNameUtil.autoBuildPartitionName(partPosition);
//            RelDataType bndValDt = getDataTypeForBoundVal(typeFactory, strategy, null);
//
//            SearchDatumInfo datum = null;
//            if (strategy == PartitionStrategy.HASH || (strategy == PartitionStrategy.KEY && !isMultiCols)) {
//                // auto build hash partition boundVal
//                Long bndJavaVal = (Long) partBoundValBuilder.getPartBoundVal((int) partPosition);
//                PartitionBoundVal boundVal =
//                    buildOneHashBoundValByLong(context, bndJavaVal, bndValDt, PartFieldAccessType.DDL_EXECUTION);
//                datum = new SearchDatumInfo(boundVal);
//            } else {
//                // build bound value for multi-column key
//                Long[] bndJavaVals = (Long[]) partBoundValBuilder.getPartBoundVal((int) partPosition);
//                PartitionBoundVal boundVals[] = new PartitionBoundVal[bndJavaVals.length];
//                for (int i = 0; i < bndJavaVals.length; i++) {
//                    Long bndJavaVal = bndJavaVals[i];
//                    PartitionBoundVal bndValForOneCol =
//                        buildOneHashBoundValByLong(context, bndJavaVal, bndValDt, PartFieldAccessType.DDL_EXECUTION);
//                    boundVals[i] = bndValForOneCol;
//                }
//                datum = new SearchDatumInfo(boundVals);
//            }
//            partBoundValues.add(datum);
//        }
//        PartitionBoundSpec boundSpec = createPartitionBoundSpec(strategy, value, partBoundValues);
//        partSpec.setBoundSpec(boundSpec);
//        partSpec.setComment("");
//        partSpec.setStrategy(strategy);
//        partSpec.setName(partName);
//        partSpec.setPosition(partPosition);
//        partSpec.setBoundSpaceComparator(PartitionByDefinition.buildBoundSpaceComparator(pruningComparator, strategy));
//        return partSpec;
//    }

    public static PartitionBoundVal buildOneHashBoundValByLong(ExecutionContext context,
                                                               Long bndJavaVal, RelDataType bndValDt,
                                                               PartFieldAccessType accessType) {
        DataType bndValDataType = DataTypeUtil.calciteToDrdsType(bndValDt);
        PartitionField partFld =
            PartitionPrunerUtils.buildPartField(bndJavaVal, bndValDataType, bndValDataType, null, context,
                accessType);
        return PartitionBoundVal.createNormalValue(partFld);
    }

    public static PartitionInfo buildNewPartitionInfoByAddingPartition(ExecutionContext context,
                                                                       PartitionInfo partitionInfo,
                                                                       boolean isAlterTableGroup,
                                                                       SqlAlterTableAddPartition addPartition,
                                                                       Map<Integer, Map<SqlNode, RexNode>> partBoundExprInfoByLevel,
                                                                       List<PartitionGroupRecord> invisiblePartitionGroupRecords,
                                                                       Map<String, Pair<String, String>> physicalTableAndGroupPairs,
                                                                       boolean isColumnarIndex) {
        assert physicalTableAndGroupPairs.size() == invisiblePartitionGroupRecords.size();
        assert addPartition.getPartitions().size() == invisiblePartitionGroupRecords.size();

        Map<String, PartitionGroupRecord> invisiblePartitionGroupRecordMap = new HashMap<>();
        for (PartitionGroupRecord partGroupRecord : invisiblePartitionGroupRecords) {
            invisiblePartitionGroupRecordMap.put(partGroupRecord.getPartition_name(), partGroupRecord);
        }

        PartitionByDefinition partByDef = partitionInfo.getPartitionBy();
        PartitionByDefinition subPartByDef = partByDef.getSubPartitionBy();

        int partitionPos = partByDef.getPartitions().size() + 1;
        AtomicInteger phyPartCounter = new AtomicInteger(partitionPos);
        AtomicInteger partPosCounter = new AtomicInteger(partitionPos);

        List<SqlPartition> newPartitions = new ArrayList<>();
        for (SqlNode sqlNode : addPartition.getPartitions()) {
            SqlPartition newSqlPartition = (SqlPartition) sqlNode;
            newPartitions.add(newSqlPartition);
        }

        List<Integer> newActualPartColCnts =
            PartitionInfoUtil.getNewAllLevelPrefixPartColCntByPartInfoAndNewParts(isAlterTableGroup, partitionInfo,
                newPartitions, false);

        boolean isExistingPartitionModified = false;
        List<PartitionSpec> existingPartSpecs = partByDef.copy().getPartitions();
        List<PartitionSpec> newPartitionSpecs = new ArrayList<>();

        boolean isNewSubPartSpecAdded = false;
        List<PartitionSpec> existingSubPartSpecs = null;
        if (subPartByDef != null) {
            existingSubPartSpecs = subPartByDef.copy().getPartitions();
        }
        List<PartitionSpec> newSubPartitionSpecs = new ArrayList<>();

        boolean isAddSubPartitions = subPartByDef != null && addPartition.isSubPartition();

        for (int i = 0; i < newPartitions.size(); i++) {
            SqlPartition newSqlPartition = newPartitions.get(i);

            PartitionSpec newPartitionSpec = null;
            if (!isAddSubPartitions) {
                newPartitionSpec =
                    buildPartSpec(newSqlPartition, partByDef, newActualPartColCnts, partBoundExprInfoByLevel,
                        phyPartCounter, partPosCounter, isColumnarIndex, context);
            }

            if (subPartByDef != null) {
                // Two-level Partitioned Table
                if (subPartByDef.isUseSubPartTemplate()) {
                    // Templated Subpartition
                    if (addPartition.isSubPartition()) {
                        assert newPartitions.size() == 1;
                        assert newSqlPartition.getName() == null;
                        assert GeneralUtil.isNotEmpty(newSqlPartition.getSubPartitions());

                        // ADD SUBPARTITION
                        for (PartitionSpec partitionSpec : partByDef.getPartitions()) {
                            PartitionSpec partSpecWithNewSubParts = partitionSpec.copy();

                            // Add new subpartitions to all existing partitions
                            List<PartitionSpec> newSubPartSpecs =
                                buildAllSubPartSpec(newSqlPartition.getSubPartitions(), subPartByDef,
                                    partSpecWithNewSubParts, newActualPartColCnts,
                                    partBoundExprInfoByLevel, invisiblePartitionGroupRecordMap,
                                    physicalTableAndGroupPairs, isColumnarIndex, context);

                            if (!isNewSubPartSpecAdded) {
                                PartitionInfoUtil.validateAddSubPartitions(partitionInfo, existingSubPartSpecs,
                                    newSubPartitionSpecs);

                                List<PartitionSpec> newSubPartSpecsTemplate = new ArrayList<>();
                                for (PartitionSpec newSubPartSpec : newSubPartSpecs) {
                                    PartitionSpec copy = newSubPartSpec.copy();
                                    copy.setName(copy.getTemplateName());
                                    newSubPartSpecsTemplate.add(copy);
                                }

                                newSubPartitionSpecs.addAll(newSubPartSpecsTemplate);

                                isNewSubPartSpecAdded = true;
                            }

                            newPartitionSpecs.add(partSpecWithNewSubParts);
                        }

                        isExistingPartitionModified = true;

                        break;
                    } else {
                        // ADD PARTITION
                        // Add all subpartitions from spec to new partitions.
                        for (int j = 0; j < subPartByDef.getPartitions().size(); j++) {
                            PartitionSpec subPartSpec = partByDef.getPartitions().get(0).getSubPartitions().get(j);
                            PartitionSpec newSubPartSpec = subPartSpec.copy();

                            newSubPartSpec.setName(
                                PartitionNameUtil.autoBuildSubPartitionName(newPartitionSpec.getName(),
                                    subPartSpec.getTemplateName()));
                            newSubPartSpec.setId(null);
                            newSubPartSpec.setParentId(null);
                            newSubPartSpec.setLogical(false);

                            if (newSubPartSpec.getLocality() == null) {
                                newSubPartSpec.setLocality("");
                            }

                            Pair<String, String> physicalTableAndGroupPair =
                                physicalTableAndGroupPairs.get(newSubPartSpec.getName());

                            PartitionLocation location =
                                new PartitionLocation(physicalTableAndGroupPair.getValue(),
                                    physicalTableAndGroupPair.getKey(),
                                    invisiblePartitionGroupRecordMap.get(newSubPartSpec.getName()).id);
                            location.setVisiable(false);

                            newSubPartSpec.setLocation(location);

                            newPartitionSpec.getSubPartitions().add(newSubPartSpec);
                        }
                    }
                } else {
                    // Non-Templated Subpartition
                    if (addPartition.isSubPartition()) {
                        assert newPartitions.size() == 1;
                        assert newSqlPartition.getName() != null;
                        assert GeneralUtil.isNotEmpty(newSqlPartition.getSubPartitions());

                        // MODIFY PARTITION ADD SUBPARTITION
                        for (PartitionSpec partitionSpec : partByDef.getPartitions()) {
                            if (partitionSpec.getName().equalsIgnoreCase(newSqlPartition.getName().toString())) {
                                PartitionSpec partSpecWithNewSubParts = partitionSpec.copy();

                                // Add new subpartitions to a specified existing partition.
                                List<PartitionSpec> newSubPartSpecs =
                                    buildAllSubPartSpec(newSqlPartition.getSubPartitions(), subPartByDef,
                                        partSpecWithNewSubParts, newActualPartColCnts,
                                        partBoundExprInfoByLevel, invisiblePartitionGroupRecordMap,
                                        physicalTableAndGroupPairs, isColumnarIndex, context);

                                newSubPartitionSpecs.addAll(newSubPartSpecs);
                                newPartitionSpecs.add(partSpecWithNewSubParts);
                            } else {
                                newPartitionSpecs.add(partitionSpec);
                            }
                        }

                        isExistingPartitionModified = true;

                        break;
                    } else {
                        // ADD PARTITION
                        if (GeneralUtil.isEmpty(newSqlPartition.getSubPartitions())) {
                            // We should already add default subpartition before, so this is unexpected.
                            throw new TddlRuntimeException(ErrorCode.ERR_ADD_PARTITION, "default subpartition missed");
                        } else {
                            // Add new partitions with new subpartitions
                            List<PartitionSpec> newSubPartSpecs =
                                buildAllSubPartSpec(newSqlPartition.getSubPartitions(), subPartByDef,
                                    newPartitionSpec, newActualPartColCnts,
                                    partBoundExprInfoByLevel, invisiblePartitionGroupRecordMap,
                                    physicalTableAndGroupPairs, isColumnarIndex, context);
                            newSubPartitionSpecs.addAll(newSubPartSpecs);
                        }
                    }
                }
            } else {
                // One-level Partitioned Table
                Pair<String, String> physicalTableAndGroupPair =
                    physicalTableAndGroupPairs.get(newPartitionSpec.getName());
                PartitionLocation location =
                    new PartitionLocation(physicalTableAndGroupPair.getValue(), physicalTableAndGroupPair.getKey(),
                        invisiblePartitionGroupRecordMap.get(newPartitionSpec.getName()).id);
                newPartitionSpec.setLocation(location);
                location.setVisiable(false);
            }

            PartitionInfoUtil.validateAddPartition(partitionInfo, existingPartSpecs, newPartitionSpec);

            existingPartSpecs.add(newPartitionSpec);
            newPartitionSpecs.add(newPartitionSpec);
        }

        PartitionInfo newPartInfo = partitionInfo.copy();

        if (isExistingPartitionModified) {
            newPartInfo.getPartitionBy().getPartitions().clear();
        }

        newPartInfo.getPartitionBy().getPartitions().addAll(newPartitionSpecs);

        if (subPartByDef != null && subPartByDef.isUseSubPartTemplate()) {
            newPartInfo.getPartitionBy().getSubPartitionBy().getPartitions().addAll(newSubPartitionSpecs);
        }

        newPartInfo.getPartitionBy().getPhysicalPartitions().clear();

        PartitionInfoUtil.validatePartitionInfoForDdl(newPartInfo, context);

        return newPartInfo;
    }

    public static List<PartitionSpec> buildAllSubPartSpec(List<SqlNode> newSubPartitions,
                                                          PartitionByDefinition subPartByDef,
                                                          PartitionSpec parentPartitionSpec,
                                                          List<Integer> newActualPartColCnts,
                                                          Map<Integer, Map<SqlNode, RexNode>> partBoundExprInfoByLevel,
                                                          Map<String, PartitionGroupRecord> invisiblePartitionGroupRecordMap,
                                                          Map<String, Pair<String, String>> physicalTableAndGroupPairs,
                                                          boolean isColumnarIndex,
                                                          ExecutionContext context) {
        List<PartitionSpec> newSubPartitionSpecs = new ArrayList<>();

        int newSubPartPos;
        if (subPartByDef.isUseSubPartTemplate()) {
            newSubPartPos = subPartByDef.getPartitions().size() + 1;
        } else {
            newSubPartPos = parentPartitionSpec.getSubPartitions().size() + 1;
        }

        AtomicInteger phySubPartCounter = new AtomicInteger(newSubPartPos);
        AtomicInteger subPartPosCounter = new AtomicInteger(newSubPartPos);

        PartitionStrategy subStrategy = subPartByDef.getStrategy();

        PartBoundValBuilder boundValBuilder = null;
        if (subStrategy == PartitionStrategy.HASH || subStrategy == PartitionStrategy.DIRECT_HASH) {
            boundValBuilder = new HashPartBoundValBuilder(newSubPartitions.size());
        } else if (subStrategy == PartitionStrategy.KEY) {
            boundValBuilder =
                new KeyPartBoundValBuilder(newSubPartitions.size(), subPartByDef.getPartitionExprList().size());
        }

        for (SqlNode sqlNode : newSubPartitions) {
            SqlSubPartition newSubPartition = (SqlSubPartition) sqlNode;

            PartitionSpec newSubPartSpec;
            if (subStrategy == PartitionStrategy.KEY || subStrategy == PartitionStrategy.HASH
                || subStrategy == PartitionStrategy.DIRECT_HASH) {

                newSubPartSpec = buildSubPartSpecForKey(
                    newSubPartition,
                    subPartByDef,
                    partBoundExprInfoByLevel,
                    boundValBuilder,
                    parentPartitionSpec,
                    phySubPartCounter,
                    subPartPosCounter,
                    isColumnarIndex,
                    context
                );
            } else if (subStrategy == PartitionStrategy.RANGE || subStrategy == PartitionStrategy.RANGE_COLUMNS
                || subStrategy == PartitionStrategy.LIST || subStrategy == PartitionStrategy.LIST_COLUMNS) {
                newSubPartSpec = buildSubPartSpecForRangeOrList(
                    newSubPartition,
                    subPartByDef,
                    newActualPartColCnts,
                    partBoundExprInfoByLevel,
                    parentPartitionSpec,
                    phySubPartCounter,
                    subPartPosCounter,
                    isColumnarIndex,
                    context
                );
            } else {
                throw new TddlRuntimeException(ErrorCode.ERR_ADD_PARTITION,
                    "Unsupported subpartition strategy: " + subStrategy);
            }

            if (subPartByDef.isUseSubPartTemplate()) {
                newSubPartSpec.setTemplateName(newSubPartSpec.getName());
                newSubPartSpec.setName(PartitionNameUtil.autoBuildSubPartitionName(parentPartitionSpec.getName(),
                    newSubPartSpec.getName()));
            } else {
                newSubPartSpec.setName(newSubPartSpec.getName());
            }

            Pair<String, String> physicalTableAndGroupPair = physicalTableAndGroupPairs.get(newSubPartSpec.getName());

            PartitionLocation location =
                new PartitionLocation(physicalTableAndGroupPair.getValue(),
                    physicalTableAndGroupPair.getKey(),
                    invisiblePartitionGroupRecordMap.get(newSubPartSpec.getName()).id);
            location.setVisiable(false);

            newSubPartSpec.setLocation(location);

            newSubPartitionSpecs.add(newSubPartSpec);
            parentPartitionSpec.getSubPartitions().add(newSubPartSpec);
        }

        return newSubPartitionSpecs;
    }

    public static PartitionInfo buildNewPartitionInfoByDroppingPartition(PartitionInfo partitionInfo,
                                                                         SqlAlterTableDropPartition dropPartition,
                                                                         List<String> oldPartitionNameList,
                                                                         ExecutionContext context) {
        Set<String> origDroppedPartNames = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        Set<String> oldPartitionNames = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);

        origDroppedPartNames.addAll(
            dropPartition.getPartitionNames().stream().map(o -> ((SqlIdentifier) o).getLastName())
                .collect(Collectors.toList()));
        oldPartitionNames.addAll(oldPartitionNameList);

        PartitionInfo newPartInfo = partitionInfo.copy();

        PartitionByDefinition partByDef = newPartInfo.getPartitionBy();
        PartitionByDefinition subPartByDef = partByDef.getSubPartitionBy();

        List<PartitionSpec> newPartSpecList = new ArrayList<>();

        if (subPartByDef != null) {
            for (PartitionSpec spec : partByDef.getPartitions()) {
                List<PartitionSpec> newSubPartSpecList = new ArrayList<>();

                int subPartsDropped = 0;

                for (PartitionSpec subSpec : spec.getSubPartitions()) {
                    String subPartName = subSpec.getName();

                    if (oldPartitionNames.contains(subPartName)) {
                        subPartsDropped++;
                        continue;
                    }

                    newSubPartSpecList.add(subSpec);
                }

                if (subPartsDropped >= spec.getSubPartitions().size()) {
                    // The whole partition will be dropped.
                    continue;
                }

                spec.setSubPartitions(newSubPartSpecList);

                newPartSpecList.add(spec);
            }

            List<PartitionSpec> oldSubPartSpecTemplate = subPartByDef.getPartitions();
            List<PartitionSpec> newSubPartSpecTemplate = new ArrayList<>();

            if (oldSubPartSpecTemplate != null && dropPartition.isSubPartition()) {
                // Update subpartition templated as well
                for (PartitionSpec subSpec : oldSubPartSpecTemplate) {
                    if (origDroppedPartNames.contains(subSpec.getName())) {
                        continue;
                    }
                    newSubPartSpecTemplate.add(subSpec);
                }
                subPartByDef.setPartitions(newSubPartSpecTemplate);
            }
        } else {
            // DROP PARTITION
            for (PartitionSpec spec : partByDef.getPartitions()) {
                String partName = spec.getName();

                if (oldPartitionNames.contains(partName)) {
                    continue;
                }

                newPartSpecList.add(spec);
            }
        }

        partByDef.setPartitions(newPartSpecList);

        newPartInfo.getPartitionBy().getPhysicalPartitions().clear();

        PartitionInfoUtil.validatePartitionInfoForDdl(newPartInfo, context);

        return newPartInfo;
    }

    public static PartitionInfo buildNewPartitionInfoByModifyingPartitionValues(PartitionInfo partitionInfo,
                                                                                SqlNode modifyValuesAst,
                                                                                Map<SqlNode, RexNode> allRexExprInfo,
                                                                                ExecutionContext context,
                                                                                PartitionSpec[] outputNewPhyPartSpec) {
        SqlIdentifier partNameAst = null;
        SqlPartitionValue listValues = null;
        SqlAlterTableModifyPartitionValues moddifyPartValues = (SqlAlterTableModifyPartitionValues) modifyValuesAst;
        boolean isModifySubPart = moddifyPartValues.isSubPartition();
        boolean isAddValues = moddifyPartValues.isAdd();
        SqlPartition targetModifyPart = moddifyPartValues.getPartition();
        SqlSubPartition targetModifySubPart = null;
        boolean useSubPartTemp = false;
        boolean useSubPart = false;
        if (partitionInfo.getPartitionBy().getSubPartitionBy() != null) {
            useSubPart = true;
            useSubPartTemp = partitionInfo.getPartitionBy().getSubPartitionBy().isUseSubPartTemplate();
        }

        PartitionByDefinition targetPartByDef =
            !isModifySubPart ? partitionInfo.getPartitionBy() : partitionInfo.getPartitionBy().getSubPartitionBy();

//        if (modifyValuesAst instanceof SqlAlterTableModifyPartitionValues) {
//            SqlAlterTableModifyPartitionValues modifyValues = (SqlAlterTableModifyPartitionValues) modifyValuesAst;
//            isAddValues = modifyValues.isAdd();
//            SqlPartition targetModifyPart = modifyValues.getPartition();
//            partNameAst = (SqlIdentifier) targetModifyPart.getName();
//            listValues = targetModifyPart.getValues();
//        } else if (modifyValuesAst instanceof SqlAlterTableModifySubPartitionValues) {
//            SqlAlterTableModifySubPartitionValues modifyValues =
//                (SqlAlterTableModifySubPartitionValues) modifyValuesAst;
//            isAddValues = modifyValues.isAdd();
//            SqlSubPartition targetModifyPart = modifyValues.getSubPartition();
//            partNameAst = (SqlIdentifier) targetModifyPart.getName();
//            listValues = targetModifyPart.getValues();
//        }

        /**
         * Fetch the target values to be added or dropped from ast
         */
        if (!isModifySubPart) {
            partNameAst = (SqlIdentifier) targetModifyPart.getName();
            listValues = targetModifyPart.getValues();
        } else {
            targetModifySubPart = (SqlSubPartition) targetModifyPart.getSubPartitions().get(0);
            partNameAst = (SqlIdentifier) targetModifySubPart.getName();
            listValues = targetModifySubPart.getValues();
        }

        boolean isMultiCols = targetPartByDef.getPartitionColumnNameList().size() > 1;
        String partNameToBeModified = partNameAst.getLastName();

        PartitionInfo newPartInfo = partitionInfo.copy();
        PartitionByDefinition newTargetPartByDef =
            !isModifySubPart ? newPartInfo.getPartitionBy() : newPartInfo.getPartitionBy().getSubPartitionBy();

        PartitionStrategy strategy = newTargetPartByDef.getStrategy();
        if (strategy != PartitionStrategy.LIST && strategy != PartitionStrategy.LIST_COLUMNS) {
            throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_INVALID_PARAMS,
                String.format("Not support to do this operation for the table [%s]", partitionInfo.getTableName()));
        }

        /**
         * Find the target partition by partition name which is to validate its bound values
         */
        PartitionSpec tarNewSpec = null;
        List<PartitionSpec> partSpecList = null;
        if (isModifySubPart) {
            if (useSubPartTemp) {
                /**
                 * For the modification of templated subpart, use its template part spec to do bound value validation
                 */
                partSpecList = newPartInfo.getPartitionBy().getSubPartitionBy().getPartitions();
            } else {
                PartitionSpec targetSubPartSpec =
                    newPartInfo.getPartSpecSearcher().getPartSpecByPartName(partNameToBeModified);
                PartitionSpec parentSpec = newPartInfo.getPartitionBy().getPartitions()
                    .get(targetSubPartSpec.getParentPartPosi().intValue() - 1);
                partSpecList = parentSpec.getSubPartitions();
            }
        } else {
            partSpecList = newPartInfo.getPartitionBy().getPartitions();
        }

        for (int i = 0; i < partSpecList.size(); i++) {
            PartitionSpec pSpec = partSpecList.get(i);
            if (pSpec.getName().equalsIgnoreCase(partNameToBeModified)) {
                tarNewSpec = pSpec;
                break;
            }
        }

        if (tarNewSpec == null) {
            throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_INVALID_PARAMS, String
                .format("No found the partition[%s] in table [%s]", partNameToBeModified,
                    partitionInfo.getTableName()));
        }

        /**
         * Convert new list/listCols values into SearchDatumInfo and put it into a new TreeSet
         */
        SearchDatumComparator cmp = targetPartByDef.getPruningSpaceComparator();
        TreeSet<SearchDatumInfo> newListColValSet = new TreeSet<>(cmp);
        List<SqlPartitionValueItem> itemsOfVals = listValues.getItems();
        for (int i = 0; i < itemsOfVals.size(); i++) {
            SqlNode oneItem = itemsOfVals.get(i).getValue();
            RexNode bndRexValsOfOneItem = allRexExprInfo.get(oneItem);

            List<PartitionBoundVal> oneColsVal = new ArrayList<>();
            if (isMultiCols) {
                // oneItem is a SqlCall of ROW,
                // So bndRexValsOfOneItem is a RexCall of ROW
                if (!((oneItem instanceof SqlCall) && (oneItem.getKind() == SqlKind.ROW))) {
                    throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_INVALID_PARAMS, String
                        .format("Failed to modify partition[%s] of table[%s] because of the invalid list values",
                            partNameToBeModified, partitionInfo.getTableName()));
                }

                List<RexNode> opList = ((RexCall) bndRexValsOfOneItem).getOperands();
                for (int j = 0; j < opList.size(); j++) {
                    RexNode oneBndExpr = opList.get(j);

                    RelDataType bndValDt = cmp.getDatumRelDataTypes()[j];
                    PartitionBoundVal bndVal = PartitionPrunerUtils.getBoundValByRexExpr(oneBndExpr, bndValDt,
                        PartFieldAccessType.DDL_EXECUTION, context);
                    oneColsVal.add(bndVal);
                }
            } else {
                if ((oneItem instanceof SqlCall) && (oneItem.getKind() == SqlKind.ROW)) {
                    throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_INVALID_PARAMS, String
                        .format("Failed to modify partition[%s] of table[%s] because of the invalid list values",
                            partNameToBeModified, partitionInfo.getTableName()));
                }

                // oneItem is a SqlLiteral or a SqlCall With func,
                // So bndRexValsOfOneItem is a RexCall or RexLiteral
                RelDataType bndValDt = cmp.getDatumRelDataTypes()[0];
                PartitionBoundVal bndVal =
                    PartitionPrunerUtils.getBoundValByRexExpr(bndRexValsOfOneItem, bndValDt,
                        PartFieldAccessType.DDL_EXECUTION, context);
                oneColsVal.add(bndVal);
            }
            SearchDatumInfo searchDatumInfo = new SearchDatumInfo(oneColsVal);
            newListColValSet.add(searchDatumInfo);
        }

        /**
         * 1. For addValues: check if has duplicate values of adding in target partition;
         * 2. For dropValues: check if can find value to be dropped in target partition.
         */
        List<SearchDatumInfo> originalDatums = tarNewSpec.getBoundSpec().getMultiDatums();
        List<SearchDatumInfo> newDatums = new ArrayList<>();
        boolean isFoundTargetVal = false;
        if (isAddValues) {
            newDatums.addAll(newListColValSet);
        }
        for (SearchDatumInfo datum : originalDatums) {

            if (isAddValues) {
                if (newListColValSet.contains(datum)) {
                    // Found duplicate value in original datums during add values,
                    // so need throw exception
                    isFoundTargetVal = true;
                    break;
                } else {
                    // No duplicate value, add the datum into newDatums;
                    newDatums.add(datum.copy());
                }
            } else {
                if (newListColValSet.contains(datum)) {
                    // Found duplicate value in original datums during add values,
                    // so remove the datum from newDatums;
                    isFoundTargetVal = true;
                    continue;
                } else {
                    // The current datums is not in the values list to be drop,
                    // so add it into newDatums
                    newDatums.add(datum.copy());
                }
            }
        }

        if (isAddValues && isFoundTargetVal) {
            // Found duplicate values of target partition in alter ddl
            throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_INVALID_PARAMS, String
                .format("Found duplicate values in the partition[%s] of table[%s]", partNameToBeModified,
                    partitionInfo.getTableName()));
        } else if (!isAddValues) {
            if (!isFoundTargetVal) {
                // drop values from partition
                // No Found any values in target partition
                throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_INVALID_PARAMS, String
                    .format("No found any values in the partition[%s] of table[%s]", partNameToBeModified,
                        partitionInfo.getTableName()));
            } else if (newDatums.size() + newListColValSet.size() != originalDatums.size()) {
                TreeSet<SearchDatumInfo> originColValSet = new TreeSet<>(cmp);
                originColValSet.addAll(originColValSet);
                for (SearchDatumInfo sdi : newListColValSet) {
                    if (!originColValSet.contains(sdi)) {
                        throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_INVALID_PARAMS, String
                            .format("No found value[%s] in the partition[%s] of table[%s]", sdi.toString(),
                                partNameToBeModified,
                                partitionInfo.getTableName()));
                    }
                }
            }
        }

        /**
         * Update the partition config of metadb
         */
        PartitionBoundSpec boundSpec = tarNewSpec.getBoundSpec();
        if (isAddValues) {
            boundSpec.setMultiDatums(newDatums);
        } else {
            boundSpec.setMultiDatums(newDatums);
        }

        List<PartitionSpec> phyPartSpecs = new ArrayList<>();
        if (isModifySubPart) {
            if (useSubPartTemp) {
                /**
                 * For the modification of templated subpart, should update the bound values of all its physical subpartitions
                 */
                String targetPartTempName = partNameToBeModified;
                PartitionSpec subPartSpecTemp = tarNewSpec;
                PartitionBoundSpec boundSpecTemp = subPartSpecTemp.getBoundSpec();
                phyPartSpecs = newPartInfo.getPartitionBy().getPhysicalPartitionsBySubPartTempName(targetPartTempName);
                for (int i = 0; i < phyPartSpecs.size(); i++) {
                    PartitionSpec phyPart = phyPartSpecs.get(i);
                    phyPart.setBoundSpec(boundSpecTemp.copy());
                }
            } else {
                phyPartSpecs.add(tarNewSpec);
            }
        } else {
            if (useSubPart) {
                /**
                 * For the modification of part with subparts, should update the bound values of all its physical subpartitions
                 */
                PartitionSpec targetPartSpec = tarNewSpec;
                List<PartitionSpec> phyPartSpecsOfTargetPartSpec = targetPartSpec.getSubPartitions();
                phyPartSpecs.addAll(phyPartSpecsOfTargetPartSpec);
            } else {
                phyPartSpecs.add(tarNewSpec);
            }
        }

//        if (useSubPartTemp) {
//            String targetPartTempName = partNameToBeModified;
//            PartitionSpec subPartSpecTemp = tarSpec;
//            PartitionBoundSpec boundSpecTemp = subPartSpecTemp.getBoundSpec();
//            phyPartSpecs = newPartInfo.getPartitionBy().getPhysicalPartitionsBySubPartTempName(targetPartTempName);
//            for (int i = 0; i < phyPartSpecs.size(); i++) {
//                PartitionSpec phyPart = phyPartSpecs.get(i);
//                phyPart.setBoundSpec(boundSpecTemp.copy());
//            }
//        } else {
//            phyPartSpecs.add(tarSpec);
//        }

        if (outputNewPhyPartSpec != null && outputNewPhyPartSpec.length == phyPartSpecs.size()) {
            //outputNewPartSpec[0] = tarSpec;
            for (int i = 0; i < outputNewPhyPartSpec.length; i++) {
                outputNewPhyPartSpec[i] = phyPartSpecs.get(i);
            }
        }

        PartitionInfoUtil.validatePartitionInfoForDdl(newPartInfo, context);
        return newPartInfo;
    }

    /**
     * The the data type of a bound value
     */
    public static RelDataType getDataTypeForBoundVal(RelDataTypeFactory typeFactory, PartitionStrategy strategy,
                                                     RelDataType partExprRelDataType) {
        RelDataType boundValDataType = null;
        switch (strategy) {
        case DIRECT_HASH:
        case HASH:
        case KEY: {
            /**
             * For HASH / KEY, the colExpr (a col or fun(col)) will be hashed as an integer,
             * so the relDataType of boundVal SqlLiteral stored in metaDb must be
             * an BIGINT( javaType: long ) after computing hashCode.
             */
            boundValDataType = typeFactory.createSqlType(SqlTypeName.BIGINT);
        }
        break;
        case LIST:
        case RANGE:
        case LIST_COLUMNS:
        case RANGE_COLUMNS: {

            /**
             * <pre>
             *
             *
             * For RANGE / LIST, the colExpr will be a column or a columns with func,such as fun(col),
             * so the the relDataType of bound value stored in metaDb may NOT be same as the data type
             * of the col in fun(col), it always is  an integer after finishing calc expr of func(col),
             * so the relDataType of boundVal SqlLiteral stored in metaDb should use BIGINT.
             *
             * But if the partCol data type is defined as bigint unsigned, then the datatype of expr
             * of func(col) maybe just int
             *
             * </pre>
             *
             */

            /**
             *
             * For LIST/RANGE COLUMNS, the colExpr will be only a column or row expr of multi columns such as (col1,col2, col3),
             * so the the relDataType of bound value stored in metaDb must be same as the datatype
             * of the col defined in table, so the relDataType of boundVal stored in metaDb should be the datatype of column meta.
             *
             * But the string from date/datetime/timestamp/time, its datatype will be inferred as a CHAR type by the parser,
             * so the relDataType of these SqlLiteral should be VARCHAR or INT (partCol dataType is YEAR)
             *
             */
            boundValDataType = partExprRelDataType;
        }
        break;
        case UDF_HASH: {
            /**
             * For UDF_HASH, the colExpr (a col or fun(col)) will be hashed as an LONG,
             * so the relDataType of boundVal SqlLiteral stored in metaDb must be
             * an BIGINT( javaType: long ) after computing hashCode.
             */
            boundValDataType = typeFactory.createSqlType(SqlTypeName.BIGINT);
        }
        break;
        case CO_HASH: {
            /**
             * For CO_HASH, the colExpr (a col or fun(col)) will be hashed as an LONG,
             * so the relDataType of boundVal SqlLiteral stored in metaDb must be
             * an BIGINT( javaType: long ) after computing hashCode.
             */
            boundValDataType = typeFactory.createSqlType(SqlTypeName.BIGINT);
        }
        break;
        }
        return boundValDataType;
    }

    //========private method==========

    private static List<RelDataType> inferPartExprDataTypes(List<SqlNode> partExprList,
                                                            List<ColumnMeta> partColMetas) {

        List<RelDataType> partExprTypeList = new ArrayList<>();
        RelDataTypeFactory typeFactory = new TddlTypeFactoryImpl(TddlRelDataTypeSystemImpl.getInstance());
        assert partExprList.size() == partColMetas.size();
        int partColCnt = partColMetas.size();
        for (int i = 0; i < partColCnt; i++) {
            ColumnMeta partColMeta = partColMetas.get(i);
            SqlNode partExpr = partExprList.get(i);
            RelDataType partColType = partColMeta.getField().getRelType();
            if (partExpr instanceof SqlCall) {
                SqlCall partExprCall = (SqlCall) partExpr;
                List<RelDataType> opDtList = new ArrayList<>();
                opDtList.add(partColType);
                RelDataType exprDataType = partExprCall.getOperator().inferReturnType(typeFactory, opDtList);
                if (partExprCall.getOperator() == TddlOperatorTable.UNIX_TIMESTAMP) {
                    exprDataType = PartitionPrunerUtils.getTypeFactory().createSqlType(SqlTypeName.BIGINT);
                }
                partExprTypeList.add(exprDataType);
            } else {
                partExprTypeList.add(partColType);
            }
        }
        return partExprTypeList;
    }

    public static PartitionBoundSpec createPartitionBoundSpec(PartitionStrategy strategy,
                                                              SqlPartitionValue partRawValueAst,
                                                              List<SearchDatumInfo> boundValues) {
        PartitionBoundSpec boundSpec = PartitionBoundSpec.createByStrategy(strategy);
        boundSpec.setBoundSqlValue(partRawValueAst);

        if (strategy.isSingleValue()) {
            boundSpec.setSingleDatum(boundValues.get(0));
        } else {
            boundSpec.setMultiDatums(boundValues);
        }
        if (!boundValues.isEmpty() && boundValues.get(0).containDefaultValue()) {
            if (boundSpec instanceof MultiValuePartitionBoundSpec) {
                ((MultiValuePartitionBoundSpec) boundSpec).setDefault(true);
            }
        }
        return boundSpec;
    }

    private static PartitionBoundSpec buildBoundSpecByPartitionDesc(String partDescStr,
                                                                    List<RelDataType> partExprDataTypes,
                                                                    PartitionStrategy strategy) {

        /**
         * The raw partition expression from create tbl ddl or meta db
         */
        List<SqlPartitionValueItem> bndSpecVal = PartitionInfoUtil.buildPartitionExprByString(partDescStr);

        boolean isMultiPartCols = partExprDataTypes.size() > 1;
        RelDataTypeFactory typeFactory = PartitionPrunerUtils.getTypeFactory();
        RexBuilder rexBuilder = PartitionPrunerUtils.getRexBuilder();

        SqlPartitionValue.Operator op = SqlPartitionValue.Operator.LessThan;
        if (strategy == PartitionStrategy.LIST || strategy == PartitionStrategy.LIST_COLUMNS) {
            op = SqlPartitionValue.Operator.In;
        }
        SqlPartitionValue partitionVal = new SqlPartitionValue(op, SqlParserPos.ZERO);
        List<SearchDatumInfo> boundValues = new ArrayList<>();

        if (strategy == PartitionStrategy.LIST_COLUMNS && isMultiPartCols) {
            for (SqlPartitionValueItem oneValItem : bndSpecVal) {
                partitionVal.getItems().add(oneValItem);

                // oneListValItem must be a SqlCall of ROW
                assert oneValItem.getValue() instanceof SqlCall;

                SqlCall oneListValItemRow = (SqlCall) oneValItem.getValue();
                List<PartitionBoundVal> boundValList = new ArrayList<>();
                if (oneListValItemRow.getKind() != SqlKind.DEFAULT) {
                    List<SqlNode> partColValListOfOneItem = oneListValItemRow.getOperandList();
                    for (int j = 0; j < partColValListOfOneItem.size(); j++) {
                        SqlNode onePartColVal = partColValListOfOneItem.get(j);
                        assert onePartColVal instanceof SqlLiteral;
                        SqlLiteral literal = (SqlLiteral) onePartColVal;
                        RelDataType partExprDt = partExprDataTypes.get(j);
                        PartitionBoundVal boundVal =
                            buildBoundValFromSqlLiteral(typeFactory, rexBuilder, strategy, literal, partExprDt);
                        boundValList.add(boundVal);
                    }
                } else {
                    PartitionBoundVal boundVal = PartitionBoundVal.createDefaultValue();
                    boundValList.add(boundVal);
                }

                boundValues.add(new SearchDatumInfo(boundValList));
            }
        } else {
            if (strategy == PartitionStrategy.LIST || strategy == PartitionStrategy.LIST_COLUMNS) {
                for (int i = 0; i < bndSpecVal.size(); i++) {
                    SqlPartitionValueItem oneValItem = bndSpecVal.get(i);
                    partitionVal.getItems().add(oneValItem);
                    // oneListValItem must be a SqlLiteral because it is loaded from metadb
                    PartitionBoundVal boundVal;
                    if (oneValItem.getValue().getKind() != SqlKind.DEFAULT) {
                        SqlLiteral literal = (SqlLiteral) oneValItem.getValue();
                        //ColumnMeta cm = partitionBy.getPartitionFieldList().get(0);
                        RelDataType partExprDt = partExprDataTypes.get(0);
                        boundVal = buildBoundValFromSqlLiteral(typeFactory, rexBuilder, strategy, literal, partExprDt);
                    } else {
                        boundVal = PartitionBoundVal.createDefaultValue();

                    }
                    boundValues.add(new SearchDatumInfo(boundVal));
                }
            } else {
                // RANGE/RANGE COLUMNS
                // HASH/KEY

                List<PartitionBoundVal> boundValList = new ArrayList<>();
                for (int i = 0; i < bndSpecVal.size(); i++) {
                    SqlPartitionValueItem oneValItem = bndSpecVal.get(i);
                    partitionVal.getItems().add(oneValItem);
                    // oneListValItem must be a SqlLiteral
                    RelDataType partExprDt = partExprDataTypes.get(i);
                    PartitionBoundVal boundVal;
                    if (!oneValItem.isMaxValue()) {
                        assert oneValItem.getValue() instanceof SqlLiteral;
                        SqlLiteral literal = (SqlLiteral) oneValItem.getValue();
                        boundVal =
                            buildBoundValFromSqlLiteral(typeFactory, rexBuilder, strategy, literal, partExprDt);
                    } else {
//                        boundVal = PartitionBoundVal.createMaxValue();
                        if (strategy.isHashed() || strategy.isUdfHashed()) {
                            boundVal = PartitionBoundVal.createHashBoundMaxValue();
                        } else {
                            boundVal = PartitionBoundVal.createMaxValue();
                        }
                    }
                    boundValList.add(boundVal);
                }
                boundValues.add(new SearchDatumInfo(boundValList));
            }
        }
        PartitionBoundSpec boundSpec = createPartitionBoundSpec(strategy, partitionVal, boundValues);
        return boundSpec;
    }

    private static PartitionBoundVal buildBoundValFromSqlLiteral(RelDataTypeFactory typeFactory, RexBuilder rexBuilder,
                                                                 PartitionStrategy strategy, SqlLiteral literal,
                                                                 RelDataType partExprDataType) {

        RelDataType boundValDataType = getDataTypeForBoundVal(typeFactory, strategy, partExprDataType);

        /**
         * Change all SqlLiteral to RexLiteral of CHAR
         */
        RexLiteral rexExpr;
        boolean isNullVal = literal.getTypeName() == SqlTypeName.NULL;
        if (!isNullVal) {
            if (strategy == PartitionStrategy.HASH || strategy == PartitionStrategy.KEY
                || strategy == PartitionStrategy.DIRECT_HASH) {
                rexExpr = rexBuilder.makeBigIntLiteral(Long.valueOf(literal.toValue()));
            } else {
                rexExpr = rexBuilder.makeLiteral(literal.toValue());
            }

        } else {
            rexExpr = rexBuilder.makeNullLiteral(boundValDataType);
        }

        /**
         * Build the bndVal by RexLiteral of CHAR and covert its datatype to boundValDataType
         */
        PartitionBoundVal bndVal =
            PartitionPrunerUtils.getBoundValByRexExpr(rexExpr, boundValDataType, PartFieldAccessType.META_LOADING,
                new ExecutionContext());
        return bndVal;
    }

    protected static boolean checkNeedDoEnumRange(PartitionStrategy strategy, List<ColumnMeta> partFields,
                                                  SqlOperator partFuncOp) {
        if (strategy == PartitionStrategy.KEY && partFields.size() == 1) {
            DataType dataType = partFields.get(0).getField().getDataType();
            if (DataTypeUtil.isNumberSqlType(dataType)) {
                if (dataType.getSqlType() == DataTypes.DecimalType.getSqlType()) {
                    /**
                     * For decimal(x, scale) with scale > 0 , no need to do interval enum
                     */
                    return dataType.getScale() <= 0;
                }
                return true;
            }
        } else if (strategy == PartitionStrategy.HASH && partFields.size() == 1
            && (partFuncOp == null || partFuncOp != null && partFuncCanDoEnumInHashStrategy(partFuncOp.getName()))) {
            return true;
        } else if (strategy == PartitionStrategy.DIRECT_HASH && partFields.size() == 1
            && (partFuncOp == null || partFuncOp != null && partFuncCanDoEnumInHashStrategy(partFuncOp.getName()))) {
        } else if ((strategy == PartitionStrategy.RANGE || strategy == PartitionStrategy.LIST) && partFuncOp != null) {
            return partFuncNeedDoEnumInRangeAndListStrategy(partFuncOp.getName());
        }
        return false;
    }

    //all the partition function can be used to do enumrate, except substr
    protected static boolean partFuncCanDoEnumInHashStrategy(String partIntFunctionName) {
        return "DAYOFMONTH".equalsIgnoreCase(partIntFunctionName)
            || "DAYOFWEEK".equalsIgnoreCase(partIntFunctionName)
            || "DAYOFYEAR".equalsIgnoreCase(partIntFunctionName)
            || "WEEKOFYEAR".equalsIgnoreCase(partIntFunctionName)
            || "MONTH".equalsIgnoreCase(partIntFunctionName)
            || "TO_DAYS".equalsIgnoreCase(partIntFunctionName)
            || "TO_MONTHS".equalsIgnoreCase(partIntFunctionName)
            || "TO_SECONDS".equalsIgnoreCase(partIntFunctionName)
            || "TO_WEEKS".equalsIgnoreCase(partIntFunctionName)
            || "UNIX_TIMESTAMP".equalsIgnoreCase(partIntFunctionName)
            || "YEAR".equalsIgnoreCase(partIntFunctionName);
    }

    /**
     * all the NON_MONOTONIC partition function, except substr, can be used to do enumerate in range and list case
     */
    protected static boolean partFuncNeedDoEnumInRangeAndListStrategy(String partIntFunctionName) {
        return "DAYOFMONTH".equalsIgnoreCase(partIntFunctionName)
            || "DAYOFWEEK".equalsIgnoreCase(partIntFunctionName)
            || "DAYOFYEAR".equalsIgnoreCase(partIntFunctionName)
            || "MONTH".equalsIgnoreCase(partIntFunctionName)
            || "WEEKOFYEAR".equalsIgnoreCase(partIntFunctionName);
    }

    protected static SqlOperator getPartFuncSqlOperator(PartitionStrategy strategy, SqlNode partColExpr) {
        if (strategy == PartitionStrategy.HASH
            || strategy == PartitionStrategy.DIRECT_HASH
            || strategy == PartitionStrategy.RANGE
            || strategy == PartitionStrategy.LIST
            || strategy == PartitionStrategy.UDF_HASH
            || strategy == PartitionStrategy.CO_HASH) {
            SqlNode colExpr = partColExpr;
            if (colExpr instanceof SqlCall) {
                SqlCall sqlCall = (SqlCall) colExpr;
                SqlOperator op = sqlCall.getOperator();
                return op;
            }
        }
        return null;
    }

    /**
     * Build a new PartitionSpec from new astNode(partSpecAst) by specifying prefix partition columns
     * <pre>
     *     When prefixPartColCnt is specified ( prefixPartColCnt > 0 ),
     *     this method will only check and validate the data-type and bound-value of the prefix partition columns
     * </pre>
     */

    public static PartitionSpec buildPartSpec(SqlPartition newSqlPartition,
                                              PartitionByDefinition partByDef,
                                              List<Integer> newActualPartColCnts,
                                              Map<Integer, Map<SqlNode, RexNode>> partBoundExprInfoByLevel,
                                              AtomicInteger phyPartCounter,
                                              AtomicInteger partPosCounter,
                                              boolean isColumnarIndex,
                                              ExecutionContext context) {
        List<ColumnMeta> partColMetaList = partByDef.getPartitionFieldList();
        SearchDatumComparator comparator = partByDef.getPruningSpaceComparator();
        PartitionIntFunction partIntFunc = partByDef.getPartIntFunc();

        BuildPartSpecFromAstParams partSpecAstParams = new BuildPartSpecFromAstParams();
        partSpecAstParams.setContext(context);
        partSpecAstParams.setPartKeyLevel(PartKeyLevel.PARTITION_KEY);
        partSpecAstParams.setPartColMetaList(partColMetaList);
        partSpecAstParams.setPartIntFunc(partIntFunc);
        partSpecAstParams.setPruningComparator(comparator);
        partSpecAstParams.setPartBoundExprInfo(partBoundExprInfoByLevel.get(PARTITION_LEVEL_PARTITION));
        partSpecAstParams.setPartBoundValBuilder(null);
        partSpecAstParams.setStrategy(partByDef.getStrategy());
        partSpecAstParams.setPartPosition(partPosCounter.getAndIncrement());
        partSpecAstParams.setAllLevelPrefixPartColCnts(newActualPartColCnts);
        partSpecAstParams.setPartNameAst(newSqlPartition.getName());
        partSpecAstParams.setPartComment(newSqlPartition.getComment());
        partSpecAstParams.setPartLocality(newSqlPartition.getLocality());
        partSpecAstParams.setPartBndValuesAst(newSqlPartition.getValues());
        partSpecAstParams.setLogical(partByDef.getSubPartitionBy() != null);
        partSpecAstParams.setSpecTemplate(false);
        partSpecAstParams.setSubPartSpec(false);
        partSpecAstParams.setParentPartSpec(null);
        partSpecAstParams.setPhySpecCounter(phyPartCounter);
        partSpecAstParams.setPartEngine(isColumnarIndex ?
            TablePartitionRecord.PARTITION_ENGINE_COLUMNAR :
            TablePartitionRecord.PARTITION_ENGINE_INNODB);

        return buildPartSpecByAstParamInner(partSpecAstParams);
    }

    /**
     * ============================================
     */

    public static PartitionSpec buildSubPartSpecForKey(SqlSubPartition newSubPartition,
                                                       PartitionByDefinition subPartByDef,
                                                       Map<Integer, Map<SqlNode, RexNode>> partBoundExprInfoByLevel,
                                                       PartBoundValBuilder partBoundValBuilder,
                                                       PartitionSpec parentPartitionSpec,
                                                       AtomicInteger phySubPartCounter,
                                                       AtomicInteger subPartPosCounter,
                                                       boolean isColumnarIndex,
                                                       ExecutionContext context) {
        List<ColumnMeta> subPartColMetaList = subPartByDef.getPartitionFieldList();
        SearchDatumComparator comparator = subPartByDef.getPruningSpaceComparator();
        PartitionIntFunction subPartIntFunc = subPartByDef.getPartIntFunc();

        BuildPartSpecFromAstParams partSpecAstParams = new BuildPartSpecFromAstParams();

        partSpecAstParams.setContext(context);
        partSpecAstParams.setPartKeyLevel(PartKeyLevel.SUBPARTITION_KEY);
        partSpecAstParams.setPartColMetaList(subPartColMetaList);
        partSpecAstParams.setPartIntFunc(subPartIntFunc);
        partSpecAstParams.setPruningComparator(comparator);
        partSpecAstParams.setPartBoundExprInfo(partBoundExprInfoByLevel.get(PARTITION_LEVEL_SUBPARTITION));
        partSpecAstParams.setPartBoundValBuilder(partBoundValBuilder);
        partSpecAstParams.setStrategy(subPartByDef.getStrategy());
        partSpecAstParams.setAutoBuildPart(true);
        partSpecAstParams.setPartPosition(subPartPosCounter.getAndIncrement());
        partSpecAstParams.setAllLevelPrefixPartColCnts(PartitionInfoUtil.ALL_LEVEL_FULL_PART_COL_COUNT_LIST);
        partSpecAstParams.setPartNameAst(newSubPartition.getName());
        partSpecAstParams.setPartComment(newSubPartition.getComment());
        partSpecAstParams.setPartLocality(newSubPartition.getLocality());
        partSpecAstParams.setLogical(subPartByDef.getSubPartitionBy() != null);
        partSpecAstParams.setSpecTemplate(false);
        partSpecAstParams.setSubPartSpec(true);
        partSpecAstParams.setParentPartSpec(parentPartitionSpec);
        partSpecAstParams.setPhySpecCounter(phySubPartCounter);
        partSpecAstParams.setPartEngine(isColumnarIndex ?
            TablePartitionRecord.PARTITION_ENGINE_COLUMNAR :
            TablePartitionRecord.PARTITION_ENGINE_INNODB);

        return buildPartSpecByAstParamInner(partSpecAstParams);
    }

    public static PartitionSpec buildSubPartSpecForRangeOrList(SqlSubPartition newSubPartition,
                                                               PartitionByDefinition subPartByDef,
                                                               List<Integer> newActualPartColCnts,
                                                               Map<Integer, Map<SqlNode, RexNode>> partBoundExprInfoByLevel,
                                                               PartitionSpec parentPartitionSpec,
                                                               AtomicInteger phySubPartCounter,
                                                               AtomicInteger subPartPosCounter,
                                                               boolean isColumnarIndex,
                                                               ExecutionContext context) {
        List<ColumnMeta> subPartColMetaList = subPartByDef.getPartitionFieldList();
        SearchDatumComparator comparator = subPartByDef.getPruningSpaceComparator();
        PartitionIntFunction subPartIntFunc = subPartByDef.getPartIntFunc();
        BuildPartSpecFromAstParams partSpecAstParams = new BuildPartSpecFromAstParams();
        partSpecAstParams.setContext(context);
        partSpecAstParams.setPartKeyLevel(PartKeyLevel.SUBPARTITION_KEY);
        partSpecAstParams.setPartColMetaList(subPartColMetaList);
        partSpecAstParams.setPartIntFunc(subPartIntFunc);
        partSpecAstParams.setPruningComparator(comparator);
        partSpecAstParams.setPartBoundExprInfo(partBoundExprInfoByLevel.get(PARTITION_LEVEL_SUBPARTITION));
        partSpecAstParams.setPartBoundValBuilder(null);
        partSpecAstParams.setStrategy(subPartByDef.getStrategy());
        partSpecAstParams.setPartPosition(subPartPosCounter.getAndIncrement());
        partSpecAstParams.setAllLevelPrefixPartColCnts(newActualPartColCnts);
        partSpecAstParams.setPartNameAst(newSubPartition.getName());
        partSpecAstParams.setPartComment(newSubPartition.getComment());
        partSpecAstParams.setPartLocality(newSubPartition.getLocality());
        partSpecAstParams.setPartBndValuesAst(newSubPartition.getValues());
        partSpecAstParams.setLogical(subPartByDef.getSubPartitionBy() != null);
        partSpecAstParams.setSpecTemplate(false);
        partSpecAstParams.setSubPartSpec(true);
        partSpecAstParams.setParentPartSpec(parentPartitionSpec);
        partSpecAstParams.setPhySpecCounter(phySubPartCounter);
        partSpecAstParams.setPartEngine(isColumnarIndex ?
            TablePartitionRecord.PARTITION_ENGINE_COLUMNAR :
            TablePartitionRecord.PARTITION_ENGINE_INNODB);

        return buildPartSpecByAstParamInner(partSpecAstParams);
    }

    public static PartitionSpec buildPartSpecByAstParams(BuildPartSpecFromAstParams buildParams) {
        return buildPartSpecByAstParamInner(buildParams);
    }

    public static PartitionSpec buildPartSpecByAstParamInner(BuildPartSpecFromAstParams buildParams) {

        ExecutionContext context = buildParams.getContext();
        List<ColumnMeta> partColMetaList = buildParams.getPartColMetaList();
        PartKeyLevel partKeyLevel = buildParams.getPartKeyLevel();
        PartitionIntFunction partIntFunc = buildParams.getPartIntFunc();
        SearchDatumComparator pruningComparator = buildParams.getPruningComparator();

        Map<SqlNode, RexNode> partBoundExprInfo = buildParams.getPartBoundExprInfo();
        PartBoundValBuilder partBoundValBuilder = buildParams.getPartBoundValBuilder();
        boolean useAutoPart = buildParams.isAutoBuildPart();
        SqlNode partNameAst = buildParams.getPartNameAst();
        SqlPartitionValue partBndValuesAst = buildParams.getPartBndValuesAst();
        String partComment = buildParams.getPartComment();
        String partLocality = buildParams.getPartLocality();

        PartitionStrategy strategy = buildParams.getStrategy();
        long partPosition = buildParams.getPartPosition();

        List<Integer> allLevelPrefixPartColCnts = buildParams.getAllLevelPrefixPartColCnts();
        int partLevelIndex = partKeyLevel == PartKeyLevel.SUBPARTITION_KEY ? 1 : 0;
        int prefixPartColCnt = allLevelPrefixPartColCnts.get(partLevelIndex);

        boolean isLogicalSpec = buildParams.isLogical();
        boolean isSpecTemplate = buildParams.isSpecTemplate();
        boolean useSubPartByTemp = buildParams.isUseSpecTemplate();
        AtomicInteger phySpecCounter = buildParams.getPhySpecCounter();
        boolean needCountPhySpec = !isLogicalSpec;

        Long phyPartPosition = null;
        if (needCountPhySpec) {
            phySpecCounter.incrementAndGet();
            phyPartPosition = phySpecCounter.longValue();
        }

        Long parentPosition = 0L;
        PartitionSpec parentPartSpec = buildParams.getParentPartSpec();
        if (parentPartSpec != null) {
            parentPosition = parentPartSpec.getPosition();
        }

        PartitionSpec partSpec = new PartitionSpec();
        String partName = null;
        SqlPartitionValue value = null;
        List<SqlPartitionValueItem> itemsOfVal = null;
        int partColCnt = partColMetaList.size();
        boolean isMultiCols = partColCnt > 1;
        RelDataTypeFactory typeFactory = new TddlTypeFactoryImpl(TddlRelDataTypeSystemImpl.getInstance());
        List<SearchDatumInfo> partBoundValues = new ArrayList<>();
        PartitionRouter router = null;

        if (!useAutoPart) {
            SqlIdentifier partNameId = (SqlIdentifier) partNameAst;
            partName = SQLUtils.normalizeNoTrim(partNameId.toString());

            // all part name should convert to lower case
            partName = PartitionNameUtil.toLowerCase(partName);

            value = partBndValuesAst;
            itemsOfVal = value.getItems();

            PartitionInfoUtil.validatePartitionValueFormats(strategy, partColCnt, prefixPartColCnt, partName,
                value);
            if (strategy == PartitionStrategy.LIST_COLUMNS && isMultiCols) {

                // each item is SqlCall of ROW, such "p1 values in ( (2+1,'a','1999-01-01'), (4, 'b', '2000-01-01') )"
                // which each operand of the SqlCall
                // will be SqlCall (such as "2+1" ) or SqlLiteral(such as '2000-01-01' )
                for (int i = 0; i < itemsOfVal.size(); i++) {
                    SqlCall item = (SqlCall) itemsOfVal.get(i).getValue();
                    List<PartitionBoundVal> oneBndVal = new ArrayList<>();
                    // The item must be SqlCall of ROW,
                    // So bndExprRex also must be RexCall of ROW
                    if (item.getKind() != SqlKind.DEFAULT) {
                        RexCall bndExprRex = (RexCall) partBoundExprInfo.get(item);
                        List<RexNode> bndRexValsOfOneItem = bndExprRex.getOperands();
                        for (int j = 0; j < bndRexValsOfOneItem.size(); j++) {
                            RexNode oneBndExpr = bndRexValsOfOneItem.get(j);
                            RelDataType bndValDt = pruningComparator.getDatumRelDataTypes()[j];
                            PartitionInfoUtil.validateBoundValueExpr(oneBndExpr, bndValDt, partIntFunc, strategy);
                            PartitionBoundVal bndVal =
                                PartitionPrunerUtils.getBoundValByRexExpr(oneBndExpr, bndValDt,
                                    PartFieldAccessType.DDL_EXECUTION, context);
                            oneBndVal.add(bndVal);
                        }
                    } else {
                        partSpec.setDefaultPartition(true);
                        PartitionBoundVal bndVal = PartitionBoundVal.createDefaultValue();
                        oneBndVal.add(bndVal);
                    }

                    SearchDatumInfo datum = new SearchDatumInfo(oneBndVal);
                    partBoundValues.add(datum);
                }

            } else {
                // each item is SqlNode may be the SqlCall like Func(const) or the SqlLiteral of const

                if (strategy == PartitionStrategy.LIST || strategy == PartitionStrategy.LIST_COLUMNS) {
                    RelDataType bndValDt = pruningComparator.getDatumRelDataTypes()[0];
                    for (int i = 0; i < itemsOfVal.size(); i++) {
                        SqlNode item = itemsOfVal.get(i).getValue();
                        RexNode bndExprRex = partBoundExprInfo.get(item);
                        PartitionBoundVal bndVal;
                        if (item != null && item.getKind() == SqlKind.DEFAULT) {
                            partSpec.setDefaultPartition(true);
                            bndVal = PartitionBoundVal.createDefaultValue();
                        } else {
                            PartitionInfoUtil.validateBoundValueExpr(bndExprRex, bndValDt, partIntFunc, strategy);
                            bndVal = PartitionPrunerUtils.getBoundValByRexExpr(bndExprRex, bndValDt,
                                PartFieldAccessType.DDL_EXECUTION, context);
                        }
                        List<PartitionBoundVal> oneBndVal = Collections.singletonList(bndVal);
                        SearchDatumInfo datum = new SearchDatumInfo(oneBndVal);
                        partBoundValues.add(datum);
                    }
                } else {
                    List<PartitionBoundVal> oneBndVal = new ArrayList<>();
                    int itemsValCnt = itemsOfVal.size();
                    for (int i = 0; i < itemsValCnt; i++) {
                        SqlNode item = itemsOfVal.get(i).getValue();
                        RexNode bndExprRex = partBoundExprInfo.get(item);
                        RelDataType cmpValDt = pruningComparator.getDatumRelDataTypes()[i];
                        RelDataType bndValDt = getDataTypeForBoundVal(typeFactory, strategy, cmpValDt);

                        PartitionBoundVal boundVal;
                        if (!itemsOfVal.get(i).isMaxValue()) {
                            PartitionInfoUtil.validateBoundValueExpr(bndExprRex, bndValDt, partIntFunc, strategy);
                            boundVal =
                                PartitionPrunerUtils.getBoundValByRexExpr(bndExprRex, bndValDt,
                                    PartFieldAccessType.DDL_EXECUTION, context);
                        } else {
                            if (strategy.isHashed() || strategy.isUdfHashed()) {
                                boundVal = PartitionBoundVal.createHashBoundMaxValue();
                            } else {
                                boundVal = PartitionBoundVal.createMaxValue();
                            }
                        }
                        oneBndVal.add(boundVal);
                    }

                    if ((strategy == PartitionStrategy.RANGE || strategy == PartitionStrategy.RANGE_COLUMNS) &&
                        strategy != PartitionStrategy.DIRECT_HASH) {

                        /**
                         * For Range/Range Columns, build virtual value
                         */
                        if (itemsValCnt < partColCnt && prefixPartColCnt != PartitionInfoUtil.FULL_PART_COL_COUNT
                            && partColCnt > 1) {
                            /**
                             * Auto make up maxvalue for the columns after prefix part columns
                             */
                            for (int i = itemsValCnt; i < partColCnt; i++) {
                                PartitionBoundVal maxVal = PartitionBoundVal.createMaxValue();
                                oneBndVal.add(maxVal);
                            }
                        }
                    } else if (strategy == PartitionStrategy.KEY) {
                        /**
                         * For Key, build hash bound max value
                         */
                        if (itemsValCnt < partColCnt && prefixPartColCnt != PartitionInfoUtil.FULL_PART_COL_COUNT
                            && partColCnt > 1) {
                            /**
                             * Auto make up maxvalue for the columns after prefix part columns
                             */
                            for (int i = itemsValCnt; i < partColCnt; i++) {
                                PartitionBoundVal maxVal = PartitionBoundVal.createHashBoundMaxValue();
                                oneBndVal.add(maxVal);
                            }
                        }
                    }

                    SearchDatumInfo datum = new SearchDatumInfo(oneBndVal);
                    partBoundValues.add(datum);
                }
            }
            if (!StringUtils.isEmpty(partLocality)) {
                partSpec.setLocality(partLocality);
            } else {
                partSpec.setLocality("");
            }

            if (!StringUtils.isEmpty(partComment)) {
                partSpec.setComment(partComment);
            } else {
                partSpec.setComment("");
            }
        } else {

            SqlIdentifier partNameId = (SqlIdentifier) partNameAst;
            if (partNameId != null) {
                partName = SQLUtils.normalizeNoTrim(partNameId.toString());
            } else {
                // auto build hash partition name

                if (partKeyLevel == PartKeyLevel.PARTITION_KEY) {
                    partName = PartitionNameUtil.autoBuildPartitionName(partPosition);
                } else {
                    if (isSpecTemplate) {
                        partName = PartitionNameUtil.autoBuildSubPartitionTemplateName(partPosition);
                    } else {
                        partName = PartitionNameUtil.autoBuildSubPartitionName(parentPartSpec.getName(),
                            PartitionNameUtil.autoBuildSubPartitionTemplateName(partPosition));
                    }
                }
            }

            if (!StringUtils.isEmpty(partComment)) {
                partSpec.setComment(partComment);
            } else {
                partSpec.setComment("");
            }

            RelDataType bndValDt = getDataTypeForBoundVal(typeFactory, strategy, null);
            SearchDatumInfo datum = null;
            if (strategy == PartitionStrategy.HASH || strategy == PartitionStrategy.CO_HASH
                || (strategy == PartitionStrategy.KEY && !isMultiCols)
                || strategy == PartitionStrategy.DIRECT_HASH) {
                // auto build hash partition boundVal
                Long bndJavaVal;
                Object obj = partBoundValBuilder.getPartBoundVal((int) partPosition);
                if (obj instanceof Long[]) {
                    bndJavaVal = ((Long[]) obj)[0];
                } else {
                    bndJavaVal = (Long) obj;
                }
                PartitionBoundVal boundVal =
                    buildOneHashBoundValByLong(context, bndJavaVal, bndValDt, PartFieldAccessType.DDL_EXECUTION);
                datum = new SearchDatumInfo(boundVal);
            } else {
                // build bound value for multi-column key
                Long[] bndJavaVals = (Long[]) partBoundValBuilder.getPartBoundVal((int) partPosition);
                PartitionBoundVal[] boundVals = new PartitionBoundVal[bndJavaVals.length];
                for (int i = 0; i < bndJavaVals.length; i++) {
                    Long bndJavaVal = bndJavaVals[i];
                    PartitionBoundVal bndValForOneCol =
                        buildOneHashBoundValByLong(context, bndJavaVal, bndValDt, PartFieldAccessType.DDL_EXECUTION);
                    boundVals[i] = bndValForOneCol;
                }
                datum = new SearchDatumInfo(boundVals);
            }
            partBoundValues.add(datum);

        }
        PartitionBoundSpec boundSpec = createPartitionBoundSpec(strategy, value, partBoundValues);
        partSpec.setBoundSpec(boundSpec);
        partSpec.setDefaultPartition(boundSpec.isDefaultPartSpec());
        partSpec.setStrategy(strategy);
        partSpec.setName(partName);
        partSpec.setPartLevel(partKeyLevel);
        partSpec.setLogical(isLogicalSpec);
        partSpec.setPosition(partPosition);
        partSpec.setPhyPartPosition(phyPartPosition);
        partSpec.setParentPartPosi(parentPosition);
        partSpec.setSpecTemplate(isSpecTemplate);
        partSpec.setTemplateName(isSpecTemplate ? partName : "");
        partSpec.setUseSpecTemplate(useSubPartByTemp);
        partSpec.setBoundSpaceComparator(PartitionByDefinition.buildBoundSpaceComparator(pruningComparator, strategy));
        partSpec.setEngine(buildParams.getPartEngine());
        return partSpec;
    }

    public static PartitionByDefinition buildCompletePartByDefByAstParams(BuildPartByDefFromAstParams buildParams,
                                                                          SqlPartitionBy sqlPartitionBy) {

        PartitionByDefinition partByDef = buildPartByDefByAstParams(buildParams);
        PartitionTableType tblType = buildParams.getTblType();
        if (tblType.isA(PartitionTableType.PARTITIONED_TABLE)) {
            SqlSubPartitionBy subPartitionBy = sqlPartitionBy.getSubPartitionBy();
            if (subPartitionBy != null) {
                List<SqlNode> subPartCols = subPartitionBy.getColumns();
                SqlNode subPartCntAst = subPartitionBy.getSubPartitionsCount();
                List<SqlNode> subPartSpecsTempOutput = new ArrayList<>();
                PartitionStrategy subPartStrategy =
                    buildSubPartByStrategy(buildParams.getSchemaName(), subPartitionBy, buildParams.getTblType());
                boolean useSubPartTemplate = checkIfUseSubPartTemplate(
                    buildParams,
                    partByDef.getStrategy(),
                    sqlPartitionBy,
                    subPartStrategy,
                    subPartitionBy,
                    subPartSpecsTempOutput);
                BuildPartByDefFromAstParams subPartByAstParam = new BuildPartByDefFromAstParams();
                subPartByAstParam.setSchemaName(buildParams.getSchemaName());
                subPartByAstParam.setTableName(buildParams.getTableName());
                subPartByAstParam.setTableGroupName(buildParams.getTableGroupName());
                subPartByAstParam.setJoinGroupName(buildParams.getJoinGroupName());
                subPartByAstParam.setPartByAstColumns(subPartCols);
                subPartByAstParam.setPartByAstPartitions(subPartSpecsTempOutput);
                subPartByAstParam.setPartCntAst(subPartCntAst);
                subPartByAstParam.setPartByStrategy(subPartStrategy);
                subPartByAstParam.setBoundExprInfo(buildParams.getBoundExprInfo());
                subPartByAstParam.setPkColMetas(buildParams.getPkColMetas());
                subPartByAstParam.setAllColMetas(buildParams.getAllColMetas());
                subPartByAstParam.setTblType(buildParams.getTblType());
                subPartByAstParam.setEc(buildParams.getEc());
                subPartByAstParam.setBuildSubPartBy(true);
                subPartByAstParam.setUseSubPartTemplate(useSubPartTemplate);
                subPartByAstParam.setParentPartSpecs(partByDef.getPartitions());
                subPartByAstParam.setParentPartSpecAstList(sqlPartitionBy.getPartitions());
                subPartByAstParam.setContainNextLevelPartSpec(false);
                subPartByAstParam.setTtlTemporary(buildParams.isTtlTemporary());
                PartitionByDefinition subPartByDef = buildPartByDefByAstParams(subPartByAstParam);
                partByDef.setSubPartitionBy(subPartByDef);
            }
        }
        return partByDef;
    }

    private static boolean checkIfUseSubPartTemplate(BuildPartByDefFromAstParams buildParams,
                                                     PartitionStrategy partStrategy,
                                                     SqlPartitionBy partBy,
                                                     PartitionStrategy subPartStrategy,
                                                     SqlSubPartitionBy subPartBy,
                                                     List<SqlNode> subPartSpecsTempOutput) {
        List<SqlSubPartition> subPartSpecAstList = subPartBy.getSubPartitions();
        SqlNode subPartCntAst = subPartBy.getSubPartitionsCount();

        boolean useSubPartTemplate = false;
        if ((subPartCntAst != null) || (subPartSpecAstList != null && !subPartSpecAstList.isEmpty())) {
            /**
             * The subPartBy specify the subpartitionCount,
             * such as
             * partition by key(a,b) ...
             * subpartition by key(a,b) subpartitions n
             * (...)
             */
            useSubPartTemplate = true;
            for (int i = 0; i < subPartSpecAstList.size(); i++) {
                subPartSpecsTempOutput.add(subPartSpecAstList.get(i));
            }
        } else if (subPartCntAst == null && (subPartSpecAstList == null || subPartSpecAstList.isEmpty())) {
            /**
             * The subPartBy does NOT specify the subpartitionCount,
             * such as
             * partition by key(a,b) ...
             * subpartition by key(a,b)
             * (...)
             */
            if (subPartStrategy == PartitionStrategy.KEY || subPartStrategy == PartitionStrategy.HASH) {
                PartitionStrategy parentPartStrategy = partStrategy;
                SqlNode parentHashPartCntAst = buildParams.getPartCntAst();
                List<SqlNode> parentPartSpecAstList = buildParams.getParentPartSpecAstList();
                if (parentHashPartCntAst == null && (parentPartSpecAstList == null
                    || parentPartSpecAstList.isEmpty())) {
                    /**
                     * The parent partBy specify the partitionCount,
                     * such as
                     * partition by key(a,b) partitions m
                     * subpartition by key(a,b)
                     * (...)
                     */
                    if (parentPartStrategy == PartitionStrategy.KEY
                        || parentPartStrategy == PartitionStrategy.HASH) {
                        /**
                         * Make sure the parent partBy use the strategy of key/hash
                         */

                        boolean specifySubPartSpecForHashStrategy = false;
                        List<SqlNode> partSpecsAst = partBy.getPartitions();
                        if (partSpecsAst != null) {
                            /**
                             * If found the parent partBy contains partSpecs, then need
                             * check if all the partSpecs specifying some subpartSpecs or the count of subpartSpecs
                             */
                            for (int i = 0; i < partSpecsAst.size(); i++) {
                                SqlPartition partAst = (SqlPartition) partSpecsAst.get(i);
                                List<SqlNode> subPartSpecsAst = partAst.getSubPartitions();
                                SqlNode subPartCntOfSpecsAst = partAst.getSubPartitionCount();
                                if (subPartSpecsAst != null && !subPartSpecsAst.isEmpty()) {
                                    /**
                                     * If found any subPartSpecs in any one of parentPartSpec,
                                     * that means a non-tempalated-subpart def is using,
                                     * such as
                                     * partition by key(a,b) partitions m
                                     * subpartition by key(a,b)
                                     * (
                                     *    partition p1 (subpartition p1sp1),
                                     *    partition p2 (subpartition p2sp1,subpartition p2sp2),
                                     *    ...
                                     * )
                                     */
                                    specifySubPartSpecForHashStrategy = true;
                                    break;
                                } else {
                                    if (subPartCntOfSpecsAst != null
                                        && subPartCntOfSpecsAst instanceof SqlNumericLiteral) {
                                        int subPartCountVal = Integer.valueOf(subPartCntOfSpecsAst.toString());
                                        if (subPartCountVal > 0) {
                                            /**
                                             * If found any subPartSpec count ast but with empty subPartSpec in any one of parentPartSpec,
                                             * that means a non-tempalated-subpart def is using,
                                             * such as
                                             * partition by key(a,b) partitions m
                                             * subpartition by key(a,b)
                                             * (
                                             *    partition p1 subpartitions 1,
                                             *    partition p2 subpartitions 2,
                                             *    ...
                                             * )
                                             */
                                            specifySubPartSpecForHashStrategy = true;
                                            break;
                                        }
                                    }
                                }
                            }
                        }

                        if (!specifySubPartSpecForHashStrategy) {
                            useSubPartTemplate = true;
                        }
                    }
                }
            }
        }
        return useSubPartTemplate;
    }

    public static PartitionByDefinition buildPartByDefByAstParams(BuildPartByDefFromAstParams buildParams) {

        String schemaName = buildParams.getSchemaName();
        String tableName = buildParams.getTableName();
        List<SqlNode> partByAstColumns = buildParams.getPartByAstColumns();
        List<SqlNode> partByAstPartitions = buildParams.getPartByAstPartitions();
        PartitionStrategy partByStrategy = buildParams.getPartByStrategy();
        SqlNode partCntAst = buildParams.getPartCntAst();
        Map<SqlNode, RexNode> boundExprInfo = buildParams.getBoundExprInfo();
        List<ColumnMeta> pkColMetas = buildParams.getPkColMetas();
        List<ColumnMeta> allColMetas = buildParams.getAllColMetas();
        PartitionTableType tblType = buildParams.getTblType();
        ExecutionContext ec = buildParams.getEc();

        /**
         * The following properties are only used for building subpartitions
         */
        boolean buildSubPartBy = buildParams.isBuildSubPartBy();
        boolean useSubPartTemplate = buildParams.isUseSubPartTemplate();
        boolean containNextLevelPartSpec = buildParams.isContainNextLevelPartSpec();
        List<PartitionSpec> parentPartSpecs = buildParams.getParentPartSpecs();
        PartKeyLevel partKeyLevel = !buildSubPartBy ? PartKeyLevel.PARTITION_KEY : PartKeyLevel.SUBPARTITION_KEY;

        PartitionByDefinition partitionByDef = new PartitionByDefinition();
        List<SqlNode> columns = new ArrayList<>();
        PartitionStrategy strategy = partByStrategy;
        List<SqlNode> partExprList = new ArrayList<>();
        List<String> partColList = new ArrayList<>();
        List<ColumnMeta> partColMetaList = new ArrayList<>();
        List<RelDataType> partExprTypeList = new ArrayList<>();
        int allPhyGroupCnt = HintUtil.allGroup(schemaName).size();

        boolean ttlTemporary = buildParams.isTtlTemporary();

        if (tblType.isA(PartitionTableType.PARTITIONED_TABLE)) {

            columns = partByAstColumns;
            Map<String, ColumnMeta> allColMetaMap = new HashMap<>();
            for (int j = 0; j < allColMetas.size(); j++) {
                ColumnMeta cm = allColMetas.get(j);
                allColMetaMap.put(cm.getOriginColumnName().toLowerCase(), cm);
            }

            if (!columns.isEmpty()) {
                for (int i = 0; i < columns.size(); i++) {
                    SqlNode colExpr = columns.get(i);
                    partExprList.add(colExpr);
                    String colName = PartitionInfoUtil.findPartitionColumn(colExpr);
                    if (colName == null) {
                        throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_INVALID_PARAMS,
                            String.format("No found the column[%s] from Create Table DDL", colName));
                    }
                    partColList.add(colName.toLowerCase());
                    ColumnMeta partColMeta = allColMetaMap.get(colName.toLowerCase());
                    if (partColMeta == null) {
                        throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_INVALID_PARAMS,
                            String.format("No found the column[%s] from Create Table DDL", colName));
                    }
                    partColMetaList.add(partColMeta);
                }
            } else {
                if (strategy == PartitionStrategy.KEY) {
                    /**
                     * convert to "partition by key()" to "partition by key(cols of primary key)"
                     */
                    initPartColMetasByPkColMetas(pkColMetas, partExprList, partColList, partColMetaList);
                } else {
                    throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_INVALID_PARAMS,
                        "No found any columns in Creating Table");
                }
            }

            int maxPartColCnt = ec.getParamManager().getInt(ConnectionParams.MAX_PARTITION_COLUMN_COUNT);
            if (partColList.size() > maxPartColCnt) {
                throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_INVALID_PARAMS,
                    String.format(
                        "Use too many partition column during create table[%s], the allowed max partition column is %s",
                        tableName, maxPartColCnt));
            }

        } else if (tblType == PartitionTableType.SINGLE_TABLE || tblType == PartitionTableType.GSI_SINGLE_TABLE) {
            // Construct partition info for single table
            strategy = PartitionStrategy.KEY;
            initPartColMetasByPkColMetas(pkColMetas, partExprList, partColList, partColMetaList);
        } else {
            // tblType == PartitionTableType.BROADCAST_TABLE

            // Construct partition info for broadcast table
            strategy = PartitionStrategy.KEY;
            initPartColMetasByPkColMetas(pkColMetas, partExprList, partColList, partColMetaList);
        }

        partExprTypeList = PartitionInfoBuilder.inferPartExprDataTypes(partExprList, partColMetaList);

        SqlOperator partFuncOp = null;
        PartitionIntFunction partIntFunc = null;
        Monotonicity partIntFuncMonotonicity = null;

        PartitionIntFunction[] partFnArr = buildPartFuncArr(strategy, partExprList, partColMetaList);
//            partFuncOp = getPartFuncSqlOperator(strategy, partExprList.get(0));
        partFuncOp = partFnArr[0] == null ? null : partFnArr[0].getSqlOperator();
        if (partFnArr[0] != null) {
//        if (partFuncOp != null) {
//            partIntFunc = partitionByDef.getPartIntFunc();
//            partIntFuncMonotonicity = partitionByDef.getPartIntFuncMonotonicity();
//        }
            partIntFunc = partFnArr[0];
            partFuncOp = partIntFunc.getSqlOperator();
            partIntFuncMonotonicity = partIntFunc.getMonotonicity(partColMetaList.get(0).getDataType());
        } else {

            /**
             *  No use part func, convert all "partition by hash(col)" to "partition by key(col)"
             */
            if (strategy == PartitionStrategy.HASH && columns.size() == 1) {
                strategy = PartitionStrategy.KEY;
            }

            /**
             *  No use part func, convert all "partition by range/list(c1[,c2,...])" to "partition by range/list columns(col)"
             */
            Boolean enableAutoUseColumnsPartition =
                Boolean.valueOf(ConnectionParams.ENABLE_AUTO_USE_COLUMNS_PARTITION.getDefault());
            if (ec != null) {
                enableAutoUseColumnsPartition =
                    ec.getParamManager().getBoolean(ConnectionParams.ENABLE_AUTO_USE_COLUMNS_PARTITION);
            }
            if (enableAutoUseColumnsPartition) {
                if (strategy == PartitionStrategy.LIST) {
                    if (columns.size() > 1) {
                        strategy = PartitionStrategy.LIST_COLUMNS;
                    } else {
                        ColumnMeta partFldCm = partColMetaList.get(0);
                        DataType partFldDt = partFldCm.getField().getDataType();
                        if (!DataTypeUtil.isUnderBigintType(partFldDt)) {
                            strategy = PartitionStrategy.LIST_COLUMNS;
                        }
                    }
                } else if (strategy == PartitionStrategy.RANGE) {
                    if (columns.size() > 1) {
                        strategy = PartitionStrategy.RANGE_COLUMNS;
                    } else {
                        ColumnMeta partFldCm = partColMetaList.get(0);
                        DataType partFldDt = partFldCm.getField().getDataType();
                        if (!DataTypeUtil.isUnderBigintType(partFldDt)) {
                            strategy = PartitionStrategy.RANGE_COLUMNS;
                        }
                    }
                }
            }
        }

        SearchDatumComparator querySpaceComparator = PartitionByDefinition.buildQuerySpaceComparator(partColMetaList);
        SearchDatumComparator pruningSpaceComparator =
            PartitionByDefinition.buildPruningSpaceComparator(partExprTypeList);
        SearchDatumComparator boundSpaceComparator =
            PartitionByDefinition.buildBoundSpaceComparator(pruningSpaceComparator, strategy);
        SearchDatumHasher hasher = PartitionByDefinition.buildHasher(strategy, partColMetaList, partExprTypeList);

        partitionByDef.setPartLevel(partKeyLevel);
        partitionByDef.setPartitionColumnNameList(partColList);
        partitionByDef.setPartitionFieldList(partColMetaList);
        partitionByDef.setPartitionExprList(partExprList);
        partitionByDef.setPartitionExprTypeList(partExprTypeList);
        partitionByDef.setStrategy(strategy);
        partitionByDef.setQuerySpaceComparator(querySpaceComparator);
        partitionByDef.setPruningSpaceComparator(pruningSpaceComparator);
        partitionByDef.setBoundSpaceComparator(boundSpaceComparator);
        partitionByDef.setHasher(hasher);
        partitionByDef.setPartIntFuncOperator(partFuncOp);
        partitionByDef.setPartIntFunc(partIntFunc);
        partitionByDef.setPartIntFuncMonotonicity(partIntFuncMonotonicity);
        partitionByDef.setPartFuncArr(partFnArr);
        partitionByDef.setNeedEnumRange(checkNeedDoEnumRange(strategy, partColMetaList, partFuncOp));
        if (buildSubPartBy) {
            /**
             * current building is doing for subpartitionBy
             */
            partitionByDef.setUseSubPartTemplate(useSubPartTemplate);
        } else {
            /**
             * current building is doing for partitionBy
             */
            partitionByDef.setUseSubPartTemplate(false);
        }

        /**
         * Validate and check partition columns for partition tbl and gsi table
         */
        if (tblType.isA(PartitionTableType.PARTITIONED_TABLE)) {
            PartitionInfoUtil.validatePartitionColumns(partitionByDef);
        }

        AtomicInteger phyPartCounter = new AtomicInteger(0);
        List<PartitionSpec> partSpecList = new ArrayList<>();
        BuildAllPartSpecsFromAstParams buildAllPartSpecsAstParams = new BuildAllPartSpecsFromAstParams();
        buildAllPartSpecsAstParams.setPartKeyLevel(partKeyLevel);
        buildAllPartSpecsAstParams.setPartColMetaList(partColMetaList);
        buildAllPartSpecsAstParams.setPartIntFunc(partIntFunc);
        buildAllPartSpecsAstParams.setPruningSpaceComparator(pruningSpaceComparator);
        buildAllPartSpecsAstParams.setPartitions(partByAstPartitions);
        buildAllPartSpecsAstParams.setPartitionCntAst(partCntAst);
        buildAllPartSpecsAstParams.setPartBoundExprInfo(boundExprInfo);
        buildAllPartSpecsAstParams.setPartStrategy(strategy);
        buildAllPartSpecsAstParams.setTblType(tblType);
        buildAllPartSpecsAstParams.setEc(ec);
        buildAllPartSpecsAstParams.setAllPhyGroupCnt(allPhyGroupCnt);
        buildAllPartSpecsAstParams.setBuildSubPartBy(buildSubPartBy);
        buildAllPartSpecsAstParams.setBuildSubPartSpecTemplate(buildSubPartBy);
        buildAllPartSpecsAstParams.setUseSubPartTemplate(useSubPartTemplate);
        buildAllPartSpecsAstParams.setContainNextLevelPartSpec(containNextLevelPartSpec);
        buildAllPartSpecsAstParams.setPhyPartCounter(phyPartCounter);
        buildAllPartSpecsAstParams.setTtlTemporary(ttlTemporary);
        partSpecList = buildAllPartBySpecList(buildAllPartSpecsAstParams);
        partitionByDef.setPartitions(partSpecList);
        /**
         * Prebuild router of dynamic pruning for first-level-partition or subpartition template
         */
        //PartitionRouter router = PartitionByDefinition.buildPartRouter(partitionByDef, partSpecList);
        PartitionRouter router = PartitionByDefinition.buildPartRouterInner(pruningSpaceComparator,
            boundSpaceComparator, hasher, strategy, partColMetaList, partFnArr, partSpecList);
        partitionByDef.setRouter(router);

        if (buildSubPartBy) {
            /**
             * Build all partitions for second-level-partition
             */
            if (useSubPartTemplate) {
                /**
                 * Build all templated subpartitions
                 */
                if (partSpecList.isEmpty()) {
                    // throw error
                }

                List<PartitionSpec> subPartSpecTemplateList = partSpecList;
                AtomicInteger tmpPhyPartCntCounter = new AtomicInteger(0);
                for (int i = 0; i < parentPartSpecs.size(); i++) {
                    PartitionSpec parentPartSpec = parentPartSpecs.get(i);
                    List<PartitionSpec> subPartSpecList = new ArrayList<>();
                    for (int j = 0; j < subPartSpecTemplateList.size(); j++) {
                        PartitionSpec subPartSpecTemplate = subPartSpecTemplateList.get(j);
                        String parentPartName = parentPartSpec.getName();
                        String subPartName = subPartSpecTemplate.getName();
                        PartitionSpec subPartSpec = subPartSpecTemplate.copy();
                        subPartSpec.setSpecTemplate(false);
                        subPartSpec.setLogical(false);
                        subPartSpec.setName(PartitionNameUtil.autoBuildSubPartitionName(parentPartName, subPartName));
                        /**
                         * all phy subpart from subpart templated should has its own phy position
                         */
                        subPartSpec.setPhyPartPosition(Long.valueOf(tmpPhyPartCntCounter.incrementAndGet()));
                        subPartSpec.setParentPartPosi(parentPartSpec.getPosition());

                        subPartSpecList.add(subPartSpec);
                    }
                    parentPartSpec.setSubPartitions(subPartSpecList);

                    /**
                     * For templated-subpartition, all first-level-partitions have the same bound definitions of subpartitions,
                     * so the sub router of each first-level-partition can use the same router.
                     */
                    parentPartSpec.setSubRouter(router);
                }

            } else {
                /**
                 * Build all actual subpartitions that are use for subpartition pruning
                 */

                if (!partSpecList.isEmpty()) {
                    // should throw error
                }

                List<SqlNode> parentPartSpecAstList = buildParams.getParentPartSpecAstList();
                for (int i = 0; i < parentPartSpecs.size(); i++) {
                    PartitionSpec parentPartSpec = parentPartSpecs.get(i);
                    SqlPartition partBySpecAst = (SqlPartition) parentPartSpecAstList.get(i);
                    List<SqlNode> subPartSpecAstList = partBySpecAst.getSubPartitions();
                    SqlNode subPartHashPartCntAst = partBySpecAst.getSubPartitionCount();
                    BuildAllPartSpecsFromAstParams buildAllSubPartSpecsAstParams = new BuildAllPartSpecsFromAstParams();
                    buildAllSubPartSpecsAstParams.setPartColMetaList(partColMetaList);
                    buildAllSubPartSpecsAstParams.setPartKeyLevel(partKeyLevel);
                    buildAllSubPartSpecsAstParams.setPartIntFunc(partIntFunc);
                    buildAllSubPartSpecsAstParams.setPruningSpaceComparator(pruningSpaceComparator);
                    buildAllSubPartSpecsAstParams.setPartitions(subPartSpecAstList);
                    buildAllSubPartSpecsAstParams.setPartitionCntAst(subPartHashPartCntAst);
                    buildAllSubPartSpecsAstParams.setPartBoundExprInfo(boundExprInfo);
                    buildAllSubPartSpecsAstParams.setPartStrategy(strategy);
                    buildAllSubPartSpecsAstParams.setTblType(tblType);
                    buildAllSubPartSpecsAstParams.setEc(ec);
                    buildAllSubPartSpecsAstParams.setAllPhyGroupCnt(allPhyGroupCnt);
                    buildAllSubPartSpecsAstParams.setBuildSubPartBy(true);
                    buildAllSubPartSpecsAstParams.setBuildSubPartSpecTemplate(false);
                    buildAllSubPartSpecsAstParams.setUseSubPartTemplate(false);
                    buildAllSubPartSpecsAstParams.setContainNextLevelPartSpec(false);
                    buildAllSubPartSpecsAstParams.setParentPartSpec(parentPartSpec);
                    buildAllSubPartSpecsAstParams.setPhyPartCounter(phyPartCounter);
                    buildAllSubPartSpecsAstParams.setTtlTemporary(ttlTemporary);
                    List<PartitionSpec> subPartSpecList = buildAllPartBySpecList(buildAllSubPartSpecsAstParams);
                    parentPartSpec.setSubPartitions(subPartSpecList);

                    /**
                     * For non-templated-subpartition, first-level-partitions may have different bound definitions of subpartitions,
                     * so the sub router of each first-level-partition be build independently.
                     */
                    /**
                     * Prebuild the router of actual subpartitions and save it into their parent partition
                     */
                    parentPartSpec.setSubRouter(
                        PartitionByDefinition.buildPartRouterInner(pruningSpaceComparator, boundSpaceComparator, hasher,
                            strategy, partColMetaList, partFnArr, subPartSpecList));
                }

            }
        }

        return partitionByDef;
    }

    public static PartitionStrategy getPartitionStrategy(SqlPartitionBy sqlPartitionBy) {
        if (sqlPartitionBy instanceof SqlPartitionByRange) {
            boolean isColumns = ((SqlPartitionByRange) sqlPartitionBy).isColumns();
            if (isColumns) {
                return PartitionStrategy.RANGE_COLUMNS;
            } else {
                return PartitionStrategy.RANGE;
            }
        } else if (sqlPartitionBy instanceof SqlPartitionByList) {
            boolean isColumns = ((SqlPartitionByList) sqlPartitionBy).isColumns();
            if (isColumns) {
                return PartitionStrategy.LIST_COLUMNS;
            } else {
                return PartitionStrategy.LIST;
            }
        } else if (sqlPartitionBy instanceof SqlPartitionByHash) {
            boolean isKey = ((SqlPartitionByHash) (sqlPartitionBy)).isKey();
            if (isKey) {
                return PartitionStrategy.KEY;
            } else {
                return PartitionStrategy.DIRECT_HASH;
            }
        }
        throw new UnsupportedOperationException("unreachable: " + sqlPartitionBy.getClass());
    }

    protected static PartitionStrategy buildPartByStrategy(String schemaName, SqlPartitionBy sqlPartitionBy,
                                                           PartitionTableType tblType) {
        PartitionStrategy partStrategy = null;
        boolean isKey;
        if (tblType.isA(PartitionTableType.PARTITIONED_TABLE)) {
            if (!DbInfoManager.getInstance().isNewPartitionDb(schemaName)) {
                throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR, String
                    .format("Failed to create partition tables on db[%s] with mode='drds'", schemaName));
            }
            if (null == sqlPartitionBy) {
                throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR, String
                    .format("Failed to create sharding tables on db[%s] with mode='auto'",
                        schemaName));
            }
            if (sqlPartitionBy instanceof SqlPartitionByRange) {
                boolean isColumns = ((SqlPartitionByRange) sqlPartitionBy).isColumns();
                partStrategy = PartitionStrategy.RANGE;
                if (isColumns) {
                    partStrategy = PartitionStrategy.RANGE_COLUMNS;
                }
            } else if (sqlPartitionBy instanceof SqlPartitionByList) {
                boolean isColumns = ((SqlPartitionByList) sqlPartitionBy).isColumns();
                partStrategy = PartitionStrategy.LIST;
                if (isColumns) {
                    partStrategy = PartitionStrategy.LIST_COLUMNS;
                }
            } else if (sqlPartitionBy instanceof SqlPartitionByHash) {
                partStrategy = tblType == PartitionTableType.COLUMNAR_TABLE ? PartitionStrategy.DIRECT_HASH :
                    PartitionStrategy.HASH;
                isKey = ((SqlPartitionByHash) sqlPartitionBy).isKey();
                if (isKey) {
                    partStrategy = PartitionStrategy.KEY;
                }
            } else if (sqlPartitionBy instanceof SqlPartitionByUdfHash) {
                partStrategy = PartitionStrategy.UDF_HASH;
            } else if (sqlPartitionBy instanceof SqlPartitionByCoHash) {
                partStrategy = PartitionStrategy.CO_HASH;
            }
        } else if (tblType == PartitionTableType.SINGLE_TABLE || tblType == PartitionTableType.GSI_SINGLE_TABLE) {
            // tblType == PartitionTableType.SINGLE_TABLE or PartitionTableType.GSI_SINGLE_TABLE
            partStrategy = PartitionStrategy.KEY;
        } else {
            // tblType == PartitionTableType.BROADCAST_TABLE
            partStrategy = PartitionStrategy.KEY;
        }
        return partStrategy;
    }

    protected static PartitionStrategy buildSubPartByStrategy(String schemaName, SqlSubPartitionBy sqlSubPartitionBy,
                                                              PartitionTableType tblType) {
        PartitionStrategy partStrategy = null;
        boolean isKey;
        if (tblType.isA(PartitionTableType.PARTITIONED_TABLE)) {
            if (sqlSubPartitionBy instanceof SqlSubPartitionByRange) {
                boolean isColumns = sqlSubPartitionBy.isColumns();
                partStrategy = PartitionStrategy.RANGE;
                if (isColumns) {
                    partStrategy = PartitionStrategy.RANGE_COLUMNS;
                }
            } else if (sqlSubPartitionBy instanceof SqlSubPartitionByList) {
                boolean isColumns = sqlSubPartitionBy.isColumns();
                partStrategy = PartitionStrategy.LIST;
                if (isColumns) {
                    partStrategy = PartitionStrategy.LIST_COLUMNS;
                }
            } else if (sqlSubPartitionBy instanceof SqlSubPartitionByHash) {
                partStrategy = tblType == PartitionTableType.COLUMNAR_TABLE ? PartitionStrategy.DIRECT_HASH :
                    PartitionStrategy.HASH;
                isKey = ((SqlSubPartitionByHash) sqlSubPartitionBy).isKey();
                if (isKey) {
                    partStrategy = PartitionStrategy.KEY;
                }
            } else if (sqlSubPartitionBy instanceof SqlSubPartitionByUdfHash) {
                partStrategy = PartitionStrategy.UDF_HASH;
            } else if (sqlSubPartitionBy instanceof SqlSubPartitionByCoHash) {
                partStrategy = PartitionStrategy.CO_HASH;
            }
        } else {
            throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_INVALID_PARAMS,
                "Not allowed to use subpartition on single/broadcast table");
        }
        return partStrategy;
    }

    public static PartitionInfo buildPartInfoByAstParams(BuildPartInfoFromAstParams buildParams) {

        String schemaName = buildParams.getSchemaName();
        String tableName = buildParams.getTableName();
        String tableGroupName = buildParams.getTableGroupName();
        String joinGroupName = buildParams.getJoinGroupName();
        SqlPartitionBy sqlPartitionBy = buildParams.getSqlPartitionBy();
        Map<SqlNode, RexNode> boundExprInfo = buildParams.getBoundExprInfo();
        List<ColumnMeta> pkColMetas = buildParams.getPkColMetas();
        List<ColumnMeta> allColMetas = buildParams.getAllColMetas();
        PartitionTableType tblType = buildParams.getTblType();
        ExecutionContext ec = buildParams.getEc();
        LocalityDesc tblLocality = buildParams.getLocality();
        TableGroupConfig tableGroupConfig = null;
        boolean ttlTemporary = buildParams.isTtlTemporary();
        if (tblLocality.isEmpty() && !StringUtils.isEmpty(tableGroupName)) {
            TableGroupInfoManager tableGroupInfoManager =
                OptimizerContext.getContext(schemaName).getTableGroupInfoManager();
            tableGroupConfig = tableGroupInfoManager.getTableGroupConfigByName(tableGroupName);
            if (!(buildParams.isWithImplicitTableGroup() && tableGroupConfig == null)) {
                if (tableGroupConfig != null) {
                    tblLocality = tableGroupConfig.getLocalityDesc();
                }
            }
        }

        PartitionInfo partitionInfo = new PartitionInfo();
        String tbName = tableName;
        String tbSchema = schemaName;
        PartitionStrategy partStrategy = buildPartByStrategy(schemaName, sqlPartitionBy, tblType);
        boolean containNextLevelPartSpec = sqlPartitionBy != null && sqlPartitionBy.getSubPartitionBy() != null;

        long partFlags = 0L;
        if (ttlTemporary) {
            partFlags |= TablePartitionRecord.FLAG_TTL_TEMPORARY_TABLE;
        }

        /**
         * Build PartitionBy for partition
         */
        BuildPartByDefFromAstParams partByAstParam = new BuildPartByDefFromAstParams();
        partByAstParam.setSchemaName(schemaName);
        partByAstParam.setTableName(tableName);
        partByAstParam.setTableGroupName(tableGroupName);
        partByAstParam.setJoinGroupName(joinGroupName);
        partByAstParam.setPartByAstColumns(sqlPartitionBy == null ? null : sqlPartitionBy.getColumns());
        partByAstParam.setPartByAstPartitions(sqlPartitionBy == null ? null : sqlPartitionBy.getPartitions());
        partByAstParam.setPartCntAst(sqlPartitionBy == null ? null : sqlPartitionBy.getPartitionsCount());
        partByAstParam.setPartByStrategy(partStrategy);
        partByAstParam.setBoundExprInfo(boundExprInfo);
        partByAstParam.setPkColMetas(pkColMetas);
        partByAstParam.setAllColMetas(allColMetas);
        partByAstParam.setTblType(tblType);
        partByAstParam.setEc(ec);
        partByAstParam.setBuildSubPartBy(false);
        partByAstParam.setContainNextLevelPartSpec(containNextLevelPartSpec);
        partByAstParam.setTtlTemporary(ttlTemporary);
        PartitionByDefinition partByDef = buildCompletePartByDefByAstParams(partByAstParam, sqlPartitionBy);
        PartitionByDefinition subPartByDef = partByDef.getSubPartitionBy();
        partitionInfo.setPartitionBy(partByDef);
        partitionInfo.getPartitionBy().setSubPartitionBy(subPartByDef);

        PartitionInfoUtil.validateLocality(schemaName, tblLocality, partByDef);

        partitionInfo.setMetaVersion(1L);
        partitionInfo.setSpTemplateFlag(subPartByDef == null ? TablePartitionRecord.SUBPARTITION_TEMPLATE_NOT_EXISTED :
            subPartByDef.isUseSubPartTemplate() ? TablePartitionRecord.SUBPARTITION_TEMPLATE_USING :
                TablePartitionRecord.SUBPARTITION_TEMPLATE_UNUSED);
        partitionInfo.setTableGroupId(TableGroupRecord.INVALID_TABLE_GROUP_ID);
        partitionInfo.setTableName(tbName);
        partitionInfo.setTableSchema(tbSchema);
        partitionInfo.setAutoFlag(TablePartitionRecord.PARTITION_AUTO_BALANCE_DISABLE);
        partitionInfo.setPartFlags(partFlags);
        partitionInfo.setTableType(tblType);
        partitionInfo.setRandomTableNamePatternEnabled(ec.isRandomPhyTableEnabled());
        partitionInfo.setSessionVars(saveSessionVars(ec));
        partitionInfo.setLocality(tblLocality.toString());

        PartitionInfoUtil.generateTableNamePattern(partitionInfo, tbName);
        PartitionInfoUtil.generatePartitionLocation(partitionInfo, tableGroupName,
            buildParams.isWithImplicitTableGroup(), joinGroupName, ec, tblLocality);

        partitionInfo.initPartSpecSearcher();
        PartitionInfoUtil.validatePartitionInfoForDdl(partitionInfo, ec);
        return partitionInfo;
    }

    protected static PartitionByDefinition buildCompletePartByDefByMetaDbParams(
        BuildPartByDefFromMetaDbParams buildParams) {

        List<TablePartitionSpecConfig> partSpecConfigList = buildParams.getPartitionSpecConfigs();
        Integer spSubPartTempFlag = buildParams.getSpPartTempFlag();
        String defaultDbIndex = buildParams.getDefaultDbIndex();
        AtomicLong phyPartSpecCounter = buildParams.getPhyPartSpecCounter();
        boolean useSubPartBy = false;
        boolean useSubPartByTemp = false;
        if (spSubPartTempFlag != TablePartitionRecord.SUBPARTITION_TEMPLATE_NOT_EXISTED) {
            useSubPartBy = true;
            if (spSubPartTempFlag == TablePartitionRecord.SUBPARTITION_TEMPLATE_USING) {
                useSubPartByTemp = true;
            }
        }
        TablePartitionSpecConfig partSpecConfig0 = partSpecConfigList.get(0);
        TablePartitionRecord logTblConfig = buildParams.getLogTblConfig();
        PartitionTableType tblType = PartitionTableType.getTypeByIntVal(logTblConfig.getTblType());

        /**
         * Build the partBy and the partSpecList for 1st-level partition
         */
        PartitionByDefinition partByDef = buildPartByDefByMetaDbParams(buildParams);
        if (!useSubPartBy) {
            return partByDef;
        }

        /**
         * Build the subPartByTemlate only for 2nd-level partition
         */
        List<TablePartitionSpecConfig> partRecList = buildParams.getPartitionSpecConfigs();
        BuildPartByDefFromMetaDbParams subPartByDefMetaDbParams = new BuildPartByDefFromMetaDbParams();
        subPartByDefMetaDbParams.setBuildSubPartBy(true);
        subPartByDefMetaDbParams.setBuildSubPartByTemp(true);
        subPartByDefMetaDbParams.setLogTblConfig(buildParams.getLogTblConfig());
        subPartByDefMetaDbParams.setParentSpecConfig(partSpecConfig0);
        subPartByDefMetaDbParams.setAllColumnMetas(buildParams.getAllColumnMetas());
        subPartByDefMetaDbParams.setPartitionSpecConfigs(partSpecConfig0.getSubPartitionSpecConfigs());
        subPartByDefMetaDbParams.setPartitionGroupRecordsMap(buildParams.getPartitionGroupRecordsMap());
        subPartByDefMetaDbParams.setSpPartTempFlag(buildParams.getSpPartTempFlag());
        subPartByDefMetaDbParams.setDefaultDbIndex(defaultDbIndex);
        subPartByDefMetaDbParams.setPhyPartSpecCounter(phyPartSpecCounter);
        PartitionByDefinition subPartBy = buildPartByDefByMetaDbParams(subPartByDefMetaDbParams);
        partByDef.setSubPartitionBy(subPartBy);

        /**
         * Build the all partSpec list of subpartitions for each partition
         */
        List<PartitionSpec> partSpecList = partByDef.getPartitions();
        for (int i = 0; i < partSpecList.size(); i++) {
            PartitionSpec partSpec = partSpecList.get(i);
            TablePartitionSpecConfig partSpecConfig = partRecList.get(i);

            BuildAllPartSpecsFromMetaDbParams allPartSpecsMetaDbParams = new BuildAllPartSpecsFromMetaDbParams();
            allPartSpecsMetaDbParams.setPartitionSpecConfigs(partSpecConfig.getSubPartitionSpecConfigs());
            allPartSpecsMetaDbParams.setPartitionGroupRecordsMap(buildParams.getPartitionGroupRecordsMap());
            allPartSpecsMetaDbParams.setPartStrategy(subPartBy.getStrategy());
            allPartSpecsMetaDbParams.setPartExprTypeList(subPartBy.getPartitionExprTypeList());
            allPartSpecsMetaDbParams.setBoundSpaceComparator(subPartBy.getBoundSpaceComparator());
            allPartSpecsMetaDbParams.setDefaultDbIndex(defaultDbIndex);
            allPartSpecsMetaDbParams.setTblType(tblType);
            allPartSpecsMetaDbParams.setBuildSubPartBy(true);
            allPartSpecsMetaDbParams.setBuildSubPartByTemp(false);
            allPartSpecsMetaDbParams.setUseSubPartByTemp(useSubPartByTemp);
            allPartSpecsMetaDbParams.setUseSubPartBy(useSubPartBy);
            allPartSpecsMetaDbParams.setPhyPartSpecCounter(phyPartSpecCounter);
            allPartSpecsMetaDbParams.setParentSpecPosition(partSpec.getPosition());
            List<PartitionSpec> subPartSpecList = buildAllPartSpecsByMetaParams(allPartSpecsMetaDbParams);
            partSpec.setSubPartitions(subPartSpecList);

            PartitionRouter subRouter =
                PartitionByDefinition.buildPartRouterInner(subPartBy.getPruningSpaceComparator(),
                    subPartBy.getBoundSpaceComparator(), subPartBy.getHasher(), subPartBy.getStrategy(),
                    subPartBy.getPartitionFieldList(), subPartBy.getPartFuncArr(), subPartSpecList);
            partSpec.setSubRouter(subRouter);
        }
        return partByDef;
    }

    protected static PartitionByDefinition buildPartByDefByMetaDbParams(BuildPartByDefFromMetaDbParams buildParams) {
        TablePartitionRecord logTblConfig = buildParams.getLogTblConfig();
        List<TablePartitionSpecConfig> partitionSpecConfigs = buildParams.getPartitionSpecConfigs();
        List<ColumnMeta> allColumnMetas = buildParams.getAllColumnMetas();
        Map<Long, PartitionGroupRecord> partitionGroupRecordsMap = buildParams.getPartitionGroupRecordsMap();
        Integer spPartTempFlag = buildParams.getSpPartTempFlag();
        String defaultDbIndex = buildParams.getDefaultDbIndex();
        AtomicLong phyPartSpecCounter = buildParams.getPhyPartSpecCounter();
        PartitionTableType tblType = PartitionTableType.getTypeByIntVal(logTblConfig.getTblType());

        boolean buildSubPartBy = buildParams.isBuildSubPartBy();
        boolean buildSubPartByTemp = buildParams.isBuildSubPartByTemp();
        boolean useSubPartBy = spPartTempFlag != TablePartitionRecord.SUBPARTITION_TEMPLATE_NOT_EXISTED;
        boolean useSubPartByTemp = spPartTempFlag == TablePartitionRecord.SUBPARTITION_TEMPLATE_USING;

        List<SqlNode> partExprList = new ArrayList<>();
        List<String> partColList = new ArrayList<>();
        List<ColumnMeta> partColMetaList = new ArrayList<>();
        PartKeyLevel partKeyLevel = PartKeyLevel.PARTITION_KEY;
        if (buildSubPartBy) {
            partKeyLevel = PartKeyLevel.SUBPARTITION_KEY;
        }

        PartitionIntFunction partIntFunc = null;
        Monotonicity partIntFuncMonotonicity = null;
        SqlOperator partFuncOp = null;

        TablePartitionRecord p0Config = partitionSpecConfigs.get(0).getSpecConfigInfo();
        PartitionStrategy partStrategy = null;
        List<RelDataType> partExprTypeList = new ArrayList<>();
        String partMethod = p0Config.getPartMethod();
        String partExprStr = p0Config.getPartExpr();
        partStrategy = PartitionStrategy.valueOf(partMethod);

        /**
         * The raw partition expression from create tbl ddl or meta db
         */
        List<SqlPartitionValueItem> partitionExprList = PartitionInfoUtil.buildPartitionExprByString(partExprStr);
        partExprList = partitionExprList.stream().map(o -> o.getValue()).collect(Collectors.toList());

        /**
         * list of column name of partition fields
         */
        for (int i = 0; i < partitionExprList.size(); i++) {
            SqlNode partExpr = partitionExprList.get(i).getValue();
            String colName = PartitionInfoUtil.findPartitionColumn(partExpr);
            partColList.add(colName.toLowerCase());
        }

        /**
         * find all the column fields according to the list of partition column names
         */
        if (allColumnMetas != null && !allColumnMetas.isEmpty()) {
            for (int i = 0; i < partColList.size(); i++) {
                String partColName = partColList.get(i);
                ColumnMeta cm = null;
                for (int j = 0; j < allColumnMetas.size(); j++) {
                    ColumnMeta colMeta = allColumnMetas.get(j);
                    if (partColName.equalsIgnoreCase(colMeta.getOriginColumnName())) {
                        cm = colMeta;
                        break;
                    }
                }
                if (cm == null) {
                    throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_INVALID_PARAMS,
                        "Failed to init the column field for the partition key " + partColName);
                }
                partColMetaList.add(cm);
            }
        }
        partExprTypeList = PartitionInfoBuilder.inferPartExprDataTypes(partExprList, partColMetaList);

        PartitionByDefinition partitionBy = new PartitionByDefinition();
        PartitionIntFunction[] partFnArr = buildPartFuncArr(partStrategy, partExprList, partColMetaList);
//        partFuncOp = getPartFuncSqlOperator(partStrategy, partExprList.get(0));
        partFuncOp = partFnArr[0] == null ? null : partFnArr[0].getSqlOperator();
        if (partFuncOp != null) {
//            initPartFunc(partExprList, partColMetaList, partFuncOp, partitionBy);
            partIntFunc = partFnArr[0];
            partIntFuncMonotonicity = partFnArr[0].getMonotonicity(partColMetaList.get(0).getDataType());
        }

        SearchDatumComparator querySpaceComparator = PartitionByDefinition.buildQuerySpaceComparator(partColMetaList);
        SearchDatumComparator pruningSpaceComparator =
            PartitionByDefinition.buildPruningSpaceComparator(partExprTypeList);
        SearchDatumComparator boundSpaceComparator =
            PartitionByDefinition.buildBoundSpaceComparator(pruningSpaceComparator, partStrategy);
        SearchDatumHasher hasher = PartitionByDefinition.buildHasher(partStrategy, partColMetaList, partExprTypeList);

        partitionBy.setPartitionColumnNameList(partColList);
        partitionBy.setPartitionFieldList(partColMetaList);
        partitionBy.setPartitionExprList(partExprList);
        partitionBy.setPartitionExprTypeList(partExprTypeList);
        partitionBy.setStrategy(partStrategy);
        partitionBy.setQuerySpaceComparator(querySpaceComparator);
        partitionBy.setPruningSpaceComparator(pruningSpaceComparator);
        partitionBy.setBoundSpaceComparator(boundSpaceComparator);
        partitionBy.setHasher(hasher);
        partitionBy.setPartIntFuncOperator(partFuncOp);
        partitionBy.setPartIntFunc(partIntFunc);
        partitionBy.setPartIntFuncMonotonicity(partIntFuncMonotonicity);
        partitionBy.setPartFuncArr(partFnArr);
        partitionBy.setNeedEnumRange(checkNeedDoEnumRange(partStrategy, partColMetaList, partFuncOp));
        partitionBy.setPartLevel(partKeyLevel);

        BuildAllPartSpecsFromMetaDbParams allPartSpecsMetaDbParams = new BuildAllPartSpecsFromMetaDbParams();
        allPartSpecsMetaDbParams.setPartitionSpecConfigs(partitionSpecConfigs);
        allPartSpecsMetaDbParams.setPartitionGroupRecordsMap(partitionGroupRecordsMap);
        allPartSpecsMetaDbParams.setPartStrategy(partStrategy);
        allPartSpecsMetaDbParams.setPartExprTypeList(partExprTypeList);
        allPartSpecsMetaDbParams.setBoundSpaceComparator(boundSpaceComparator);
        allPartSpecsMetaDbParams.setDefaultDbIndex(defaultDbIndex);
        allPartSpecsMetaDbParams.setTblType(tblType);

        allPartSpecsMetaDbParams.setBuildSubPartBy(buildSubPartBy);
        allPartSpecsMetaDbParams.setBuildSubPartByTemp(buildSubPartByTemp);
        allPartSpecsMetaDbParams.setUseSubPartByTemp(useSubPartByTemp);
        allPartSpecsMetaDbParams.setUseSubPartBy(useSubPartBy);
        allPartSpecsMetaDbParams.setPhyPartSpecCounter(phyPartSpecCounter);
        List<PartitionSpec> partSpecList = buildAllPartSpecsByMetaParams(allPartSpecsMetaDbParams);
        PartitionRouter router = PartitionByDefinition.buildPartRouterInner(pruningSpaceComparator,
            boundSpaceComparator, hasher, partStrategy, partColMetaList, partFnArr, partSpecList);
        //PartitionRouter router = PartitionByDefinition.buildPartRouter(partitionBy, partSpecList);
        partitionBy.setRouter(router);

        if (!buildSubPartBy) {
            partitionBy.setPartitions(partSpecList);
        } else {
            partitionBy.setUseSubPartTemplate(useSubPartByTemp);
            if (useSubPartByTemp) {
                if (buildSubPartByTemp) {
                    partitionBy.setPartitions(partSpecList);
                }
            } else {
                partitionBy.setPartitions(new ArrayList<>());
            }
        }

        return partitionBy;
    }

    private static List<PartitionSpec> buildAllPartSpecsByMetaParams(BuildAllPartSpecsFromMetaDbParams buildParams) {

        List<TablePartitionSpecConfig> partitionSpecConfigs = buildParams.getPartitionSpecConfigs();
        Map<Long, PartitionGroupRecord> partitionGroupRecordsMap = buildParams.getPartitionGroupRecordsMap();
        PartitionStrategy partStrategy = buildParams.getPartStrategy();
        List<RelDataType> partExprTypeList = buildParams.getPartExprTypeList();
        SearchDatumComparator boundSpaceComparator = buildParams.getBoundSpaceComparator();
        String defaultDbIndex = buildParams.getDefaultDbIndex();
        PartitionTableType tblType = buildParams.getTblType();

        boolean buildSubPartBy = buildParams.isBuildSubPartBy();
        boolean buildSubPartByTemp = buildParams.isBuildSubPartByTemp();
        boolean useSubPartBy = buildParams.isUseSubPartBy();
        boolean useSubPartByTemp = buildParams.isUseSubPartByTemp();
        AtomicLong phyPartSpecCounter = buildParams.getPhyPartSpecCounter();
        Long parentSpecPosition = buildParams.getParentSpecPosition();

        List<PartitionSpec> partSpecList = new ArrayList<>();
        for (int i = 0; i < partitionSpecConfigs.size(); i++) {
            TablePartitionSpecConfig partSpecConf = partitionSpecConfigs.get(i);
            TablePartitionRecord confInfo = partSpecConf.getSpecConfigInfo();

            Long id = confInfo.getId();
            Long pid = confInfo.getParentId();
            String schema = confInfo.getTableSchema();
            String tblName = confInfo.getTableName();
            Integer spTempFlag = confInfo.getSpTempFlag();
            Long groupId = confInfo.getGroupId();
            Long metaVer = confInfo.getMetaVersion();
            Integer autoFlag = confInfo.getAutoFlag();
            Integer tblTypeVal = confInfo.getTblType();
            String partName = confInfo.getPartName();
            String partTempName = confInfo.getPartTempName();
            Integer partLevel = confInfo.getPartLevel();
            Integer nextLevel = confInfo.getNextLevel();
            Integer partStatus = confInfo.getPartStatus();
            Long partPosition = confInfo.getPartPosition();
            String partMethod = confInfo.getPartMethod();
            String partExpr = confInfo.getPartExpr();
            String partDesc = confInfo.getPartDesc();
            String partComment = confInfo.getPartComment();
            String partEngine = confInfo.getPartEngine();
            ExtraFieldJSON extras = confInfo.getPartExtras();
            Long partFlags = confInfo.getPartFlags();
            String phyTbl = confInfo.getPhyTable();

            PartitionSpec partitionSpec = new PartitionSpec();
            partitionSpec.setId(id);
            partitionSpec.setParentId(pid);
            partitionSpec.setName(partName);
            partitionSpec.setTemplateName(partTempName);
            partitionSpec.setPosition(partPosition);
            partitionSpec.setStatus(partStatus);
            partitionSpec.setVersion(metaVer);
            partitionSpec.setStrategy(partStrategy);
            // Covert to SqlNode from str for partition desc
            PartitionBoundSpec boundSpec = buildBoundSpecByPartitionDesc(partDesc, partExprTypeList, partStrategy);
            partitionSpec.setBoundSpec(boundSpec);
            partitionSpec.setDefaultPartition(boundSpec.isDefaultPartSpec());
            partitionSpec.setComment(partComment);
            partitionSpec.setEngine(partEngine);
            partitionSpec.setExtras(extras);
            partitionSpec.setFlags(partFlags);
            partitionSpec.setPartLevel(PartKeyLevel.getPartKeyLevelFromIntVal(partLevel));
            partitionSpec.setBoundSpaceComparator(boundSpaceComparator);
            partitionSpec.setUseSpecTemplate(useSubPartByTemp);
            partitionSpec.setParentPartPosi(parentSpecPosition);

            PartitionGroupRecord partitionGroupRecord = partitionGroupRecordsMap.get(confInfo.getGroupId());
            String groupKey = partitionGroupRecord == null ? "" :
                GroupInfoUtil.buildGroupNameFromPhysicalDb(partitionGroupRecord.getPhy_db());
            if (tblType == PartitionTableType.BROADCAST_TABLE && StringUtils.isEmpty(groupKey)) {
                groupKey = defaultDbIndex;
            }
            PartitionLocation location =
                new PartitionLocation(groupKey, phyTbl,
                    partitionGroupRecord == null ? PartitionLocation.INVALID_PARTITION_GROUP_ID :
                        partitionGroupRecord.getId());
            partitionSpec.setLocation(location);

            if (extras != null) {

                if (TStringUtil.isNotEmpty(extras.getLocality())) {
                    LocalityDesc locality = LocalityDesc.parse(extras.getLocality());
                    partitionSpec.setLocality(locality.toString());
                }

                if (extras.getArcState() != null) {
                    partitionSpec.setArcState(extras.getArcState());
                }
            }

            if (!buildSubPartBy) {
                if (useSubPartBy) {
                    partitionSpec.setLogical(true);
                    partitionSpec.setLocation(null);
                    partitionSpec.setSpecTemplate(false);
                    partitionSpec.setPhyPartPosition(null);
                    partitionSpec.setEngine(null);
                } else {
                    partitionSpec.setLogical(false);
                    partitionSpec.setSpecTemplate(false);
                    partitionSpec.setPhyPartPosition(phyPartSpecCounter.incrementAndGet());
                }
            } else {
                if (useSubPartByTemp) {
                    if (buildSubPartByTemp) {
                        partitionSpec.setLogical(true);
                        partitionSpec.setLocation(null);
                        partitionSpec.setSpecTemplate(true);
                        partitionSpec.setName(partitionSpec.getTemplateName());
                        partitionSpec.setPhyPartPosition(null);
                        partitionSpec.setEngine(null);
                    } else {
                        partitionSpec.setLogical(false);
                        partitionSpec.setSpecTemplate(false);
                        partitionSpec.setPhyPartPosition(phyPartSpecCounter.incrementAndGet());
                    }
                } else {
                    if (!buildSubPartByTemp) {
                        partitionSpec.setLogical(false);
                        partitionSpec.setSpecTemplate(false);
                        partitionSpec.setPhyPartPosition(phyPartSpecCounter.incrementAndGet());
                    }
                }
            }
            partSpecList.add(partitionSpec);
        }
        return partSpecList;
    }

    public static PartitionInfo buildPartInfoByMetaDbParams(BuildPartInfoFromMetaDbParams buildParams) {

        TablePartitionConfig tbPartConf = buildParams.getTbPartConf();
        List<ColumnMeta> allColumnMetas = buildParams.getAllColumnMetas();
        Map<Long, PartitionGroupRecord> partitionGroupRecordsMap = buildParams.getPartitionGroupRecordsMap();
        TablePartitionRecord logTblConfig = tbPartConf.getTableConfig();
        PartitionInfo partitionInfo = new PartitionInfo();
        TablePartitionRecord logTableConfig = tbPartConf.getTableConfig();
        PartitionTableType tblType = PartitionTableType.getTypeByIntVal(logTblConfig.getTblType());

        Long tblId = logTableConfig.getId();
        String tableSchema = logTableConfig.getTableSchema();
        String tableName = logTableConfig.getTableName();
        Long tblGroupId = logTableConfig.getGroupId();
        Long metaDbVersion = logTableConfig.getMetaVersion();
        Integer spTempFlag = logTableConfig.getSpTempFlag();
        Integer logTbStatus = logTableConfig.getPartStatus();
        Integer autoFlag = logTableConfig.getAutoFlag();
        Long partFlags = logTableConfig.getPartFlags();
        ExtraFieldJSON partExtras = logTableConfig.getPartExtras();

        partitionInfo.setTableId(tblId);
        partitionInfo.setTableSchema(tableSchema);
        partitionInfo.setTableName(tableName);
        partitionInfo.setTableGroupId(tblGroupId);
        partitionInfo.setMetaVersion(metaDbVersion);
        partitionInfo.setSpTemplateFlag(spTempFlag);
        partitionInfo.setStatus(logTbStatus);
        partitionInfo.setAutoFlag(autoFlag);
        partitionInfo.setPartFlags(partFlags);
        partitionInfo.setTableType(tblType);

        PartInfoSessionVars sessionVars = new PartInfoSessionVars();
        if (partExtras != null) {
            partitionInfo.setLocality(logTableConfig.getPartExtras().getLocality());
            String tablePattern = partExtras.getPartitionPattern();
            if (StringUtils.isEmpty(tablePattern)) {
                partitionInfo.setRandomTableNamePatternEnabled(false);
            } else {
                partitionInfo.setTableNamePattern(tablePattern);
            }
            if (partExtras.getTimeZone() != null) {
                sessionVars.setTimeZone(partExtras.getTimeZone());
            }
            if (partExtras.getCharset() != null) {
                sessionVars.setCharset(partExtras.getCharset());
            }
            if (partExtras.getCollation() != null) {
                sessionVars.setCollation(partExtras.getCollation());
            }
        } else {
            partitionInfo.setRandomTableNamePatternEnabled(false);
        }
        partitionInfo.setSessionVars(sessionVars);
        List<TablePartitionSpecConfig> partSpecConfigs = tbPartConf.getPartitionSpecConfigs();

        String defaultDbIndex = null;
        if (tblType == PartitionTableType.BROADCAST_TABLE) {
            defaultDbIndex = TableInfoManager.getSchemaDefaultDbIndex(logTblConfig.getTableSchema());
        }
        AtomicLong phyPartSpecCounter = new AtomicLong(0L);

        BuildPartByDefFromMetaDbParams partByDefMetaDbParams = new BuildPartByDefFromMetaDbParams();
        partByDefMetaDbParams.setLogTblConfig(tbPartConf.getTableConfig());
        partByDefMetaDbParams.setAllColumnMetas(allColumnMetas);
        partByDefMetaDbParams.setParentSpecConfig(null);
        partByDefMetaDbParams.setPartitionSpecConfigs(partSpecConfigs);
        partByDefMetaDbParams.setPartitionGroupRecordsMap(partitionGroupRecordsMap);
        partByDefMetaDbParams.setSpPartTempFlag(spTempFlag);
        partByDefMetaDbParams.setDefaultDbIndex(defaultDbIndex);
        partByDefMetaDbParams.setPhyPartSpecCounter(phyPartSpecCounter);
        PartitionByDefinition partByDef = buildCompletePartByDefByMetaDbParams(partByDefMetaDbParams);
        partitionInfo.setPartitionBy(partByDef);

        partitionInfo.initPartSpecSearcher();
        return partitionInfo;
    }

    public static List<PartitionSpec> buildAllPartBySpecList(BuildAllPartSpecsFromAstParams buildParams) {

        List<ColumnMeta> partColMetaList = buildParams.getPartColMetaList();
        PartKeyLevel partKeyLevel = buildParams.getPartKeyLevel();
        PartitionIntFunction partIntFunc = buildParams.getPartIntFunc();
        SearchDatumComparator pruningSpaceComparator = buildParams.getPruningSpaceComparator();

        /**
         * If buildSubPartBy = false, the partitions are the first-level partitionSpec definitions,
         * If buildSubPartBy = true,
         *      if buildSubPartSpecTemplate=true
         *       then the partitions are the subpartition-templated partitionSpec definitions.
         *      if buildSubPartSpecTemplate=false
         *       then the partitions are a subpartition partitionSpec definitions of one partition.
         */
        List<SqlNode> partitions = buildParams.getPartitions();

        AtomicInteger phyPartCounter = buildParams.getPhyPartCounter();
        SqlNode partitionCntAst = buildParams.getPartitionCntAst();
        Map<SqlNode, RexNode> partBoundExprInfo = buildParams.getPartBoundExprInfo();
        PartitionStrategy strategy = buildParams.getPartStrategy();
        PartitionTableType tblType = buildParams.getTblType();
        ExecutionContext ec = buildParams.getEc();
        PartitionSpec parentPartSpec = buildParams.getParentPartSpec();
        int allPhyGroupCnt = buildParams.getAllPhyGroupCnt();
        boolean buildSubPartBy = buildParams.isBuildSubPartBy();
        /**
         * Label if current building is doing for subpartition template.
         */
        boolean buildSubPartSpecTemplate = buildParams.isBuildSubPartSpecTemplate();
        boolean useSubPartSpecTemplate = buildParams.isUseSubPartTemplate();
        boolean containNextPartLevelSpec = buildParams.isContainNextLevelPartSpec();

        /**
         * Label if a partition has its own non-templated subpartitions definitions.
         */
        boolean notExistSubPartSpecDef =
            (!buildSubPartBy) || (buildSubPartBy && !useSubPartSpecTemplate && (partitions == null
                || partitions.isEmpty()));

        List<PartitionSpec> partSpecList = new ArrayList<>();
        Long initHashPartCnt = 0L;
        if (tblType == PartitionTableType.SINGLE_TABLE || tblType == PartitionTableType.GSI_SINGLE_TABLE) {
            initHashPartCnt = 1L;
        } else if (tblType == PartitionTableType.BROADCAST_TABLE || tblType == PartitionTableType.GSI_BROADCAST_TABLE) {
            initHashPartCnt = Long.valueOf(allPhyGroupCnt);
        } else {
            if (partitions != null) {
                /**
                 * Handle for the following cases:
                 *  <pre>
                 *      case1:
                 *          partition by key(pk)
                 *          partitions 3 (
                 *              partition p1 values less than (100000000),
                 *              partition p2 values less than (200000000),
                 *              partition p3 values less than (maxvalue)
                 *          )
                 *     case2:
                 *          ...
                 *          partition p1 values less than (100000000) (
                 *              subpartition sp1
                 *              subpartition sp2
                 *              subpartition sp3
                 *          )
                 *          ...
                 *
                 *     case3:
                 *          ...
                 *          partition p1 values less than (100000000) (
                 *              subpartition sp1 values less than (100000000),
                 *              subpartition sp2 values less than (200000000),
                 *              subpartition sp3 values less than (maxvalue)
                 *          )
                 *          ...
                 *
                 *  </pre>
                 *  The above cases specify hash partitions by both the list of (sub)partition and (sub)partition count
                 *  so initHashPartCnt should be set the value to the size of partitions
                 */
                initHashPartCnt = Long.valueOf(partitions.size());
            }
        }
        String partEngine = TablePartitionRecord.PARTITION_ENGINE_INNODB;
        if (tblType == PartitionTableType.COLUMNAR_TABLE) {
            partEngine = TablePartitionRecord.PARTITION_ENGINE_COLUMNAR;
        }

        Long finalHashPartCnt;
        Integer maxPhysicalPartitions = Integer.valueOf(ConnectionParams.MAX_PHYSICAL_PARTITION_COUNT.getDefault());
        if (ec != null) {
            maxPhysicalPartitions = ec.getParamManager().getInt(ConnectionParams.MAX_PHYSICAL_PARTITION_COUNT);
        }

        if (partitionCntAst != null && partitionCntAst instanceof SqlNumericLiteral) {
            if (partitions != null && !partitions.isEmpty()) {
                int partCountIntVal = Integer.valueOf(partitionCntAst.toString());
                if (partCountIntVal != partitions.size()) {
                    /**
                     * Such as wrong definition:
                     *    create table (...)
                     *    partition by xxx_strategy(pk) partitions 4
                     *    (partition p1 ...);
                     *
                     */
                    throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_INVALID_PARAMS,
                        String
                            .format("Wrong number of (sub)partitions defined"));
                }
            }
        }

        if (!(strategy == PartitionStrategy.HASH || strategy == PartitionStrategy.KEY
            || strategy == PartitionStrategy.DIRECT_HASH || strategy == PartitionStrategy.CO_HASH)) {

            if (partitions.size() > maxPhysicalPartitions) {
                throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_INVALID_PARAMS,
                    String
                        .format("Too many partitions [%s] (including subpartitions) were defined", partitions.size()));
            }

            boolean needCheckIfPartSpecsDefEmpty = true;
            if (buildSubPartBy) {
                if (!useSubPartSpecTemplate && buildSubPartSpecTemplate) {
                    /**
                     * only the following cases can ignore the check:
                     * 1. use non-template subpart range/list/udf tbl
                     * 2. current partSpecs buildings are NOT for non-template subpart specs
                     */
                    needCheckIfPartSpecsDefEmpty = false;
                }
            }
            if (needCheckIfPartSpecsDefEmpty && partitions.isEmpty()) {
                // For RANGE/LIST/UDF_HASH partitions each partition must be defined
                throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_INVALID_PARAMS,
                    String
                        .format("For range/list partitions each partition must be defined"));
            }

            for (int i = 0; i < partitions.size(); i++) {

                BuildPartSpecFromAstParams partSpecAstParams = new BuildPartSpecFromAstParams();
                partSpecAstParams.setContext(ec);
                partSpecAstParams.setPartColMetaList(partColMetaList);
                partSpecAstParams.setPartKeyLevel(partKeyLevel);
                partSpecAstParams.setPartIntFunc(partIntFunc);
                partSpecAstParams.setPruningComparator(pruningSpaceComparator);
                partSpecAstParams.setPartBoundExprInfo(partBoundExprInfo);
                partSpecAstParams.setPartBoundValBuilder(null);
                partSpecAstParams.setAutoBuildPart(false);
                partSpecAstParams.setUseSpecTemplate(useSubPartSpecTemplate);

                if (!buildSubPartBy) {
                    SqlPartition partSpecAst = (SqlPartition) partitions.get(i);
                    partSpecAstParams.setPartNameAst(partSpecAst.getName());
                    partSpecAstParams.setPartBndValuesAst(partSpecAst.getValues());
                    partSpecAstParams.setPartComment(partSpecAst.getComment());
                    partSpecAstParams.setPartLocality(partSpecAst.getLocality());
                } else {
                    SqlSubPartition partSpecAst = (SqlSubPartition) partitions.get(i);
                    partSpecAstParams.setPartNameAst(partSpecAst.getName());
                    partSpecAstParams.setPartBndValuesAst(partSpecAst.getValues());
                    partSpecAstParams.setPartComment(partSpecAst.getComment());
                    partSpecAstParams.setPartLocality(partSpecAst.getLocality());
                }

                //todo (luoyanxin,chengbi) why consider buildSubPartSpecTemplate here
                partSpecAstParams.setLogical(
                    containNextPartLevelSpec || (buildSubPartSpecTemplate));
                partSpecAstParams.setSpecTemplate(buildSubPartSpecTemplate);
                partSpecAstParams.setSubPartSpec(buildSubPartBy);
                partSpecAstParams.setPhySpecCounter(phyPartCounter);

                partSpecAstParams.setStrategy(strategy);
                partSpecAstParams.setPartPosition(i + 1);
                //partSpecAstParams.setPrefixPartColCnt(PartitionInfoUtil.FULL_PART_COL_COUNT);
                partSpecAstParams.setAllLevelPrefixPartColCnts(PartitionInfoUtil.ALL_LEVEL_FULL_PART_COL_COUNT_LIST);
                partSpecAstParams.setParentPartSpec(parentPartSpec);
                partSpecAstParams.setPartEngine(partEngine);
                PartitionSpec partSpec = buildPartSpecByAstParams(partSpecAstParams);

                partSpecList.add(partSpec);
            }

            if (strategy == PartitionStrategy.LIST_COLUMNS || strategy == PartitionStrategy.LIST) {
                /**
                 * validate:
                 * 1. only allow at most one default partition
                 * 2. default partition must be in last position
                 * */
                int defaultPartSpecCnt = 0;
                int defaultPartPosition = 0;
                for (int idx = 0; idx < partSpecList.size(); idx++) {
                    PartitionSpec spec = partSpecList.get(idx);
                    if (spec.isDefaultPartition()) {
                        defaultPartSpecCnt++;
                        defaultPartPosition = idx;
                    }
                }
                if (defaultPartSpecCnt > 1) {
                    throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_INVALID_PARAMS,
                        "Only at most one default partition is allowed");
                }
                if (defaultPartSpecCnt == 1 && defaultPartPosition != partSpecList.size() - 1) {
                    throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_INVALID_PARAMS,
                        "Default partition must be the last partition");
                }
            }

        } else {

            /**
             * On Hash/Key Policy, the size of partitions will be 0.
             * When partitions.size = 0, it may be one of the following cases:
             *
             * <pre>
             *     case1-0:
             *          create table (...)
             *          partition by key(pk);
             *          so the partition size is the default config of partition size
             *          [partition use the definition of key policy without specifying partition count]
             *     case1-1:
             *          create table (...)
             *          partition by key(pk)
             *          partitions 8;
             *          so the partition size is the default config of partition size
             *          [partition use the definition of key policy with specifying partition count]
             *     case2:
             *          create table (...)
             *          partition by key(pk)
             *          subpartition by key(name);
             *          [subpartition use the templated definition of key policy without specifying subpartition count]
             *          so the partition size is the default config of partition size,
             *          so the subpartition size is the default config of partition size
             *     case3-0:
             *          create table (...)
             *          partition by range(pk)
             *          subpartition by key(name)
             *          subpartitions 8
             *          (
             *              partition p1 values less than (100),
             *              partition p2 values less than (maxvalue)
             *          )
             *          [subpartition use the non-templated definition of key policy with specifying subpartition count]
             *     case3-1:
             *          create table (...)
             *          partition by range(pk)
             *          subpartition by key(name)
             *          (
             *              partition p1 values less than (100) (
             *                  subpartitions 3
             *              ),
             *              partition p2 values less than (maxvalue) (
             *                  subpartition sp4,
             *                  subpartition sp5
             *              )
             *          )
             *          [subpartition use the non-templated definition of key policy with specifying subpartition count
             *           by using "subpartitions" or list of "subpartition" ]
             *     case4:
             *          create table (...)
             *          partition by range(pk)
             *          subpartition by key(name)
             *          (
             *              partition p1 values less than (100) (
             *                  subpartitions 5
             *              ),
             *              partition p2 values less than (maxvalue)
             *          )
             *          [subpartition use the non-templated definition of key policy,
             *           but some partition has no definition of its subpartition]
             * </pre>
             */

            /**
             * Only the following case need auto generate hash partition count and partitions :
             * case1-0: buildSubPartBy=false and hashPartitionCntAst == null
             * case2: buildSubPartBy=true and hashPartitionCntAst == null and useSubPartTemplate=true
             * case4: buildSubPartBy=true and hashPartitionCntAst == null and useSubPartTemplate=false
             *        and some partition has no subpartition definition.
             *
             */
            finalHashPartCnt = autoDecideHashPartCountIfNeed(
                ec,
                tblType,
                buildSubPartBy,
                buildSubPartSpecTemplate,
                notExistSubPartSpecDef,
                maxPhysicalPartitions,
                initHashPartCnt,
                partitionCntAst);

            boolean specifyPartitionsAst = partitions != null && !partitions.isEmpty();
            if (!specifyPartitionsAst) {
                /**
                 * If buildSubPartBy = false, the partitions are the first-level partitionSpec definitions,
                 * If buildSubPartBy = true, the partitions are the subpartition-templated partitionSpec definitions.
                 */

                /**
                 * On Hash/Key Policy, the size of partitions will be 0.
                 * When partitions.size() = 0, it may be the following cases and need auto build partitionSpec:
                 *
                 * <pre>
                 *     case1-0:
                 *          create table (...)
                 *          partition by key(pk);
                 *          so the partition size is the default config of partition size
                 *          [partition use the definition of key policy without specifying partition count]
                 *     case1-1:
                 *          create table (...)
                 *          partition by key(pk)
                 *          partitions 8;
                 *          so the partition size is the default config of partition size
                 *          [partition use the definition of key policy with specifying partition count]
                 *     case2:
                 *          create table (...)
                 *          partition by key(pk)
                 *          subpartition by key(name);
                 *          [subpartition use the templated definition of key policy without specifying subpartition count]
                 *          so the partition size is the default config of partition size,
                 *          so the subpartition size is the default config of partition size
                 *     case3-0:
                 *          create table (...)
                 *          partition by range(pk)
                 *          subpartition by key(name)
                 *          subpartitions 8
                 *          (
                 *              partition p1 values less than (100),
                 *              partition p2 values less than (maxvalue)
                 *          )
                 *          [subpartition use the non-templated definition of key policy with specifying subpartition count]
                 *     case3-1:
                 *          create table (...)
                 *          partition by range(pk)
                 *          subpartition by key(name)
                 *          (
                 *              partition p1 values less than (100) (
                 *                  subpartitions 4
                 *              ),
                 *              partition p2 values less than (maxvalue) (
                 *                  subpartitions 5
                 *              )
                 *          )
                 *          [subpartition use the non-templated definition of key policy]
                 * </pre>
                 */
                boolean needAutoGenHashPartSpecBndVal =
                    checkNeedAutoGenPartSpecByHashPartCntVal(buildSubPartBy, buildSubPartSpecTemplate,
                        useSubPartSpecTemplate,
                        notExistSubPartSpecDef);

                if (needAutoGenHashPartSpecBndVal) {
                    PartBoundValBuilder boundValBuilder = null;
                    boolean isMultiCol = partColMetaList.size() > 1;
                    if (strategy == PartitionStrategy.HASH || strategy == PartitionStrategy.DIRECT_HASH
                        || strategy == PartitionStrategy.CO_HASH
                        || (strategy == PartitionStrategy.KEY && !isMultiCol)) {
                        boundValBuilder = new HashPartBoundValBuilder(finalHashPartCnt.intValue());
                    } else {
                        boundValBuilder =
                            new KeyPartBoundValBuilder(finalHashPartCnt.intValue(), partColMetaList.size());
                    }
                    for (int i = 0; i < finalHashPartCnt; i++) {
                        BuildPartSpecFromAstParams partSpecAstParams = new BuildPartSpecFromAstParams();
                        partSpecAstParams.setContext(ec);
                        partSpecAstParams.setPartColMetaList(partColMetaList);
                        partSpecAstParams.setPartKeyLevel(partKeyLevel);
                        partSpecAstParams.setPartIntFunc(partIntFunc);
                        partSpecAstParams.setPruningComparator(pruningSpaceComparator);
                        partSpecAstParams.setPartBoundExprInfo(partBoundExprInfo);
                        partSpecAstParams.setPartBoundValBuilder(boundValBuilder);
                        partSpecAstParams.setAutoBuildPart(true);
                        partSpecAstParams.setPartNameAst(null);
                        partSpecAstParams.setPartBndValuesAst(null);
                        partSpecAstParams.setPartComment(null);
                        partSpecAstParams.setPartLocality(null);
                        partSpecAstParams.setStrategy(strategy);
                        partSpecAstParams.setPartPosition(i + 1);
                        //partSpecAstParams.setPrefixPartColCnt(PartitionInfoUtil.FULL_PART_COL_COUNT);
                        partSpecAstParams.setAllLevelPrefixPartColCnts(
                            PartitionInfoUtil.ALL_LEVEL_FULL_PART_COL_COUNT_LIST);

                        partSpecAstParams.setLogical(containNextPartLevelSpec);
                        partSpecAstParams.setSpecTemplate(buildSubPartSpecTemplate);
                        partSpecAstParams.setSubPartSpec(buildSubPartBy);
                        partSpecAstParams.setPhySpecCounter(phyPartCounter);
                        partSpecAstParams.setUseSpecTemplate(useSubPartSpecTemplate);

                        partSpecAstParams.setParentPartSpec(parentPartSpec);
                        partSpecAstParams.setPartEngine(partEngine);
                        PartitionSpec partSpec = buildPartSpecByAstParams(partSpecAstParams);
                        partSpecList.add(partSpec);
                    }
                }

            } else {

                /**
                 * Come here, it must be a partitionBy/subpartitionBy definition of key/hash policy
                 * are defined by partitions count and partitionSpec list
                 * which partitionSpec list are the manually-specifying hash bound definitions , such as
                 * <pre>
                 * case 1:
                 *          create table (...)
                 *          partition by key(pk)
                 *          partitions 3 (
                 *              partition p1 values less than (100000000),
                 *              partition p2 values less than (200000000),
                 *              partition p3 values less than (maxvalue)
                 *          )
                 * case 2:
                 *          create table (...)
                 *          partition by range(pk)
                 *
                 *          subpartition by key(pk)
                 *          subpartitions 3 (
                 *              subpartition p1 values less than (100000000),
                 *              subpartition p2 values less than (200000000),
                 *              subpartition p3 values less than (maxvalue)
                 *          )
                 *
                 *          (
                 *              partition p1 values less than (100000000),
                 *              partition p2 values less than (200000000),
                 *              partition p3 values less than (maxvalue)
                 *          )
                 * case 3:
                 *          create table (...)
                 *          partition by range(pk)
                 *
                 *          subpartition by key(pk)
                 *          subpartitions 3 (
                 *              subpartition p1,
                 *              subpartition p2,
                 *              subpartition p3
                 *          )
                 *
                 *          (
                 *              partition p1 values less than (100000000),
                 *              partition p2 values less than (200000000),
                 *              partition p3 values less than (maxvalue)
                 *          )
                 * </pre>
                 * , so the above cases need check if the hashPartCnt is invalid
                 */
                boolean needCheckHashCntInvalid =
                    (!buildSubPartBy) || (buildSubPartBy && ((useSubPartSpecTemplate && buildSubPartSpecTemplate)
                        || (!useSubPartSpecTemplate && !buildSubPartSpecTemplate)));
                if (needCheckHashCntInvalid) {
                    if (partitions.size() != finalHashPartCnt) {
                        throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_INVALID_PARAMS,
                            String.format(
                                "the count[%s] of the manual defined hash partitions mismatch the count[%s] of `PARTITIONS`",
                                partitions.size(), finalHashPartCnt));
                    }
                }

                boolean specHashSubPartCntByNameList = false;
                if (!buildSubPartBy) {
                    SqlPartition partSpecAst0 = (SqlPartition) partitions.get(0);
                    if (partSpecAst0.getValues() == null) {
                        specHashSubPartCntByNameList = true;
                    }
                } else {
                    SqlSubPartition partSpecAst0 = (SqlSubPartition) partitions.get(0);
                    if (partSpecAst0.getValues() == null) {
                        specHashSubPartCntByNameList = true;
                    }
                }

                PartBoundValBuilder boundValBuilder = null;
                if (specHashSubPartCntByNameList) {
                    boolean isMultiCol = partColMetaList.size() > 1;
                    if (strategy == PartitionStrategy.DIRECT_HASH || strategy == PartitionStrategy.HASH || (
                        strategy == PartitionStrategy.KEY && !isMultiCol)) {
                        boundValBuilder = new HashPartBoundValBuilder(partitions.size());
                    } else {
                        boundValBuilder = new KeyPartBoundValBuilder(partitions.size(), partColMetaList.size());
                    }
                }

                for (int i = 0; i < partitions.size(); i++) {

                    BuildPartSpecFromAstParams partSpecAstParams = new BuildPartSpecFromAstParams();
                    partSpecAstParams.setContext(ec);
                    partSpecAstParams.setPartColMetaList(partColMetaList);
                    partSpecAstParams.setPartKeyLevel(partKeyLevel);
                    partSpecAstParams.setPartIntFunc(partIntFunc);
                    partSpecAstParams.setPruningComparator(pruningSpaceComparator);
                    partSpecAstParams.setPartBoundExprInfo(partBoundExprInfo);
                    partSpecAstParams.setPartBoundValBuilder(boundValBuilder);
                    partSpecAstParams.setAutoBuildPart(specHashSubPartCntByNameList);
                    partSpecAstParams.setUseSpecTemplate(useSubPartSpecTemplate);

                    if (!buildSubPartBy) {
                        SqlPartition partSpecAst = (SqlPartition) partitions.get(i);
                        partSpecAstParams.setPartNameAst(partSpecAst.getName());
                        partSpecAstParams.setPartBndValuesAst(partSpecAst.getValues());
                        partSpecAstParams.setPartComment(partSpecAst.getComment());
                        partSpecAstParams.setPartLocality(partSpecAst.getLocality());
                    } else {
                        SqlSubPartition partSpecAst = (SqlSubPartition) partitions.get(i);
                        partSpecAstParams.setPartNameAst(partSpecAst.getName());
                        partSpecAstParams.setPartBndValuesAst(partSpecAst.getValues());
                        partSpecAstParams.setPartComment(partSpecAst.getComment());
                        partSpecAstParams.setPartLocality(partSpecAst.getLocality());
                    }

                    partSpecAstParams.setLogical(containNextPartLevelSpec);
                    partSpecAstParams.setSpecTemplate(buildSubPartSpecTemplate);
                    partSpecAstParams.setSubPartSpec(buildSubPartBy);
                    partSpecAstParams.setPhySpecCounter(phyPartCounter);

                    partSpecAstParams.setStrategy(strategy);
                    partSpecAstParams.setPartPosition(i + 1);
                    //partSpecAstParams.setPrefixPartColCnt(PartitionInfoUtil.FULL_PART_COL_COUNT);
                    partSpecAstParams.setAllLevelPrefixPartColCnts(
                        PartitionInfoUtil.ALL_LEVEL_FULL_PART_COL_COUNT_LIST);

                    partSpecAstParams.setParentPartSpec(parentPartSpec);
                    partSpecAstParams.setPartEngine(partEngine);
                    PartitionSpec partSpec = buildPartSpecByAstParams(partSpecAstParams);
                    partSpecList.add(partSpec);
                }
            }
        }
        return partSpecList;
    }

    private static boolean checkNeedAutoGenPartSpecByHashPartCntVal(boolean buildSubPartBy,
                                                                    boolean buildSubPartSpecTemplate,
                                                                    boolean useSubPartSpecTemplate,
                                                                    boolean notExistSubPartSpecDef) {

        if (!buildSubPartBy) {
            /**
             *     case1-0:
             *          create table (...)
             *          partition by key(pk);
             *  build-> partitions autoPartCnt
             *
             *     case1-1:
             *          create table (...)
             *          partition by key(pk)
             *  build-> partitions 2;
             *
             */
            return true;
        } else {
            if (useSubPartSpecTemplate) {
                /**
                 *     case2:
                 *          create table (...)
                 *          partition by key(pk)
                 *          subpartition by key(name);
                 *  build-> subpartitions autoPartCnt
                 *
                 *     case3-0:
                 *          create table (...)
                 *          partition by range(pk)
                 *          subpartition by key(name)
                 *  build-> subpartitions 2
                 *          (
                 *              partition p1 ...,
                 *              partition p2 ...
                 *          )
                 *
                 */
                return buildSubPartSpecTemplate;
            } else {
                if (buildSubPartSpecTemplate) {
                    /**
                     *     case4-0:
                     *          create table (...)
                     *          partition by range(pk)
                     *  build-> subpartition by key(name)
                     *          (
                     *              partition p1 values less than (100) (
                     *                  subpartitions 4
                     *              ),
                     *              partition p2 values less than (maxvalue) (
                     *                  subpartitions 3
                     *              )
                     *          )
                     */
                    return false;
                } else {
                    if (notExistSubPartSpecDef) {
                        /**
                         *     case4-1:
                         *          create table (...)
                         *          partition by range(pk)
                         *          subpartition by key(name)
                         *          (
                         *              partition p1 values less than (100) (
                         *        build->  autoSubPartCnt(=1)
                         *              ),
                         *              partition p2 ...
                         *          )
                         */
                        return true;
                    } else {
                        /**
                         *     case4-2:
                         *          create table (...)
                         *          partition by range(pk)
                         *          subpartition by key(name)
                         *          (
                         *              partition p1 values less than (100) (
                         *       build->    subpartitions 4
                         *              ),
                         *              partition p2 ...
                         *          )
                         */
                        return true;
                    }
                }

            }
        }
    }

    protected static Long autoDecideHashPartCountIfNeed(ExecutionContext ec,
                                                        PartitionTableType tblType,
                                                        boolean buildSubPartBy,
                                                        boolean buildSubPartSpecTemplate,
                                                        boolean notExistSubPartSpecDef,
                                                        Integer maxPhysicalPartitions,
                                                        Long initHashPartCnt,
                                                        SqlNode hashPartitionCntAst) {

        /**
         * Only the following case need auto generate hash partition count and partitions :
         * case1-0: buildSubPartBy=false and hashPartitionCntAst == null
         * case2: buildSubPartBy=true and hashPartitionCntAst == null and useSubPartTemplate=true
         * case4: buildSubPartBy=true and hashPartitionCntAst == null and useSubPartTemplate=false
         *        and some partition has no subpartition definition.
         *
         */

        Long finalHashPartCnt = initHashPartCnt;
        if (hashPartitionCntAst == null && tblType.isA(PartitionTableType.PARTITIONED_TABLE)) {
            if (!buildSubPartBy) {
                /**
                 * Create Table ddl does NOT specify partition count for hash/key partition strategy
                 */
                if (initHashPartCnt == 0) {
                    /**
                     * no specify hashPartCntAst and any partitionSpecsAst, so use default partition count of metadb
                     */
                    if (tblType == PartitionTableType.COLUMNAR_TABLE) {
                        finalHashPartCnt = ec.getParamManager().getLong(ConnectionParams.COLUMNAR_DEFAULT_PARTITIONS);
                    } else {
                        finalHashPartCnt = ec.getParamManager().getLong(ConnectionParams.AUTO_PARTITION_PARTITIONS);
                    }
                }

            } else {
                if (buildSubPartSpecTemplate) {
                    /**
                     *  auto generate hash partition count for subpartitions
                     */
                    if (initHashPartCnt == 0) {
                        /**
                         * no specify hashPartCntAst and any partitionSpecsAst, so use default partition count of metadb
                         */
                        if (tblType == PartitionTableType.COLUMNAR_TABLE) {
                            finalHashPartCnt =
                                ec.getParamManager().getLong(ConnectionParams.COLUMNAR_DEFAULT_PARTITIONS);
                        } else {
                            finalHashPartCnt = ec.getParamManager().getLong(ConnectionParams.AUTO_PARTITION_PARTITIONS);
                        }
                    }
                } else {
                    if (notExistSubPartSpecDef) {
                        /**
                         * no specify any subPartitionSpecsAst, so treat it as the only one subpartition
                         */
                        finalHashPartCnt = 1L;
                    }
                }
            }
        } else if ((hashPartitionCntAst != null) && (hashPartitionCntAst instanceof SqlLiteral)) {
            finalHashPartCnt = ((SqlLiteral) hashPartitionCntAst).getValueAs(Long.class);
        }

        /**
         * Check the finalHashPartCnt is invalid
         */
        if (finalHashPartCnt > maxPhysicalPartitions) {
            throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_INVALID_PARAMS,
                String.format("Too many partitions [%s] (including subpartitions) were defined", finalHashPartCnt));
        } else if (finalHashPartCnt <= 0) {
            if (buildSubPartSpecTemplate) {
                throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_INVALID_PARAMS,
                    String.format("partitions [%s] (including subpartitions) can not be less then 1",
                        finalHashPartCnt));
            }
        }
        return finalHashPartCnt;
    }

    public static PartitionInfo buildPartInfoWithKeyStrategyForNewGsiMeta(String schemaName,
                                                                          String tableName,
                                                                          TableMeta primaryTblMeta,
                                                                          List<String> newGsiAllCols,
                                                                          List<String> newGsiPartCols,
                                                                          long hashPartCnt) {
        List<ColumnMeta> newGsiAllColMetas = new ArrayList<>();
        for (int i = 0; i < newGsiAllCols.size(); i++) {
            String col = newGsiAllCols.get(i);
            ColumnMeta cm = primaryTblMeta.getColumn(col);
            newGsiAllColMetas.add(cm);
        }

        List<SqlIdentifier> partKeyAst = new ArrayList<>();
        for (int i = 0; i < newGsiPartCols.size(); i++) {
            SqlIdentifier id = new SqlIdentifier(newGsiPartCols.get(i), SqlParserPos.ZERO);
            partKeyAst.add(id);
        }

        // Generate the partitioning clause.
        final SqlPartitionByHash sqlPartitionByHash = new SqlPartitionByHash(true, false, SqlParserPos.ZERO);
        Long partCnt = hashPartCnt;
        SqlLiteral partCntAst =
            SqlLiteral.createLiteralForIntTypes(partCnt, SqlParserPos.ZERO, SqlTypeName.INTEGER_UNSIGNED);
        sqlPartitionByHash.setPartitionsCount(partCntAst);
        sqlPartitionByHash.getColumns().addAll(partKeyAst);
        final StringBuilder builder = new StringBuilder();
        for (int i = 0; i < partKeyAst.size(); ++i) {
            if (i != 0) {
                builder.append(", ");
            }
            builder.append(SqlIdentifier.surroundWithBacktick(partKeyAst.get(i).getLastName()));
        }
        sqlPartitionByHash.setSourceSql("KEY(" + builder + ") PARTITIONS " + partCnt);
        SqlPartitionBy partByKeyOfNewGsi = sqlPartitionByHash;

        PartitionInfo newGsiPartInfo = PartitionInfoBuilder
            .buildPartitionInfoByPartDefAst(schemaName,
                tableName, null, false, null,
                partByKeyOfNewGsi, null,
                new ArrayList<>(),
                newGsiAllColMetas,
                PartitionTableType.PARTITION_TABLE,
                new ExecutionContext());
        return newGsiPartInfo;
    }

    public static class PartSpecSortedQueue extends PriorityQueue<PartitionSpec> {
        public PartSpecSortedQueue() {
            super(new Comparator<PartitionSpec>() {
                @Override
                public int compare(PartitionSpec o1, PartitionSpec o2) {
                    Long o1Posi = o1.getPhyPartPosition();
                    Long o2Posi = o2.getPhyPartPosition();
                    if (o1Posi > o2Posi) {
                        return 1;
                    } else if (o1Posi < o2Posi) {
                        return -1;
                    } else {
                        return 0;
                    }
                }
            });
        }
    }
}
