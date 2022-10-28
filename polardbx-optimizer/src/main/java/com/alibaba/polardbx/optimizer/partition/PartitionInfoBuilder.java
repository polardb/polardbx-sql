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

import com.alibaba.polardbx.common.exception.NotSupportException;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.gms.locality.LocalityDesc;
import com.alibaba.polardbx.gms.metadb.table.TableInfoManager;
import com.alibaba.polardbx.gms.partition.ExtraFieldJSON;
import com.alibaba.polardbx.gms.partition.TablePartitionConfig;
import com.alibaba.polardbx.gms.partition.TablePartitionRecord;
import com.alibaba.polardbx.gms.partition.TablePartitionSpecConfig;
import com.alibaba.polardbx.gms.tablegroup.PartitionGroupRecord;
import com.alibaba.polardbx.gms.tablegroup.TableGroupRecord;
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.gms.topology.GroupDetailInfoExRecord;
import com.alibaba.polardbx.gms.util.GroupInfoUtil;
import com.alibaba.polardbx.gms.util.PartitionNameUtil;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.TddlRelDataTypeSystemImpl;
import com.alibaba.polardbx.optimizer.core.TddlTypeFactoryImpl;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypeUtil;
import com.alibaba.polardbx.optimizer.hint.util.HintUtil;
import com.alibaba.polardbx.optimizer.locality.LocalityInfo;
import com.alibaba.polardbx.optimizer.locality.LocalityManager;
import com.alibaba.polardbx.optimizer.partition.datatype.PartitionField;
import com.alibaba.polardbx.optimizer.partition.datatype.PartitionFieldBuilder;
import com.alibaba.polardbx.optimizer.partition.datatype.function.PartitionIntFunction;
import com.alibaba.polardbx.optimizer.partition.pruning.PartFieldAccessType;
import com.alibaba.polardbx.optimizer.partition.pruning.PartFuncMonotonicityUtil;
import com.alibaba.polardbx.optimizer.partition.pruning.PartitionPrunerUtils;
import com.alibaba.polardbx.optimizer.partition.pruning.PartitionRouter;
import com.alibaba.polardbx.optimizer.partition.pruning.SearchDatumComparator;
import com.alibaba.polardbx.optimizer.partition.pruning.SearchDatumHasher;
import com.alibaba.polardbx.optimizer.partition.pruning.SearchDatumInfo;
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
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlPartition;
import org.apache.calcite.sql.SqlPartitionBy;
import org.apache.calcite.sql.SqlPartitionByHash;
import org.apache.calcite.sql.SqlPartitionByList;
import org.apache.calcite.sql.SqlPartitionByRange;
import org.apache.calcite.sql.SqlPartitionValue;
import org.apache.calcite.sql.SqlPartitionValueItem;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.commons.lang.StringUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

import static com.alibaba.polardbx.gms.tablegroup.TableGroupLocation.getFullOrderedGroupList;
import static com.alibaba.polardbx.gms.tablegroup.TableGroupLocation.getOrderedGroupList;

/**
 * @author chenghui.lch
 */
public class PartitionInfoBuilder {

    static Set<String> supportedFunctions = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);

    static {
        supportedFunctions.addAll(PartitionPrunerUtils.getAllSupportedPartitionIntFunctions());
    }

    public static class PartSpecSortedQueue extends PriorityQueue<PartitionSpec> {
        public PartSpecSortedQueue() {
            super(new Comparator<PartitionSpec>() {
                @Override
                public int compare(PartitionSpec o1, PartitionSpec o2) {
                    Long o1Posi = o1.getPosition();
                    Long o2Posi = o2.getPosition();
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

    public static void prepareOrderNumForPartitions(PartitionTableType tblType, List<PartitionSpec> partitions) {
        if (tblType != PartitionTableType.PARTITION_TABLE && tblType != PartitionTableType.GSI_TABLE) {
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

    public static boolean isSupportedPartitionFunctions(String funcName) {
        return supportedFunctions.contains(funcName);
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
        }
        return isSupportedDataType;
    }

    public static PartitionInfo buildPartitionInfoByMetaDbConfig(TablePartitionConfig tbPartConf,
                                                                 List<ColumnMeta> allColumnMetas,
                                                                 Map<Long, PartitionGroupRecord> partitionGroupRecordsMap) {

        PartitionInfo partitionInfo = new PartitionInfo();

        TablePartitionRecord logTableConfig = tbPartConf.getTableConfig();

        String tableSchema = logTableConfig.tableSchema;
        String tableName = logTableConfig.tableName;
        Long tblGroupId = logTableConfig.groupId;
        Long metaDbVersion = logTableConfig.metaVersion;
        Integer spTempFlag = logTableConfig.spTempFlag;
        Integer logTbStatus = logTableConfig.partStatus;
        Integer autoFlag = logTableConfig.autoFlag;
        Long partFlags = logTableConfig.partFlags;
        Integer tableType = logTableConfig.tblType;
        ExtraFieldJSON partExtras = logTableConfig.partExtras;

        partitionInfo.tableSchema = tableSchema;
        partitionInfo.tableName = tableName;
        partitionInfo.tableGroupId = tblGroupId;
        partitionInfo.metaVersion = metaDbVersion;
        partitionInfo.spTemplateFlag = spTempFlag;
        partitionInfo.status = logTbStatus;
        partitionInfo.autoFlag = autoFlag;
        partitionInfo.partFlags = partFlags;
        partitionInfo.tableType = PartitionTableType.getTypeByIntVal(tableType);
        if (logTableConfig.partExtras != null) {
            partitionInfo.locality = logTableConfig.partExtras.locality;
        }

        PartInfoSessionVars sessionVars = new PartInfoSessionVars();
        if (partExtras != null) {
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

        PartitionByDefinition partitionBy =
            buildPartitionByDef(partSpecConfigs, logTableConfig, allColumnMetas, partitionGroupRecordsMap);
        partitionInfo.setPartitionBy(partitionBy);

        if (partitionInfo.spTemplateFlag == TablePartitionRecord.SUBPARTITION_TEMPLATE_NOT_EXISTED) {
            partitionInfo.setSubPartitionBy(null);
        }

        //partitionInfo.setPartSpecSearcher(PartSpecSearcher.buildPartSpecSearcher(partitionInfo.getTableType(),partitionInfo.getPartitionBy()));
        partitionInfo.initPartSpecSearcher();

        return partitionInfo;
    }

    public static PartitionInfo buildPartitionInfoByPartDefAst(String schemaName,
                                                               String tableName,
                                                               String tableGroupName,
                                                               String joinGroupName,
                                                               SqlPartitionBy sqlPartitionBy,
                                                               Map<SqlNode, RexNode> boundExprInfo,
                                                               List<ColumnMeta> pkColMetas,
                                                               List<ColumnMeta> allColMetas,
                                                               PartitionTableType tblType,
                                                               ExecutionContext ec) {
        return buildPartitionInfoByPartDefAst(schemaName, tableName, tableGroupName, joinGroupName, sqlPartitionBy,
            boundExprInfo, pkColMetas, allColMetas, tblType, ec, new LocalityDesc());
    }

    public static PartitionInfo buildPartitionInfoByPartDefAst(String schemaName,
                                                               String tableName,
                                                               String tableGroupName,
                                                               String joinGroupName,
                                                               SqlPartitionBy sqlPartitionBy,
                                                               Map<SqlNode, RexNode> boundExprInfo,
                                                               List<ColumnMeta> pkColMetas,
                                                               List<ColumnMeta> allColMetas,
                                                               PartitionTableType tblType,
                                                               ExecutionContext ec,
                                                               LocalityDesc locality) {

        PartitionInfo partitionInfo = new PartitionInfo();

        String tbName = tableName;
        String tbSchema = schemaName;
        String tgName = tableGroupName;
        PartitionByDefinition partitionByDef = new PartitionByDefinition();
        boolean isKey;
        boolean isUnique;
        SqlNode hashPartitonCntSqlNode = null;
        List<SqlNode> columns = new ArrayList<>();
        PartitionStrategy strategy = null;
        Long hashPartCnt = 0L;

        List<SqlNode> partitions = new ArrayList<>();
        if (tblType == PartitionTableType.PARTITION_TABLE || tblType == PartitionTableType.GSI_TABLE) {

            if (!DbInfoManager.getInstance().isNewPartitionDb(schemaName)) {
                throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR, String
                    .format("Failed to create partition tables on db[%s] with mode='drds'", schemaName));
            }

            if (null == sqlPartitionBy) {
                throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR, String
                    .format("Failed to create sharding tables on db[%s] with mode='auto'",
                        schemaName));
            }

            columns = sqlPartitionBy.getColumns();
            partitions = sqlPartitionBy.getPartitions();
            if (sqlPartitionBy instanceof SqlPartitionByRange) {
                boolean isColumns = ((SqlPartitionByRange) sqlPartitionBy).isColumns();
                strategy = PartitionStrategy.RANGE;
                if (isColumns) {
                    strategy = PartitionStrategy.RANGE_COLUMNS;
                }

            } else if (sqlPartitionBy instanceof SqlPartitionByList) {
                boolean isColumns = ((SqlPartitionByList) sqlPartitionBy).isColumns();
                strategy = PartitionStrategy.LIST;
                if (isColumns) {
                    strategy = PartitionStrategy.LIST_COLUMNS;
                }
            } else if (sqlPartitionBy instanceof SqlPartitionByHash) {
                strategy = PartitionStrategy.HASH;
                isKey = ((SqlPartitionByHash) (sqlPartitionBy)).isKey();
                isUnique = ((SqlPartitionByHash) (sqlPartitionBy)).isUnique();
                hashPartitonCntSqlNode = sqlPartitionBy.getPartitionsCount();
                if (isKey) {
                    strategy = PartitionStrategy.KEY;
                }
            }

            Map<String, ColumnMeta> allColMetaMap = new HashMap<>();
            for (int j = 0; j < allColMetas.size(); j++) {
                ColumnMeta cm = allColMetas.get(j);
                allColMetaMap.put(cm.getOriginColumnName().toLowerCase(), cm);
            }

            List<SqlNode> partExprList = partitionByDef.getPartitionExprList();
            List<String> partColList = partitionByDef.getPartitionColumnNameList();
            List<ColumnMeta> partColMetaList = partitionByDef.getPartitionFieldList();
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
                    // convert to "partition by key()" to "partition by key(cols of primary key)"
                    initPartColMetasByPkColMetas(pkColMetas, partExprList, partColList, partColMetaList);
                } else {
                    throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_INVALID_PARAMS,
                        String.format("No found any columns in Creating Table"));
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
            hashPartCnt = 1L;

            List<SqlNode> partExprList = partitionByDef.getPartitionExprList();
            List<String> partColList = partitionByDef.getPartitionColumnNameList();
            List<ColumnMeta> partColMetaList = partitionByDef.getPartitionFieldList();
            initPartColMetasByPkColMetas(pkColMetas, partExprList, partColList, partColMetaList);
        } else {
            // tblType == PartitionTableType.BROADCAST_TABLE

            // Construct partition info for broadcast table
            strategy = PartitionStrategy.KEY;
            List<String> phyGrpList = HintUtil.allGroup(schemaName);
            hashPartCnt = Long.valueOf(phyGrpList.size());
            List<SqlNode> partExprList = partitionByDef.getPartitionExprList();
            List<String> partColList = partitionByDef.getPartitionColumnNameList();
            List<ColumnMeta> partColMetaList = partitionByDef.getPartitionFieldList();
            initPartColMetasByPkColMetas(pkColMetas, partExprList, partColList, partColMetaList);
        }

        List<SqlNode> partExprList = partitionByDef.getPartitionExprList();
        List<ColumnMeta> partColMetaList = partitionByDef.getPartitionFieldList();
        List<RelDataType> partExprTypeList = PartitionInfoBuilder.inferPartExprDataTypes(partExprList, partColMetaList);
        partitionByDef.setPartitionExprTypeList(partExprTypeList);
        partitionByDef.setStrategy(strategy);

        SearchDatumComparator querySpaceComparator =
            PartitionByDefinition.getQuerySpaceComparator(partitionByDef.getPartitionFieldList());
        partitionByDef.setQuerySpaceComparator(querySpaceComparator);

        SearchDatumComparator pruningSpaceComparator =
            PartitionByDefinition.getPruningSpaceComparator(partitionByDef.getPartitionExprTypeList());
        partitionByDef.setPruningSpaceComparator(pruningSpaceComparator);

        SearchDatumComparator boundSpaceComparator =
            PartitionByDefinition.getBoundSpaceComparator(pruningSpaceComparator, partitionByDef.getStrategy());
        partitionByDef.setBoundSpaceComparator(boundSpaceComparator);

        SearchDatumHasher hasher = PartitionByDefinition
            .getHasher(strategy, partitionByDef.getPartitionFieldList(), partitionByDef.getPartitionExprTypeList());
        partitionByDef.setHasher(hasher);

        Map<SqlNode, RexNode> partBoundExprInfo = boundExprInfo;
        SqlOperator partFuncOp = getPartFuncSqlOperator(strategy, partitionByDef.getPartitionExprList());
        if (partFuncOp != null && !supportedFunctions.contains(partFuncOp.getName())) {
            throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_INVALID_PARAMS,
                String.format("Unsupported to use partition function '%s'", partFuncOp.getName()));
        }
        partitionByDef.setPartIntFuncOperator(partFuncOp);
        if (partFuncOp != null) {
            partitionByDef.setPartIntFunc(PartitionIntFunction.create(partFuncOp));
            partitionByDef.setPartIntFuncMonotonicity(PartFuncMonotonicityUtil
                .getPartFuncMonotonicity(partFuncOp, partColMetaList.get(0).getField().getRelType()));
        } else {
            /**
             *  No use part func, convert all "partition by hash(col)" to "partition by key(col)"
             */
            if (strategy == PartitionStrategy.HASH && columns.size() == 1) {
                strategy = PartitionStrategy.KEY;
                partitionByDef.setStrategy(strategy);
            }
        }
        PartitionIntFunction partIntFunc = partitionByDef.getPartIntFunc();
        partitionByDef.setNeedEnumRange(
            checkNeedDoEnumRange(strategy, partitionByDef.getPartitionFieldList(), partFuncOp));

        /**
         * Validate and check partition columns for partition tbl and gsi table
         */
        if (tblType == PartitionTableType.PARTITION_TABLE || tblType == PartitionTableType.GSI_TABLE) {
            PartitionInfoUtil.validatePartitionColumns(partitionByDef);
        }

        List<PartitionSpec> partSpecList = partitionByDef.getPartitions();
        Integer maxPhysicalPartitions = Integer.valueOf(ConnectionParams.MAX_PHYSICAL_PARTITION_COUNT.getDefault());
        if (ec != null) {
            maxPhysicalPartitions = ec.getParamManager().getInt(ConnectionParams.MAX_PHYSICAL_PARTITION_COUNT);
        }

        if (!(strategy == PartitionStrategy.HASH || strategy == PartitionStrategy.KEY)) {
            if (partitions.size() > maxPhysicalPartitions) {
                throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_INVALID_PARAMS,
                    String
                        .format("Too many partitions [%s] (including subpartitions) were defined", partitions.size()));
            }

            for (int i = 0; i < partitions.size(); i++) {
                SqlPartition partSpecAst = (SqlPartition) partitions.get(i);
                PartitionSpec partSpec =
                    buildPartitionSpecByPartSpecAst(ec, partColMetaList, partIntFunc, pruningSpaceComparator,
                        partBoundExprInfo,
                        null,
                        partSpecAst,
                        strategy, i + 1, PartitionInfoUtil.FULL_PART_COL_COUNT);
                partSpecList.add(partSpec);
            }

            if (strategy == PartitionStrategy.LIST_COLUMNS || strategy == PartitionStrategy.LIST) {
                //only allow at most one default partition
                int defaultPartSpecCnt = 0;
                for (PartitionSpec spec : partSpecList) {
                    if (spec.getIsDefaultPartition()) {
                        defaultPartSpecCnt++;
                    }
                }
                if (defaultPartSpecCnt > 1) {
                    throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_INVALID_PARAMS,
                        "Only at most one default partition is allowed");
                }
            }

        } else {

            if (hashPartitonCntSqlNode == null && (tblType == PartitionTableType.GSI_TABLE
                || tblType == PartitionTableType.PARTITION_TABLE)) {

                /**
                 * Create Table ddl does NOT specify partition count for hash/key partition strategy
                 */
                hashPartCnt = ec.getParamManager().getLong(ConnectionParams.AUTO_PARTITION_PARTITIONS);

            } else if ((hashPartitonCntSqlNode != null) && (hashPartitonCntSqlNode instanceof SqlLiteral)) {
                hashPartCnt = ((SqlLiteral) hashPartitonCntSqlNode).getValueAs(Long.class);
            }

            if (hashPartCnt > maxPhysicalPartitions) {
                throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_INVALID_PARAMS,
                    String.format("Too many partitions [%s] (including subpartitions) were defined", hashPartCnt));
            } else if (hashPartCnt <= 0) {
                throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_INVALID_PARAMS,
                    String.format("partitions [%s] (including subpartitions) can not be less then 1", hashPartCnt));
            }
            if (partitions.size() == 0) {

                boolean isMultiCol = partColMetaList.size() > 1;
                PartBoundValBuilder boundValBuilder = null;
                if (strategy == PartitionStrategy.HASH || (strategy == PartitionStrategy.KEY && !isMultiCol)) {
                    boundValBuilder = new HashPartBoundValBuilder(hashPartCnt.intValue());
                } else {
                    boundValBuilder = new KeyPartBoundValBuilder(hashPartCnt.intValue(), partColMetaList.size());
                }

                for (int i = 0; i < hashPartCnt; i++) {
                    PartitionSpec partSpec =
                        buildPartitionSpecByPartSpecAst(ec, partColMetaList, partIntFunc, pruningSpaceComparator,
                            partBoundExprInfo,
                            boundValBuilder,
                            null, strategy, i + 1, PartitionInfoUtil.FULL_PART_COL_COUNT);
                    //TODO: we would support locality for partition group
                    partSpecList.add(partSpec);
                }

            } else {
                if (partitions.size() != hashPartCnt) {
                    throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_INVALID_PARAMS,
                        String.format(
                            "the count[%s] of the manual defined hash partitions mismatch the count[%s] of `PARTITIONS`",
                            partitions.size(), hashPartCnt));
                }

                for (int i = 0; i < partitions.size(); i++) {
                    SqlPartition partSpecAst = (SqlPartition) partitions.get(i);
                    PartitionSpec partSpec =
                        buildPartitionSpecByPartSpecAst(ec, partColMetaList, partIntFunc, pruningSpaceComparator,
                            partBoundExprInfo,
                            null,
                            partSpecAst,
                            strategy, i + 1, PartitionInfoUtil.FULL_PART_COL_COUNT);
                    partSpecList.add(partSpec);
                }
            }
        }
        /**
         * Prebuild router for dynamic pruning
         */
        Long dbId = DbInfoManager.getInstance().getDbInfo(schemaName).id;
        LocalityInfo dbLocalityInfo = LocalityManager.getInstance().getLocalityOfDb(dbId);
        LocalityDesc dbLocalityDesc = new LocalityDesc();
        if (dbLocalityInfo != null) {
            dbLocalityDesc = LocalityDesc.parse(dbLocalityInfo.getLocality());
        }
        Set<String> fullStorageList = new HashSet<>();
        for (GroupDetailInfoExRecord groupDetailInfoExRecord : getFullOrderedGroupList(schemaName)) {
            String storageInstId = groupDetailInfoExRecord.getStorageInstId();
            fullStorageList.add(storageInstId);
        }
        List<String> storageList =
            getOrderedGroupList(schemaName).stream().map(o -> o.getStorageInstId()).collect(Collectors.toList());

        if (locality != null) {
            if (!fullStorageList.containsAll(locality.getDnList())) {
                throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                    " Table locality definition contains illegal storage ID: " + String.join(",",
                        locality.getDnList()));
            }
            if (!dbLocalityDesc.compactiableWith(locality) || !storageList.containsAll(locality.getDnList())) {
                throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                    " Table locality definition is not compatible with database locality! ");
            }
        }
        for (PartitionSpec partitionSpec : partitionByDef.getPartitions()) {
            LocalityDesc partitionLocality = LocalityDesc.parse(partitionSpec.getLocality());
            if (!fullStorageList.containsAll(partitionLocality.getDnList())) {
                throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                    " Partition locality definition contains illegal storage ID: " + String.join(",",
                        partitionLocality.getDnList()));
            }
            if (!dbLocalityDesc.compactiableWith(partitionLocality) || !storageList.containsAll(
                partitionLocality.getDnList())) {
                throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                    " Partition locality definition is not compatible with database locality! ");
            }
        }

        PartitionRouter router = PartitionByDefinition.getPartRouter(partitionByDef);
        partitionByDef.setRouter(router);

        partitionInfo.setPartitionBy(partitionByDef);
        partitionInfo.setSubPartitionBy(null);
        partitionInfo.setMetaVersion(1L);
        partitionInfo.setSpTemplateFlag(TablePartitionRecord.SUBPARTITION_TEMPLATE_NOT_EXISTED);
        partitionInfo.setTableGroupId(TableGroupRecord.INVALID_TABLE_GROUP_ID);
        partitionInfo.setTableName(tbName);
        partitionInfo.setTableSchema(tbSchema);
        partitionInfo.setAutoFlag(TablePartitionRecord.PARTITION_AUTO_BALANCE_DISABLE);
        partitionInfo.setPartFlags(0L);
        partitionInfo.setTableType(tblType);
        partitionInfo.setRandomTableNamePatternEnabled(ec.isRandomPhyTableEnabled());
        partitionInfo.setSessionVars(saveSessionVars(ec));
        partitionInfo.setLocality(locality.getDnString());
        partitionInfo.setBuildNoneDefaultSingleGroup(false);
        PartitionInfoUtil.generateTableNamePattern(partitionInfo, tbName);
        PartitionInfoUtil.generatePartitionLocation(partitionInfo, tableGroupName, joinGroupName, ec, locality);

        //partitionInfo.setPartSpecSearcher(PartSpecSearcher.buildPartSpecSearcher(partitionInfo.getTableType(),partitionInfo.getPartitionBy()));
        partitionInfo.initPartSpecSearcher();
        PartitionInfoUtil.validatePartitionInfoForDdl(partitionInfo, ec);
        return partitionInfo;
    }

    private static void initPartColMetasByPkColMetas(List<ColumnMeta> pkColMetas, List<SqlNode> partExprList,
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
            partExprStr += String.format("`%s`", partColList.get(i));
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

    /**
     * Build a new PartitionSpec from new astNode(partSpecAst) by specifying prefix partition columns
     * <pre>
     *     When prefixPartColCnt is specified ( prefixPartColCnt > 0 ),
     *     this method will only check and validate the data-type and bound-value of the prefix partition columns
     * </pre>
     */
    public static PartitionSpec buildPartitionSpecByPartSpecAst(ExecutionContext context,
                                                                List<ColumnMeta> partColMetaList,
                                                                PartitionIntFunction partIntFunc,
                                                                SearchDatumComparator pruningComparator,
                                                                Map<SqlNode, RexNode> partBoundExprInfo,
                                                                PartBoundValBuilder partBoundValBuilder,
                                                                SqlPartition partSpecAst,
                                                                PartitionStrategy strategy,
                                                                long partPosition,
                                                                int prefixPartColCnt) {
        PartitionSpec partSpec = new PartitionSpec();
        String partName = null;
        SqlPartitionValue value = null;
        List<SqlPartitionValueItem> itemsOfVal = null;
        int partColCnt = partColMetaList.size();
        boolean isMultiCols = partColCnt > 1;
        RelDataTypeFactory typeFactory = new TddlTypeFactoryImpl(TddlRelDataTypeSystemImpl.getInstance());
        List<SearchDatumInfo> partBoundValues = new ArrayList<>();


        if (partSpecAst != null) {
            SqlIdentifier partNameId = (SqlIdentifier) partSpecAst.getName();
            partName = partNameId.toString();

            // all part name should convert to lower case
            partName = PartitionNameUtil.toLowerCase(partName);

            value = partSpecAst.getValues();
            itemsOfVal = value.getItems();

            PartitionInfoUtil.validatePartitionValueFormats(strategy, partColCnt, prefixPartColCnt, partName,
                partSpecAst);
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
                        partSpec.setIsDefaultPartition(true);
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
                            partSpec.setIsDefaultPartition(true);
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

                        PartitionBoundVal bndVal;
                        if (!itemsOfVal.get(i).isMaxValue()) {
                            PartitionInfoUtil.validateBoundValueExpr(bndExprRex, bndValDt, partIntFunc, strategy);
                            bndVal =
                                PartitionPrunerUtils.getBoundValByRexExpr(bndExprRex, bndValDt,
                                    PartFieldAccessType.DDL_EXECUTION, context);
                        } else {
                            bndVal = PartitionBoundVal.createMaxValue();
                        }
                        oneBndVal.add(bndVal);
                    }
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

                    SearchDatumInfo datum = new SearchDatumInfo(oneBndVal);
                    partBoundValues.add(datum);
                }
            }
            if (!StringUtils.isEmpty(partSpecAst.getLocality())) {
                partSpec.setLocality(partSpecAst.getLocality());
            } else {
                partSpec.setLocality("");
            }
        } else {

            // auto build hash partition name
            partName = PartitionNameUtil.autoBuildPartitionName(partPosition);
            RelDataType bndValDt = getDataTypeForBoundVal(typeFactory, strategy, null);

            SearchDatumInfo datum = null;
            if (strategy == PartitionStrategy.HASH || (strategy == PartitionStrategy.KEY && !isMultiCols)) {
                // auto build hash partition boundVal
                Long bndJavaVal = (Long) partBoundValBuilder.getPartBoundVal((int) partPosition);
                PartitionBoundVal boundVal =
                    buildOneHashBoundValByLong(context, bndJavaVal, bndValDt, PartFieldAccessType.DDL_EXECUTION);
                datum = new SearchDatumInfo(boundVal);
            } else {
                // build bound value for multi-column key
                Long[] bndJavaVals = (Long[]) partBoundValBuilder.getPartBoundVal((int) partPosition);
                PartitionBoundVal boundVals[] = new PartitionBoundVal[bndJavaVals.length];
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
        partSpec.setComment("");
        partSpec.setStrategy(strategy);
        partSpec.setName(partName);
        partSpec.setPosition(partPosition);
        partSpec.setBoundSpaceComparator(PartitionByDefinition.getBoundSpaceComparator(pruningComparator, strategy));
        return partSpec;
    }

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
                                                                       SqlAlterTableAddPartition addPartition,
                                                                       Map<SqlNode, RexNode> partBoundExprInfo,
                                                                       List<PartitionGroupRecord> unVisiablePartitionGroupRecords,
                                                                       List<Pair<String, String>> physicalTableAndGroupPairs) {
        assert physicalTableAndGroupPairs.size() == unVisiablePartitionGroupRecords.size();
        assert addPartition.getPartitions().size() == unVisiablePartitionGroupRecords.size();

        List<ColumnMeta> partColMetaList = partitionInfo.getPartitionBy().getPartitionFieldList();
        SearchDatumComparator comparator = partitionInfo.getPartitionBy().getPruningSpaceComparator();
        int partitionPos = partitionInfo.getPartitionBy().getPartitions().size() + 1;
        int i = 0;
        List<PartitionSpec> existingPartSpecs = partitionInfo.getPartitionBy().copy().getPartitions();
        List<PartitionSpec> newPartitionSpecs = new ArrayList<>();
        PartitionIntFunction partIntFunc = partitionInfo.getPartitionBy().getPartIntFunc();
        PartitionStrategy strategy = partitionInfo.getPartitionBy().getStrategy();
        List<SqlPartition> newPartitionsAst = new ArrayList<>();
        for (SqlNode sqlNode : addPartition.getPartitions()) {
            SqlPartition newSqlPartition = (SqlPartition) sqlNode;
            newPartitionsAst.add(newSqlPartition);
        }
        int fullPartColCnt = partColMetaList.size();
        int actualPartColCnt = PartitionInfoUtil.getActualPartitionColumns(partitionInfo).size();
        int newPrefixColCnt =
            PartitionInfoUtil.getNewPrefixPartColCntBySqlPartitionAst(fullPartColCnt, actualPartColCnt, strategy,
                newPartitionsAst);
        for (SqlPartition newSqlPartition : newPartitionsAst) {
            PartitionSpec newPartitionSpec = PartitionInfoBuilder
                .buildPartitionSpecByPartSpecAst(
                    context,
                    partColMetaList,
                    partIntFunc,
                    comparator,
                    partBoundExprInfo,
                    null,
                    newSqlPartition,
                    partitionInfo.getPartitionBy().getStrategy(),
                    partitionPos++,
                    newPrefixColCnt);

            PartitionInfoUtil.validateAddPartition(partitionInfo,
                existingPartSpecs,
                newPartitionSpec);

            Pair<String, String> physicalTableAndGroupPair = physicalTableAndGroupPairs.get(i);
            PartitionLocation location =
                new PartitionLocation(physicalTableAndGroupPair.getValue(), physicalTableAndGroupPair.getKey(),
                    unVisiablePartitionGroupRecords.get(i).id);
            newPartitionSpec.setLocation(location);
            location.setVisiable(false);
            existingPartSpecs.add(newPartitionSpec);
            newPartitionSpecs.add(newPartitionSpec);
            i++;
        }
        PartitionInfo newPartInfo = partitionInfo.copy();
        newPartInfo.getPartitionBy().getPartitions().addAll(newPartitionSpecs);

        return newPartInfo;
    }

    public static PartitionInfo buildNewPartitionInfoByDroppingPartition(PartitionInfo partitionInfo,
                                                                         SqlAlterTableDropPartition dropPartition,
                                                                         List<PartitionGroupRecord> unVisiablePartitionGroupRecords,
                                                                         List<Pair<String, String>> physicalTableAndGroupPairs) {
        Set<String> oldPartitionName = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        Set<String> newPartitionName = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);

        oldPartitionName
            .addAll(dropPartition.getPartitionNames().stream().map(o -> ((SqlIdentifier) o).getLastName()).collect(
                Collectors.toList()));
        if (GeneralUtil.isNotEmpty(unVisiablePartitionGroupRecords)) {
            assert physicalTableAndGroupPairs.size() == unVisiablePartitionGroupRecords.size();
            newPartitionName.addAll(unVisiablePartitionGroupRecords.stream().map(o -> o.getPartition_name()).collect(
                Collectors.toList()));
        }

        PartitionInfo newPartInfo = partitionInfo.copy();
        List<PartitionSpec> oldPartSpecList = newPartInfo.getPartitionBy().getPartitions();
        List<PartitionSpec> newPartSpecList = new ArrayList<>();
        int index = 0;
        for (int i = 0; i < oldPartSpecList.size(); i++) {
            PartitionSpec spec = oldPartSpecList.get(i);
            if (oldPartitionName.contains(spec.getName())) {
                continue;
            } else if (newPartitionName.contains(spec.getName())) {
                Pair<String, String> phyTableAndGroup = physicalTableAndGroupPairs.get(index++);
                spec.getLocation().setVisiable(false);
                spec.getLocation().setPhyTableName(phyTableAndGroup.getKey());
                spec.getLocation().setGroupKey(phyTableAndGroup.getValue());
            }
            newPartSpecList.add(spec);
        }
        newPartInfo.getPartitionBy().setPartitions(newPartSpecList);

        return newPartInfo;
    }

    public static PartitionInfo buildNewPartitionInfoByModifyingPartitionValues(PartitionInfo partitionInfo,
                                                                                SqlAlterTableModifyPartitionValues modifyValues,
                                                                                Map<SqlNode, RexNode> allRexExprInfo,
                                                                                ExecutionContext context,
                                                                                PartitionSpec[] outputNewPartSpec) {

        SqlPartition targetModifyPart = modifyValues.getPartition();
        boolean isAddValues = modifyValues.isAdd();
        SqlIdentifier partNameAst = (SqlIdentifier) targetModifyPart.getName();
        String partNameToBeModified = partNameAst.getLastName();
        boolean isMultiCols = partitionInfo.getPartitionBy().getPartitionColumnNameList().size() > 1;
        PartitionInfo newPartInfo = partitionInfo.copy();
        List<PartitionSpec> partSpecList = newPartInfo.getPartitionBy().getPartitions();

        PartitionStrategy strategy = newPartInfo.getPartitionBy().getStrategy();
        if (strategy != PartitionStrategy.LIST && strategy != PartitionStrategy.LIST_COLUMNS) {
            throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_INVALID_PARAMS,
                String.format("Not support to do this operation for the table [%s]", partitionInfo.getTableName()));
        }

        /**
         * Find the target partition by partition name
         */
        PartitionSpec tarSpec = null;
        for (int i = 0; i < partSpecList.size(); i++) {
            PartitionSpec pSpec = partSpecList.get(i);
            if (pSpec.getName().equals(partNameToBeModified)) {
                tarSpec = pSpec;
                break;
            }
        }
        if (tarSpec == null) {
            throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_INVALID_PARAMS, String
                .format("No found the partition[%s] in table [%s]", partNameToBeModified,
                    partitionInfo.getTableName()));
        }

        /**
         * Convert new list/listCols vals into SearchDatumInfo and put it into TreeSet
         */
        SearchDatumComparator cmp = partitionInfo.getPartitionBy().getPruningSpaceComparator();
        TreeSet<SearchDatumInfo> newListColValSet = new TreeSet<>(cmp);
        List<SqlPartitionValueItem> itemsOfVals = targetModifyPart.getValues().getItems();
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
        List<SearchDatumInfo> originalDatums = tarSpec.getBoundSpec().getMultiDatums();
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
        PartitionBoundSpec boundSpec = tarSpec.getBoundSpec();

        // TODO(moyi) write a builder to add/drop values from BoundSpec
        if (isAddValues) {
            boundSpec.setMultiDatums(newDatums);
        } else {
            boundSpec.setMultiDatums(newDatums);
        }

        if (outputNewPartSpec != null && outputNewPartSpec.length == 1) {
            outputNewPartSpec[0] = tarSpec;
        }
        return newPartInfo;
    }

    //========private method==========

    /**
     * The the data type of a bound value
     */
    protected static RelDataType getDataTypeForBoundVal(RelDataTypeFactory typeFactory, PartitionStrategy strategy,
                                                        RelDataType partExprRelDataType) {
        RelDataType boundValDataType = null;
        switch (strategy) {
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
             * But if the partCol data type is defined as bigint unsigned, then the
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
        }
        return boundValDataType;
    }

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

    private static PartitionByDefinition buildPartitionByDef(List<TablePartitionSpecConfig> partitionSpecConfigs,
                                                             TablePartitionRecord parentConfig,
                                                             List<ColumnMeta> allColumnMetas,
                                                             Map<Long, PartitionGroupRecord> partitionGroupRecordsMap) {
        TablePartitionRecord p0Config = partitionSpecConfigs.get(0).getSpecConfigInfo();

        PartitionByDefinition partitionBy = new PartitionByDefinition();
        String partMethod = p0Config.partMethod;
        String partExprStr = p0Config.partExpr;

        PartitionStrategy partStrategy = PartitionStrategy.valueOf(partMethod);
        partitionBy.setStrategy(partStrategy);

        /**
         * The raw partition expression from create tbl ddl or meta db
         */
        List<SqlPartitionValueItem> partitionExprList = PartitionInfoUtil.buildPartitionExprByString(partExprStr);

        /**
         * list of column name of partition fields
         */
        List<String> partitionColumnNameList = new ArrayList<>();
        for (int i = 0; i < partitionExprList.size(); i++) {
            SqlNode partExpr = partitionExprList.get(i).getValue();
            String colName = PartitionInfoUtil.findPartitionColumn(partExpr);
            partitionColumnNameList.add(colName.toLowerCase());
        }

        /**
         * find all the column fields according to the list of partition column names
         */
        List<ColumnMeta> partColFldList = new ArrayList<>();
        if (allColumnMetas != null && !allColumnMetas.isEmpty()) {
            for (int i = 0; i < partitionColumnNameList.size(); i++) {
                String partColName = partitionColumnNameList.get(i);
                ColumnMeta cm = null;
                for (int j = 0; j < allColumnMetas.size(); j++) {
                    ColumnMeta colMeta = allColumnMetas.get(j);
                    if (colMeta.getOriginColumnName().equalsIgnoreCase(partColName)) {
                        cm = colMeta;
                        break;
                    }
                }
                if (cm == null) {
                    throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_INVALID_PARAMS,
                        "Failed to init the column field for the partition key " + partColName);
                }
                partColFldList.add(cm);
            }
        }
        partitionBy.setPartitionFieldList(partColFldList);

        List<RelDataType> exprTypeList = PartitionInfoBuilder
            .inferPartExprDataTypes(partitionExprList.stream().map(o -> o.getValue()).collect(Collectors.toList()),
                partColFldList);
        partitionBy
            .setPartitionExprList(partitionExprList.stream().map(o -> o.getValue()).collect(Collectors.toList()));
        partitionBy.setPartitionExprTypeList(exprTypeList);
        partitionBy.setPartitionColumnNameList(partitionColumnNameList);

        SearchDatumComparator querySpaceComparator =
            PartitionByDefinition.getQuerySpaceComparator(partitionBy.getPartitionFieldList());
        partitionBy.setQuerySpaceComparator(querySpaceComparator);

        SearchDatumComparator pruningSpaceComparator =
            PartitionByDefinition.getPruningSpaceComparator(partitionBy.getPartitionExprTypeList());
        partitionBy.setPruningSpaceComparator(pruningSpaceComparator);

        SearchDatumComparator boundSpaceComparator = PartitionByDefinition
            .getBoundSpaceComparator(pruningSpaceComparator, partitionBy.getStrategy());
        partitionBy.setBoundSpaceComparator(boundSpaceComparator);

        SearchDatumHasher hasher = PartitionByDefinition
            .getHasher(partStrategy, partitionBy.getPartitionFieldList(), partitionBy.getPartitionExprTypeList());
        partitionBy.setHasher(hasher);

        SqlOperator partFuncOp = getPartFuncSqlOperator(partitionBy.getStrategy(), partitionBy.getPartitionExprList());
        if (partFuncOp != null) {
            partitionBy.setPartIntFunc(PartitionIntFunction.create(partFuncOp));
            partitionBy.setPartIntFuncMonotonicity(PartFuncMonotonicityUtil
                .getPartFuncMonotonicity(partFuncOp, partColFldList.get(0).getField().getRelType()));
        }

        partitionBy.setPartIntFuncOperator(partFuncOp);

        partitionBy.setNeedEnumRange(
            checkNeedDoEnumRange(partitionBy.getStrategy(), partitionBy.getPartitionFieldList(), partFuncOp));

        String defaultDbIndex = null;
        PartitionTableType tblType = PartitionTableType.getTypeByIntVal(parentConfig.tblType);
        if (tblType == PartitionTableType.BROADCAST_TABLE) {
            defaultDbIndex = TableInfoManager.getSchemaDefaultDbIndex(parentConfig.tableSchema);
        }

        for (int i = 0; i < partitionSpecConfigs.size(); i++) {
            TablePartitionSpecConfig partSpecConf = partitionSpecConfigs.get(i);
            TablePartitionRecord confInfo = partSpecConf.getSpecConfigInfo();

            Long id = confInfo.id;
            Long pid = confInfo.parentId;
            String name = confInfo.partName;
            Integer status = confInfo.partStatus;
            Long position = confInfo.partPosition;
            String desc = confInfo.partDesc;
            Long metaVer = confInfo.metaVersion;
            String comment = confInfo.partComment;
            String engine = confInfo.partEngine;
            ExtraFieldJSON extras = confInfo.partExtras;
            Long flags = confInfo.partFlags;
            String phyTbl = confInfo.phyTable;
            String method = confInfo.partMethod;
            PartitionStrategy strategy = PartitionStrategy.valueOf(method);

            PartitionSpec partitionSpec = new PartitionSpec();
            partitionSpec.setId(id);
            partitionSpec.setParentId(pid);
            partitionSpec.setName(name);
            partitionSpec.setPosition(position);
            partitionSpec.setStatus(status);
            partitionSpec.setVersion(metaVer);
            partitionSpec.setStrategy(strategy);

            // Covert to SqlNode from str for partition desc
            PartitionBoundSpec boundSpec = buildBoundSpecByPartitionDesc(desc, partitionBy);
            partitionSpec.setBoundSpec(boundSpec);
            if (boundSpec.isDefaultPartSpec()) {
                partitionSpec.setIsDefaultPartition(true);
            }

            partitionSpec.setComment(comment);
            partitionSpec.setEngine(engine);
            partitionSpec.setExtras(extras);
            partitionSpec.setFlags(flags);
            partitionSpec.setBoundSpaceComparator(partitionBy.getBoundSpaceComparator());

            PartitionGroupRecord partitionGroupRecord = partitionGroupRecordsMap.get(confInfo.groupId);
            String groupKey = partitionGroupRecord == null ? "" :
                GroupInfoUtil.buildGroupNameFromPhysicalDb(partitionGroupRecord.phy_db);
            if (tblType == PartitionTableType.BROADCAST_TABLE && StringUtils.isEmpty(groupKey)) {
                groupKey = defaultDbIndex;
            }

            PartitionLocation location =
                new PartitionLocation(groupKey, phyTbl,
                    partitionGroupRecord == null ? PartitionLocation.INVALID_PARTITION_GROUP_ID :
                        partitionGroupRecord.id);
            partitionSpec.setLocation(location);

            if (extras != null && TStringUtil.isNotEmpty(extras.getLocality())) {
                LocalityDesc locality = LocalityDesc.parse(extras.getLocality());
                partitionSpec.setLocality(locality.toString());
            }

            partitionBy.getPartitions().add(partitionSpec);
            if (partSpecConf.getSubPartitionSpecConfigs() != null
                && partSpecConf.getSubPartitionSpecConfigs().size() > 0) {
                throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_INVALID_PARAMS,
                    new NotSupportException("build partition definition with subpartition"));
            }
        }
        PartitionRouter router = PartitionByDefinition.getPartRouter(partitionBy);
        partitionBy.setRouter(router);

        /**
         * Prepare the orderNum in one phyDb for each partition
         */
        PartitionInfoBuilder.prepareOrderNumForPartitions(tblType, partitionBy.getPartitions());

        return partitionBy;
    }

    private static PartitionBoundSpec buildBoundSpecByPartitionDesc(String partDescStr,
                                                                    PartitionByDefinition partitionBy) {

        /**
         * The raw partition expression from create tbl ddl or meta db
         */
        List<SqlPartitionValueItem> bndSpecVal = PartitionInfoUtil.buildPartitionExprByString(partDescStr);

        List<RelDataType> partExprDataTypes = partitionBy.getPartitionExprTypeList();
        boolean isMultiPartCols = partExprDataTypes.size() > 1;
        RelDataTypeFactory typeFactory = PartitionPrunerUtils.getTypeFactory();
        RexBuilder rexBuilder = PartitionPrunerUtils.getRexBuilder();
        PartitionStrategy strategy = partitionBy.getStrategy();

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
                    assert oneValItem.getValue() instanceof SqlLiteral;
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
                        boundVal = PartitionBoundVal.createMaxValue();
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
            if (strategy == PartitionStrategy.HASH || strategy == PartitionStrategy.KEY) {
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
            if (DataTypeUtil.isNumberSqlType(partFields.get(0).getField().getDataType())) {
                return true;
            }
        } else if (strategy == PartitionStrategy.HASH && partFields.size() == 1 && partFuncOp == null) {
            return true;
        } else if ((strategy == PartitionStrategy.RANGE || strategy == PartitionStrategy.LIST) && partFuncOp != null) {
            if (partFuncOp.getName() == "MONTH") {
                return true;
            } else {
                return false;
            }
        }
        return false;
    }

    protected static SqlOperator getPartFuncSqlOperator(PartitionStrategy strategy, List<SqlNode> partColExprList) {
        if (strategy == PartitionStrategy.HASH
            || strategy == PartitionStrategy.RANGE
            || strategy == PartitionStrategy.LIST) {
            SqlNode colExpr = partColExprList.get(0);
            if (colExpr instanceof SqlCall) {
                SqlCall sqlCall = (SqlCall) colExpr;
                return sqlCall.getOperator();
            }
        }
        return null;
    }
}
