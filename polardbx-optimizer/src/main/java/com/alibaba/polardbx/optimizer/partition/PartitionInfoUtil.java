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

import com.alibaba.polardbx.common.charset.CharsetName;
import com.alibaba.polardbx.common.charset.CollationName;
import com.alibaba.polardbx.common.charset.MySQLCharsetDDLValidator;
import com.alibaba.polardbx.common.exception.NotSupportException;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.CaseInsensitive;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.druid.sql.ast.SQLExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.polardbx.druid.sql.dialect.mysql.parser.MySqlExprParser;
import com.alibaba.polardbx.druid.sql.parser.ByteString;
import com.alibaba.polardbx.druid.sql.parser.Lexer;
import com.alibaba.polardbx.druid.sql.parser.Token;
import com.alibaba.polardbx.gms.locality.LocalityDesc;
import com.alibaba.polardbx.gms.metadb.table.TableInfoManager;
import com.alibaba.polardbx.gms.partition.ExtraFieldJSON;
import com.alibaba.polardbx.gms.partition.TablePartRecordInfoContext;
import com.alibaba.polardbx.gms.partition.TablePartitionRecord;
import com.alibaba.polardbx.gms.tablegroup.JoinGroupInfoRecord;
import com.alibaba.polardbx.gms.tablegroup.JoinGroupUtils;
import com.alibaba.polardbx.gms.tablegroup.PartitionGroupRecord;
import com.alibaba.polardbx.gms.tablegroup.TableGroupConfig;
import com.alibaba.polardbx.gms.tablegroup.TableGroupLocation;
import com.alibaba.polardbx.gms.tablegroup.TableGroupRecord;
import com.alibaba.polardbx.gms.tablegroup.TableGroupUtils;
import com.alibaba.polardbx.gms.topology.DbGroupInfoManager;
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.gms.topology.GroupDetailInfoExRecord;
import com.alibaba.polardbx.gms.util.GroupInfoUtil;
import com.alibaba.polardbx.gms.util.PartitionNameUtil;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.ComplexTaskMetaManager;
import com.alibaba.polardbx.optimizer.config.table.Field;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.TddlOperatorTable;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypeUtil;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.function.SqlSubStrFunction;
import com.alibaba.polardbx.optimizer.exception.OptimizerException;
import com.alibaba.polardbx.optimizer.hint.util.HintUtil;
import com.alibaba.polardbx.optimizer.locality.LocalityInfo;
import com.alibaba.polardbx.optimizer.locality.LocalityInfoUtils;
import com.alibaba.polardbx.optimizer.locality.LocalityManager;
import com.alibaba.polardbx.optimizer.parse.custruct.FastSqlConstructUtils;
import com.alibaba.polardbx.optimizer.parse.visitor.ContextParameters;
import com.alibaba.polardbx.optimizer.partition.boundspec.MultiValuePartitionBoundSpec;
import com.alibaba.polardbx.optimizer.partition.boundspec.SingleValuePartitionBoundSpec;
import com.alibaba.polardbx.optimizer.partition.common.GetNewActPartColCntFromAstParams;
import com.alibaba.polardbx.optimizer.partition.common.PartKeyLevel;
import com.alibaba.polardbx.optimizer.partition.common.PartitionLocation;
import com.alibaba.polardbx.optimizer.partition.common.PartitionStrategy;
import com.alibaba.polardbx.optimizer.partition.common.PartitionTableType;
import com.alibaba.polardbx.optimizer.partition.datatype.function.PartitionFunctionBuilder;
import com.alibaba.polardbx.optimizer.partition.datatype.function.PartitionIntFunction;
import com.alibaba.polardbx.optimizer.partition.datatype.function.udf.UdfJavaFunctionHelper;
import com.alibaba.polardbx.optimizer.partition.pruning.SearchDatumComparator;
import com.alibaba.polardbx.optimizer.partition.pruning.SearchDatumInfo;
import com.alibaba.polardbx.optimizer.rule.TddlRuleManager;
import com.alibaba.polardbx.optimizer.tablegroup.TableGroupInfoManager;
import com.alibaba.polardbx.optimizer.utils.RelUtils;
import com.alibaba.polardbx.rule.TableRule;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlAlterTableDropPartition;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlCreateTable;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlPartition;
import org.apache.calcite.sql.SqlPartitionValue;
import org.apache.calcite.sql.SqlPartitionValueItem;
import org.apache.calcite.sql.SqlSubPartition;
import org.apache.calcite.sql.SqlUnresolvedFunction;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.fun.SqlSubstringFunction;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.util.SqlShuttle;
import org.apache.calcite.util.Pair;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;
import org.jetbrains.annotations.NotNull;

import java.sql.Connection;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static com.alibaba.polardbx.common.ddl.newengine.DdlConstants.MAX_TABLE_NAME_LENGTH_MYSQL_ALLOWS;
import static com.alibaba.polardbx.common.ddl.newengine.DdlConstants.RANDOM_SUFFIX_LENGTH_OF_PHYSICAL_TABLE_NAME;
import static com.alibaba.polardbx.gms.partition.TablePartitionRecord.PARTITION_LEVEL_PARTITION;
import static com.alibaba.polardbx.gms.partition.TablePartitionRecord.PARTITION_LEVEL_SUBPARTITION;
import static com.alibaba.polardbx.gms.tablegroup.TableGroupLocation.getFullOrderedGroupList;
import static com.alibaba.polardbx.gms.tablegroup.TableGroupLocation.getOrderedGroupList;
import static com.alibaba.polardbx.optimizer.partition.PartitionLocator.PHYSICAL_TABLENAME_PATTERN;
import static com.alibaba.polardbx.optimizer.partition.common.PartitionStrategy.LIST;

/**
 * @author chenghui.lch
 */
public class PartitionInfoUtil {

    static Pattern IS_NUMBER = Pattern.compile("^[\\d]*$");
    static String MAXVALUE = "MAXVALUE";
    final static long MAX_HASH_VALUE = Long.MAX_VALUE;
    final static long MIN_HASH_VALUE = Long.MIN_VALUE;
    public static final int FULL_PART_COL_COUNT = -1;
    public static final ArrayList<Integer> ALL_LEVEL_FULL_PART_COL_COUNT_LIST =
        Lists.newArrayList(FULL_PART_COL_COUNT, FULL_PART_COL_COUNT);
    public static final int IGNORE_PARTNAME_LOCALITY = 1;
    public static final int COMPARE_NEW_PART_LOCATION = 2;
    public static final int COMPARE_EXISTS_PART_LOCATION = 4;

    protected static class PartitionColumnFinder extends SqlShuttle {

        protected SqlIdentifier partColumn;

        protected PartitionColumnFinder() {
        }

        public boolean find(SqlNode partExpr) {
            partExpr.accept(this);
            return partColumn != null;
        }

        @Override
        public SqlNode visit(SqlIdentifier id) {
            partColumn = id;
            return id;
        }

        @Override
        public SqlNode visit(SqlCall call) {
            SqlOperator sqlOp = call.getOperator();
            if (sqlOp instanceof SqlSubStrFunction || sqlOp instanceof SqlSubstringFunction) {
                SqlNode firstParams = call.getOperandList().get(0);
                PartitionColumnFinder tmpColFinder = new PartitionColumnFinder();
                firstParams.accept(tmpColFinder);
                this.partColumn = tmpColFinder.getPartColumn();
                return call;
            } else {
                return super.visit(call);
            }
        }

        @Override
        public SqlNode visit(SqlLiteral id) {
            partColumn = new SqlIdentifier(id.toValue(), SqlParserPos.ZERO);
            return partColumn;
        }

        public SqlIdentifier getPartColumn() {
            return partColumn;
        }

    }

    public static ExtraFieldJSON genPartExtrasRecord(PartitionInfo partitionInfo) {
        ExtraFieldJSON extras = new ExtraFieldJSON();
        extras.setLocality("");
        return extras;
    }

    public static TablePartitionRecord prepareRecordForLogicalTable(PartitionInfo partitionInfo) {

        TablePartitionRecord record = new TablePartitionRecord();

        record.parentId = TablePartitionRecord.NO_PARENT_ID;
        record.tableSchema = partitionInfo.tableSchema;
        record.tableName = partitionInfo.tableName;
        record.groupId = partitionInfo.tableGroupId;
        record.spTempFlag = partitionInfo.spTemplateFlag;
        record.metaVersion = partitionInfo.metaVersion;
        record.autoFlag = partitionInfo.getAutoFlag();
        record.partFlags = partitionInfo.getPartFlags();
        record.tblType = partitionInfo.getTableType().getTableTypeIntValue();
        record.partLevel = TablePartitionRecord.PARTITION_LEVEL_LOGICAL_TABLE;
        if (partitionInfo.getPartitionBy() != null) {
            record.nextLevel = PARTITION_LEVEL_PARTITION;
        }
        // during creating ddl, the status of log table should be creating ,
        // this is just for test to use serving directly
        record.partStatus = TablePartitionRecord.PARTITION_STATUS_LOGICAL_TABLE_ABSENT;

        record.partTempName = "";
        record.partName = "";
        record.partPosition = TablePartitionRecord.NO_PARTITION_POSITION;
        record.partMethod = "";
        record.partExpr = "";
        record.partDesc = "";
        record.partComment = "";
        record.partEngine = "";
        record.phyTable = "";

        ExtraFieldJSON partExtras = new ExtraFieldJSON();
        String tableNamePattern = partitionInfo.getTableNamePattern();
        if (tableNamePattern != null) {
            partExtras.setPartitionPattern(tableNamePattern);
        }
        partExtras.setLocality(partitionInfo.getLocality());
        if (!StringUtils.isEmpty(partitionInfo.getSessionVars().getTimeZone())) {
            partExtras.setTimeZone(partitionInfo.getSessionVars().getTimeZone());
        }
        if (!StringUtils.isEmpty(partitionInfo.getSessionVars().getCharset())) {
            partExtras.setCharset(partitionInfo.getSessionVars().getCharset());
        }
        if (!StringUtils.isEmpty(partitionInfo.getSessionVars().getCollation())) {
            partExtras.setCollation(partitionInfo.getSessionVars().getCollation());
        }
        record.partExtras = partExtras;

        return record;
    }

    public static List<TablePartitionRecord> prepareRecordForAllPartitions(PartitionInfo partitionInfo) {

        List<TablePartitionRecord> partRecords = new ArrayList<>();
        List<PartitionSpec> partitionSpecs = partitionInfo.getPartitionBy().getPartitions();
        for (int i = 0; i < partitionSpecs.size(); i++) {
            PartitionSpec partitionSpec = partitionSpecs.get(i);
            TablePartitionRecord record = prepareRecordForOnePartition(partitionInfo, null, partitionSpec);
            if (record.groupId == null) {
                record.groupId = TableGroupRecord.INVALID_TABLE_GROUP_ID;
            }
            partRecords.add(record);
        }

        return partRecords;
    }

    public static TablePartitionRecord prepareRecordForOnePartition(PartitionInfo partitionInfo,
                                                                    PartitionSpec parentPartSpec,
                                                                    PartitionSpec partitionSpec) {
        TablePartitionRecord record = new TablePartitionRecord();

        // The string of partition expression should be normalization
        String partExprListStr = "";
        PartKeyLevel currPartKeyLevel = partitionSpec.getPartLevel();
        PartitionByDefinition partByDef = partitionInfo.getPartitionBy();
        PartitionByDefinition subPartByDef = partitionInfo.getPartitionBy().getSubPartitionBy();

        List<SqlNode> partExprList = null;
        if (currPartKeyLevel == PartKeyLevel.SUBPARTITION_KEY) {
            partExprList = subPartByDef.getPartitionExprList();
        } else {
            partExprList = partByDef.getPartitionExprList();
        }

        for (int i = 0; i < partExprList.size(); i++) {
            SqlNode partExpr = partExprList.get(i);
            String partExprStr = partExpr.toString();
            if (!partExprListStr.isEmpty()) {
                partExprListStr += ",";
            }
            if (!(partExpr instanceof SqlCall)) {
                partExprListStr += "`" + partExprStr + "`";
            } else {
                partExprListStr += partExprStr;
            }
        }

        record.tableSchema = partitionInfo.getTableSchema();
        record.tableName = partitionInfo.getTableName();

        record.spTempFlag = partitionInfo.getSpTemplateFlag();
        record.metaVersion = partitionInfo.getMetaVersion();
        record.autoFlag = TablePartitionRecord.PARTITION_AUTO_BALANCE_DISABLE;
        record.tblType = partitionInfo.getTableType().getTableTypeIntValue();
        record.parentId = partitionSpec.parentId;
        record.id = partitionSpec.id;

        if (parentPartSpec == null) {
            record.partLevel = PARTITION_LEVEL_PARTITION;
        } else {
            record.partLevel = TablePartitionRecord.PARTITION_LEVEL_SUBPARTITION;
        }

        if (partitionSpec.isLogical()) {
            record.groupId = TableGroupRecord.NO_TABLE_GROUP_ID;
            record.nextLevel = TablePartitionRecord.PARTITION_LEVEL_SUBPARTITION;
        } else {
            record.groupId = partitionSpec.getLocation().getPartitionGroupId();
            record.nextLevel = TablePartitionRecord.PARTITION_LEVEL_NO_NEXT_PARTITION;
        }

        record.partStatus = TablePartitionRecord.PARTITION_STATUS_LOGICAL_TABLE_ABSENT;
        record.partName = partitionSpec.getName();
        record.partTempName = partitionSpec.getTemplateName() == null ? "" : partitionSpec.getTemplateName();
        record.partPosition = Long.valueOf(partitionSpec.getPosition());

        // covert partition strategy to string
        record.partMethod = partitionSpec.getStrategy().toString();
        record.partExpr = partExprListStr;

        record.partDesc =
            partitionSpec.getBoundSpec().getPartitionBoundDescription(PartitionInfoUtil.FULL_PART_COL_COUNT);

        ExtraFieldJSON partExtras = new ExtraFieldJSON();
        partExtras.setLocality(partitionSpec.getLocality() == null ? "" : partitionSpec.getLocality());
        record.partExtras = partExtras;

        record.partFlags = 0L;
        record.partComment = partitionSpec.getComment();

        // fill location info, only fill location for physical partition
        if (!partitionSpec.isLogical()) {
            record.partEngine = partitionSpec.getEngine();
            record.phyTable = partitionSpec.getLocation().getPhyTableName();
        } else {
            record.phyTable = "";
            record.partEngine = "";
        }

        return record;
    }

    public static Map<String, List<TablePartitionRecord>> prepareRecordForAllSubpartitions(
        List<TablePartitionRecord> parentRecords,
        PartitionInfo partitionInfo,
        List<PartitionSpec> partitionSpecs) {

        Map<String, List<TablePartitionRecord>> subPartRecordInfos = new HashMap<>();
        if (partitionInfo.getPartitionBy().getSubPartitionBy() == null) {
            return subPartRecordInfos;
        }

        assert parentRecords.size() == partitionSpecs.size();
        for (int k = 0; k < parentRecords.size(); k++) {
            TablePartitionRecord parentRecord = parentRecords.get(k);
            PartitionSpec partitionSpec = partitionSpecs.get(k);
            List<PartitionSpec> subPartitionSpecs = partitionSpec.getSubPartitions();
            List<TablePartitionRecord> subPartRecList = new ArrayList<>();
            for (int i = 0; i < subPartitionSpecs.size(); i++) {
                PartitionSpec subPartitionSpec = subPartitionSpecs.get(i);
                TablePartitionRecord record =
                    prepareRecordForOnePartition(partitionInfo, partitionSpec, subPartitionSpec);
                if (record.groupId == null) {
                    record.groupId = TableGroupRecord.INVALID_TABLE_GROUP_ID;
                }
                subPartRecList.add(record);
            }
            subPartRecordInfos.put(parentRecord.getPartName(), subPartRecList);
        }
        return subPartRecordInfos;
    }

    public static TableGroupRecord prepareRecordForTableGroup(PartitionInfo partitionInfo) {
        return prepareRecordForTableGroup(partitionInfo, false);
    }

    public static TableGroupRecord prepareRecordForTableGroup(PartitionInfo partitionInfo, boolean isOss) {
        TableGroupRecord tableGroupRecord = new TableGroupRecord();
        tableGroupRecord.schema = partitionInfo.tableSchema;
        tableGroupRecord.locality = partitionInfo.locality;
        tableGroupRecord.meta_version = 0L;

        if (isOss) {
            tableGroupRecord.tg_type = TableGroupRecord.TG_TYPE_OSS_TBL_TG;
            return tableGroupRecord;
        }
        if (partitionInfo.getTableType() == PartitionTableType.SINGLE_TABLE
            || partitionInfo.getTableType() == PartitionTableType.GSI_SINGLE_TABLE) {
            if ((partitionInfo.getLocality() == null || partitionInfo.getLocality().isEmpty())
                && !partitionInfo.getBuildNoneDefaultSingleGroup()) {
                tableGroupRecord.tg_type = TableGroupRecord.TG_TYPE_DEFAULT_SINGLE_TBL_TG;
            } else {
                tableGroupRecord.tg_type = TableGroupRecord.TG_TYPE_NON_DEFAULT_SINGLE_TBL_TG;
            }
        } else if (partitionInfo.getTableType() == PartitionTableType.BROADCAST_TABLE
            || partitionInfo.getTableType() == PartitionTableType.GSI_BROADCAST_TABLE) {
            tableGroupRecord.tg_type = TableGroupRecord.TG_TYPE_BROADCAST_TBL_TG;
        } else {
            tableGroupRecord.tg_type = TableGroupRecord.TG_TYPE_PARTITION_TBL_TG;
        }
        return tableGroupRecord;
    }

    public static List<PartitionGroupRecord> prepareRecordForPartitionGroups(List<PartitionSpec> partitionSpecs) {
        List<PartitionGroupRecord> partitionGroupRecords = new ArrayList<>();
        for (PartitionSpec partitionSpec : partitionSpecs) {
            PartitionLocation location = partitionSpec.getLocation();
            PartitionGroupRecord partitionGroupRecord = new PartitionGroupRecord();
            partitionGroupRecord.setPhy_db(GroupInfoUtil.buildPhysicalDbNameFromGroupName(location.getGroupKey()));
            partitionGroupRecord.setPartition_name(partitionSpec.getName());

            partitionGroupRecord.setId(location.getPartitionGroupId());
            partitionGroupRecord.setLocality(partitionSpec.getLocality());
            partitionGroupRecords.add(partitionGroupRecord);
        }
        return partitionGroupRecords;
    }

    /**
     * The partition expression stored in meta db.
     * <p>
     * This method will return the list of sql node
     * <p>
     * <pre>
     * e.g.
     *  for range, it may by "year(`gmt_create`)" ,
     *  for range_columns, it may be "(`pk`,`gmt_create`)",
     *  and so on
     * </pre>
     */
    public static List<SqlPartitionValueItem> buildPartitionExprByString(String partExpr) {

        List<SQLExpr> exprList = new ArrayList<>();
        List<SqlPartitionValueItem> partExprSqlNodeList = new ArrayList<>();
        MySqlExprParser parser = new MySqlExprParser(ByteString.from(partExpr));
        Lexer lexer = parser.getLexer();
        while (true) {
            SQLExpr expr = parser.expr();
            exprList.add(expr);
            if (lexer.token() == Token.COMMA) {
                lexer.nextToken();
            }

            if (lexer.token() == Token.EOF) {
                break;
            }
        }

        ContextParameters context = new ContextParameters(false);
        for (int i = 0; i < exprList.size(); i++) {
            SQLExpr expr = exprList.get(i);
            SqlNode tmpSqlNodePartExpr = FastSqlConstructUtils.convertToSqlNode(expr, context, null);
            SqlNode sqlNodePartExpr = rewritePartExprIfNeed(tmpSqlNodePartExpr);
            SqlPartitionValueItem valueItem = new SqlPartitionValueItem(sqlNodePartExpr);
            if (expr instanceof SQLIdentifierExpr) {
                if (MAXVALUE.equalsIgnoreCase(((SQLIdentifierExpr) expr).getSimpleName())) {
                    valueItem.setMaxValue(true);
                }
            }
            partExprSqlNodeList.add(valueItem);
        }
        return partExprSqlNodeList;
    }

    private static SqlNode rewritePartExprIfNeed(SqlNode rawPartExpr) {
        if (rawPartExpr instanceof SqlCall) {
            SqlCall funcExpr = (SqlCall) rawPartExpr;
            SqlOperator op = funcExpr.getOperator();
            List<SqlNode> params = funcExpr.getOperandList();
            if (op instanceof SqlUnresolvedFunction) {
                String funcName = op.getName();
                PartitionFunctionBuilder.validatePartitionFunction(funcName);
                SqlNode newPartExpr = UdfJavaFunctionHelper.getUdfPartFuncCallAst(funcName, params);
                return newPartExpr;
            } else {
                return rawPartExpr;
            }
        } else {
            return rawPartExpr;
        }

    }

    protected static String findPartitionColumn(SqlNode partExprSqlNode) {

        SqlCreateTable.PartitionColumnFinder finder = new SqlCreateTable.PartitionColumnFinder();
        boolean isFound = finder.find(partExprSqlNode);

        if (!isFound) {
            return null;
        }

        if (finder.getAllPartColAsts().size() != 1) {
            throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_INVALID_PARAMS,
                String.format("multi columns used as inputs of a partition function are not allowed"));
        }

        SqlIdentifier partCol = finder.getPartColumn();
        String partColName = RelUtils.lastStringValue(partCol);
        return partColName;
    }

    public static void generateTableNamePattern(PartitionInfo partitionInfo, String tableName) {
        if (!partitionInfo.isRandomTableNamePatternEnabled()) {
            return;
        }

        int lengthPlaceHolder = 5;
        int numExtraUnderScores = 2;

        int maxAllowedLengthOfTableNamePrefix =
            MAX_TABLE_NAME_LENGTH_MYSQL_ALLOWS - RANDOM_SUFFIX_LENGTH_OF_PHYSICAL_TABLE_NAME - numExtraUnderScores
                - lengthPlaceHolder;
        if (maxAllowedLengthOfTableNamePrefix < 0) {
            throw new OptimizerException("Unexpected large length of table name place holder: " + lengthPlaceHolder);
        }

        String randomSuffix;
        String existingRandomSuffix = partitionInfo.getTableNamePattern();

        if (tableName.length() > maxAllowedLengthOfTableNamePrefix) {
            tableName = TStringUtil.substring(tableName, 0, maxAllowedLengthOfTableNamePrefix);
        }

        if (TStringUtil.isEmpty(existingRandomSuffix)) {
            randomSuffix = RandomStringUtils.randomAlphanumeric(RANDOM_SUFFIX_LENGTH_OF_PHYSICAL_TABLE_NAME);
            partitionInfo.setTableNamePattern(tableName + "_" + randomSuffix);
        }
    }

    public static List<com.alibaba.polardbx.common.utils.Pair<GroupDetailInfoExRecord, TableGroupRecord>> getOrderedGroupListForSingleTable(
        String logicalDbName,
        boolean includeToBeRemoveGroup) {
        TableGroupInfoManager tableGroupInfoManager =
            OptimizerContext.getContext(logicalDbName).getTableGroupInfoManager();
        Map<Long, TableGroupConfig> tableGroupConfigMap = tableGroupInfoManager.getTableGroupConfigInfoCache();
        Map<String, GroupDetailInfoExRecord> groups =
            getOrderedGroupList(logicalDbName).stream().collect(Collectors.toMap(
                o -> o.phyDbName, o -> o
            ));
        Map<String, Integer> groupTableCount = groups.keySet().stream().collect(Collectors.toMap(
            o -> o, o -> 0
        ));
        Map<String, TableGroupRecord> groupTgMap = new HashMap<>();
        groups.keySet().forEach(o -> groupTgMap.put(o, null));
        for (Long tableGroupId : tableGroupConfigMap.keySet()) {
            TableGroupRecord tableGroupRecord = tableGroupConfigMap.get(tableGroupId).getTableGroupRecord();
            if (tableGroupRecord.isSingleTableGroup() && tableGroupRecord.withBalanceSingleTableLocality()) {
                TableGroupConfig tableGroupConfig = tableGroupConfigMap.get(tableGroupId);
                String phyDb = tableGroupConfig.getPartitionGroupRecords().get(0).getPhy_db();
                int tableCount = tableGroupConfig.getTableCount();
                groupTableCount.put(phyDb, tableCount);
                groupTgMap.put(phyDb, tableGroupRecord);
            }
        }
        List<String> phyDbs =
            groupTableCount.keySet().stream().sorted(Comparator.comparingInt(o -> groupTableCount.get(o))).collect(
                Collectors.toList());
        List<com.alibaba.polardbx.common.utils.Pair<GroupDetailInfoExRecord, TableGroupRecord>> results =
            phyDbs.stream().map(
                phyDb -> com.alibaba.polardbx.common.utils.Pair.of(groups.get(phyDb), groupTgMap.get(phyDb))).collect(
                Collectors.toList());
        return results;
    }

    public static void generatePartitionLocation(PartitionInfo partitionInfo,
                                                 String tableGroupName,
                                                 String joinGroupName,
                                                 ExecutionContext executionContext,
                                                 LocalityDesc localityDesc) {

        PartitionTableType tblType = partitionInfo.getTableType();
        Boolean singleGroupMissed = false;
        Boolean singleGroupEmpty = false;
        TableGroupConfig tableGroupConfig;
        List<GroupDetailInfoExRecord> groups = new ArrayList<>();
        if (localityDesc.hasBalanceSingleTable()) {
            if (tblType != PartitionTableType.SINGLE_TABLE) {
                throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_INVALID_PARAMS, String.format(
                    "Failed to create table  [%s] because option 'balance_single_table' is supported only for single table",
                    partitionInfo.getTableName()));
            }
            String dbLocality = LocalityManager.getInstance().getLocalityOfDb(partitionInfo.tableSchema).getLocality();
            LocalityDesc dbLocalityDesc = LocalityInfoUtils.parse(dbLocality);
            List<com.alibaba.polardbx.common.utils.Pair<GroupDetailInfoExRecord, TableGroupRecord>>
                orderedStorageAndTableGroupList =
                getOrderedGroupListForSingleTable(partitionInfo.getTableSchema(),
                    false);
            singleGroupMissed = orderedStorageAndTableGroupList.stream().anyMatch(o -> o.getValue() == null);
            singleGroupEmpty = orderedStorageAndTableGroupList.stream().noneMatch(o -> o.getValue() != null);
            if (singleGroupMissed) {
                if (!singleGroupEmpty) {
                    partitionInfo.setBuildNoneDefaultSingleGroup(true);
                }
                tableGroupConfig = null;
                groups.add(orderedStorageAndTableGroupList.get(0).getKey());
            } else {
                partitionInfo.setBuildNoneDefaultSingleGroup(true);
                TableGroupInfoManager tableGroupInfoManager =
                    OptimizerContext.getContext(partitionInfo.getTableSchema()).getTableGroupInfoManager();
                tableGroupConfig = tableGroupInfoManager.getTableGroupConfigByName(
                    orderedStorageAndTableGroupList.get(0).getValue().getTg_name());
            }
        } else if (localityDesc.getHashRangeSequentialPlacement() && StringUtils.isEmpty(tableGroupName)) {
            if (tblType != PartitionTableType.PARTITION_TABLE
                || partitionInfo.getAllLevelPartitionStrategies().size() == 1) {
                PartitionStrategy partitionStrategy = partitionInfo.getAllLevelPartitionStrategies().get(0);
                if (!(partitionStrategy.isHashed() || partitionStrategy.isRange() || partitionStrategy.isKey()
                    || partitionStrategy.isUdfHashed())) {
                    throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_INVALID_PARAMS, String.format(
                        "Failed to create table  [%s] because option 'hash_range_sequential_placement' is supported only for range or hash partition table without subpartition",
                        partitionInfo.getTableName()));
                }
            }
            groups = getOrderedGroupList(executionContext.getSchemaName());
            tableGroupConfig = null;
        } else {
            if (localityDesc.getHashRangeSequentialPlacement()) {
                groups = getOrderedGroupList(executionContext.getSchemaName());
            }
            tableGroupConfig =
                getTheBestTableGroupInfo(partitionInfo, tableGroupName, joinGroupName, null, 0, false,
                    null, null, null, executionContext);
        }
        if (!StringUtils.isEmpty(tableGroupName) && !localityDesc.holdEmptyDnList() && tableGroupConfig == null) {
            throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_INVALID_PARAMS, String.format(
                "Failed to create table  [%s] because the partition info and locality you specify conflict with that of tablegroup [%s]",
                partitionInfo.getTableName(), tableGroupName));
        }
        String schema = partitionInfo.getTableSchema();
        String logTbName = partitionInfo.getPrefixTableName();
        boolean phyTblNameSameAsLogicalTblName = false;
        if (tblType == PartitionTableType.SINGLE_TABLE || tblType == PartitionTableType.BROADCAST_TABLE
            || tblType == PartitionTableType.GSI_SINGLE_TABLE || tblType == PartitionTableType.GSI_BROADCAST_TABLE) {
            phyTblNameSameAsLogicalTblName = true;
        }

        if (tblType == PartitionTableType.BROADCAST_TABLE || tblType == PartitionTableType.GSI_BROADCAST_TABLE) {
            if (localityDesc != null && !localityDesc.holdEmptyDnList()) {
                throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_INVALID_PARAMS,
                    String.format("Unable to set locality for broadcast table: [%s]", partitionInfo.getTableName()));
            }
        }
        if (tblType == PartitionTableType.SINGLE_TABLE) {
            if (localityDesc != null && !localityDesc.holdEmptyDnList() && localityDesc.getDnList().size() != 1) {
                throw new TddlRuntimeException(ErrorCode.ERR_INVALID_DDL_PARAMS, String.format(
                    "invalid locality for single table! you can only set one dn as locality for single table [%s]",
                    partitionInfo.getTableName()));
            }
            if (localityDesc != null && !localityDesc.holdEmptyDnList()
                && localityDesc.hasBalanceSingleTable() != false) {
                throw new TddlRuntimeException(ErrorCode.ERR_INVALID_DDL_PARAMS, String.format(
                    "invalid locality for single table! you can only set one of dn list and balance option for single table [%s]",
                    partitionInfo.getTableName()));
            }
        }
        // locality property override, pg locality => table locality => tg locality => schema locality
        // case 0, tg null, table null: from db allocator.
        // case 1, tg not null. table null: from tg locality desc.
        // case 2, table not null: from table locality desc.
        // case 3, partition not null: from partition locality desc.
        // TODO: now table locality passed by partition locality desc!!!!

        // case 0
        LocalityDesc dbLocalityDesc =
            LocalityInfoUtils.parse(LocalityManager.getInstance().getLocalityOfDb(schema).getLocality());
        // for db specified locality, only default dn is used
        TableGroupLocation.GroupAllocator groupAllocator =
            TableGroupLocation.buildGroupAllocator(schema, dbLocalityDesc);
        LocalityDesc tableGroupLocality = new LocalityDesc();
        int partNum = partitionInfo.getPartitionBy().getPartitions().size();
        if (localityDesc != null && !localityDesc.holdEmptyDnList()) { // case 2
            tableGroupLocality = localityDesc;
            groupAllocator = TableGroupLocation.buildGroupAllocatorByLocality(schema, localityDesc);
        } else if (localityDesc != null && localityDesc.hasBalanceSingleTable()) {
            groupAllocator = TableGroupLocation.buildGroupAllocatorByGroup(schema, groups);
        } else if (localityDesc != null && localityDesc.getHashRangeSequentialPlacement()) {
            //TODO support hash range sequentail placement
            groupAllocator = TableGroupLocation.buildGroupAllocatorByGroup(schema, groups, partNum);
        } else if (tableGroupConfig != null && tableGroupConfig.getLocalityDesc() != null
            && !tableGroupConfig.getLocalityDesc().holdEmptyDnList()) { // case 1
            tableGroupLocality = tableGroupConfig.getLocalityDesc();
            partitionInfo.setLocality(tableGroupLocality.toString());
            groupAllocator =
                TableGroupLocation.buildGroupAllocatorByLocality(schema, tableGroupConfig.getLocalityDesc());
        } else if (tableGroupConfig != null && tableGroupConfig.getLocalityDesc() != null
            && tableGroupConfig.getLocalityDesc().getHashRangeSequentialPlacement()) {
            groups = getOrderedGroupList(executionContext.getSchemaName());
            groupAllocator = TableGroupLocation.buildGroupAllocatorByGroup(schema, groups, partNum);
        }

        // TODO: we will support locality for partition table.
        // no table group match current table or matched empty tablegroup.
        // for empty tablegroup, partitionInfo is still empty.
        if (tableGroupConfig == null || tableGroupConfig.isEmpty()) {
            if (tableGroupConfig != null) {
                partitionInfo.setTableGroupId(tableGroupConfig.getTableGroupRecord().getId());
            }

            PartitionByDefinition partByDef = partitionInfo.getPartitionBy();
            PartitionByDefinition subPartByDef = partByDef.getSubPartitionBy();
            List<PartitionSpec> orderedPartSpecs = partByDef.getOrderedPartitionSpecs();
            Map<String, PartitionSpec> nameToPhySpecMapping = partByDef.getPartNameToPhyPartMap();
            List<String> allPhyGrpList = HintUtil.allGroup(schema);
            if (tblType == PartitionTableType.BROADCAST_TABLE || tblType == PartitionTableType.GSI_BROADCAST_TABLE) {
                if (allPhyGrpList.size() != orderedPartSpecs.size()) {
                    throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_INVALID_PARAMS,
                        String.format(
                            "Failed to create table group for the broadcast table [%s] because the number of partitions mismatch the phy group count",
                            logTbName));
                }
            }
            // keep the identical group allocator among partitions, we have to trace the allocator to support continious allocation.
            Map<String, TableGroupLocation.GroupAllocator> partitionPgGroupAllocators = new HashMap<>();
            Integer allPhyPartCnt = 0;
            for (int i = 0; i < orderedPartSpecs.size(); i++) {
                PartitionSpec partSpec = orderedPartSpecs.get(i);
                LocalityDesc partLocality = LocalityDesc.parse(partSpec.getLocality());
                boolean isLogicalSpec = partSpec.isLogical();
                List<PartitionSpec> phySpecList = new ArrayList<>();
                if (isLogicalSpec) {
                    phySpecList = PartitionByDefinition.getOrderedPhyPartSpecsByPartStrategy(subPartByDef.getStrategy(),
                        subPartByDef.getBoundSpaceComparator(), partSpec.getSubPartitions());
                } else {
                    phySpecList.add(partSpec);
                }
                for (int j = 0; j < phySpecList.size(); j++) {
                    ++allPhyPartCnt;
                    PartitionSpec curPhyPartSpec = nameToPhySpecMapping.get(phySpecList.get(j).getName());
                    Integer curPhyPartIndex = allPhyPartCnt - 1;
                    PartitionLocation phySpecLocation =
                        genLocationForPhyPartSpecByNewTgConfig(tblType,
                            schema,
                            logTbName,
                            phyTblNameSameAsLogicalTblName,
                            groupAllocator,
                            partLocality,
                            allPhyGrpList,
                            partitionPgGroupAllocators,
                            curPhyPartIndex,
                            curPhyPartSpec);
                    curPhyPartSpec.setLocation(phySpecLocation);
                }
            }
        } else {
            PartitionByDefinition partByDef = partitionInfo.getPartitionBy();
            PartitionByDefinition subPartByDef = partByDef.getSubPartitionBy();
            Map<String, PartitionSpec> nameToPhySpecMapping = partByDef.getPartNameToPhyPartMap();

            List<PartitionSpec> orderedPartSpecs = partByDef.getOrderedPartitionSpecs();
            partitionInfo.setTableGroupId(tableGroupConfig.getTableGroupRecord().getId());
            boolean usePartitionGroupName = false;
            if (partitionInfo.isGsiBroadcastOrBroadcast()) {
                usePartitionGroupName = broadCastPartitionGroupNameChange(partitionInfo, tableGroupConfig);
            }

            Integer allPhyPartCnt = 0;
            for (int i = 0; i < orderedPartSpecs.size(); i++) {
                final PartitionSpec partSpec = orderedPartSpecs.get(i);
                boolean isLogicalSpec = partSpec.isLogical();
                List<PartitionSpec> phySpecList = new ArrayList<>();
                if (isLogicalSpec) {
                    phySpecList = PartitionByDefinition.getOrderedPhyPartSpecsByPartStrategy(subPartByDef.getStrategy(),
                        subPartByDef.getBoundSpaceComparator(), partSpec.getSubPartitions());
                } else {
                    phySpecList.add(partSpec);
                }
                for (int j = 0; j < phySpecList.size(); j++) {
                    allPhyPartCnt++;
                    PartitionSpec curPhySpec = nameToPhySpecMapping.get(phySpecList.get(j).getName());
                    PartitionLocation phyPartLocation = genLocationForPhyPartSpecByTgConfig(
                        tableGroupConfig,
                        logTbName,
                        phyTblNameSameAsLogicalTblName,
                        usePartitionGroupName,
                        allPhyPartCnt - 1,
                        curPhySpec);
                    curPhySpec.setLocation(phyPartLocation);
                }
            }
        }
    }

    private static PartitionLocation genLocationForPhyPartSpecByTgConfig(TableGroupConfig tableGroupConfig,
                                                                         String logTbName,
                                                                         boolean phyTblNameSameAsLogicalTblName,
                                                                         boolean usePartitionGroupName,
                                                                         Integer phySpecIndex,
                                                                         PartitionSpec tarPhyPartSpec) {
        String partName = tarPhyPartSpec.getName();
        PartitionGroupRecord partitionGroupRecord = tableGroupConfig.getPartitionGroupRecords().get(phySpecIndex);
        if (!partitionGroupRecord.getPartition_name().equalsIgnoreCase(partName)
            && !usePartitionGroupName) {
            partitionGroupRecord = tableGroupConfig.getPartitionGroupRecords().stream()
                .filter(o -> o.getPartition_name().equalsIgnoreCase(partName)).findFirst().orElse(null);
        }
        if (usePartitionGroupName) {
            partitionGroupRecord = tableGroupConfig.getPartitionGroupRecords().get(phySpecIndex);
            tarPhyPartSpec.setName(partitionGroupRecord.getPartition_name());
        }
        PartitionLocation location = new PartitionLocation();
        if (partitionGroupRecord != null) {
            String partPhyTbName =
                PartitionNameUtil.autoBuildPartitionPhyTableName(logTbName, tarPhyPartSpec.getPhyPartPosition() - 1);
            if (phyTblNameSameAsLogicalTblName) {
                partPhyTbName = logTbName;
            }
            String grpKey = GroupInfoUtil.buildGroupNameFromPhysicalDb(partitionGroupRecord.getPhy_db());
            Long pgId = partitionGroupRecord.getId();
            location.setPhyTableName(partPhyTbName);
            location.setGroupKey(grpKey);
            location.setPartitionGroupId(pgId);
            return location;
        } else {
            throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_INVALID_PARAMS,
                String.format("Can NOT find the right partition group for the partition name[%s]", partName));
        }

    }

    private static PartitionLocation genLocationForPhyPartSpecByNewTgConfig(PartitionTableType tblType,
                                                                            String schema,
                                                                            String logTbName,
                                                                            boolean phyTblNameSameAsLogicalTblName,
                                                                            TableGroupLocation.GroupAllocator groupAllocator,
                                                                            LocalityDesc partLocality,
                                                                            List<String> allPhyGrpList,
                                                                            Map<String, TableGroupLocation.GroupAllocator> partitionPgGroupAllocators,
                                                                            Integer phyPartSpecIndex,
                                                                            PartitionSpec tarPhyPartSpec) {
        String partPhyTbName;
        if (phyTblNameSameAsLogicalTblName) {
            partPhyTbName = logTbName;
        } else {
            partPhyTbName =
                PartitionNameUtil.autoBuildPartitionPhyTableName(logTbName, tarPhyPartSpec.getPhyPartPosition() - 1);
        }

        String grpKey;
        if (tblType == PartitionTableType.BROADCAST_TABLE
            || tblType == PartitionTableType.GSI_BROADCAST_TABLE) {
            if (!ConfigDataMode.getMode().isMock()
                && !DbGroupInfoManager.isNormalGroup(schema, allPhyGrpList.get(phyPartSpecIndex))) {
                throw new TddlRuntimeException(ErrorCode.ERR_PHYSICAL_TOPOLOGY_CHANGING,
                    String.format("the physical group[%s] is changing, please retry this command later",
                        allPhyGrpList.get(phyPartSpecIndex)));
            }
            grpKey = allPhyGrpList.get(phyPartSpecIndex);
        } else if (TStringUtil.isNotEmpty(tarPhyPartSpec.getLocality())) {
            // case 3
            LocalityDesc locality = LocalityInfoUtils.parse(tarPhyPartSpec.getLocality());
            String localityStrVal = locality.toString();
            TableGroupLocation.GroupAllocator pgGroupAllocator;
            if (partitionPgGroupAllocators.containsKey(localityStrVal)) {
                pgGroupAllocator = partitionPgGroupAllocators.get(localityStrVal);
            } else {
                pgGroupAllocator = TableGroupLocation.buildGroupAllocatorOfPartitionByLocality(schema, locality);
                partitionPgGroupAllocators.put(localityStrVal, pgGroupAllocator);
            }
            grpKey = pgGroupAllocator.allocate();
        } else if (!partLocality.isEmpty()) {
            TableGroupLocation.GroupAllocator pgGroupAllocator;
            if (partitionPgGroupAllocators.containsKey(partLocality.toString())) {
                pgGroupAllocator = partitionPgGroupAllocators.get(partLocality.toString());
            } else {
                pgGroupAllocator = TableGroupLocation.buildGroupAllocatorOfPartitionByLocality(schema, partLocality);
                partitionPgGroupAllocators.put(partLocality.toString(), pgGroupAllocator);
            }
            grpKey = pgGroupAllocator.allocate();
        } else {
            grpKey = groupAllocator.allocate();
        }
        Long pgId = PartitionLocation.INVALID_PARTITION_GROUP_ID;

        PartitionLocation location = new PartitionLocation();
        location.setPhyTableName(partPhyTbName);
        location.setGroupKey(grpKey);
        location.setPartitionGroupId(pgId);

//        PartitionSpec originalPartSpec = partitionInfo.getPartitionBy().getPartitions().get(i);
//        if (!originalPartSpec.getName().equalsIgnoreCase(tarPhyPartSpec.getName())) {
//            originalPartSpec = partitionInfo.getPartitionBy().getPartitions().stream()
//                .filter(o -> o.getPosition().equals(tarPhyPartSpec.getPosition()))
//                .findFirst().orElse(null);
//        }
//        if (originalPartSpec == null) {
//            throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_INVALID_PARAMS,
//                String.format("Can NOT find the right partition group for the partition name[%s]", tarPhyPartSpec.getName()));
//        }

        return location;
    }

    /**
     * @return true if the the partitiongroup name for tg_broadcast is not p1/p2/.../pn
     */
    private static boolean broadCastPartitionGroupNameChange(PartitionInfo partitionInfo,
                                                             TableGroupConfig tableGroupConfig) {
        assert GeneralUtil.isNotEmpty(tableGroupConfig.getPartitionGroupRecords())
            && partitionInfo.isGsiBroadcastOrBroadcast();
        for (PartitionSpec partitionSpec : partitionInfo.getPartitionBy().getPartitions()) {
            PartitionGroupRecord partitionGroupRecord = tableGroupConfig.getPartitionGroupRecords().stream()
                .filter(o -> o.partition_name.equalsIgnoreCase(partitionSpec.getName())).findFirst().orElse(null);
            if (partitionGroupRecord == null) {
                return true;
            }
        }
        return false;
    }

    /**
     * for physical table name pattern please refer to
     */
    public static int getTablePostfixNumberForNewPartition(PartitionInfo partitionInfo) {
        PartitionByDefinition partitionBy = partitionInfo.getPartitionBy();
        int tableIndex = 0;
        List<String> physicalTableName = new ArrayList<>();
        for (PartitionSpec partitionSpec : partitionBy.getPartitions()) {
            final PartitionLocation location = partitionSpec.getLocation();
            if (location != null && location.isValidLocation()) {
                String physicalName = location.getPhyTableName();
                int index = extraPostfixNumberFromPhysicaTableName(physicalName);
                if (index > tableIndex) {
                    if (index > tableIndex) {
                        tableIndex = index;
                    }
                }
            } else {
                for (PartitionSpec subPartitionSpec : partitionSpec.getSubPartitions()) {
                    final PartitionLocation subLocation = subPartitionSpec.getLocation();

                    String physicalName = subLocation.getPhyTableName();
                    int index = extraPostfixNumberFromPhysicaTableName(physicalName);
                    if (index > tableIndex) {
                        if (index > tableIndex) {
                            tableIndex = index;
                        }
                    }
                }
            }
        }
        return tableIndex + 1;
    }

    public static void validateLocality(String schemaName, LocalityDesc locality, PartitionByDefinition partByDef) {
        Long dbId = DbInfoManager.getInstance().getDbInfo(schemaName).id;
        LocalityInfo dbLocalityInfo = LocalityManager.getInstance().getLocalityOfDb(dbId);
        LocalityDesc dbLocalityDesc = new LocalityDesc();
        if (dbLocalityInfo != null) {
            dbLocalityDesc = LocalityInfoUtils.parse(dbLocalityInfo.getLocality());
        }
        List<String> fullStorageList = new ArrayList<>();
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
            if (!dbLocalityDesc.fullCompactiableWith(locality) || !storageList.containsAll(locality.getDnList())) {
                throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                    " Table locality definition is not compatible with database locality! ");
            }
        }
        for (PartitionSpec partitionSpec : partByDef.getPartitions()) {
            LocalityDesc partitionLocality = LocalityInfoUtils.parse(partitionSpec.getLocality());
            if (!fullStorageList.containsAll(partitionLocality.getDnList())) {
                throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                    " Partition locality definition contains illegal storage ID: " + String.join(",",
                        partitionLocality.getDnList()));
            }
            if (!dbLocalityDesc.fullCompactiableWith(partitionLocality) || !storageList.containsAll(
                partitionLocality.getDnList())) {
                throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                    " Partition locality definition is not compatible with database locality! ");
            }
        }
    }

    public static void validatePartitionInfoForDdl(PartitionInfo partitionInfo, ExecutionContext ec) {

        PartitionTableType tblType = partitionInfo.getTableType();
        PartitionByDefinition partByDef = partitionInfo.getPartitionBy();
        List<PartitionSpec> partSpecs = partByDef.getPartitions();
        PartitionByDefinition subPartByDef = partitionInfo.getPartitionBy().getSubPartitionBy();

        /**
         * validate for 1st-level partition
         */
        validatePartitionInfoForDdlInner(tblType, partByDef, null, partSpecs, ec);

        /**
         * validate for 2nd-level partition
         */
        if (subPartByDef != null) {

            boolean useSubPartTemp = subPartByDef.isUseSubPartTemplate();
            if (useSubPartTemp) {
                /**
                 * validate templated subpartSpecs for 2nd-level partition
                 */
                List<PartitionSpec> subPartSpecTemps = subPartByDef.getPartitions();
                validatePartitionInfoForDdlInner(tblType, subPartByDef, null, subPartSpecTemps, ec);
            }

            /**
             * validate actual subpartSpecs for 2nd-level partition
             */
            for (int i = 0; i < partSpecs.size(); i++) {
                PartitionSpec parentSpec = partSpecs.get(i);
                List<PartitionSpec> subPartSpecs = parentSpec.getSubPartitions();
                validatePartitionInfoForDdlInner(tblType, subPartByDef, parentSpec, subPartSpecs, ec);
            }

            /**
             * Validate all the names of physical part spec
             */
            validatePartitionNames(partByDef.getPhysicalPartitions(), partSpecs);
        }
    }

    private static void validatePartitionInfoForDdlInner(PartitionTableType tblType,
                                                         PartitionByDefinition partByDef,
                                                         PartitionSpec parentSpec,
                                                         List<PartitionSpec> partSpecs,
                                                         ExecutionContext ec) {

        if (tblType != PartitionTableType.PARTITION_TABLE && tblType != PartitionTableType.GSI_TABLE) {
            return;
        }

        // validate partition columns data type
        validatePartitionColumns(partByDef);

        // validate partition name
        validatePartitionNames(partSpecs, parentSpec == null ? Arrays.asList() : Arrays.asList(parentSpec));

        // validate partition count
        validatePartitionCount(partSpecs, partByDef, ec);

        // validate bound values expressions
        validatePartitionSpecBounds(tblType, partByDef, partSpecs, ec);

    }

    private static boolean checkIfNullLiteral(RexNode bndExpr) {
        RexLiteral literal = (RexLiteral) bndExpr;
        if (literal.getType().getSqlTypeName().getName().equalsIgnoreCase("NULL") && literal.getValue() == null) {
            return true;
        }
        return false;
    }

    public static void validateBoundValueExpr(RexNode bndExpr, RelDataType bndColRelDataType,
                                              PartitionIntFunction partIntFunc, PartitionStrategy strategy) {

        DataType bndColDataType = DataTypeUtil.calciteToDrdsType(bndColRelDataType);
        switch (strategy) {
        case HASH:
        case LIST: {
            //specially for "list default"
            if (strategy.equals(LIST) && bndExpr instanceof RexCall) {
                RexCall bndExprCall = (RexCall) bndExpr;
                SqlOperator exprOp = bndExprCall.getOperator();
                if (exprOp.kind == SqlKind.DEFAULT) {
                    //validate list default
                    break;
                }
            }
        }
        case RANGE: {

            if (bndExpr instanceof RexCall) {
                RexCall bndExprCall = (RexCall) bndExpr;
                SqlOperator exprOp = bndExprCall.getOperator();
                if (partIntFunc != null) {
                    // Use partIntFunc
                    if (exprOp != partIntFunc.getSqlOperator()) {
                        throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_INVALID_PARAMS,
                            String.format("Partition column values use a invalid function %s", exprOp.getName()));
                    }

                } else {

                    // No use partIntFunc

                    throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_INVALID_PARAMS,
                        String.format("Partition column values use a invalid function %s", exprOp.getName()));
                }

            } else {

                if (!(bndExpr instanceof RexLiteral)) {
                    throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_INVALID_PARAMS,
                        String.format("Partition column values of incorrect expression"));
                } else {
                    if (checkIfNullLiteral(bndExpr) && strategy == PartitionStrategy.RANGE) {
                        throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_INVALID_PARAMS,
                            String.format(" Not allowed to use NULL value in VALUES LESS THAN"));
                    }
                }
            }

        }
        break;
        case KEY:
        case RANGE_COLUMNS:
        case LIST_COLUMNS: {

            if (bndExpr instanceof RexCall) {
                RexCall bndExprCall = (RexCall) bndExpr;
                SqlOperator exprOp = bndExprCall.getOperator();

                if (exprOp.equals(TddlOperatorTable.UNIX_TIMESTAMP)) {
                    if (!DataTypeUtil.equalsSemantically(bndColDataType, DataTypes.TimestampType)) {
                        throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_INVALID_PARAMS, String.format(
                            "Partition column values use a invalid function %s, only UNIX_TIMESTAMP allowed",
                            exprOp.getName()));
                    }
                } else {
                    if (!DataTypeUtil.equalsSemantically(bndColDataType, DataTypes.TimestampType)) {
                        throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_INVALID_PARAMS,
                            String.format("Partition column values are NOT support to use a function %s",
                                exprOp.getName()));
                    }
                }

            } else {

                if (!(bndExpr instanceof RexLiteral)) {
                    throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_INVALID_PARAMS,
                        String.format("Partition column values of incorrect expression"));
                } else {
                    if (checkIfNullLiteral(bndExpr) && strategy == PartitionStrategy.RANGE_COLUMNS) {
                        throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_INVALID_PARAMS,
                            String.format(" Not allowed to use NULL value in VALUES LESS THAN"));
                    }
                }
            }
        }

        break;
        default:
            break;
        }
    }

    protected boolean useFullPartColCnt(int prefixColCnt) {
        return prefixColCnt == FULL_PART_COL_COUNT;
    }

    protected static void validatePartitionValueFormats(PartitionStrategy partStrategy,
                                                        int partColCnt,
                                                        int prefixPartCol,
                                                        String partName,
                                                        SqlPartitionValue value) {

        SqlPartitionValue.Operator operator = value.getOperator();
        List<SqlPartitionValueItem> itemsOfVal = value.getItems();
        switch (partStrategy) {
        case HASH: {
            if (itemsOfVal.size() != 1 && (prefixPartCol > 0
                || prefixPartCol == PartitionInfoUtil.FULL_PART_COL_COUNT)) {
                throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_INVALID_PARAMS, String.format(
                    "only one column count is allowed in the definition of the bound value of partition[%s] in hash strategy",
                    partName));
            }
        }
        break;
        case KEY: {
            if (prefixPartCol == PartitionInfoUtil.FULL_PART_COL_COUNT) {

                if (itemsOfVal.size() != partColCnt) {
                    throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_INVALID_PARAMS,
                        String.format("the bound value of partition[%s] must match the partition columns", partName));
                }
            } else {
                if (itemsOfVal.size() != prefixPartCol) {
                    throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_INVALID_PARAMS,
                        String.format("the bound value of partition[%s] must match the partition columns", partName));
                }
            }
        }
        break;

        case RANGE:
        case RANGE_COLUMNS: {
            int newPrefixColCnt = itemsOfVal.size();
            if (operator != SqlPartitionValue.Operator.LessThan) {
                throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_INVALID_PARAMS,
                    String.format("RANGE PARTITIONING can only use VALUES LESS THAN in partition definition"));
            }

            /**
             * For RANGE_COLUMNS
             *
             * each SqlPartitionValueItem of itemsOfVal is SqlLiteral,
             *
             * so the count of SqlPartitionValueItem should be the same as partition column count
             */
            if (prefixPartCol == PartitionInfoUtil.FULL_PART_COL_COUNT) {
                if (newPrefixColCnt != partColCnt) {
                    throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_INVALID_PARAMS,
                        String.format("the bound value of partition[%s] must match the partition columns", partName));
                }
            } else {
                if (prefixPartCol > partColCnt) {
                    throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_INVALID_PARAMS, String.format(
                        "the prefix partition column count [%s] is not allowed to be more than the partition columns count [%s] ",
                        prefixPartCol, partColCnt));
                }

                if (newPrefixColCnt != prefixPartCol) {
                    throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_INVALID_PARAMS, String.format(
                        "the column count[%s] of the bound value of partition[%s] must match the actual partition column count[%s]",
                        newPrefixColCnt, partName, newPrefixColCnt));
                }
            }

            if (partColCnt > 1) {
                /**
                 * only RANGE_COLUMNS may exists multi-partition-columns
                 */
                boolean containInvalidVal = false;
                for (int i = 0; i < newPrefixColCnt; i++) {
                    SqlNode valExpr = itemsOfVal.get(i).getValue();
                    if (!(valExpr instanceof SqlLiteral)) {
                        if (valExpr instanceof SqlIdentifier) {
                            String valStr = ((SqlIdentifier) valExpr).getLastName();
                            if (valStr.equalsIgnoreCase(PartitionInfoUtil.MAXVALUE)) {
                                continue;
                            }
                        }
                        containInvalidVal = true;
                        break;
                    }
                }
                if (containInvalidVal) {
                    throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_INVALID_PARAMS,
                        String.format("the bound value of partition[%s] must match the partition columns", partName));
                }
            }
        }
        break;
        case LIST: {

            if (operator != SqlPartitionValue.Operator.In) {
                throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_INVALID_PARAMS,
                    String.format("LIST PARTITIONING can only use VALUES IN in partition definition"));
            }

            /**
             * For LIST
             *
             * each SqlPartitionValueItem of itemsOfVal is SqlLiteral or SqlCall
             *
             */
            if (itemsOfVal.isEmpty()) {
                throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_INVALID_PARAMS,
                    String.format("the bound value of partition[%s] must be not empty", partName));
            }
            int listValCnt = itemsOfVal.size();
            for (int i = 0; i < listValCnt; i++) {
                SqlPartitionValueItem val = itemsOfVal.get(i);
                SqlNode expr = val.getValue();
                if (expr instanceof SqlCall) {
                    SqlCall call = (SqlCall) expr;
                    if (call.getKind() == SqlKind.ROW) {
                        if (call.getOperandList().size() != partColCnt) {
                            throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_INVALID_PARAMS,
                                String.format("the bound value of partition[%s] must match the partition columns",
                                    partName));
                        }
                    } else if (call.getKind() == SqlKind.DEFAULT) {
                        /**
                         * validate 'default' for List Partition
                         * */
                        if (listValCnt > 1) {
                            throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_INVALID_PARAMS,
                                String.format("Default partition[%s] definition error", partName));
                        }

                    }
                } else if (expr instanceof SqlIdentifier) {
                    String valStr = ((SqlIdentifier) expr).getLastName();
                    if (valStr.equalsIgnoreCase(PartitionInfoUtil.MAXVALUE)) {
                        throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_INVALID_PARAMS,
                            String.format("Cannot use MAXVALUE as value in VALUES IN near 'maxvalue'"));
                    }
                }
            }

        }
        break;

        case LIST_COLUMNS: {

            if (operator != SqlPartitionValue.Operator.In) {
                throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_INVALID_PARAMS,
                    String.format("LIST PARTITIONING can only use VALUES IN in partition definition"));
            }

            /**
             * For LIST_COLUMNS
             *
             * each SqlPartitionValueItem of itemsOfVal is SqlCall (ROW expr or Default expr),
             *
             * so the count of SqlPartitionValueItem should be more than one,
             *
             * and all the expr of SqlCall should be sqlLiteral
             *
             */
            if (itemsOfVal.isEmpty()) {
                throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_INVALID_PARAMS,
                    String.format("the bound value of partition[%s] must be not empty", partName));
            }

            int listValCnt = itemsOfVal.size();
            boolean containInvalidVal = false;
            SqlNode invalidValAst = null;
            for (int i = 0; i < listValCnt; i++) {
                SqlPartitionValueItem val = itemsOfVal.get(i);
                SqlNode expr = val.getValue();
                if (partColCnt > 1) {
                    /**
                     * when partCnt > 1, each of SqlPartitionValueItem can be a row expr or a default expr
                     */
                    if (expr instanceof SqlCall) {
                        SqlCall rowExprCall = (SqlCall) expr;
                        if (rowExprCall.getKind() == SqlKind.ROW && rowExprCall.getOperandList().size() == partColCnt) {
                            for (int j = 0; j < rowExprCall.getOperandList().size(); j++) {
                                SqlNode e = rowExprCall.getOperandList().get(j);
                                if (!(e instanceof SqlLiteral)) {
                                    containInvalidVal = true;
                                    invalidValAst = rowExprCall;
                                }
                            }
                            continue;
                        } else if (rowExprCall.getKind() == SqlKind.DEFAULT) {
                            /**
                             * default partition only allows one default expr
                             * */
                            if (listValCnt > 1) {
                                throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_INVALID_PARAMS,
                                    String.format("Default partition[%s] definition error", partName));
                            }
                        } else {

                            if (expr instanceof SqlIdentifier) {
                                String valStr = ((SqlIdentifier) expr).getLastName();
                                if (valStr.equalsIgnoreCase(PartitionInfoUtil.MAXVALUE)) {
                                    throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_INVALID_PARAMS,
                                        String.format("Cannot use MAXVALUE as value in VALUES IN near 'maxvalue'"));
                                }
                            }

                            containInvalidVal = true;
                            invalidValAst = rowExprCall;
                            break;
                        }

                    } else {
                        containInvalidVal = true;
                        invalidValAst = expr;
                        break;
                    }
                } else {
                    /**
                     * when partCnt = 1, each of SqlPartitionValueItem is a SqlLiteral or default expr
                     */
                    if (!(expr instanceof SqlLiteral) && !(expr instanceof SqlBasicCall
                        && expr.getKind() == SqlKind.DEFAULT)) {
                        containInvalidVal = true;
                        invalidValAst = expr;
                        break;
                    }
                }

            }
            if (containInvalidVal) {
                throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_INVALID_PARAMS,
                    String.format("the bound value[%s] of partition[%s] must match the partition columns",
                        invalidValAst.toString(), partName));
            }
        }
        break;
        default:
            break;
        }

    }

    protected static void validatePartitionColumns(PartitionByDefinition partitionBy) {
        PartitionStrategy realStrategy = partitionBy.getStrategy();
        int partCol = partitionBy.getPartitionColumnNameList().size();
        List<ColumnMeta> partColMetas = partitionBy.getPartitionFieldList();
        SqlOperator partIntOp = partitionBy.getPartIntFuncOperator();
        List<SqlNode> partExprs = partitionBy.getPartitionExprList();

        /**
         * Check if partBy contain duplicated partition columns
         */
        TreeSet<String> colSet = new TreeSet<>(CaseInsensitive.CASE_INSENSITIVE_ORDER);
        List<String> partCols = partitionBy.getPartitionColumnNameList();
        for (int i = 0; i < partCols.size(); i++) {
            String col = partCols.get(i);
            if (!colSet.contains(col)) {
                colSet.add(col);
            } else {
                throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_INVALID_PARAMS,
                    String.format("Not allowed to use a duplicated partition columns[%s] in partition ", col));
            }
        }

        PartitionStrategy checkStrategy = realStrategy;
        if (checkStrategy == PartitionStrategy.HASH && partIntOp == null) {
            /**
             * The validation of columns of "Partition By Hash(col)" or "Partition By Hash(col1,col2,...,colN)"
             * should be the same of 
             * "Partition By Key(col)" or "Partition By Key(col1,col2,...,colN)"
             */
            checkStrategy = PartitionStrategy.KEY;
        }

        for (int i = 0; i < partExprs.size(); i++) {
            SqlNode partExpr = partExprs.get(i);
            SqlCreateTable.PartitionColumnFinder columnFinder = new SqlCreateTable.PartitionColumnFinder();
            partExpr.accept(columnFinder);
            if (columnFinder.getPartColumn() == null) {
                throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_INVALID_PARAMS,
                    String.format("Not allowed to use unknown column[%s] as partition column", partExpr.toString()));
            } else {
                if (checkStrategy == PartitionStrategy.KEY || checkStrategy == PartitionStrategy.RANGE_COLUMNS
                    || checkStrategy == PartitionStrategy.LIST_COLUMNS) {
                    if (columnFinder.isContainPartFunc()) {
                        throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_INVALID_PARAMS, String.format(
                            "Not allowed to use partition column[%s] with partition function in key or range/list columns policy",
                            partExpr.toString()));
                    }
                } else {
                    if (columnFinder.isUseNestingPartFunc()) {
                        throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_INVALID_PARAMS, String.format(
                            "Not allowed to use nesting partition function [%s] in hash/range/list policy",
                            partExpr.toString()));
                    }
                }
            }
        }

        if (checkStrategy == PartitionStrategy.RANGE
            || checkStrategy == LIST
            || checkStrategy == PartitionStrategy.HASH) {

            if (checkStrategy == PartitionStrategy.HASH) {
                PartitionIntFunction partIntFunc = partitionBy.getPartIntFunc();
                if (partCol != 1 && partIntFunc != null) {
                    throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_INVALID_PARAMS, String.format(
                        "Not supported to use more than one partition columns on hash strategy with partition functions"));
                }
            } else {
                if (partCol != 1) {
                    throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_INVALID_PARAMS,
                        String.format("Not supported to use more than one partition columns on this strategy"));
                }
            }

            ColumnMeta partFldCm = partitionBy.getPartitionFieldList().get(0);
            DataType partFldDt = partFldCm.getField().getDataType();

            if (partIntOp == null) {
                if (!DataTypeUtil.isUnderBigintType(partFldDt)) {
                    throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_INVALID_PARAMS,
                        String.format("Not supported to use the dataType[%s] as partition columns on this strategy[%s]",
                            partFldDt.getStringSqlType(), checkStrategy.toString()));
                }
            } else {
                String partFuncName = partIntOp.getName();
                SqlNode partFuncCall = partitionBy.getPartitionExprList().get(0);
                List<DataType> partColDataTypes = new ArrayList<>();
                partColDataTypes.add(partFldDt);
                boolean isBuildInPartFunc = PartitionFunctionBuilder.isBuildInPartFuncName(partFuncName);
                if (!isBuildInPartFunc) {
                    /**
                     * Check if the user-defined function is allowed using as partition function
                     */
                    PartitionFunctionBuilder.validatePartitionFunction(partFuncName);
                }

                if (isBuildInPartFunc) {
                    if (DataTypeUtil.anyMatchSemantically(partFldDt, DataTypes.TimestampType, DataTypes.TimeType)) {
                        if (partIntOp != TddlOperatorTable.UNIX_TIMESTAMP) {
                            throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_INVALID_PARAMS, String.format(
                                "Constant, random or timezone-dependent expressions in (sub)partitioning function are not allowed"));
                        }
                    } else if ((DataTypeUtil.anyMatchSemantically(partFldDt, DataTypes.DatetimeType,
                        DataTypes.DateType))) {
                        if (partIntOp == TddlOperatorTable.UNIX_TIMESTAMP) {
                            throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_INVALID_PARAMS, String.format(
                                "Constant, random or timezone-dependent expressions in (sub)partitioning function are not allowed"));
                        }
                    } else if ((DataTypeUtil.anyMatchSemantically(partFldDt, DataTypes.VarcharType,
                        DataTypes.CharType))) {
                        if (!(partIntOp instanceof SqlSubStrFunction)) {
                            throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_INVALID_PARAMS, String.format(
                                "Constant, random or timezone-dependent expressions in (sub)partitioning function are not allowed"));
                        }
                    } else if ((DataTypeUtil.anyMatchSemantically(partFldDt, DataTypes.LongType))) {
                        // Ignore
                    } else {
                        throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_INVALID_PARAMS,
                            String.format("The data type %s are not allowed", partFldDt.getStringSqlType()));
                    }
                } else {

                    /**
                     * Check if the partition column datatypes are allowed using as input of user-defined partition function
                     */
                    PartitionFunctionBuilder.checkIfPartColDataTypesAllowedUsingOnUdfPartitionFunction(partColMetas);

                    /**
                     * Check if the dataTypes of partition column match the input datatypes of user-defined function
                     */
                    PartitionFunctionBuilder.checkIfPartColDataTypesMatchUdfInputDataTypes(partFuncCall,
                        partColDataTypes);
                }
            }

        } else if (checkStrategy == PartitionStrategy.KEY || checkStrategy == PartitionStrategy.RANGE_COLUMNS
            || checkStrategy == PartitionStrategy.LIST_COLUMNS) {

            if (partIntOp != null) {
                throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_INVALID_PARAMS,
                    String.format("Partitioning function are not allowed"));
            }

            if (partCol == 0) {
                throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_INVALID_PARAMS,
                    String.format("empty partition columns are not allowed"));
            }

            List<ColumnMeta> partFldMetaList = partitionBy.getPartitionFieldList();
            for (int i = 0; i < partFldMetaList.size(); i++) {
                ColumnMeta partColMeta = partFldMetaList.get(i);
                DataType fldDataType = partColMeta.getDataType();
                if (!PartitionInfoBuilder.isSupportedPartitionDataType(fldDataType)) {
                    throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_INVALID_PARAMS,
                        String.format("This data type %s are not allowed", fldDataType.getStringSqlType()));
                }

                int fldSqlType = fldDataType.getSqlType();
                if (fldSqlType == Types.BINARY || ((fldSqlType == Types.CHAR || fldSqlType == Types.VARCHAR)
                    && fldDataType.getCollationName() == CollationName.BINARY)) {
                    if (checkStrategy == PartitionStrategy.RANGE_COLUMNS
                        || checkStrategy == PartitionStrategy.LIST_COLUMNS) {
                        throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_INVALID_PARAMS,
                            String.format(
                                "This data type 'CHARACTER SET %s COLLATE %s' are not allowed in range/list partition strategy",
                                fldDataType.getStringSqlType(), CollationName.BINARY.name()));
                    }
                }

                if (DataTypeUtil.anyMatchSemantically(fldDataType, DataTypes.TimestampType, DataTypes.TimeType)) {
                    if (checkStrategy == PartitionStrategy.RANGE_COLUMNS
                        || checkStrategy == PartitionStrategy.LIST_COLUMNS) {
                        throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_INVALID_PARAMS,
                            String.format("Field '%s' is of a not allowed type[%s] for this type of partitioning",
                                partColMeta.getName(), partColMeta.getDataType().getStringSqlType()));
                    }
                }

                if (DataTypeUtil.anyMatchSemantically(fldDataType, DataTypes.DecimalType)) {
                    int precision = fldDataType.getPrecision();
                    int scale = fldDataType.getScale();
                    if (scale > 0 && precision > 0) {
                        if (checkStrategy == PartitionStrategy.RANGE_COLUMNS
                            || checkStrategy == PartitionStrategy.LIST_COLUMNS) {
                            throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_INVALID_PARAMS,
                                String.format(
                                    "Field '%s' with scale > 0 is of a not allowed type[%s] for this type of partitioning",
                                    partColMeta.getName(), partColMeta.getDataType().getStringSqlType()));
                        }
                    }
                }

                if (fldSqlType == Types.CHAR || fldSqlType == Types.VARCHAR) {
                    DataType dataType = partColMeta.getField().getDataType();
                    CollationName collationOfType = dataType.getCollationName();
                    CharsetName charsetOfType = dataType.getCharsetName();
                    try {
                        String targetCharset = charsetOfType.name();
                        String targetCollate = collationOfType.name();
                        if (!MySQLCharsetDDLValidator.checkCharsetSupported(targetCharset, targetCollate)) {
                            throw new TddlRuntimeException(ErrorCode.ERR_GMS_GENERIC, String.format(
                                "Failed to create table because the charset[%s] or collate[%s] is not supported in partitioning",
                                targetCharset, targetCollate));
                        }

                    } catch (Throwable ex) {
                        throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_INVALID_PARAMS,
                            String.format("Field '%s' is of a not allowed type[%s] for this type of partitioning",
                                partColMeta.getName(), partColMeta.getDataType().getStringSqlType()));
                    }
                }
            }
        } else if (checkStrategy == PartitionStrategy.UDF_HASH) {
            // do partition column validation

            if (partIntOp != null) {
                boolean isBuildInPartFunc = PartitionFunctionBuilder.isBuildInPartFuncName(partIntOp.getName());
                if (isBuildInPartFunc) {
                    throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_INVALID_PARAMS,
                        String.format(
                            "It is not allowed to used bulit-in partition function[%s] on this partition strategy[%s]",
                            partIntOp.getName(), PartitionStrategy.UDF_HASH.getStrategyExplainName()));
                }

                PartitionFunctionBuilder.checkIfUdfAllowedUsingAsPartFunc(partIntOp.getName());

                /**
                 * Check if the partition column datatypes are allowed using as input of user-defined partition function
                 */
                PartitionFunctionBuilder.checkIfPartColDataTypesAllowedUsingOnUdfPartitionFunction(partColMetas);
            } else {
//                throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_INVALID_PARAMS,
//                    String.format(
//                        "It is not allowed to use partition strategy[%s] without any user-defined partition functions",
//                        PartitionStrategy.UDF_HASH.getStrategyExplainName()));

                if (partColMetas.size() != 1) {
                    throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_INVALID_PARAMS,
                        String.format(
                            "Only one partition column is allowed on partition strategy[%s]",
                            PartitionStrategy.UDF_HASH.getStrategyExplainName()));
                }
            }

        }
    }

    protected static void validatePartitionNames(List<PartitionSpec> partSpecs, List<PartitionSpec> allParentSpecs) {
        Set<String> partNameSet = new TreeSet<>(CaseInsensitive.CASE_INSENSITIVE_ORDER);
        if (allParentSpecs != null && !allParentSpecs.isEmpty()) {
            for (int i = 0; i < allParentSpecs.size(); i++) {
                partNameSet.add(allParentSpecs.get(i).getName());
            }
        }

        List<PartitionSpec> pSpecList = partSpecs;
        for (int i = 0; i < pSpecList.size(); i++) {
            PartitionSpec pSpec = pSpecList.get(i);
            String name = pSpec.getName();
            if (!partNameSet.contains(name)) {
                partNameSet.add(name);
            } else {
                throw new TddlRuntimeException(ErrorCode.ERR_DUPLICATED_PARTITION_NAME,
                    "partition name:" + name + " is duplicated");
            }
            PartKeyLevel partLevel = pSpec.getPartLevel();
            // If validate error , "PartitionNameUtil.validatePartName" will throw exceptions.
            PartitionNameUtil.validatePartName(name, false, partLevel == PartKeyLevel.SUBPARTITION_KEY);
        }
    }

    protected static void validatePartitionCount(List<PartitionSpec> partSpecs,
                                                 PartitionByDefinition partByDef,
                                                 ExecutionContext ec) {
        Integer maxPhysicalPartitions = Integer.valueOf(ConnectionParams.MAX_PHYSICAL_PARTITION_COUNT.getDefault());
        if (ec != null) {
            maxPhysicalPartitions = ec.getParamManager().getInt(ConnectionParams.MAX_PHYSICAL_PARTITION_COUNT);
        }

        if (partSpecs.size() > maxPhysicalPartitions) {
            throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_INVALID_PARAMS,
                String.format("Too many partitions [%s] (including subpartitions) is not allowed",
                    partSpecs.size()));
        }

        if (partByDef.getSubPartitionBy() != null && partByDef.getPartLevel() == PartKeyLevel.PARTITION_KEY) {
            /**
             * Use subpart
             */
            List<PartitionSpec> allPhyPartSpecs = partByDef.getPhysicalPartitions();
            int allPhyPartSize = allPhyPartSpecs.size();
            if (allPhyPartSize > maxPhysicalPartitions) {
                throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_INVALID_PARAMS,
                    String.format("Too many partitions [%s] (including subpartitions) is not allowed",
                        allPhyPartSize));
            }
        }
    }

    protected static void validatePartitionSpecBounds(PartitionTableType tblType,
                                                      PartitionByDefinition partByDef,
                                                      List<PartitionSpec> partSpecs,
                                                      ExecutionContext ec) {

        PartitionStrategy strategy = partByDef.getStrategy();

        int partColCnt = partByDef.getPartitionFieldList().size();
        if (strategy == PartitionStrategy.RANGE || strategy == PartitionStrategy.RANGE_COLUMNS
            || strategy == PartitionStrategy.HASH || strategy == PartitionStrategy.KEY
            || strategy == PartitionStrategy.UDF_HASH) {

            List<PartitionSpec> partSpecList = partByDef.getPartitions();
            for (int i = 0; i < partSpecList.size(); i++) {
                String partName = partSpecList.get(i).getName();
                SingleValuePartitionBoundSpec pSpec =
                    (SingleValuePartitionBoundSpec) partSpecList.get(i).getBoundSpec();
                int valLen = pSpec.getSingleDatum().getDatumInfo().length;
                if (strategy == PartitionStrategy.RANGE || strategy == PartitionStrategy.RANGE_COLUMNS
                    || strategy == PartitionStrategy.KEY) {
                    if (partColCnt != valLen) {
                        throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_INVALID_PARAMS,
                            String.format("the bound value[%s] of partition[%s] must match the partition columns",
                                pSpec, partName));
                    }
                }
            }

            //===== validate Range/RangeColumns/Key/Hash =====
            List<PartitionSpec> sortedPartSpecs =
                PartitionByDefinition.getOrderedPhyPartSpecsByPartStrategy(partByDef.getStrategy(),
                    partByDef.getBoundSpaceComparator(), partSpecs);
            List<PartitionSpec> sortedPartSpecList = sortedPartSpecs;
            PartitionSpec lastPartSpec = null;
            for (int i = 0; i < sortedPartSpecList.size(); i++) {
                PartitionSpec partSpec = sortedPartSpecList.get(i);
                if (i <= 0) {
                    continue;
                } else {
                    lastPartSpec = sortedPartSpecList.get(i - 1);
                }

                // Validate if boundInfos exists null values
                if (partSpec.getBoundSpec().containNullValues()) {
                    throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_INVALID_PARAMS,
                        String.format("the bound of partition[%s] should NOT contains null values",
                            partSpec.getName()));
                }

                // Validate if boundInfos are in ascending order
                if (partSpec.getPosition() <= lastPartSpec.getPosition()) {
                    throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_INVALID_PARAMS, String.format(
                        "the position of partition[%s] should be larger than the position of partition[%s]",
                        partSpec.getName(), lastPartSpec.getName()));

                } else {
                    SearchDatumComparator boundCmp = partByDef.getBoundSpaceComparator();
                    int cmpRs = boundCmp.compare(partSpec.getBoundSpec().getSingleDatum(),
                        lastPartSpec.getBoundSpec().getSingleDatum());
                    if (cmpRs < 1 && !(tblType == PartitionTableType.BROADCAST_TABLE)) {
                        // partSpec < lastPartSpec
                        throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_INVALID_PARAMS,
                            String.format("the bound of partition[%s] should be larger than the bound of partition[%s]",
                                partSpec.getName(), lastPartSpec.getName()));
                    }
                }
            }

            if (strategy == PartitionStrategy.UDF_HASH) {
                if (sortedPartSpecList.isEmpty()) {
                    throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_INVALID_PARAMS,
                        String.format("Specifying no partition bound values on partition strategy[%s] is not allowed",
                            strategy));
                }
            }
        } else if (strategy == LIST || strategy == PartitionStrategy.LIST_COLUMNS) {
            //===== validate List/ListColumns =====

            List<PartitionSpec> partSpecList = partSpecs;
            for (int i = 0; i < partSpecList.size(); i++) {
                String partName = partSpecList.get(i).getName();
                MultiValuePartitionBoundSpec pSpec = (MultiValuePartitionBoundSpec) partSpecList.get(i).getBoundSpec();

                List<SearchDatumInfo> datums = pSpec.getMultiDatums();
                for (int j = 0; j < datums.size(); j++) {
                    SearchDatumInfo datum = datums.get(j);
                    int valLen = datum.getDatumInfo().length;
                    if (partColCnt != valLen && !pSpec.isDefault()) {
                        throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_INVALID_PARAMS,
                            String.format("the bound value[%s] of partition[%s] must match the partition columns",
                                pSpec, partName));
                    }
                }
            }

            List<PartitionSpec> sortedPartSpecs =
                PartitionByDefinition.getOrderedPhyPartSpecsByPartStrategy(partByDef.getStrategy(),
                    partByDef.getBoundSpaceComparator(), partSpecs);
            List<PartitionSpec> sortedPartSpecList = sortedPartSpecs;
            for (int i = 0; i < sortedPartSpecList.size(); i++) {
                PartitionSpec partSpec = sortedPartSpecList.get(i);

                if (partSpec.getBoundSpec().containMaxValues()) {
                    throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_INVALID_PARAMS,
                        String.format("the bound value of 'maxvalue' of of partition[%s] are NOT allowed",
                            partSpec.getName()));
                }

                // Validate if boundInfos exists max values
                /*if (partSpec.getBoundSpec().isDefaultPartSpec()) {
                    throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_INVALID_PARAMS,
                        String.format("the partition[%s] is NOT support to be defined as default partition",
                            partSpec.getName()));
                }*/

            }
        } else {
            throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_INVALID_PARAMS, "Unsupported partition strategy");
        }
    }

    public static void validateAddPartition(PartitionInfo partitionInfo,
                                            List<PartitionSpec> existPartitionSpecs,
                                            PartitionSpec newPartitionSpec) {
        validateAddPartition(partitionInfo, existPartitionSpecs, newPartitionSpec, PARTITION_LEVEL_PARTITION);
    }

    public static void validateAddSubPartitions(PartitionInfo partitionInfo,
                                                List<PartitionSpec> existingSubPartitionSpecs,
                                                List<PartitionSpec> newSubPartitionSpecs) {
        for (PartitionSpec newSubPartitionSpec : newSubPartitionSpecs) {
            validateAddPartition(partitionInfo, existingSubPartitionSpecs, newSubPartitionSpec,
                PARTITION_LEVEL_SUBPARTITION);
            existingSubPartitionSpecs.add(newSubPartitionSpec);
        }
    }

    public static void validateAddPartition(PartitionInfo partitionInfo,
                                            List<PartitionSpec> existingPartitionSpecs,
                                            PartitionSpec newPartitionSpec,
                                            int partitionLevel) {
        PartitionStrategy partitionStrategy = existingPartitionSpecs.get(0).getStrategy();
        if (!newPartitionSpec.getStrategy().equals(partitionStrategy)) {
            throw new NotSupportException(
                "add " + newPartitionSpec.getStrategy() + " to " + partitionStrategy + " partition table");
        }
        String newPartitionName = newPartitionSpec.getName();

        switch (partitionStrategy) {
        case RANGE:
        case RANGE_COLUMNS: {
            SearchDatumComparator comparator = partitionInfo.getPartitionBy().getPruningSpaceComparator();
            if (partitionLevel == PARTITION_LEVEL_SUBPARTITION) {
                comparator = partitionInfo.getPartitionBy().getSubPartitionBy().getPruningSpaceComparator();
            }
            SearchDatumInfo newDatumInfo = newPartitionSpec.getBoundSpec().getSingleDatum();

            for (int i = 0; i < existingPartitionSpecs.size(); i++) {
                PartitionSpec partitionSpec = existingPartitionSpecs.get(i);
                if (partitionSpec.getName().equalsIgnoreCase(newPartitionName)) {
                    throw new TddlRuntimeException(ErrorCode.ERR_DUPLICATED_PARTITION_NAME,
                        "partition name:" + newPartitionName + " already exists");
                }
                if (partitionSpec.isMaxValueRange()) {
                    throw new TddlRuntimeException(ErrorCode.ERR_ADD_PARTITION,
                        "can't add partition for table which contains the maxValue bound partition");
                }

                if (comparator.compare(partitionSpec.getBoundSpec().getSingleDatum(), newDatumInfo) >= 0) {
                    throw new TddlRuntimeException(ErrorCode.ERR_ADD_PARTITION,
                        String.format("the new partition value[%s] must greater than other partitions",
                            newDatumInfo.toString()));
                }
            }
        }
        break;
        case LIST:
        case LIST_COLUMNS: {
            SearchDatumComparator cmp = partitionInfo.getPartitionBy().getPruningSpaceComparator();
            if (partitionLevel == PARTITION_LEVEL_SUBPARTITION) {
                cmp = partitionInfo.getPartitionBy().getSubPartitionBy().getPruningSpaceComparator();
            }
            Set<SearchDatumInfo> newPartValSet = new TreeSet<>(cmp);
            newPartValSet.addAll(newPartitionSpec.getBoundSpec().getMultiDatums());

            for (PartitionSpec partitionSpec : existingPartitionSpecs) {
                if (partitionSpec.getName().equalsIgnoreCase(newPartitionName)) {
                    throw new TddlRuntimeException(ErrorCode.ERR_DUPLICATED_PARTITION_NAME,
                        "partition name:" + newPartitionName + " already exists");
                }
                if (partitionSpec.isMaxValueRange()) {
                    throw new TddlRuntimeException(ErrorCode.ERR_ADD_PARTITION,
                        "can't add partition for table which contains the maxValue bound partition");
                }

                for (SearchDatumInfo datum : partitionSpec.getBoundSpec().getMultiDatums()) {
                    if (newPartValSet.contains(datum)) {
                        throw new TddlRuntimeException(ErrorCode.ERR_ADD_PARTITION,
                            "can't add partition for table which contains the duplicate values");
                    }
                }
            }
        }

        break;
        default:
            throw new NotSupportException("add partition for " + partitionStrategy.toString() + " type");
        }
    }

    public static void validateDropPartition(List<PartitionSpec> existPartitionSpecs,
                                             SqlAlterTableDropPartition sqlDropPartition) {
        for (SqlNode part : sqlDropPartition.getPartitionNames()) {
            String partitionName = ((SqlIdentifier) part).getLastName();
            boolean match = false;
            for (PartitionSpec partitionSpec : existPartitionSpecs) {
                if (partitionSpec.getName().equalsIgnoreCase(partitionName)) {
                    match = true;
                    break;
                }
            }
            if (!match) {
                throw new TddlRuntimeException(ErrorCode.ERR_ADD_PARTITION,
                    "the partition " + partitionName + " is not exist");
            }
        }
    }

    private static int extraPostfixNumberFromPhysicaTableName(String physicalTableName) {
        String postfixStr = physicalTableName.substring(Math.max(1, physicalTableName.length() - 5));
        if (IS_NUMBER.matcher(postfixStr).matches()) {
            return Integer.valueOf(postfixStr);
        } else {
            return -1;
        }
    }

    public static TableGroupConfig getTheBestTableGroupInfo(PartitionInfo partitionInfo,
                                                            String tableGroupName,
                                                            String joinGroup,
                                                            String partitionNamePrefix,
                                                            int flag,
                                                            boolean operateOnSubPartition,
                                                            ComplexTaskMetaManager.ComplexTaskType taskType,
                                                            Map<String, String> outNewPartNamesMap,
                                                            Map<String, Map<String, String>> outSubNewPartNamesMap,
                                                            ExecutionContext executionContext) {

        String schemaName = partitionInfo.getTableSchema();
        TddlRuleManager tddlRuleManager = executionContext.getSchemaManager(schemaName).getTddlRuleManager();
        TableGroupInfoManager tableGroupInfoManager = tddlRuleManager.getTableGroupInfoManager();
        PartitionInfoManager partitionInfoManager = tddlRuleManager.getPartitionInfoManager();

        PartitionTableType tblType = partitionInfo.getTableType();
        Map<Long, TableGroupConfig> copyTableGroupInfo = null;
        if (tblType == PartitionTableType.SINGLE_TABLE || tblType == PartitionTableType.GSI_SINGLE_TABLE) {
            copyTableGroupInfo =
                tableGroupInfoManager.copyTableGroupConfigInfoFromCache(TableGroupRecord.TG_TYPE_DEFAULT_SINGLE_TBL_TG);
            copyTableGroupInfo.putAll(tableGroupInfoManager.copyTableGroupConfigInfoFromCache(
                TableGroupRecord.TG_TYPE_NON_DEFAULT_SINGLE_TBL_TG));
        } else if (tblType == PartitionTableType.BROADCAST_TABLE || tblType == PartitionTableType.GSI_BROADCAST_TABLE) {
            TableGroupConfig broadcastTgConf = tableGroupInfoManager.getBroadcastTableGroupConfig();
            copyTableGroupInfo = new HashMap<>();
            if (broadcastTgConf != null) {
                copyTableGroupInfo.put(broadcastTgConf.getTableGroupRecord().id, broadcastTgConf);
            }
        } else {
            copyTableGroupInfo = tableGroupInfoManager.copyTableGroupConfigInfoFromCache(null);
        }

        PartitionStrategy strategy = partitionInfo.getPartitionBy().getStrategy();
        boolean isVectorStrategy = (strategy == PartitionStrategy.KEY || strategy == PartitionStrategy.RANGE_COLUMNS);

        TableGroupConfig targetTableGroupConfig = null;

        if (tableGroupName != null) {
            targetTableGroupConfig = tableGroupInfoManager.getTableGroupConfigByName(tableGroupName);
            if (targetTableGroupConfig == null) {
                throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_INVALID_PARAMS,
                    String.format("Failed to create partition table because the table group[%s] does NOT exists",
                        tableGroupName));
            }
            final boolean onlyManualTableGroupAllow =
                executionContext.getParamManager().getBoolean(ConnectionParams.ONLY_MANUAL_TABLEGROUP_ALLOW);

            if (!targetTableGroupConfig.isManuallyCreated() && onlyManualTableGroupAllow) {
                throw new TddlRuntimeException(ErrorCode.ERR_TABLE_GROUP_IS_AUTO_CREATED, String.format(
                    "only the tablegroup create by user manually could by use explicitly, the table group[%s] is created internally",
                    tableGroupName));
            }
            if (GeneralUtil.isNotEmpty(targetTableGroupConfig.getAllTables())) {
                String tableName = targetTableGroupConfig.getTables().get(0).getLogTbRec().tableName;

                PartitionInfo comparePartitionInfo = partitionInfoManager.getPartitionInfo(tableName);

                boolean match = false;

                /**
                 * Check if the partInfo of ddl is equals to the partInfo of the first table of the candidate table group
                 */
                if (isVectorStrategy) {
                    if (actualPartColsEquals(partitionInfo, comparePartitionInfo,
                        PartitionInfoUtil.fetchAllLevelMaxActualPartColsFromPartInfos(partitionInfo,
                            comparePartitionInfo))) {
                        match = true;
                    }
                } else if (partitionInfo.equals(comparePartitionInfo)) {
                    match = true;
                }

                if (!match) {
                    throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_INVALID_PARAMS, String.format(
                        "Failed to create partition table because the partition definition of the table mismatch the table group[%s]",
                        tableGroupName));
                }
            } else if (GeneralUtil.isEmpty(targetTableGroupConfig.getPartitionGroupRecords())) {
//                throw new TddlRuntimeException(ErrorCode.ERR_TABLE_GROUP_NOT_INIT,
//                    String.format(
//                        "the table group[%s] is not inited yet",
//                        tableGroupName));
            } else {
                if (partitionInfo.getPartitionBy().getPartitions().size()
                    != targetTableGroupConfig.getPartitionGroupRecords().size()) {
                    throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_INVALID_PARAMS, String.format(
                        "Failed to create partition table because the partition definition of the table mismatch the table group[%s]",
                        tableGroupName));
                }
                for (int i = 0; i < partitionInfo.getPartitionBy().getPartitions().size(); i++) {
                    PartitionSpec partitionSpec = partitionInfo.getPartitionBy().getPartitions().get(i);
                    PartitionGroupRecord partitionGroupRecord =
                        targetTableGroupConfig.getPartitionGroupRecords().get(i);
                    if (partitionSpec.getName().equalsIgnoreCase(partitionGroupRecord.getPartition_name())) {
                        continue;
                    } else {
                        partitionGroupRecord = targetTableGroupConfig.getPartitionGroupRecords().stream()
                            .filter(o -> o.getPartition_name().equalsIgnoreCase(partitionSpec.getName())).findFirst()
                            .orElse(null);
                        if (partitionGroupRecord == null) {
                            throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_INVALID_PARAMS, String.format(
                                "Failed to create partition table because the partition definition of the table mismatch the table group[%s]",
                                tableGroupName));
                        }
                    }
                }
            }
        } else {
            final boolean onlyAutoCreatedTableGroupAllowMatch =
                executionContext.getParamManager().getBoolean(ConnectionParams.MANUAL_TABLEGROUP_NOT_ALLOW_AUTO_MATCH);

            boolean compareLocation =
                (((flag & IGNORE_PARTNAME_LOCALITY) == IGNORE_PARTNAME_LOCALITY) || ((flag & COMPARE_NEW_PART_LOCATION)
                    == COMPARE_NEW_PART_LOCATION) || ((flag & COMPARE_EXISTS_PART_LOCATION)
                    == COMPARE_EXISTS_PART_LOCATION));

            int maxTableCount = -1;
            PartitionByDefinition originalPartitionBy = partitionInfo.getPartitionBy().copy();
            for (Map.Entry<Long, TableGroupConfig> entry : copyTableGroupInfo.entrySet()) {
                if (entry.getKey().longValue() == partitionInfo.getTableGroupId().longValue()) {
                    // don't match itself
                    continue;
                }
                if (entry.getValue().getTableGroupRecord().tg_type == TableGroupRecord.TG_TYPE_OSS_TBL_TG) {
                    continue;
                }
                if (entry.getValue().isManuallyCreated() && onlyAutoCreatedTableGroupAllowMatch) {
                    continue;
                }
                if (entry.getValue().isAutoSplit()) {
                    continue;
                }

                if (entry.getValue().getTableCount() > maxTableCount) {
                    if (partitionInfo.isGsiBroadcastOrBroadcast()) {
                        maxTableCount = entry.getValue().getTableCount();
                        targetTableGroupConfig = entry.getValue();
                    } else if (partitionInfo.isGsiSingleOrSingleTable()) {
                        LocalityDesc tableLocality = LocalityDesc.parse(partitionInfo.getLocality());
                        LocalityDesc compareTableGroupLocality = entry.getValue().getLocalityDesc();
                        boolean match = false;
                        if (tableLocality.match(compareTableGroupLocality)) {
                            match = true;
                        }

                        String tableName = entry.getValue().getTables().get(0).getLogTbRec().tableName;

                        PartitionInfo comparePartitionInfo = partitionInfoManager.getPartitionInfo(tableName);

                        PartitionByDefinition partitionBy = partitionInfo.getPartitionBy();
                        if ((flag & IGNORE_PARTNAME_LOCALITY) == IGNORE_PARTNAME_LOCALITY) {
                            changePartitionNameAndLocality(comparePartitionInfo, partitionBy, partitionNamePrefix,
                                operateOnSubPartition, taskType, outNewPartNamesMap, outSubNewPartNamesMap);
                        }

                        if (match) {
                            match = partitionInfo.equals(comparePartitionInfo);
                        }
                        if (match && compareLocation) {
                            match = comparePartitionInfoLocation(comparePartitionInfo, partitionInfo, true);
                        }

                        if (match) {
                            maxTableCount = entry.getValue().getTableCount();
                            targetTableGroupConfig = entry.getValue();
                            partitionInfo.setPartitionBy(partitionBy);
                            originalPartitionBy = partitionBy.copy();
                        } else {
                            if ((flag & IGNORE_PARTNAME_LOCALITY) == IGNORE_PARTNAME_LOCALITY) {
                                partitionInfo.setPartitionBy(originalPartitionBy.copy());
                            }
                        }

                        if (match)  {
                            match = true;
                        }
                    } else if (entry.getValue().getTableCount() > 0) {
                        int count = entry.getValue().getTableCount();
                        PartitionInfo comparePartitionInfo = null;
                        int i = 0;
                        do {
                            String tableName = entry.getValue().getTables().get(i).getLogTbRec().tableName;
                            comparePartitionInfo = partitionInfoManager.getPartitionInfo(tableName);
                            i++;
                        } while (comparePartitionInfo == null && i < count);

                        if (comparePartitionInfo == null) {
                            continue;
                        }

                        JoinGroupInfoRecord record = null;
                        if (!(ConfigDataMode.isFastMock() || ConfigDataMode.isMock())) {
                            record = JoinGroupUtils.getJoinGroupInfoByTable(comparePartitionInfo.tableSchema,
                                comparePartitionInfo.tableName, null);
                        }
                        boolean differentJoinGroup =
                            (record == null && StringUtils.isNotEmpty(joinGroup)) || (record != null
                                && !record.joinGroupName.equalsIgnoreCase(joinGroup));
                        if (differentJoinGroup) {
                            continue;
                        }
                        PartitionByDefinition partitionBy = partitionInfo.getPartitionBy();

                        Map<String, String> newPartNamesMap = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
                        Map<String, Map<String, String>> subNewPartNamesMap =
                            new TreeMap<>(String.CASE_INSENSITIVE_ORDER);

                        if ((flag & IGNORE_PARTNAME_LOCALITY) == IGNORE_PARTNAME_LOCALITY) {
                            changePartitionNameAndLocality(comparePartitionInfo, partitionBy, partitionNamePrefix,
                                operateOnSubPartition, taskType, newPartNamesMap, subNewPartNamesMap);
                        }
                        /**
                         * Check if the partInfo of ddl is equals to the partInfo of the first table of the candidate table group
                         */
                        boolean match = false;
//                        if (isVectorStrategy) {
//                            if (actualPartColsEquals(partitionInfo, comparePartitionInfo,
//                                partitionInfo.getAllLevelActualPartColCounts())) {
//                                match = true;
//                            }
//                        } else if (partitionInfo.equals(comparePartitionInfo)) {
//                            match = true;
//                        }

                        if (actualPartColsEquals(partitionInfo, comparePartitionInfo,
                            partitionInfo.getAllLevelActualPartColCounts())) {
                            match = true;
                        }

                        if (match && compareLocation) {
                            match = comparePartitionInfoLocation(comparePartitionInfo, partitionInfo,
                                ((flag & COMPARE_NEW_PART_LOCATION) == COMPARE_NEW_PART_LOCATION));
                        }

                        if (match) {
                            if (outNewPartNamesMap != null && outSubNewPartNamesMap != null) {
                                outNewPartNamesMap.clear();
                                outSubNewPartNamesMap.clear();
                                outNewPartNamesMap.putAll(newPartNamesMap);
                                outSubNewPartNamesMap.putAll(subNewPartNamesMap);
                            }
                            maxTableCount = entry.getValue().getTableCount();
                            targetTableGroupConfig = entry.getValue();
                            partitionInfo.setPartitionBy(partitionBy);
                            originalPartitionBy = partitionBy.copy();

                        } else {
                            if ((flag & IGNORE_PARTNAME_LOCALITY) == IGNORE_PARTNAME_LOCALITY) {
                                partitionInfo.setPartitionBy(originalPartitionBy.copy());
                            }
                        }
                    }
                }
            }
        }
        return targetTableGroupConfig;
    }

    /**
     * Generate n names of the added physical tables that must NOT be duplicated with any other physical tables
     */
    public static List<String> getNextNPhyTableNames1(PartitionInfo partitionInfo, int n) {
        List<String> phyTableNames = new ArrayList<>(n);
        if (partitionInfo.isBroadcastTable()) {
            String phyTbName = partitionInfo.getPartitionBy().getPartitions().get(0).getLocation().getPhyTableName();
            while (n > 0) {
                phyTableNames.add(phyTbName);
                n--;
            }
            return phyTableNames;
        } else {
            int maxPostfix = -1;
            for (PartitionSpec spec : partitionInfo.getPartitionBy().getPartitions()) {
                // ref to com.alibaba.polardbx.optimizer.partition.PartitionLocator.PHYSICAL_TABLENAME_PATTERN
                Integer postfix = Integer.valueOf(
                        spec.getLocation().getPhyTableName().substring(spec.getLocation().getPhyTableName().length() - 5))
                    .intValue();
                maxPostfix = Math.max(maxPostfix, postfix.intValue());
            }

            for (int i = 0; i < n; i++) {
                String phyTableName =
                    String.format(PHYSICAL_TABLENAME_PATTERN, partitionInfo.getPrefixTableName(), i + 1 + maxPostfix);
                phyTableNames.add(phyTableName);
            }
            assert phyTableNames.size() == n;
            return phyTableNames;
        }
    }

    public static List<String> getNextNPhyTableNames(PartitionInfo partitionInfo, int newTableCount, int[] minPostfix) {
        List<String> phyTableNames = new ArrayList<>(newTableCount);
        if (partitionInfo.isBroadcastTable()) {
            String phyTbName = partitionInfo.getPartitionBy().getPartitions().get(0).getLocation().getPhyTableName();
            while (newTableCount > 0) {
                phyTableNames.add(phyTbName);
                newTableCount--;
            }
            return phyTableNames;
        } else {
            assert minPostfix.length == 1;
            int curIndex;
            Set<Integer> existingPostfix = new HashSet<>();
            List<PartitionSpec> targetPartSpecs = partitionInfo.getPartitionBy().getPhysicalPartitions();
            for (PartitionSpec spec : targetPartSpecs) {

                try {
                    curIndex = Integer.parseInt(spec.getLocation().getPhyTableName()
                        .substring(spec.getLocation().getPhyTableName().length() - 5));
                } catch (NumberFormatException e) {
                    curIndex = 0;
                }
                existingPostfix.add(curIndex);

//                // ref to com.alibaba.polardbx.optimizer.partition.PartitionLocator.PHYSICAL_TABLENAME_PATTERN
//                if (partitionInfo.getPartitionBy().getSubPartitionBy() != null &&
//                    GeneralUtil.isNotEmpty(spec.getSubPartitions())) {
//                    for (PartitionSpec subSpec : spec.getSubPartitions()) {
//                        try {
//                            curIndex = Integer.parseInt(subSpec.getLocation().getPhyTableName()
//                                .substring(subSpec.getLocation().getPhyTableName().length() - 5));
//                        } catch (NumberFormatException e) {
//                            curIndex = 0;
//                        }
//                        existingPostfix.add(curIndex);
//
//                    }
//                } else {
//                    try {
//                        curIndex = Integer.parseInt(spec.getLocation().getPhyTableName()
//                            .substring(spec.getLocation().getPhyTableName().length() - 5));
//                    } catch (NumberFormatException e) {
//                        curIndex = 0;
//                    }
//                    existingPostfix.add(curIndex);
//                }
            }

            while (newTableCount > 0) {
                int nextPostfix = minPostfix[0] + 1;
                minPostfix[0]++;
                if (minPostfix[0] >= PartitionNameUtil.MAX_PART_POSTFIX_NUM) {
                    //recycle
                    minPostfix[0] = 0;
                }
                if (existingPostfix.contains(nextPostfix)) {
                    continue;
                }
                String phyTableName =
                    String.format(PHYSICAL_TABLENAME_PATTERN, partitionInfo.getPrefixTableName(), nextPostfix);
                phyTableNames.add(phyTableName);
                newTableCount--;
            }
            return phyTableNames;
        }
    }

    public static List<Pair<String, String>> getInvisiblePartitionPhysicalLocation(PartitionInfo partitionInfo) {
        List<Pair<String, String>> groupAndPhyTableList = new ArrayList<>();
        for (PartitionSpec partitionSpec : partitionInfo.getPartitionBy().getPartitions()) {
            PartitionLocation location = partitionSpec.getLocation();
            if (!location.isVisiable()) {
                groupAndPhyTableList.add(new Pair<>(location.getGroupKey(), location.getPhyTableName()));
            }
        }
        return groupAndPhyTableList;
    }

    public static Map<String, List<List<String>>> buildTargetTablesFromPartitionInfo(PartitionInfo partitionInfo) {
        Map<String, List<List<String>>> targetTables = new HashMap<>();
        if (partitionInfo != null) {
            // get table type
            PartitionTableType tblType = partitionInfo.getTableType();

            // classify phyTbList for each phy grp
            Map<String, List<String>> grpPhyTbListMap = new HashMap<>();
            if (tblType != PartitionTableType.BROADCAST_TABLE) {

                // get all partitions ( included all subpartitions ) by partition
                List<PartitionSpec> partitionSpecs = partitionInfo.getPartitionBy().getPhysicalPartitions();
                for (int i = 0; i < partitionSpecs.size(); i++) {
                    PartitionSpec ps = partitionSpecs.get(i);
                    PartitionLocation location = ps.getLocation();
                    String grp = location.getGroupKey();
                    String phyTb = location.getPhyTableName();
                    List<String> phyTbList = grpPhyTbListMap.get(grp);
                    if (phyTbList == null) {
                        phyTbList = new ArrayList<>();
                        grpPhyTbListMap.put(grp, phyTbList);
                    }
                    phyTbList.add(phyTb);
                }

            } else {

                // For broadcast table only
                List<String> grpList = HintUtil.allGroup(partitionInfo.getTableSchema());
                PartitionSpec ps = partitionInfo.getPartitionBy().getPartitions().get(0);
                String phyTb = ps.getLocation().getPhyTableName();
                grpList.stream()
                    .forEach(grp -> grpPhyTbListMap.computeIfAbsent(grp, g -> new ArrayList<>()).add(phyTb));
            }

            for (Map.Entry<String, List<String>> grpPhyTbListItem : grpPhyTbListMap.entrySet()) {
                List<List<String>> allPhyTbsList = new ArrayList<>();
                String grpName = grpPhyTbListItem.getKey();
                List<String> phyTbListOfDiffIndex = grpPhyTbListItem.getValue();
                for (int i = 0; i < phyTbListOfDiffIndex.size(); i++) {
                    List<String> phyTbListOfSameIndex = new ArrayList<>();
                    phyTbListOfSameIndex.add(phyTbListOfDiffIndex.get(i));
                    allPhyTbsList.add(phyTbListOfSameIndex);
                }
                targetTables.put(grpName, allPhyTbsList);
            }
        }
        return targetTables;
    }

    /**
     * Get physical topology of table, support both sharding and partition table
     */
    public static Map<String, List<List<String>>> getTableTopology(String schema, String tableName) {
        OptimizerContext oc = OptimizerContext.getContext(schema);

        if (DbInfoManager.getInstance().isNewPartitionDb(schema)) {
            PartitionInfo partInfo = oc.getPartitionInfoManager().getPartitionInfo(tableName);
            return buildTargetTablesFromPartitionInfo(partInfo);
        } else {
            TableRule tableRule = oc.getRuleManager().getTddlRule().getTable(tableName);
            if (tableRule == null) {
                return ImmutableMap.of(schema, ImmutableList.of(ImmutableList.of(tableName)));
            } else {
                Map<String, Set<String>> topology = tableRule.getActualTopology();
                // convert set to list of list
                Map<String, List<List<String>>> result = topology.entrySet().stream().collect(
                    Collectors.toMap(Map.Entry::getKey,
                        x -> x.getValue().stream().map(ImmutableList::of).collect(Collectors.toList())));

                if (tableRule.isBroadcast()) {
                    Set<String> tables = topology.values().stream().findFirst()
                        .orElseThrow(() -> new TddlRuntimeException(ErrorCode.ERR_TABLE_NO_RULE));
                    List<List<String>> tableList = ImmutableList.of(ImmutableList.copyOf(tables));

                    Map<String, Set<String>> common = RelUtils.getCommonTopology(schema, tableName);
                    common.forEach((k, v) -> result.put(k, tableList));
                }
                return result;
            }
        }
    }

    public static PartitionInfo updatePartitionInfoByOutDatePartitionRecords(Connection conn, Long tgIdInMetadb,
                                                                             PartitionInfo partitionInfo,
                                                                             TableInfoManager tableInfoManager) {
        List<PartitionGroupRecord> outdatedPartitionRecords =
            TableGroupUtils.getOutDatePartitionGroupsByTgId(conn, tgIdInMetadb);
        partitionInfo = partitionInfo.copy();
        for (PartitionGroupRecord partitionGroupRecord : outdatedPartitionRecords) {
            if (partitionInfo.getPartitionBy().getSubPartitionBy() != null) {
                boolean found = false;
                for (PartitionSpec spec : partitionInfo.getPartitionBy().getPartitions()) {
                    PartitionSpec subSpec = spec.getSubPartitions().stream()
                        .filter(sp -> sp.getName().equalsIgnoreCase(partitionGroupRecord.partition_name)).findFirst()
                        .orElse(null);
                    if (subSpec != null) {
                        if (partitionInfo.getTableType() != PartitionTableType.BROADCAST_TABLE) {
                            subSpec.getLocation().setGroupKey(
                                GroupInfoUtil.buildGroupNameFromPhysicalDb(partitionGroupRecord.phy_db));
                        } else {
                            subSpec.getLocation()
                                .setGroupKey(tableInfoManager.getDefaultDbIndex(partitionInfo.getTableSchema()));
                        }
                        subSpec.getLocation().setVisiable(false);
                        found = true;
                        break;
                    }
                }
                if (!found) {
                    new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                        String.format("partition-group %s not found", partitionGroupRecord.partition_name));
                }
            } else {PartitionSpec spec = partitionInfo.getPartitionBy().getPartitions().stream()
                .filter(o -> o.getName().equalsIgnoreCase(partitionGroupRecord.partition_name)).findFirst().orElseThrow(
                    () -> new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                        String.format("partition-group %s not found", partitionGroupRecord.partition_name)));
            assert spec != null;
            if (partitionInfo.getTableType() != PartitionTableType.BROADCAST_TABLE) {
                spec.getLocation().setGroupKey(GroupInfoUtil.buildGroupNameFromPhysicalDb(partitionGroupRecord.phy_db));
            } else {
                spec.getLocation().setGroupKey(tableInfoManager.getDefaultDbIndex(partitionInfo.getTableSchema()));
            }

                spec.getLocation().setVisiable(false);
            }
        }

        partitionInfo.initPartSpecSearcher();
        return partitionInfo;
    }

    public static void updatePartitionInfoByNewCommingPartitionRecords(Connection conn, Long tgIdInMetadb,
                                                                       PartitionInfo partitionInfo) {
        //set visible property for newPartitionInfo here
        List<PartitionGroupRecord> newComingPartitionRecords =
            TableGroupUtils.getAllUnVisiablePartitionGroupByGroupId(conn, tgIdInMetadb);

        for (PartitionGroupRecord partitionGroupRecord : newComingPartitionRecords) {
            if (partitionInfo.getPartitionBy().getSubPartitionBy() != null) {
                boolean found = false;
                for (PartitionSpec spec : partitionInfo.getPartitionBy().getPartitions()) {
                    PartitionSpec subSpec = spec.getSubPartitions().stream()
                        .filter(sp -> sp.getName().equalsIgnoreCase(partitionGroupRecord.partition_name)).findFirst()
                        .orElse(null);
                    if (subSpec != null) {
                        subSpec.getLocation()
                            .setGroupKey(GroupInfoUtil.buildGroupNameFromPhysicalDb(partitionGroupRecord.phy_db));
                        subSpec.getLocation().setPartitionGroupId(partitionGroupRecord.getId());
                        subSpec.getLocation().setVisiable(false);
                        found = true;
                        break;
                    }
                }
                if (!found) {
                    throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                        String.format("partition-group %s not found", partitionGroupRecord.partition_name));
                }
            } else {PartitionSpec spec = partitionInfo.getPartitionBy().getPartitions().stream()
                .filter(o -> o.getName().equalsIgnoreCase(partitionGroupRecord.partition_name)).findFirst().orElseThrow(
                    () -> new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                        String.format("partition-group %s not found", partitionGroupRecord.partition_name)));
            spec.getLocation().setGroupKey(GroupInfoUtil.buildGroupNameFromPhysicalDb(partitionGroupRecord.phy_db));
            spec.getLocation().setPartitionGroupId(partitionGroupRecord.getId());
            spec.getLocation().setVisiable(false);}
        }
        partitionInfo.initPartSpecSearcher();
    }

    public static long getHashSpaceMaxValue() {
        return MAX_HASH_VALUE;
    }

    public static long getHashSpaceMinValue() {
        return MIN_HASH_VALUE;
    }

    public static boolean partitionDataTypeEquals(ColumnMeta partColMeta1, ColumnMeta partColMeta2) {

        ColumnMeta partColMeta = partColMeta1;
        ColumnMeta otherPartColMeta = partColMeta2;
        CharsetName charsetName = partColMeta.getField().getDataType().getCharsetName();
        CharsetName otherCharsetName = otherPartColMeta.getField().getDataType().getCharsetName();

        boolean isCharsetDiff =
            (charsetName == null && otherCharsetName != null) || (charsetName != null && otherCharsetName == null) || (
                charsetName != null && otherCharsetName != null && !charsetName.equals(otherCharsetName));
        if (isCharsetDiff) {
            return false;
        }

        CollationName collationName = partColMeta.getField().getDataType().getCollationName();
        CollationName otherCollationName = otherPartColMeta.getField().getDataType().getCollationName();
        boolean isCollationDiff = (collationName == null && otherCollationName != null) || (collationName != null
            && otherCollationName == null) || (collationName != null && otherCollationName != null
            && !collationName.equals(otherCollationName));
        if (isCollationDiff) {
            return false;
        }
        if (partColMeta.getDataType() == null) {
            if (otherPartColMeta.getDataType() != null) {
                return false;
            }
        } else if (!DataTypeUtil.equals(partColMeta.getDataType(), otherPartColMeta.getDataType(), true)) {
            return false;
        }
        if (!checkBinaryLengthIfNeed(partColMeta, otherPartColMeta)) {
            return false;
        }

        return true;
    }

    private static boolean checkBinaryLengthIfNeed(ColumnMeta cm1, ColumnMeta cm2) {
        Field fld1 = cm1.getField();
        Field fld2 = cm2.getField();
        RelDataType relDt1 = fld1.getRelType();
        RelDataType relDt2 = fld2.getRelType();
        SqlTypeName typeName1 = relDt1.getSqlTypeName();
        SqlTypeName typeName2 = relDt2.getSqlTypeName();
        int precision1 = relDt1.getPrecision();
        int precision2 = relDt2.getPrecision();
        if (typeName1 != typeName2) {
            return false;
        }
        if (typeName1 == SqlTypeName.VARBINARY || typeName1 == SqlTypeName.BINARY) {
            if (precision1 != precision2) {
                return false;
            }
        }
        return true;
    }

    /**
     * 如果拆分规则与原先相同，则直接返回成功
     * return true is the partitionInfo is the same as before
     */
    public static boolean checkPartitionInfoEquals(PartitionInfo sourcePartitionInfo,
                                                   PartitionInfo targetPartitionInfo) {
        assert sourcePartitionInfo != null && targetPartitionInfo != null;

        List<String> sourceShardColumns =
            sourcePartitionInfo.getPartitionColumnsNotDeduplication().stream().map(String::toLowerCase)
                .collect(Collectors.toList());
        List<String> targetShardColumns =
            targetPartitionInfo.getPartitionColumnsNotDeduplication().stream().map(String::toLowerCase)
                .collect(Collectors.toList());

        // 分区键不同
        if (!sourceShardColumns.equals(targetShardColumns)) {
            return false;
        }

        return sourcePartitionInfo.equals(targetPartitionInfo);
    }

    public static void adjustPartitionPositionsForNewPartInfo(PartitionInfo newPartInfo) {
//        // Reset the partition position for each new partition
//        List<PartitionSpec> newSpecs = newPartInfo.getPartitionBy().getPartitions();
//        long allPhyPartPosiCounter = 0L;
//        for (int i = 0; i < newSpecs.size(); i++) {
//            PartitionSpec spec = newSpecs.get(i);
//            long newPosi = i + 1;
//            spec.setPosition(newPosi);
//            spec.setParentPartPosi(0L);
//            if (spec.isLogical) {
//                for (int j = 0; j < spec.getSubPartitions().size(); j++) {
//                    long newSpPosi = j + 1;
//                    long newParentPosi = newPosi;
//                    PartitionSpec spSpec = spec.getSubPartitions().get(j);
//                    spSpec.setPosition(newSpPosi);
//                    spSpec.setParentPartPosi(newParentPosi);
//                    ++allPhyPartPosiCounter;
//                    spSpec.setPhyPartPosition(allPhyPartPosiCounter);
//                }
//            } else {
//                ++allPhyPartPosiCounter;
//                spec.setPhyPartPosition(allPhyPartPosiCounter);
//            }
//        }
//        PartitionByDefinition subPartBy = newPartInfo.getPartitionBy().getSubPartitionBy();
//        if (subPartBy != null) {
//            List<PartitionSpec> newSubPartitions = subPartBy.getPartitions();
//            for (int i = 0; i < newSubPartitions.size(); i++) {
//                PartitionSpec spec = newSubPartitions.get(i);
//                long newPosi = i + 1;
//                spec.setPosition(newPosi);
//            }
//        }
        newPartInfo.initPartSpecSearcher();
    }

//    public static List<String> getActualPartitionColumns(PartitionInfo partInfo) {
//        return partInfo.getActualPartitionColumns();
//    }

    public static List<List<String>> getAllLevelActualPartColumns(PartitionInfo partInfo) {
        return partInfo.getAllLevelActualPartCols();
    }

    /**
     * Get the actual partition columns of all part levels
     * and covert them into a list (allowed duplicated columns)
     * <p>
     * Import Notice:
     * the order of  partition columns of list to be return
     * MUST BE the same as the order of definition in create tbl/create global index ddl
     */
    public static List<String> getAllLevelActualPartColumnsAsList(PartitionInfo partInfo) {
        return partInfo.getAllLevelActualPartColsAsList();
    }

    /**
     * Get the actual partition columns of all part levels and merge them into a list without duplicated columns
     */
    public static List<String> getAllLevelActualPartColumnsAsNoDuplicatedList(PartitionInfo partInfo) {
        return partInfo.getAllLevelActualPartColsAsNoDuplicatedList();
    }

    /**
     * check the equality for tb1 and tb2 by their actual partition columns
     */
    public static boolean actualPartColsEquals(PartitionInfo tb1, PartitionInfo tb2) {
        List<List<String>> allLevelPartColListTb1 = getAllLevelActualPartColumns(tb1);
        List<List<String>> allLevelPartColListTb2 = getAllLevelActualPartColumns(tb2);
        if (allLevelPartColListTb1.size() != allLevelPartColListTb2.size()) {
            return false;
        }
        int levelCnt = allLevelPartColListTb1.size();
        List<Integer> allLevelPrefixPartColCnts = new ArrayList<>();
        for (int i = 0; i < levelCnt; i++) {
            int partColCntTb1 = allLevelPartColListTb1.get(i).size();
            int partColCntTb2 = allLevelPartColListTb2.get(i).size();
            int maxPartColVal = partColCntTb1 > partColCntTb2 ? partColCntTb1 : partColCntTb2;
            allLevelPrefixPartColCnts.add(maxPartColVal);
        }
        return tb1.equals(tb2, allLevelPrefixPartColCnts);
    }

//    public static boolean partitionEquals(PartitionInfo tb1, PartitionInfo tb2, int nPartCol) {
//        return tb1.equals(tb2, nPartCol);
//    }

    /**
     * Fetch the max actual partition columns from partition info for all partition level
     * <pre>
     *     e.g
     *      t1:
     *          partCols: a,b,c
     *          actualPartCols: a
     *          subPartCols, b,a,d
     *          actualPartCols: b,a
     *
     *      t2:
     *          partCols: a,b,c
     *          actualPartCols: a,b
     *          subPartCols, b,a,d
     *          actualPartCols: b
     *      then
     *
     *      maxActualPartCols:
     *          actualMaxPartCols: a,b
     *          actualMaxSubPartCols: b,a
     * </pre>
     */
    public static List<Integer> fetchAllLevelMaxActualPartColsFromPartInfos(PartitionInfo partInfo1,
                                                                            PartitionInfo partInfo2) {
        List<Integer> allLevelMaxActualPartCols = new ArrayList<>();
        if (!(partInfo1.isPartitionedTableOrGsiTable() && partInfo2.isPartitionedTableOrGsiTable())) {
            return allLevelMaxActualPartCols;
        }
        List<Integer> actualPartColInfoOfPartInfo1 = partInfo1.getAllLevelActualPartColCounts();
        List<Integer> actualPartColInfoOfPartInfo2 = partInfo2.getAllLevelActualPartColCounts();
        int partColCnt1 = actualPartColInfoOfPartInfo1.get(0);
        int partColCnt2 = actualPartColInfoOfPartInfo2.get(0);
        allLevelMaxActualPartCols.add(partColCnt1 > partColCnt2 ? partColCnt1 : partColCnt2);

        int subPartColCnt1 = 0;
        int subPartColCnt2 = 0;
        if (actualPartColInfoOfPartInfo1.size() > 1) {
            subPartColCnt1 = actualPartColInfoOfPartInfo1.get(1);
        }
        if (actualPartColInfoOfPartInfo2.size() > 1) {
            subPartColCnt2 = actualPartColInfoOfPartInfo2.get(1);
        }
        if (subPartColCnt1 != 0 || subPartColCnt2 != 0) {
            allLevelMaxActualPartCols.add(subPartColCnt1 > subPartColCnt2 ? subPartColCnt1 : subPartColCnt2);
        }
        return allLevelMaxActualPartCols;
    }

    /**
     * Compare the partition definition by specifying prefix part col of all part levels
     * <pre>
     *     if the
     * </pre>
     */
    public static boolean actualPartColsEquals(PartitionInfo tb1, PartitionInfo tb2,
                                               List<Integer> allLevelPartColCnts) {
        return tb1.equals(tb2, allLevelPartColCnts);
    }

//    public int checkIfActualPartitionColumnsChange(PartitionInfo beforeAlter, PartitionInfo afterAlter) {
//        List<String> partColListBeforeAlter = getActualPartitionColumns(beforeAlter);
//        List<String> partColListAlterAlter = getActualPartitionColumns(afterAlter);
//        return partColListBeforeAlter.size() - partColListAlterAlter.size();
//    }

    /**
     * If specify the prefixPartColCnt columns as multi-part-col key,
     * group all the logical tables by the definition of partInfo base on special the multi-part-col key
     *
     * <pre>
     *     For example:
     *  tg: t1,t2,t3,t4,t5,t6
     *  tg(partCols): c1,c2    ( tg use the first two part cols c1 and c2  ）
     *  t1(partCols): c1,c2,pk_int
     *  t2(partCols): c1,c2,pk_var
     *  t3(partCols): c1,c2,pk1_char,pk2_int
     *  t4(partCols): c1,c2
     *  t5(partCols): c1,c2,pk_int
     *  t6(partCols): c1,c2,pk_var
     *
     * , if tablegroup is going to do split and use the first three part col c1,c2,x as the new multi-part-col key,
     * these tables should be classified as four groups as followded:
     *
     * tg1(c1,c2): t4
     * tg2(c1,c2,pk_int): t1,t5
     * tg3(c1,c2,pk_var): t2,t6
     * tg4(c1,c2,pk1_char): t3
     *
     * </pre>
     */
    public static List<List<String>> classifyTablesByPrefixPartitionColumns(
        String schemaName,
        List<String> tableList,
        List<Integer> allLevelPrefixPartColCnts) {
        PartitionInfoManager partInfoMgr = OptimizerContext.getContext(schemaName).getPartitionInfoManager();
        List<PartitionInfo> partInfoList = new ArrayList<>();
        for (int i = 0; i < tableList.size(); i++) {
            PartitionInfo partInfo = partInfoMgr.getPartitionInfo(tableList.get(i));
            partInfoList.add(partInfo);
        }
        Map<Long, List<PartitionInfo>> outputPartInfoGrouping = new HashMap<>();
        Map<String, Long> outputTblNameToGrpIdMap = new TreeMap<>(CaseInsensitive.CASE_INSENSITIVE_ORDER);
        classifyTablesByPrefixPartitionColumns(partInfoList, allLevelPrefixPartColCnts, outputPartInfoGrouping,
            outputTblNameToGrpIdMap);

        List<List<String>> tbListAfterGrouping = new ArrayList<>();
        for (Map.Entry<Long, List<PartitionInfo>> grpInfoItem : outputPartInfoGrouping.entrySet()) {
            List<PartitionInfo> partInfoOfSameGrp = grpInfoItem.getValue();
            List<String> tbNameListOfSameGrp = new ArrayList<>();
            for (int j = 0; j < partInfoOfSameGrp.size(); j++) {
                tbNameListOfSameGrp.add(partInfoOfSameGrp.get(j).getTableName());
            }
            tbListAfterGrouping.add(tbNameListOfSameGrp);
        }
        return tbListAfterGrouping;
    }

//    public static void classifyTablesByPrefixPartitionColumns(List<PartitionInfo> partInfoListOfSameTg,
//                                                              int prefixPartColCnt,
//                                                              Map<Long, List<PartitionInfo>> outputPartInfoGrouping,
//                                                              Map<String, Long> outputTblNameToGrpIdMap) {
//        int logTbCnt = partInfoListOfSameTg.size();
//        for (int i = 0; i < logTbCnt; i++) {
//            PartitionInfo partInfo = partInfoListOfSameTg.get(i);
//            String logTblName = partInfo.getTableName();
//            boolean findGrp = false;
//            for (Map.Entry<Long, List<PartitionInfo>> grpInfoItem : outputPartInfoGrouping.entrySet()) {
//                Long grpId = grpInfoItem.getKey();
//                List<PartitionInfo> partListInfo = grpInfoItem.getValue();
//                PartitionInfo partInfoOfFirstTbOfSameGrp = partListInfo.get(0);
//                boolean rs = partInfo.equals(partInfoOfFirstTbOfSameGrp, prefixPartColCnt);
//                if (rs) {
//                    partListInfo.add(partInfo);
//                    findGrp = true;
//                    outputTblNameToGrpIdMap.put(logTblName.toLowerCase(), grpId);
//                    break;
//                }
//            }
//            if (!findGrp) {
//                List<PartitionInfo> partListInfo = new ArrayList<>();
//                Long newGrpId = outputPartInfoGrouping.size() + 1L;
//                partListInfo.add(partInfo);
//                outputPartInfoGrouping.put(newGrpId, partListInfo);
//                outputTblNameToGrpIdMap.put(logTblName.toLowerCase(), newGrpId);
//            }
//        }
//    }

    public static void classifyTablesByPrefixPartitionColumns(List<PartitionInfo> partInfoListOfSameTg,
                                                              List<Integer> allLevelPartColCnts,
                                                              Map<Long, List<PartitionInfo>> outputPartInfoGrouping,
                                                              Map<String, Long> outputTblNameToGrpIdMap) {
        int logTbCnt = partInfoListOfSameTg.size();
        for (int i = 0; i < logTbCnt; i++) {
            PartitionInfo partInfo = partInfoListOfSameTg.get(i);
            String logTblName = partInfo.getTableName();
            boolean findGrp = false;
            for (Map.Entry<Long, List<PartitionInfo>> grpInfoItem : outputPartInfoGrouping.entrySet()) {
                Long grpId = grpInfoItem.getKey();
                List<PartitionInfo> partListInfo = grpInfoItem.getValue();
                PartitionInfo partInfoOfFirstTbOfSameGrp = partListInfo.get(0);
                boolean rs = partInfo.equals(partInfoOfFirstTbOfSameGrp, allLevelPartColCnts);
                if (rs) {
                    partListInfo.add(partInfo);
                    findGrp = true;
                    outputTblNameToGrpIdMap.put(logTblName.toLowerCase(), grpId);
                    break;
                }
            }
            if (!findGrp) {
                List<PartitionInfo> partListInfo = new ArrayList<>();
                Long newGrpId = outputPartInfoGrouping.size() + 1L;
                partListInfo.add(partInfo);
                outputPartInfoGrouping.put(newGrpId, partListInfo);
                outputTblNameToGrpIdMap.put(logTblName.toLowerCase(), newGrpId);
            }
        }
    }

    public static List<List<ColumnMeta>> getAllLevelMaxPartColumnMetasInfoForTableGroup(String schemaName,
                                                                                        String tableGroupName) {
        PartitionInfoManager partInfoMgr = OptimizerContext.getContext(schemaName).getPartitionInfoManager();
        TableGroupInfoManager tgMgr = OptimizerContext.getContext(schemaName).getTableGroupInfoManager();
        TableGroupConfig tgConfig = tgMgr.getTableGroupConfigByName(tableGroupName);
        List<TablePartRecordInfoContext> tblInfos = tgConfig.getTables();
        if (tblInfos.size() == 0) {
            return new ArrayList<>();
        }

        List<PartitionInfo> partInfoList = new ArrayList<>();
        int partLevelMinPartColCnt = Integer.MAX_VALUE;
        PartitionByDefinition partByDefOfMinPartCol = null;

        boolean useSubPartBy = false;
        int subPartLevelMinPartColCnt = Integer.MAX_VALUE;
        PartitionByDefinition subPartByDefOfMinPartCol = null;

        List<List<ColumnMeta>> allLevelMaxPartColMetas = new ArrayList<>();
        for (int i = 0; i < tblInfos.size(); i++) {
            String logTbName = tblInfos.get(i).getTableName();
            PartitionInfo partInfo = partInfoMgr.getPartitionInfo(logTbName);
            partInfoList.add(partInfo);

            int partColCnt = partInfo.getPartitionBy().getPartitionColumnNameList().size();
            if (partLevelMinPartColCnt > partColCnt) {
                partLevelMinPartColCnt = partColCnt;
                partByDefOfMinPartCol = partInfo.getPartitionBy();
            }

            if (partInfo.getPartitionBy().getSubPartitionBy() != null) {
                useSubPartBy = true;
                int subPartColCnt = partInfo.getPartitionBy().getSubPartitionBy().getPartitionColumnNameList().size();
                if (subPartLevelMinPartColCnt > subPartColCnt) {
                    subPartLevelMinPartColCnt = subPartColCnt;
                    subPartByDefOfMinPartCol = partInfo.getPartitionBy().getSubPartitionBy();
                }
            }
        }

        boolean partLevelUseFullCols = false;
        boolean subPartLevelUseFullCols = false;
        if (partByDefOfMinPartCol != null) {
            PartitionStrategy strategy = partByDefOfMinPartCol.getStrategy();
            if (strategy != PartitionStrategy.KEY && strategy != PartitionStrategy.RANGE_COLUMNS) {
                partLevelUseFullCols = true;
            }
        }
        if (useSubPartBy) {
            PartitionStrategy strategy = subPartByDefOfMinPartCol.getStrategy();
            if (strategy != PartitionStrategy.KEY && strategy != PartitionStrategy.RANGE_COLUMNS) {
                subPartLevelUseFullCols = true;
            }
        }

        Map<Long, List<PartitionInfo>> outputPartInfoGrouping = new HashMap<>();
        Map<String, Long> outputTblNameToGrpIdMap = new TreeMap<>(CaseInsensitive.CASE_INSENSITIVE_ORDER);
        int maxActPartColCnt = partLevelMinPartColCnt;
        int maxActSubPartColCnt = subPartLevelMinPartColCnt;

        List<ColumnMeta> maxActPartColMetas = new ArrayList<>();
        List<ColumnMeta> maxActSubPartColMetas = new ArrayList<>();
        if (!useSubPartBy) {

            if (partLevelUseFullCols) {
                maxActPartColMetas = partByDefOfMinPartCol.getPartitionFieldList();
                allLevelMaxPartColMetas.add(maxActPartColMetas);
                return allLevelMaxPartColMetas;
            }
            for (int prefixPartColCnt = partLevelMinPartColCnt; prefixPartColCnt >= 1; --prefixPartColCnt) {
                outputPartInfoGrouping.clear();
                outputTblNameToGrpIdMap.clear();

                List<Integer> tmpAllLevelPrefixPartColCnt = new ArrayList<>();
                tmpAllLevelPrefixPartColCnt.add(prefixPartColCnt);

                PartitionInfoUtil.classifyTablesByPrefixPartitionColumns(partInfoList, tmpAllLevelPrefixPartColCnt,
                    outputPartInfoGrouping, outputTblNameToGrpIdMap);

                if (outputPartInfoGrouping.size() == 1) {
                    maxActPartColCnt = prefixPartColCnt;
                    maxActSubPartColCnt = 0;
                    break;
                }
            }
            List<ColumnMeta> minPartColMetas = partByDefOfMinPartCol.getPartitionFieldList();
            for (int i = 0; i < maxActPartColCnt; i++) {
                maxActPartColMetas.add(minPartColMetas.get(i));
            }
            allLevelMaxPartColMetas.add(maxActPartColMetas);
        } else {
            if (partLevelUseFullCols && subPartLevelUseFullCols) {
                maxActPartColMetas = partByDefOfMinPartCol.getPartitionFieldList();
                maxActSubPartColMetas = subPartByDefOfMinPartCol.getPartitionFieldList();
                allLevelMaxPartColMetas.add(maxActPartColMetas);
                allLevelMaxPartColMetas.add(maxActSubPartColMetas);
                return allLevelMaxPartColMetas;
            } else if (partLevelUseFullCols && !subPartLevelUseFullCols) {
                maxActPartColMetas = partByDefOfMinPartCol.getPartitionFieldList();
                for (int prefixSubPartColCnt = subPartLevelMinPartColCnt; prefixSubPartColCnt >= 1;
                     --prefixSubPartColCnt) {
                    outputPartInfoGrouping.clear();
                    outputTblNameToGrpIdMap.clear();

                    List<Integer> tmpAllLevelPrefixPartColCnt = new ArrayList<>();
                    tmpAllLevelPrefixPartColCnt.add(maxActPartColMetas.size());
                    tmpAllLevelPrefixPartColCnt.add(prefixSubPartColCnt);
                    PartitionInfoUtil.classifyTablesByPrefixPartitionColumns(partInfoList, tmpAllLevelPrefixPartColCnt,
                        outputPartInfoGrouping, outputTblNameToGrpIdMap);

                    if (outputPartInfoGrouping.size() == 1) {
                        maxActSubPartColCnt = prefixSubPartColCnt;
                        break;
                    }
                }

                allLevelMaxPartColMetas.add(maxActPartColMetas);
                if (subPartByDefOfMinPartCol != null && maxActSubPartColCnt > 0) {
                    List<ColumnMeta> minSubPartColMetas = subPartByDefOfMinPartCol.getPartitionFieldList();
                    for (int i = 0; i < maxActSubPartColCnt; i++) {
                        maxActSubPartColMetas.add(minSubPartColMetas.get(i));
                    }
                    allLevelMaxPartColMetas.add(maxActSubPartColMetas);
                }

            } else if (!partLevelUseFullCols && subPartLevelUseFullCols) {

                maxActSubPartColMetas = subPartByDefOfMinPartCol.getPartitionFieldList();

                for (int prefixPartColCnt = partLevelMinPartColCnt; prefixPartColCnt >= 1; --prefixPartColCnt) {
                    outputPartInfoGrouping.clear();
                    outputTblNameToGrpIdMap.clear();

                    List<Integer> tmpAllLevelPrefixPartColCnt = new ArrayList<>();
                    tmpAllLevelPrefixPartColCnt.add(prefixPartColCnt);
                    tmpAllLevelPrefixPartColCnt.add(maxActSubPartColMetas.size());
                    PartitionInfoUtil.classifyTablesByPrefixPartitionColumns(partInfoList, tmpAllLevelPrefixPartColCnt,
                        outputPartInfoGrouping, outputTblNameToGrpIdMap);

                    if (outputPartInfoGrouping.size() == 1) {
                        maxActPartColCnt = prefixPartColCnt;
                        break;
                    }
                }

                List<ColumnMeta> minPartColMetas = partByDefOfMinPartCol.getPartitionFieldList();
                for (int i = 0; i < maxActPartColCnt; i++) {
                    maxActPartColMetas.add(minPartColMetas.get(i));
                }
                allLevelMaxPartColMetas.add(maxActPartColMetas);
                allLevelMaxPartColMetas.add(maxActSubPartColMetas);

            } else {
                boolean findTargetPrefixPartColCnts = false;
                for (int prefixPartColCnt = partLevelMinPartColCnt; prefixPartColCnt >= 1; --prefixPartColCnt) {
                    for (int prefixSubPartColCnt = subPartLevelMinPartColCnt; prefixSubPartColCnt >= 1;
                         --prefixSubPartColCnt) {
                        outputPartInfoGrouping.clear();
                        outputTblNameToGrpIdMap.clear();

                        List<Integer> tmpAllLevelPrefixPartColCnt = new ArrayList<>();
                        tmpAllLevelPrefixPartColCnt.add(prefixPartColCnt);
                        tmpAllLevelPrefixPartColCnt.add(prefixSubPartColCnt);
                        PartitionInfoUtil.classifyTablesByPrefixPartitionColumns(partInfoList,
                            tmpAllLevelPrefixPartColCnt,
                            outputPartInfoGrouping, outputTblNameToGrpIdMap);
                        if (outputPartInfoGrouping.size() == 1) {
                            maxActPartColCnt = prefixPartColCnt;
                            maxActSubPartColCnt = prefixSubPartColCnt;
                            findTargetPrefixPartColCnts = true;
                            break;
                        }
                    }
                    if (findTargetPrefixPartColCnts) {
                        break;
                    }
                }
                List<ColumnMeta> minPartColMetas = partByDefOfMinPartCol.getPartitionFieldList();
                for (int i = 0; i < maxActPartColCnt; i++) {
                    maxActPartColMetas.add(minPartColMetas.get(i));
                }
                allLevelMaxPartColMetas.add(maxActPartColMetas);

                if (subPartByDefOfMinPartCol != null && maxActSubPartColCnt > 0) {
                    List<ColumnMeta> minSubPartColMetas = subPartByDefOfMinPartCol.getPartitionFieldList();
                    for (int i = 0; i < maxActSubPartColCnt; i++) {
                        maxActSubPartColMetas.add(minSubPartColMetas.get(i));
                    }
                    allLevelMaxPartColMetas.add(maxActSubPartColMetas);
                }
            }
        }
        return allLevelMaxPartColMetas;
    }

//    public static List<ColumnMeta> getActualPartColumnMetasInfoForTableGroup(String schemaName,
//                                                                             String tableGroupName) {
//        PartitionInfoManager partInfoMgr = OptimizerContext.getContext(schemaName).getPartitionInfoManager();
//        TableGroupInfoManager tgMgr = OptimizerContext.getContext(schemaName).getTableGroupInfoManager();
//        TableGroupConfig tgConfig = tgMgr.getTableGroupConfigByName(tableGroupName);
//        List<TablePartRecordInfoContext> tblInfos = tgConfig.getTables();
//        if (tblInfos.size() == 0) {
//            return new ArrayList<>();
//        }
//        String firstTblName = tblInfos.get(0).getTableName();
//        PartitionInfo partInfoOfFirstTbl = partInfoMgr.getPartitionInfo(firstTblName);
//        int actualPartColCnt = PartitionInfoUtil.getActualPartitionColumns(partInfoOfFirstTbl).size();
//        List<ColumnMeta> fullPartFldList = partInfoOfFirstTbl.getPartitionBy().getPartitionFieldList();
//        List<ColumnMeta> prefixPartFldList = new ArrayList<>();
//        for (int i = 0; i < actualPartColCnt; i++) {
//            prefixPartFldList.add(fullPartFldList.get(i));
//        }
//        return prefixPartFldList;
//    }

    public static List<List<ColumnMeta>> getAllLevelActualPartColumnMetasInfoForTableGroup(String schemaName,
                                                                                           String tableGroupName) {
        PartitionInfoManager partInfoMgr = OptimizerContext.getContext(schemaName).getPartitionInfoManager();
        TableGroupInfoManager tgMgr = OptimizerContext.getContext(schemaName).getTableGroupInfoManager();
        TableGroupConfig tgConfig = tgMgr.getTableGroupConfigByName(tableGroupName);
        List<TablePartRecordInfoContext> tblInfos = tgConfig.getTables();
        if (tblInfos.size() == 0) {
            return new ArrayList<>();
        }
        String firstTblName = tblInfos.get(0).getTableName();
        PartitionInfo partInfoOfFirstTbl = partInfoMgr.getPartitionInfo(firstTblName);
        //int actualPartColCnt = PartitionInfoUtil.getActualPartitionColumns(partInfoOfFirstTbl).size();
        List<List<ColumnMeta>> allLevelActPartColMetas = partInfoOfFirstTbl.getAllLevelActualPartColMetas();
        return allLevelActPartColMetas;
    }

    public static List<TablePartitionRecord> updatePartitionInfoLocalityForTable(String schemaName, String tableName,
                                                                                 String locality) {
        PartitionInfoManager partInfoMgr = OptimizerContext.getContext(schemaName).getPartitionInfoManager();
        PartitionInfo partInfoOfTable = partInfoMgr.getPartitionInfo(tableName);
        List<PartitionSpec> partitionSpecs = partInfoOfTable.getPartitionBy().getPartitions();
        for (PartitionSpec partitionSpec : partitionSpecs) {
            partitionSpec.setLocality(locality);
        }
        List<TablePartitionRecord> partitionRecords = prepareRecordForAllPartitions(partInfoOfTable);
        for (TablePartitionRecord tablePartitionRecord : partitionRecords) {
            tablePartitionRecord.setId(-1L);
            tablePartitionRecord.setParentId(partitionSpecs.get(0).getParentId());
        }

        return partitionRecords;
    }

//    public static List<ColumnMeta> getPrefixPartColumnMetasInfoForTableGroup(String schemaName,
//                                                                             String tableGroupName,
//                                                                             int prefixPartColCnt) {
//        PartitionInfoManager partInfoMgr = OptimizerContext.getContext(schemaName).getPartitionInfoManager();
//        TableGroupInfoManager tgMgr = OptimizerContext.getContext(schemaName).getTableGroupInfoManager();
//        TableGroupConfig tgConfig = tgMgr.getTableGroupConfigByName(tableGroupName);
//        List<TablePartRecordInfoContext> tblInfos = tgConfig.getTables();
//        if (tblInfos.size() == 0) {
//            return new ArrayList<>();
//        }
//        String firstTblName = tblInfos.get(0).getTableName();
//        PartitionInfo partInfoOfFirstTbl = partInfoMgr.getPartitionInfo(firstTblName);
//        List<ColumnMeta> fullPartFldList = partInfoOfFirstTbl.getPartitionBy().getPartitionFieldList();
//        List<ColumnMeta> prefixPartFldList = new ArrayList<>();
//        for (int i = 0; i < prefixPartColCnt; i++) {
//            prefixPartFldList.add(fullPartFldList.get(i));
//        }
//        return prefixPartFldList;
//    }

    /***
     * check if all logical tables are identical in partitioning by specifying the prefix part col count of all part levels
     * <pre>
     *     Notice :
     *      if tbl1 and tbl2 are equal by specifying the prefix part col count of all part level allLevelActualPartitionCols,
     *      that means:
     *          for each partLevel of table k, they must satisfy:
     *              (1) 1 <= prefixPartColsCnt[partLevel] <= maxPartColCnt[k][partLevel]
     *              (2) all partCols definitions from 1 to prefixPartColsCnt[partLevel] and their partSpec definitions are equality.
     *
     * </pre>
     *
     * @param schemaName
     * @param tgName
     * @param allLevelPrefixPartitionCols=PartitionInfoUtil.ALL_LEVEL_FULL_PART_COL_COUNT_LIST,
     *         all partition columns of all level will be involved
     * @return
     */
    public static boolean allTablesWithIdenticalPartitionColumns(String schemaName,
                                                                 String tgName,
                                                                 List<Integer> allLevelPrefixPartitionCols) {

        PartitionInfoManager partInfoMgr = OptimizerContext.getContext(schemaName).getPartitionInfoManager();
        TableGroupInfoManager tgMgr = OptimizerContext.getContext(schemaName).getTableGroupInfoManager();
        TableGroupConfig tgConfig = tgMgr.getTableGroupConfigByName(tgName);

        List<TablePartRecordInfoContext> tblInfos = tgConfig.getTables();
        if (tblInfos.size() == 0) {
            return true;
        }

        String firstTblName = tblInfos.get(0).getTableName();
        PartitionInfo partInfoOfFirstTbl = partInfoMgr.getPartitionInfo(firstTblName);
        //int targetPartCol = partInfoOfFirstTbl.getPartitionBy().getPartitionColumnNameList().size();
        List<Integer> targetAllLevelPartColCnts =
            partInfoOfFirstTbl.getPartitionBy().getAllLevelFullPartColumnCounts();
        List<String> tblNameList = new ArrayList<>();
        tblNameList.add(firstTblName);

        boolean useAllLevelFullPartCol = false;
        if (allLevelPrefixPartitionCols.equals(PartitionInfoUtil.ALL_LEVEL_FULL_PART_COL_COUNT_LIST)) {
            useAllLevelFullPartCol = true;
        }
        for (int i = 1; i < tblInfos.size(); i++) {
            tblNameList.add(tblInfos.get(i).getTableName());
            PartitionInfo partitionInfo = partInfoMgr.getPartitionInfo(tblInfos.get(i).getTableName());
            List<Integer> tmpAllLevelPartColCnts = partitionInfo.getPartitionBy().getAllLevelFullPartColumnCounts();
            if (useAllLevelFullPartCol) {
                if (!tmpAllLevelPartColCnts.equals(targetAllLevelPartColCnts)) {
                    return false;
                }
            }
        }
        List<List<String>> rs =
            classifyTablesByPrefixPartitionColumns(schemaName, tblNameList, allLevelPrefixPartitionCols);
        boolean allTheSame = rs.size() == 1;
        return allTheSame;
    }

    @NotNull
    public static List<Integer> getNewAllLevelPrefixPartColCntByPartInfoAndNewParts(boolean isAlterTableGroup,
                                                                                    PartitionInfo partitionInfo,
                                                                                    List<SqlPartition> newPartitions,
                                                                                    boolean isChangeSubPartOnly) {
        if (isAlterTableGroup) {
            String schemaName = partitionInfo.getTableSchema();
            String tableGroupName = OptimizerContext.getContext(schemaName).getTableGroupInfoManager()
                .getTableGroupConfigById(partitionInfo.getTableGroupId()).getTableGroupRecord().getTg_name();
            return getNewAllLevelPrefixPartColCntByPartInfoAndNewParts(schemaName, tableGroupName,
                partitionInfo.getAllLevelPartitionStrategies(), newPartitions, isChangeSubPartOnly);
        } else {
            return getNewAllLevelPrefixPartColCntByPartInfoAndNewParts(partitionInfo, newPartitions,
                isChangeSubPartOnly);
        }
    }

    @NotNull
    private static List<Integer> getNewAllLevelPrefixPartColCntByPartInfoAndNewParts(PartitionInfo partitionInfo,
                                                                                     List<SqlPartition> newPartitions,
                                                                                     boolean isChangeSubPartOnly) {
        List<Integer> allLevelFullPartColCnts = partitionInfo.getAllLevelFullPartColCounts();
        List<Integer> allLevelActPartColCnts = partitionInfo.getAllLevelActualPartColCounts();

        List<PartitionStrategy> allLevelPartStrategies = partitionInfo.getAllLevelPartitionStrategies();

        GetNewActPartColCntFromAstParams params = new GetNewActPartColCntFromAstParams();
        params.setAllLevelFullPartColCnts(allLevelFullPartColCnts);
        params.setAllLevelActualPartColCnts(allLevelActPartColCnts);
        params.setAllLevelStrategies(allLevelPartStrategies);
        params.setNewPartitions(newPartitions);
        params.setChangeSubpartition(isChangeSubPartOnly);

        List<Integer> newActualPartColCnts = PartitionInfoUtil.getNewAllLevelPrefixPartColCntBySqlPartitionAst(params);

        return newActualPartColCnts;
    }

    @NotNull
    private static List<Integer> getNewAllLevelPrefixPartColCntByPartInfoAndNewParts(String schemaName,
                                                                                     String tableGroupName,
                                                                                     List<PartitionStrategy> allLevelStrategies,
                                                                                     List<SqlPartition> newPartitions,
                                                                                     boolean isChangeSubPartOnly) {
        List<List<ColumnMeta>> allLevelFullPartCols =
            getAllLevelMaxPartColumnMetasInfoForTableGroup(schemaName, tableGroupName);
        List<Integer> allLevelFullPartColCnts =
            allLevelFullPartCols.stream().map(f -> f.size()).collect(Collectors.toList());
        if (allLevelFullPartColCnts.size() == 1) {
            allLevelFullPartColCnts.add(0);
        }

        List<List<ColumnMeta>> allLevelActualPartCols =
            getAllLevelActualPartColumnMetasInfoForTableGroup(schemaName, tableGroupName);
        List<Integer> allLevelActualPartColCnts =
            allLevelActualPartCols.stream().map(a -> a.size()).collect(Collectors.toList());
        if (allLevelActualPartColCnts.size() == 1) {
            allLevelActualPartColCnts.add(0);
        }

        GetNewActPartColCntFromAstParams params = new GetNewActPartColCntFromAstParams();
        params.setAllLevelFullPartColCnts(allLevelFullPartColCnts);
        params.setAllLevelActualPartColCnts(allLevelActualPartColCnts);
        params.setAllLevelStrategies(allLevelStrategies);
        params.setNewPartitions(newPartitions);
        params.setChangeSubpartition(isChangeSubPartOnly);

        List<Integer> newActualPartColCnts = PartitionInfoUtil.getNewAllLevelPrefixPartColCntBySqlPartitionAst(params);

        return newActualPartColCnts;
    }

    protected static List<Integer> getNewAllLevelPrefixPartColCntBySqlPartitionAst(
        GetNewActPartColCntFromAstParams params) {

        List<Integer> allLevelFullPartColCnts = params.getAllLevelFullPartColCnts();
        List<Integer> allLevelActualPartColCnts = params.getAllLevelActualPartColCnts();
        List<PartitionStrategy> allLevelPartStrategies = params.getAllLevelStrategies();
        List<SqlPartition> newPartitions = params.getNewPartitions();

        List<Integer> newActualPartColCnts = new ArrayList<>();

        boolean changeSubPartOnly = params.isChangeSubpartition();
        final int partLevelIdx = 0;
        final int subPartLevelIdx = 1;

        if (allLevelPartStrategies.size() > 2) {
            throw new TddlRuntimeException(ErrorCode.ERR_NOT_SUPPORT);
        }
        if (changeSubPartOnly) {
            newActualPartColCnts.add(allLevelActualPartColCnts.get(partLevelIdx));

            /**
             * That means the new partSpec definition of newPartitions are all for subpartition
             */
            int fullSubPartColCnt = allLevelFullPartColCnts.get(subPartLevelIdx);
            int actSubPartColCnt = allLevelActualPartColCnts.get(subPartLevelIdx);
            PartitionStrategy subpartStrategy = allLevelPartStrategies.get(subPartLevelIdx);
            int newActSubPartColCnt =
                getNewPrefixPartColCntBySqlPartitionAst(fullSubPartColCnt, actSubPartColCnt, subpartStrategy,
                    newPartitions);
            newActualPartColCnts.add(newActSubPartColCnt);
        } else {

            /**
             * That means the new partSpec definition of newPartitions are all for new partitions or new partitions with new subpartitions
             */
            int fullPartColCnt = allLevelFullPartColCnts.get(partLevelIdx);
            int actPartColCnt = allLevelActualPartColCnts.get(partLevelIdx);
            PartitionStrategy partStrategy = allLevelPartStrategies.get(partLevelIdx);
            int newActPartColCnt =
                getNewPrefixPartColCntBySqlPartitionAst(fullPartColCnt, actPartColCnt, partStrategy, newPartitions);
            newActualPartColCnts.add(newActPartColCnt);

            /**
             * Check if alter partitions with defining new subpartitions
             */
            if (allLevelPartStrategies.size() > 1) {
                List<SqlSubPartition> allNewSubPartitions = new ArrayList<>();
                for (SqlPartition partition : newPartitions) {
                    if (GeneralUtil.isNotEmpty(partition.getSubPartitions())) {
                        for (SqlNode sqlNode : partition.getSubPartitions()) {
                            allNewSubPartitions.add((SqlSubPartition) sqlNode);
                        }
                    }
                }
                if (GeneralUtil.isNotEmpty(allNewSubPartitions)) {
                    int fullSubPartColCnt = allLevelFullPartColCnts.get(subPartLevelIdx);
                    int actSubPartColCnt = allLevelActualPartColCnts.get(subPartLevelIdx);
                    PartitionStrategy subPartStrategy = allLevelPartStrategies.get(subPartLevelIdx);

                    int newActSubPartColCnt =
                        getNewPrefixSubPartColCntBySqlPartitionAst(fullSubPartColCnt, actSubPartColCnt, subPartStrategy,
                            allNewSubPartitions);

                    newActualPartColCnts.add(newActSubPartColCnt);
                } else {
                    if (allLevelPartStrategies.size() == 2) {
                        newActualPartColCnts.add(allLevelActualPartColCnts.get(subPartLevelIdx));
                    }
                }
            }
        }
        return newActualPartColCnts;
    }

    public static int getNewPrefixPartColCntBySqlPartitionAst(int fullPartColCnt,
                                                              int actualPartColCnt,
                                                              PartitionStrategy strategy,
                                                              List<SqlPartition> newPartitions) {
        Map<String, Integer> partNameSizes = new TreeMap<>(CaseInsensitive.CASE_INSENSITIVE_ORDER);
        for (SqlPartition partition : newPartitions) {
            if (partition.getValues() != null) {
                partNameSizes.put(partition.getName().toString(), partition.getValues().getItems().size());
            }
        }
        return getNewPrefixPartColCntBySqlAstInner(fullPartColCnt, actualPartColCnt, strategy, partNameSizes);
    }

    public static int getNewPrefixSubPartColCntBySqlPartitionAst(int fullPartColCnt,
                                                                 int actualPartColCnt,
                                                                 PartitionStrategy strategy,
                                                                 List<SqlSubPartition> newSubPartitions) {
        Map<String, Integer> partNameSizes = new TreeMap<>(CaseInsensitive.CASE_INSENSITIVE_ORDER);
        for (SqlSubPartition subPartition : newSubPartitions) {
            if (subPartition.getValues() != null) {
                partNameSizes.put(subPartition.getName().toString(), subPartition.getValues().getItems().size());
            }
        }
        return getNewPrefixPartColCntBySqlAstInner(fullPartColCnt, actualPartColCnt, strategy, partNameSizes);
    }

    private static int getNewPrefixPartColCntBySqlAstInner(int fullPartColCnt,
                                                           int actualPartColCnt,
                                                           PartitionStrategy strategy,
                                                           Map<String, Integer> newPartBndValColCnts) {
        int newPrefixPartColCnt = PartitionInfoUtil.FULL_PART_COL_COUNT;
        if (strategy == PartitionStrategy.KEY || strategy == PartitionStrategy.RANGE_COLUMNS) {
            int lastPartValColSize = PartitionInfoUtil.FULL_PART_COL_COUNT;
            int valPartColSize;
            for (Map.Entry<String, Integer> entry : newPartBndValColCnts.entrySet()) {
                valPartColSize = entry.getValue();
                String partName = entry.getKey();
                newPrefixPartColCnt =
                    checkAndGetNewPrefixPartColCnt(fullPartColCnt, actualPartColCnt, lastPartValColSize, valPartColSize,
                        partName);
                if (lastPartValColSize == PartitionInfoUtil.FULL_PART_COL_COUNT) {
                    lastPartValColSize = valPartColSize;
                }
            }
        }
        return newPrefixPartColCnt;
    }

    private static int checkAndGetNewPrefixPartColCnt(int fullPartColCnt,
                                                      int actualPartColCnt,
                                                      int lastPartValColSize,
                                                      int valPartColSize,
                                                      String partName) {
        int newPrefixPartColCnt = PartitionInfoUtil.FULL_PART_COL_COUNT;

        if (valPartColSize != lastPartValColSize && lastPartValColSize != -1) {
            throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                String.format("the column count[%s] of bound values of partition %s is not allowed to be different",
                    valPartColSize, partName));
        }

        if (valPartColSize > fullPartColCnt) {
            throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT, String.format(
                "the column count[%s] of bound values of partition %s is more than the full partition columns count[%s]",
                valPartColSize, partName == null ? "" : partName, fullPartColCnt));
        }

        if (newPrefixPartColCnt == PartitionInfoUtil.FULL_PART_COL_COUNT) {
            newPrefixPartColCnt = valPartColSize;
        } else {
            if (newPrefixPartColCnt != valPartColSize) {
                throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                    String.format("the column count[%s] of bound values is not allowed to be different",
                        valPartColSize));
            }
        }
        return newPrefixPartColCnt;
    }

    private static void changePartitionNameAndLocality(PartitionInfo basePartitionInfo,
                                                       PartitionByDefinition partitionBy,
                                                       String partitionNamePrefix,
                                                       boolean operateOnSubPartition,
                                                       ComplexTaskMetaManager.ComplexTaskType taskType,
                                                       Map<String, String> outNewPartNamesMap,
                                                       Map<String, Map<String, String>> outSubNewPartNamesMap) {
        if (basePartitionInfo == null || partitionBy == null) {
            return;
        }
        PartitionByDefinition basePartitionBy = basePartitionInfo.getPartitionBy();
        if (basePartitionBy.getStrategy() != partitionBy.getStrategy()
            || basePartitionBy.getPartitions().size() != partitionBy.getPartitions().size()) {
            return;
        }
        boolean hasSubPart = partitionBy.getSubPartitionBy() != null;
        if (hasSubPart) {
            if (basePartitionBy.getSubPartitionBy() == null
                || partitionBy.getSubPartitionBy().getStrategy() != basePartitionBy.getSubPartitionBy().getStrategy()
                || partitionBy.getPhysicalPartitions().size() != basePartitionBy.getPhysicalPartitions().size()
                || partitionBy.getSubPartitionBy().isUseSubPartTemplate() != basePartitionBy.getSubPartitionBy()
                .isUseSubPartTemplate()) {
                return;
            }
        }
        if (!hasSubPart && basePartitionBy.getSubPartitionBy() != null) {
            return;
        }
        Boolean changePartInfo = true;
        if (StringUtils.isNotEmpty(partitionNamePrefix)) {
            for (int i = 0; i < partitionBy.getPartitions().size(); i++) {
                PartitionSpec partitionSpec = partitionBy.getPartitions().get(i);
                if (!partitionSpec.isLogical() && !partitionSpec.getLocation().isVisiable()) {
                    PartitionSpec basePartSpec = basePartitionBy.getPartitions().get(i);
                    String newPartName = basePartSpec.getName();
                    if (StringUtils.isNotEmpty(partitionNamePrefix) && newPartName.startsWith(partitionNamePrefix)) {
                        if (newPartName.length() > partitionNamePrefix.length()) {
                            String digitString = newPartName.substring(partitionNamePrefix.length());
                            try {
                                Integer.parseInt(digitString);
                            } catch (NumberFormatException ex) {
                                changePartInfo = false;
                                break;
                            }
                        }
                    }
                }
            }
        }

        if (changePartInfo) {
            boolean useSubPartTemplate =
                partitionBy.getSubPartitionBy() != null && partitionBy.getSubPartitionBy().isUseSubPartTemplate();
            for (int i = 0; i < partitionBy.getPhysicalPartitions().size(); i++) {
                PartitionSpec partitionSpec = partitionBy.getPhysicalPartitions().get(i);
                if (!partitionSpec.isLogical() && !partitionSpec.getLocation().isVisiable()) {
                    PartitionSpec basePartSpec = basePartitionBy.getPhysicalPartitions().get(i);

                    String originNewSubPartName =
                        (useSubPartTemplate && operateOnSubPartition) ? partitionSpec.getTemplateName() :
                            partitionSpec.getName();
                    String targetNewSubPartName =
                        (useSubPartTemplate && operateOnSubPartition) ? basePartSpec.getTemplateName() :
                            basePartSpec.getName();

                    partitionSpec.setName(basePartSpec.getName());
                    partitionSpec.setTemplateName(basePartSpec.getTemplateName());
                    partitionSpec.setLocality(basePartSpec.getLocality());

                    if (hasSubPart) {
                        if (!operateOnSubPartition) {
                            String originNewPartName =
                                partitionBy.getNthPartition(partitionSpec.getParentPartPosi().intValue()).getName();
                            String targetNewPartName =
                                basePartitionBy.getNthPartition(basePartSpec.getParentPartPosi().intValue()).getName();
                            if (!outNewPartNamesMap.containsKey(originNewPartName)) {
                                outNewPartNamesMap.put(originNewPartName, targetNewPartName);
                            }
                            outSubNewPartNamesMap.computeIfAbsent(originNewPartName,
                                    o -> new TreeMap<>(String.CASE_INSENSITIVE_ORDER))
                                .put(originNewSubPartName, targetNewSubPartName);
                        } else {
                            if (!outNewPartNamesMap.containsKey(originNewSubPartName)) {
                                outNewPartNamesMap.put(originNewSubPartName, targetNewSubPartName);
                            }
                        }
                    }

                }
            }

            boolean changeLogicalPartitionName =
                !operateOnSubPartition && hasSubPart && (ComplexTaskMetaManager.ComplexTaskType.MERGE_PARTITION
                    != taskType);
            if (changeLogicalPartitionName) {
                for (int i = 0; i < partitionBy.getPhysicalPartitions().size(); i++) {
                    PartitionSpec partitionSpec = partitionBy.getPhysicalPartitions().get(i);
                    if (!partitionSpec.isLogical() && !partitionSpec.getLocation().isVisiable()) {
                        PartitionSpec basePartSpec = basePartitionBy.getPhysicalPartitions().get(i);
                        String originNewPartName =
                            partitionBy.getNthPartition(partitionSpec.getParentPartPosi().intValue()).getName();
                        String targetNewPartName =
                            basePartitionBy.getNthPartition(basePartSpec.getParentPartPosi().intValue()).getName();
                        if (!originNewPartName.equalsIgnoreCase(targetNewPartName)) {
                            partitionBy.getNthPartition(partitionSpec.getParentPartPosi().intValue())
                                .setName(targetNewPartName);
                        }
                    }
                }
            }
            if (useSubPartTemplate) {
                PartitionSpec partitionSpec = partitionBy.getNthPartition(1);
                List<PartitionSpec> subPartitionSpecs =
                    PartitionInfoUtil.updateTemplatePartitionSpec(partitionSpec.getSubPartitions(),
                        partitionBy.getSubPartitionBy().getStrategy());
                partitionBy.getSubPartitionBy().setPartitions(subPartitionSpecs);
                partitionBy.getSubPartitionBy().getPhysicalPartitions().clear();
            }
        }
    }

    private static boolean comparePartitionInfoLocation(PartitionInfo basePartitionInfo, PartitionInfo partitionInfo,
                                                        boolean compareNewLocation) {
        PartitionByDefinition basePartitionBy = basePartitionInfo.getPartitionBy();
        PartitionByDefinition partitionBy = partitionInfo.getPartitionBy();

        List<PartitionSpec> basePhySpecs = basePartitionBy.getPhysicalPartitions();
        List<PartitionSpec> phySpecs = partitionBy.getPhysicalPartitions();

        assert basePhySpecs.size() == phySpecs.size();
        for (int i = 0; i < phySpecs.size(); i++) {
            PartitionSpec partitionSpec = phySpecs.get(i);
            PartitionSpec basePartitionSpec = basePhySpecs.get(i);
            if (compareNewLocation) {
                if (!partitionSpec.getLocation().isVisiable() && !basePartitionSpec.getLocation().getGroupKey()
                    .equalsIgnoreCase(partitionSpec.getLocation().getGroupKey())) {
                    return false;
                }
            }
            if (partitionSpec.getLocation().isVisiable() && !basePartitionSpec.getLocation().getGroupKey()
                .equalsIgnoreCase(partitionSpec.getLocation().getGroupKey())) {
                return false;
            }
        }
        return true;
    }

    public static List<PartitionSpec> getAllPhyPartsWithSameParentByPhyPartName(PartitionInfo partInfo,
                                                                                String phyPartName) {
        PartitionSpec tarPhyPart = partInfo.getPartSpecSearcher().getPartSpecByPartName(phyPartName);
        if (tarPhyPart == null) {
            return new ArrayList<>();
        }

        PartKeyLevel phyPartKeyLevel = tarPhyPart.getPartLevel();
        if (phyPartKeyLevel == PartKeyLevel.PARTITION_KEY) {
            return partInfo.getPartitionBy().getPhysicalPartitions();
        }

        assert phyPartKeyLevel == PartKeyLevel.SUBPARTITION_KEY;
        int parentPosi = tarPhyPart.getParentPartPosi().intValue();
        PartitionSpec parentSpec = partInfo.getPartitionBy().getPartitions().get(parentPosi - 1);
        if (parentSpec == null) {
            throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_INVALID_PARAMS, "Found invalid subpartition");
        }
        return parentSpec.getSubPartitions();
    }

    public static Set<String> checkAndExpandPartitions(PartitionInfo partitionInfo,
                                                       TableGroupConfig tableGroupConfig,
                                                       Set<String> oldPartitionNames,
                                                       boolean isSubPartitionChanged) {
        PartitionByDefinition partByDef = partitionInfo.getPartitionBy();
        PartitionByDefinition subPartByDef = partByDef.getSubPartitionBy();

        Set<String> actualPartitionNames = new TreeSet<>(String::compareToIgnoreCase);

        for (String oldPartitionName : oldPartitionNames) {
            boolean isPartition = false, isSubPartition = false;
            final List<String> subPartitions = new ArrayList<>();

            Optional<PartitionSpec> partitionSpec =
                partByDef.getPartitions().stream().filter(p -> p.getName().equalsIgnoreCase(oldPartitionName))
                    .findFirst();

            if (partitionSpec.isPresent()) {
                isPartition = true;
                if (subPartByDef != null) {
                    partitionSpec.get().getSubPartitions().forEach(sp -> subPartitions.add(sp.getName().toLowerCase()));
                }
            } else if (subPartByDef != null) {
                for (PartitionSpec partSpec : partByDef.getPartitions()) {
                    Optional<PartitionSpec> subPartSpec = partSpec.getSubPartitions().stream()
                        .filter(sp -> sp.getName().equalsIgnoreCase(oldPartitionName)).findFirst();
                    if (subPartSpec.isPresent()) {
                        isSubPartition = true;
                        break;
                    }
                }
                if (!isSubPartition && subPartByDef.isUseSubPartTemplate()) {
                    Optional<PartitionSpec> subPartTemplateSpec = subPartByDef.getPartitions().stream()
                        .filter(spt -> spt.getName().equalsIgnoreCase(oldPartitionName)).findFirst();
                    if (subPartTemplateSpec.isPresent()) {
                        isSubPartition = true;
                        partByDef.getPartitions().forEach(p -> subPartitions.add(
                            PartitionNameUtil.autoBuildSubPartitionName(p.getName(), oldPartitionName)));
                    }
                }
            }

            if (!isPartition && !isSubPartition) {
                throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_NAME_NOT_EXISTS,
                    String.format("Partition '%s' doesn't exist", oldPartitionName));
            }

            if (isPartition && isSubPartitionChanged) {
                throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_INVALID_PARAMS,
                    String.format("'%s' is not a subpartition", oldPartitionName));
            }

            if (isSubPartition && !isSubPartitionChanged) {
                throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_INVALID_PARAMS,
                    String.format("'%s' is not a partition", oldPartitionName));
            }

            if (GeneralUtil.isNotEmpty(subPartitions)) {
                actualPartitionNames.addAll(subPartitions);
            }
        }

        if (GeneralUtil.isEmpty(actualPartitionNames)) {
            actualPartitionNames.addAll(oldPartitionNames);
        }

        // Double check
        checkPartitionGroupNames(tableGroupConfig, actualPartitionNames);

        return actualPartitionNames;
    }

    public static SqlSubPartition buildDefaultSubPartitionForKey(String partName, int numericSuffix) {
        String defaultSubPartName = buildDefaultSubPartName(partName, numericSuffix);
        return buildDefaultSubPartitionForKey(defaultSubPartName);
    }

    public static SqlSubPartition buildDefaultSubPartitionForRange(String partName, int numericSuffix,
                                                                   int subPartColCount) {
        String defaultSubPartName = buildDefaultSubPartName(partName, numericSuffix);
        return buildDefaultSubPartitionForRange(defaultSubPartName, subPartColCount);
    }

    public static SqlSubPartition buildDefaultSubPartitionForList(String partName, int numericSuffix) {
        String defaultSubPartName = buildDefaultSubPartName(partName, numericSuffix);
        return buildDefaultSubPartitionForList(defaultSubPartName);
    }

    public static SqlSubPartition buildDefaultSubPartitionForKey(String defaultSubPartName) {
        SqlIdentifier defaultSubPartNameNode = new SqlIdentifier(defaultSubPartName, SqlParserPos.ZERO);
        return new SqlSubPartition(SqlParserPos.ZERO, defaultSubPartNameNode, null);
    }

    public static SqlSubPartition buildDefaultSubPartitionForRange(String defaultSubPartName, int subPartColCount) {
        SqlIdentifier defaultSubPartNameNode = new SqlIdentifier(defaultSubPartName, SqlParserPos.ZERO);

        SqlPartitionValue defaultSubPartValue =
            new SqlPartitionValue(SqlPartitionValue.Operator.LessThan, SqlParserPos.ZERO);

        for (int i = 0; i < subPartColCount; i++) {
            SqlPartitionValueItem valueItem =
                new SqlPartitionValueItem(new SqlIdentifier("MAXVALUE", SqlParserPos.ZERO));
            valueItem.setMaxValue(true);
            defaultSubPartValue.getItems().add(valueItem);
        }

        return new SqlSubPartition(SqlParserPos.ZERO, defaultSubPartNameNode, defaultSubPartValue);
    }

    public static SqlSubPartition buildDefaultSubPartitionForList(String defaultSubPartName) {
        SqlIdentifier defaultSubPartNameNode = new SqlIdentifier(defaultSubPartName, SqlParserPos.ZERO);

        SqlPartitionValue defaultSubPartValue = new SqlPartitionValue(SqlPartitionValue.Operator.In, SqlParserPos.ZERO);

        SqlPartitionValueItem valueItem =
            new SqlPartitionValueItem(SqlStdOperatorTable.DEFAULT.createCall(SqlParserPos.ZERO));
        defaultSubPartValue.getItems().add(valueItem);

        return new SqlSubPartition(SqlParserPos.ZERO, defaultSubPartNameNode, defaultSubPartValue);
    }

    private static String buildDefaultSubPartName(String partName, int numericSuffix) {
        String subPartTemplateName = PartitionNameUtil.autoBuildSubPartitionTemplateName(Long.valueOf(numericSuffix));
        return PartitionNameUtil.autoBuildSubPartitionName(partName, subPartTemplateName);
    }

    public static void checkPartitionGroupNames(TableGroupConfig tableGroupConfig, Set<String> partitionGroupNames) {
        Set<String> allPartitionGroupNames = getAllPartitionGroupNames(tableGroupConfig);
        for (String partitionGroupName : partitionGroupNames) {
            if (!allPartitionGroupNames.contains(partitionGroupName.toLowerCase())) {
                throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                    "Partition group '" + partitionGroupName + "' doesn't exist");
            }
        }
    }

    public static Set<String> getAllPartitionGroupNames(TableGroupConfig tableGroupConfig) {
        return tableGroupConfig.getPartitionGroupRecords().stream().map(o -> o.partition_name.toLowerCase())
            .collect(Collectors.toSet());
    }

    public static List<PartitionSpec> updateTemplatePartitionSpec(List<PartitionSpec> subPartitionSpecs,
                                                                  PartitionStrategy strategy) {
        List<PartitionSpec> newSubPartitionSpecs = new ArrayList<>();
        for (PartitionSpec subPartitionSpec : subPartitionSpecs) {
            PartitionSpec newSubPartitionSpec = subPartitionSpec.copy();
            newSubPartitionSpec.setName(newSubPartitionSpec.getTemplateName());
            newSubPartitionSpec.setPartLevel(PartKeyLevel.SUBPARTITION_KEY);
            newSubPartitionSpec.setStrategy(strategy);
            newSubPartitionSpec.setLogical(true);
            newSubPartitionSpec.setLocation(null);
            newSubPartitionSpec.setSpecTemplate(true);
            newSubPartitionSpec.setPhyPartPosition(0L);
            newSubPartitionSpec.setEngine(null);

            newSubPartitionSpecs.add(newSubPartitionSpec);
        }
        return newSubPartitionSpecs;
    }

}
