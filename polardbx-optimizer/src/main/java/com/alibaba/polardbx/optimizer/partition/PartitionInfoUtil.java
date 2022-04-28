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
import com.alibaba.polardbx.gms.tablegroup.PartitionGroupRecord;
import com.alibaba.polardbx.gms.tablegroup.TableGroupConfig;
import com.alibaba.polardbx.gms.tablegroup.TableGroupLocation;
import com.alibaba.polardbx.gms.tablegroup.TableGroupRecord;
import com.alibaba.polardbx.gms.tablegroup.TableGroupUtils;
import com.alibaba.polardbx.gms.topology.DbGroupInfoManager;
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.gms.util.GroupInfoUtil;
import com.alibaba.polardbx.gms.util.PartitionNameUtil;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.TddlOperatorTable;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypeUtil;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.exception.OptimizerException;
import com.alibaba.polardbx.optimizer.hint.util.HintUtil;
import com.alibaba.polardbx.optimizer.parse.custruct.FastSqlConstructUtils;
import com.alibaba.polardbx.optimizer.parse.visitor.ContextParameters;
import com.alibaba.polardbx.optimizer.partition.datatype.function.PartitionIntFunction;
import com.alibaba.polardbx.optimizer.partition.pruning.PartitionPrunerUtils;
import com.alibaba.polardbx.optimizer.partition.pruning.SearchDatumComparator;
import com.alibaba.polardbx.optimizer.partition.pruning.SearchDatumInfo;
import com.alibaba.polardbx.optimizer.rule.TddlRuleManager;
import com.alibaba.polardbx.optimizer.tablegroup.TableGroupInfoManager;
import com.alibaba.polardbx.optimizer.utils.KeyWordsUtil;
import com.alibaba.polardbx.optimizer.utils.RelUtils;
import com.alibaba.polardbx.rule.TableRule;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlAlterTableDropPartition;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlPartition;
import org.apache.calcite.sql.SqlPartitionValue;
import org.apache.calcite.sql.SqlPartitionValueItem;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.util.SqlShuttle;
import org.apache.calcite.util.Pair;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;

import java.sql.Connection;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static com.alibaba.polardbx.common.ddl.newengine.DdlConstants.MAX_TABLE_NAME_LENGTH_MYSQL_ALLOWS;
import static com.alibaba.polardbx.common.ddl.newengine.DdlConstants.RANDOM_SUFFIX_LENGTH_OF_PHYSICAL_TABLE_NAME;
import static com.alibaba.polardbx.optimizer.partition.PartitionLocator.PHYSICAL_TABLENAME_PATTERN;

/**
 * @author chenghui.lch
 */
public class PartitionInfoUtil {

    static Pattern IS_NUMBER = Pattern.compile("^[\\d]*$");
    static String MAXVALUE = "MAXVALUE";
    final static long MAX_HASH_VALUE = Long.MAX_VALUE;
    final static long MIN_HASH_VALUE = Long.MIN_VALUE;
    public static final int FULL_PART_COL_COUNT = -1;

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
            record.nextLevel = TablePartitionRecord.PARTITION_LEVEL_PARTITION;
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
        partExtras.setLocality("");
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
            TablePartitionRecord record = prepareRecordForOnePartition(partitionInfo, partitionSpec);
            partRecords.add(record);
        }

        return partRecords;
    }

    public static TablePartitionRecord prepareRecordForOnePartition(PartitionInfo partitionInfo,
                                                                    PartitionSpec partitionSpec) {
        TablePartitionRecord record = new TablePartitionRecord();

        // The string of partition expression should be normalization
        String partExprListStr = "";
        List<SqlNode> partExprList = partitionInfo.getPartitionBy().getPartitionExprList();
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

        record.tableSchema = partitionInfo.tableSchema;
        record.tableName = partitionInfo.tableName;
        record.groupId = partitionSpec.getLocation().getPartitionGroupId();
        record.spTempFlag = TablePartitionRecord.SUBPARTITION_TEMPLATE_NOT_EXISTED;
        record.metaVersion = partitionInfo.metaVersion;
        record.autoFlag = TablePartitionRecord.PARTITION_AUTO_BALANCE_DISABLE;
        record.tblType = partitionInfo.getTableType().getTableTypeIntValue();
        record.partLevel = TablePartitionRecord.PARTITION_LEVEL_PARTITION;
        if (partitionInfo.getSubPartitionBy() != null) {
            record.nextLevel = TablePartitionRecord.PARTITION_LEVEL_SUBPARTITION;
        } else {
            record.nextLevel = TablePartitionRecord.PARTITION_LEVEL_NO_SUBPARTITION;
        }
        record.partStatus = TablePartitionRecord.PARTITION_STATUS_LOGICAL_TABLE_ABSENT;

        record.partName = partitionSpec.name;
        record.partTempName = "";
        record.partPosition = Long.valueOf(partitionSpec.position);

        // covert partition strategy to string
        record.partMethod = partitionSpec.strategy.toString();
        record.partExpr = partExprListStr;

        record.partDesc =
            partitionSpec.getBoundSpec().getPartitionBoundDescription(PartitionInfoUtil.FULL_PART_COL_COUNT);

        ExtraFieldJSON partExtras = new ExtraFieldJSON();
        partExtras.setLocality(partitionSpec.getLocality());
        record.partExtras = partExtras;

        record.partFlags = 0L;
        record.partComment = "";

        // fill location info
        if (record.nextLevel != TablePartitionRecord.PARTITION_LEVEL_NO_SUBPARTITION) {
            record.phyTable = "";
            record.partEngine = "";
        } else {
            record.partEngine = partitionSpec.getEngine();
            record.phyTable = partitionSpec.getLocation().getPhyTableName();
        }
        return record;
    }

    public static Map<String, List<TablePartitionRecord>> prepareRecordForAllSubpartitions(
        List<TablePartitionRecord> parentRecords, PartitionInfo partitionInfo, List<PartitionSpec> partitionSpecs) {

        Map<String, List<TablePartitionRecord>> subPartRecordInfos = new HashMap<>();
        if (partitionInfo.getSubPartitionBy() == null) {
            return subPartRecordInfos;
        }

        assert parentRecords.size() == partitionSpecs.size();
        for (int k = 0; k < parentRecords.size(); k++) {

            TablePartitionRecord parentRecord = parentRecords.get(k);
            PartitionSpec partitionSpec = partitionSpecs.get(k);
            List<SubPartitionSpec> subPartitionSpecs = partitionSpec.getSubPartitions();

            List<TablePartitionRecord> subPartRecList = new ArrayList<>();

            // The string of partition expression should be normalization
            String subPartExprListStr = "";
            List<SqlNode> subPartExprList = partitionInfo.getSubPartitionBy().getSubPartitionExprList();
            for (int i = 0; i < subPartExprList.size(); i++) {
                SqlNode partExpr = subPartExprList.get(i);
                String partExprStr = partExpr.toString();
                if (!subPartExprListStr.isEmpty()) {
                    subPartExprListStr += ",";
                }
                subPartExprListStr += partExprStr;
            }

            for (int i = 0; i < partitionSpecs.size(); i++) {
                SubPartitionSpec subPartitionSpec = subPartitionSpecs.get(i);
                SubPartitionSpecTemplate subPartitionSpecTemp = subPartitionSpec.getSubPartitionSpecInfo();
                TablePartitionRecord record = new TablePartitionRecord();

                record.tableSchema = partitionInfo.tableSchema;
                record.tableName = partitionInfo.tableName;

                record.groupId = -1L;
                record.spTempFlag = TablePartitionRecord.SUBPARTITION_TEMPLATE_NOT_EXISTED;
                record.metaVersion = partitionInfo.metaVersion;
                record.autoFlag = TablePartitionRecord.PARTITION_AUTO_BALANCE_DISABLE;
                record.tblType = partitionInfo.getTableType().getTableTypeIntValue();

                record.partLevel = TablePartitionRecord.PARTITION_LEVEL_SUBPARTITION;
                record.nextLevel = TablePartitionRecord.PARTITION_LEVEL_NO_SUBPARTITION;
                record.partStatus = TablePartitionRecord.PARTITION_STATUS_LOGICAL_TABLE_ABSENT;

                record.partName = subPartitionSpec.getName();
                record.partTempName = subPartitionSpecTemp.getPartTempName();
                record.partPosition = Long.valueOf(subPartitionSpec.getSubPartitionSpecInfo().getPosition());

                // covert partition strategy to string
                record.partMethod = partitionInfo.getSubPartitionBy().getStrategy().toString();
                record.partExpr = subPartExprListStr;

                // This should be support more partition method later
                if (partitionInfo.getSubPartitionBy().getStrategy() == PartitionStrategy.RANGE) {
                    RangeBoundSpec rangeBoundSpec =
                        (RangeBoundSpec) subPartitionSpec.getSubPartitionSpecInfo().getBoundSpec();
                    record.partDesc = String.valueOf(rangeBoundSpec.getRAWValue());
                }

                record.partEngine = subPartitionSpec.getSubPartitionSpecInfo().getEngine();
                record.partExtras = genPartExtrasRecord(partitionInfo);
                record.partFlags = 0L;
                record.partComment = "";

                // fill location info
                record.phyTable = subPartitionSpec.getLocation().getPhyTableName();

                subPartRecList.add(record);
            }
            subPartRecordInfos.put(parentRecord.partName, subPartRecList);
        }
        return subPartRecordInfos;
    }

    public static TableGroupRecord prepareRecordForTableGroup(PartitionInfo partitionInfo) {
        return prepareRecordForTableGroup(partitionInfo, false);
    }

    public static TableGroupRecord prepareRecordForTableGroup(PartitionInfo partitionInfo, boolean isOss) {
        TableGroupRecord tableGroupRecord = new TableGroupRecord();
        tableGroupRecord.schema = partitionInfo.tableSchema;
        tableGroupRecord.meta_version = 0L;
        if (isOss) {
            tableGroupRecord.tg_type = TableGroupRecord.TG_TYPE_OSS_TBL_TG;
            return tableGroupRecord;
        }
        if (partitionInfo.getTableType() == PartitionTableType.SINGLE_TABLE
            || partitionInfo.getTableType() == PartitionTableType.GSI_SINGLE_TABLE) {
            if (partitionInfo.getTableGroupId() == TableGroupRecord.INVALID_TABLE_GROUP_ID) {
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

    public static List<PartitionGroupRecord> prepareRecordForPartitionGroups(
        List<PartitionSpec> partitionSpecs) {
        List<PartitionGroupRecord> partitionGroupRecords = new ArrayList<>();
        for (PartitionSpec partitionSpec : partitionSpecs) {
            PartitionLocation location = partitionSpec.getLocation();
            PartitionGroupRecord partitionGroupRecord = new PartitionGroupRecord();
            partitionGroupRecord.phy_db =
                GroupInfoUtil.buildPhysicalDbNameFromGroupName(location.getGroupKey());
            partitionGroupRecord.partition_name = partitionSpec.name;
            partitionGroupRecord.id = location.partitionGroupId;
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
            SqlNode sqlNodePartExpr = FastSqlConstructUtils.convertToSqlNode(expr, context, null);
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

    protected static String findPartitionColumn(SqlNode partExprSqlNode) {

        PartitionColumnFinder finder = new PartitionColumnFinder();
        boolean isFound = finder.find(partExprSqlNode);

        if (!isFound) {
            return null;
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

        int maxAllowedLengthOfTableNamePrefix = MAX_TABLE_NAME_LENGTH_MYSQL_ALLOWS
            - RANDOM_SUFFIX_LENGTH_OF_PHYSICAL_TABLE_NAME - numExtraUnderScores
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

    public static void generatePartitionLocation(PartitionInfo partitionInfo, String tableGroupName,
                                                 ExecutionContext executionContext) {

        PartitionTableType tblType = partitionInfo.getTableType();
        TableGroupConfig tableGroupConfig = getTheBestTableGroupInfo(partitionInfo, tableGroupName, executionContext);
        String schema = partitionInfo.getTableSchema();
        String logTbName = partitionInfo.getPrefixTableName();
        boolean phyTblNameSameAsLogicalTblName = false;
        if (tblType == PartitionTableType.SINGLE_TABLE || tblType == PartitionTableType.BROADCAST_TABLE
            || tblType == PartitionTableType.GSI_SINGLE_TABLE || tblType == PartitionTableType.GSI_BROADCAST_TABLE) {
            phyTblNameSameAsLogicalTblName = true;
        }
        // haven't existing table group mapping to current partition
        if (tableGroupConfig == null || tableGroupConfig.isEmpty()) {
            if (tableGroupConfig != null) {
                partitionInfo.setTableGroupId(tableGroupConfig.getTableGroupRecord().id);
            }
            TableGroupLocation.GroupAllocator groupAllocator =
                tableGroupConfig != null && tableGroupConfig.getLocalityDesc() != null ?
                    TableGroupLocation.buildGroupAllocatorByLocality(schema, tableGroupConfig.getLocalityDesc()) :
                    TableGroupLocation.buildGroupAllocator(schema);

            List<PartitionSpec> partitionSpecs = partitionInfo.getPartitionBy().getOrderedPartitionSpec();
            List<String> allPhyGrpList = HintUtil.allGroup(schema);
            if (tblType == PartitionTableType.BROADCAST_TABLE || tblType == PartitionTableType.GSI_BROADCAST_TABLE) {
                if (allPhyGrpList.size() != partitionSpecs.size()) {
                    throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_INVALID_PARAMS,
                        String.format(
                            "Failed to create table group for the broadcast table [%s] because the number of partitions mismatch the phy group count",
                            logTbName));
                }
            }
            for (int i = 0; i < partitionSpecs.size(); i++) {
                PartitionSpec partSpec = partitionSpecs.get(i);
                String partName = partSpec.getName();
                PartitionLocation location = new PartitionLocation();
                String partPhyTbName;
                if (phyTblNameSameAsLogicalTblName) {
                    partPhyTbName = logTbName;
                } else {
                    partPhyTbName =
                        PartitionNameUtil.autoBuildPartitionPhyTableName(logTbName, partSpec.getPosition() - 1);
                }

                String grpKey;
                if (tblType == PartitionTableType.BROADCAST_TABLE
                    || tblType == PartitionTableType.GSI_BROADCAST_TABLE) {
                    if (!ConfigDataMode.getMode().isMock()
                        && !DbGroupInfoManager.isNormalGroup(schema, allPhyGrpList.get(i))) {
                        throw new TddlRuntimeException(ErrorCode.ERR_PHYSICAL_TOPOLOGY_CHANGING,
                            String.format("the physical group[%s] is changing, please retry this command later",
                                allPhyGrpList.get(i)));
                    }
                    grpKey = allPhyGrpList.get(i);
                } else if (TStringUtil.isNotEmpty(partSpec.getLocality())) {
                    LocalityDesc locality = LocalityDesc.parse(partSpec.getLocality());
                    TableGroupLocation.GroupAllocator pgGroupAllocator =
                        TableGroupLocation.buildGroupAllocatorByLocality(schema, locality);
                    grpKey = pgGroupAllocator.allocate();
                } else {
                    grpKey = groupAllocator.allocate();
                }
                Long pgId = PartitionLocation.INVALID_PARTITION_GROUP_ID;
                location.setPhyTableName(partPhyTbName);
                location.setGroupKey(grpKey);
                location.setPartitionGroupId(pgId);

                PartitionSpec originalPartSpec = partitionInfo.getPartitionBy().getPartitions().get(i);
                if (!originalPartSpec.getName().equalsIgnoreCase(partSpec.getName())) {
                    originalPartSpec = partitionInfo.getPartitionBy().getPartitions().stream()
                        .filter(o -> o.position.equals(partSpec.position))
                        .findFirst().orElse(null);
                }
                if (originalPartSpec == null) {
                    throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_INVALID_PARAMS,
                        String.format("Can NOT find the right partition group for the partition name[%s]", partName));
                }
                originalPartSpec.setLocation(location);
            }
        } else {
            assert tableGroupConfig.getPartitionGroupRecords().size() == partitionInfo.getPartitionBy().getPartitions()
                .size();
            List<PartitionSpec> partitionSpecs = partitionInfo.getPartitionBy().getPartitions();
            partitionInfo.setTableGroupId(tableGroupConfig.getTableGroupRecord().id);
            boolean usePartitionGroupName = false;
            if (partitionInfo.isGsiBroadcastOrBroadcast()) {
                usePartitionGroupName = broadCastPartitionGroupNameChange(partitionInfo, tableGroupConfig);
            }
            for (int i = 0; i < partitionSpecs.size(); i++) {
                final PartitionSpec partSpec = partitionSpecs.get(i);
                String partName = partSpec.getName();
                PartitionGroupRecord partitionGroupRecord = tableGroupConfig.getPartitionGroupRecords().get(i);
                if (!partitionGroupRecord.getPartition_name().equalsIgnoreCase(partSpec.getName())
                    && !usePartitionGroupName) {
                    partitionGroupRecord = tableGroupConfig.getPartitionGroupRecords().stream()
                        .filter(o -> o.partition_name.equalsIgnoreCase(partName)).findFirst().orElse(null);
                }
                if (usePartitionGroupName) {
                    partitionGroupRecord = tableGroupConfig.getPartitionGroupRecords().get(i);
                    partSpec.setName(partitionGroupRecord.getPartition_name());
                }
                PartitionLocation location = new PartitionLocation();
                if (partitionGroupRecord != null) {
                    String partPhyTbName =
                        PartitionNameUtil.autoBuildPartitionPhyTableName(logTbName, partSpec.getPosition() - 1);
                    if (phyTblNameSameAsLogicalTblName) {
                        partPhyTbName = logTbName;
                    }
                    String grpKey = GroupInfoUtil.buildGroupNameFromPhysicalDb(partitionGroupRecord.phy_db);
                    Long pgId = partitionGroupRecord.id;
                    location.setPhyTableName(partPhyTbName);
                    location.setGroupKey(grpKey);
                    location.setPartitionGroupId(pgId);
                    partSpec.setLocation(location);
                } else {
                    throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_INVALID_PARAMS,
                        String.format("Can NOT find the right partition group for the partition name[%s]", partName));
                }
            }
        }
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
                for (SubPartitionSpec subPartitionSpec : partitionSpec.getSubPartitions()) {
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

    public static void validatePartitionInfoForDdl(PartitionInfo partitionInfo, ExecutionContext ec) {

        PartitionTableType tblType = partitionInfo.getTableType();
        if (tblType != PartitionTableType.PARTITION_TABLE && tblType != PartitionTableType.GSI_TABLE) {
            return;
        }

        // validate partition columns data type
        validatePartitionColumns(partitionInfo.getPartitionBy());

        // validate partition name
        validatePartitionNames(partitionInfo);

        // validate partition count
        validatePartitionCount(partitionInfo, ec);

        // validate bound values expressions
        validatePartitionSpecBounds(partitionInfo, ec);

    }

    private static boolean checkIfNullLiteral(RexNode bndExpr) {
        RexLiteral literal = (RexLiteral) bndExpr;
        if (literal.getType().getSqlTypeName().getName().equalsIgnoreCase("NULL") && literal.getValue() == null) {
            return true;
        }
        return false;
    }

    public static void validateBoundValueExpr(RexNode bndExpr,
                                              RelDataType bndColRelDataType,
                                              PartitionIntFunction partIntFunc,
                                              PartitionStrategy strategy) {

        DataType bndColDataType = DataTypeUtil.calciteToDrdsType(bndColRelDataType);
        switch (strategy) {
        case HASH:
        case LIST:
        case RANGE: {

            if (bndExpr instanceof RexCall) {
                RexCall bndExprCall = (RexCall) bndExpr;
                SqlOperator exprOp = bndExprCall.getOperator();
                if (partIntFunc != null) {

                    // Use partIntFunc
                    PartitionIntFunction intFunc = PartitionPrunerUtils.getPartitionIntFunction(exprOp.getName());
                    if (intFunc == null && intFunc != partIntFunc) {
                        throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_INVALID_PARAMS, String.format(
                            "Partition column values use a invalid function %s", exprOp.getName()));
                    }

                } else {

                    // No use partIntFunc

                    throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_INVALID_PARAMS, String.format(
                        "Partition column values use a invalid function %s", exprOp.getName()));
                }

            } else {

                if (!(bndExpr instanceof RexLiteral)) {
                    throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_INVALID_PARAMS, String.format(
                        "Partition column values of incorrect expression"));
                } else {
                    if (checkIfNullLiteral(bndExpr) && strategy == PartitionStrategy.RANGE) {
                        throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_INVALID_PARAMS, String.format(
                            " Not allowed to use NULL value in VALUES LESS THAN"));
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
                        throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_INVALID_PARAMS, String.format(
                            "Partition column values are NOT support to use a function %s", exprOp.getName()));
                    }
                }

            } else {

                if (!(bndExpr instanceof RexLiteral)) {
                    throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_INVALID_PARAMS, String.format(
                        "Partition column values of incorrect expression"));
                } else {
                    if (checkIfNullLiteral(bndExpr) && strategy == PartitionStrategy.RANGE_COLUMNS) {
                        throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_INVALID_PARAMS, String.format(
                            " Not allowed to use NULL value in VALUES LESS THAN"));
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
                                                        SqlPartition partSpecAst) {

        SqlPartitionValue value = partSpecAst.getValues();
        SqlPartitionValue.Operator operator = value.getOperator();
        List<SqlPartitionValueItem> itemsOfVal = value.getItems();
        switch (partStrategy) {
        case HASH: {
            if (prefixPartCol == PartitionInfoUtil.FULL_PART_COL_COUNT) {
                if (itemsOfVal.size() != 1) {
                    throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_INVALID_PARAMS,
                        String.format(
                            "only one column count is allowed in the definition of the bound value of partition[%s] in hash strategy",
                            partName));
                }
            } else {
                if (itemsOfVal.size() != prefixPartCol || prefixPartCol != 1) {
                    throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_INVALID_PARAMS,
                        String.format(
                            "only one column count is allowed in the definition of the bound value of partition[%s] in hash strategy",
                            partName));
                }
            }
        }
        break;
        case KEY: {
            if (prefixPartCol == PartitionInfoUtil.FULL_PART_COL_COUNT) {

                if (itemsOfVal.size() != partColCnt) {
                    throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_INVALID_PARAMS,
                        String.format("the bound value of partition[%s] must match the partition columns",
                            partName));
                }
            } else {
                if (itemsOfVal.size() != prefixPartCol) {
                    throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_INVALID_PARAMS,
                        String.format("the bound value of partition[%s] must match the partition columns",
                            partName));
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
                        String.format("the bound value of partition[%s] must match the partition columns",
                            partName));
                }
            } else {
                if (prefixPartCol > partColCnt) {
                    throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_INVALID_PARAMS,
                        String.format(
                            "the prefix partition column count [%s] is not allowed to be more than the partition columns count [%s] ",
                            prefixPartCol, partColCnt));
                }

                if (newPrefixColCnt != prefixPartCol) {
                    throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_INVALID_PARAMS,
                        String.format(
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
                        String.format("the bound value of partition[%s] must match the partition columns",
                            partName));
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
                    String.format("the bound value of partition[%s] must be not empty",
                        partName));
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
             * each SqlPartitionValueItem of itemsOfVal is SqlCall (ROW expr),
             *
             * so the count of SqlPartitionValueItem should be more than one,
             *
             * and all the expr of SqlCall should be sqlLiteral
             *
             */
            if (itemsOfVal.isEmpty()) {
                throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_INVALID_PARAMS,
                    String.format("the bound value of partition[%s] must be not empty",
                        partName));
            }

            int listValCnt = itemsOfVal.size();
            boolean containInvalidVal = false;
            SqlNode invalidValAst = null;
            for (int i = 0; i < listValCnt; i++) {
                SqlPartitionValueItem val = itemsOfVal.get(i);
                SqlNode expr = val.getValue();
                if (partColCnt > 1) {
                    /**
                     * when partCnt > 1, each of SqlPartitionValueItem is a row expr
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
                     * when partCnt = 1, each of SqlPartitionValueItem is a SqlLiteral
                     */
                    if (!(expr instanceof SqlLiteral)) {
                        containInvalidVal = true;
                        invalidValAst = expr;
                        break;
                    }
                }

            }
            if (containInvalidVal) {
                throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_INVALID_PARAMS,
                    String.format("the bound value[%s] of partition[%s] must match the partition columns",
                        invalidValAst.toString(),
                        partName));
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
        SqlOperator partIntOp = partitionBy.getPartIntFuncOperator();

        PartitionStrategy checkStrategy = realStrategy;
        if (checkStrategy == PartitionStrategy.HASH && partIntOp == null) {
            /**
             * The validation of columns of "Partition By Hash(col)" or "Partition By Hash(col1,col2,...,colN)"
             * should be the same of 
             * "Partition By Key(col)" or "Partition By Key(col1,col2,...,colN)"
             */
            checkStrategy = PartitionStrategy.KEY;
        }

        if (checkStrategy == PartitionStrategy.RANGE
            || checkStrategy == PartitionStrategy.LIST || checkStrategy == PartitionStrategy.HASH) {
            if (checkStrategy == PartitionStrategy.HASH) {
                PartitionIntFunction partIntFunc = partitionBy.getPartIntFunc();
                if (partCol != 1 && partIntFunc != null) {
                    throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_INVALID_PARAMS,
                        String.format(
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
                    throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_INVALID_PARAMS, String
                        .format(
                            "Not supported to use the dataType[%s] as partition columns on this strategy[%s]",
                            partFldDt, checkStrategy.toString()));
                }
            } else {
                if (DataTypeUtil.anyMatchSemantically(partFldDt, DataTypes.TimestampType, DataTypes.TimeType)) {
                    if (partIntOp != TddlOperatorTable.UNIX_TIMESTAMP) {
                        throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_INVALID_PARAMS, String.format(
                            "Constant, random or timezone-dependent expressions in (sub)partitioning function are not allowed"));
                    }
                } else if ((DataTypeUtil.anyMatchSemantically(partFldDt, DataTypes.DatetimeType, DataTypes.DateType))) {
                    if (partIntOp == TddlOperatorTable.UNIX_TIMESTAMP) {
                        throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_INVALID_PARAMS, String.format(
                            "Constant, random or timezone-dependent expressions in (sub)partitioning function are not allowed"));
                    }
                } else {
                    throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_INVALID_PARAMS,
                        String.format("The data type %s are not allowed", partFldDt.getStringSqlType()));
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
                    throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_INVALID_PARAMS, String
                        .format("This data type %s are not allowed",
                            fldDataType.getStringSqlType()));
                }

                int fldSqlType = fldDataType.getSqlType();
                if (fldSqlType == Types.BINARY || (
                    (fldSqlType == Types.CHAR || fldSqlType == Types.VARCHAR)
                        && fldDataType.getCollationName() == CollationName.BINARY)) {
                    throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_INVALID_PARAMS, String
                        .format("This data type 'CHARACTER SET %s COLLATE %s' are not allowed",
                            fldDataType.getStringSqlType(), CollationName.BINARY.name()));
                }

                if (DataTypeUtil.anyMatchSemantically(fldDataType, DataTypes.TimestampType, DataTypes.TimeType)) {
                    if (checkStrategy == PartitionStrategy.RANGE_COLUMNS
                        || checkStrategy == PartitionStrategy.LIST_COLUMNS) {
                        throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_INVALID_PARAMS,
                            String.format("Field '%s' is of a not allowed type[%s] for this type of partitioning",
                                partColMeta.getName(),
                                partColMeta.getDataType().getStringSqlType()));
                    }
                }

                if (fldSqlType == Types.CHAR || fldSqlType == Types.VARCHAR) {
                    DataType dataType = partColMeta.getField().getDataType();
                    CollationName collationOfType = dataType.getCollationName();
                    CharsetName charsetOfType = dataType.getCharsetName();
                    try {
                        String targetCharset = charsetOfType.getJavaCharset();
                        String targetCollate = collationOfType.name();
                        if (!MySQLCharsetDDLValidator.checkCharsetSupported(targetCharset, targetCollate)) {
                            throw new TddlRuntimeException(ErrorCode.ERR_GMS_GENERIC,
                                String.format(
                                    "Failed to create table because the charset[%s] or collate[%s] is not supported in partitioning",
                                    targetCharset, targetCollate));
                        }

                    } catch (Throwable ex) {
                        throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_INVALID_PARAMS,
                            String.format("Field '%s' is of a not allowed type[%s] for this type of partitioning",
                                partColMeta.getName(),
                                partColMeta.getDataType().getStringSqlType()));
                    }
                }
            }
        }

    }

    protected static void validatePartitionNames(PartitionInfo partitionInfo) {
        List<PartitionSpec> pSpecList = partitionInfo.getPartitionBy().getPartitions();
        for (int i = 0; i < pSpecList.size(); i++) {
            PartitionSpec pSpec = pSpecList.get(i);
            String name = pSpec.getName();
            // If validate error , "PartitionNameUtil.validatePartName" will throw execeptions.
            PartitionNameUtil.validatePartName(name, KeyWordsUtil.isKeyWord(name));
        }
    }

    protected static void validatePartitionCount(PartitionInfo partitionInfo, ExecutionContext ec) {
        Integer maxPhysicalPartitions = Integer.valueOf(ConnectionParams.MAX_PHYSICAL_PARTITION_COUNT.getDefault());
        if (ec != null) {
            maxPhysicalPartitions =
                ec.getParamManager().getInt(ConnectionParams.MAX_PHYSICAL_PARTITION_COUNT);
        }

        if (partitionInfo.getPartitionBy().getPartitions().size() > maxPhysicalPartitions) {
            throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_INVALID_PARAMS,
                String
                    .format("Too many partitions [%s] (including subpartitions) after altering",
                        partitionInfo.getPartitionBy().getPartitions().size()));
        }
    }

    protected static void validatePartitionSpecBounds(PartitionInfo partitionInfo, ExecutionContext ec) {

        PartitionStrategy strategy = partitionInfo.getPartitionBy().getStrategy();

        int partColCnt = partitionInfo.getPartitionBy().getPartitionFieldList().size();
        if (strategy == PartitionStrategy.RANGE
            || strategy == PartitionStrategy.RANGE_COLUMNS
            || strategy == PartitionStrategy.HASH
            || strategy == PartitionStrategy.KEY) {

            List<PartitionSpec> partSpecList = partitionInfo.getPartitionBy().getPartitions();
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
            List<PartitionSpec> sortedPartSpecList = partitionInfo.getPartitionBy().getOrderedPartitionSpec();
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
                    throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_INVALID_PARAMS, String
                        .format("the position of partition[%s] should be larger than the position of partition[%s]",
                            partSpec.getName(), lastPartSpec.getName()));

                } else {
                    SearchDatumComparator boundCmp = partitionInfo.getPartitionBy().getBoundSpaceComparator();
                    int cmpRs = boundCmp.compare(partSpec.getBoundSpec().getSingleDatum(),
                        lastPartSpec.getBoundSpec().getSingleDatum());
                    if (cmpRs < 1 && !partitionInfo.isBroadcastTable()) {
                        // partSpec < lastPartSpec
                        throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_INVALID_PARAMS,
                            String.format("the bound of partition[%s] should be larger than the bound of partition[%s]",
                                partSpec.getName(), lastPartSpec.getName()));
                    }
                }
            }
        } else if (strategy == PartitionStrategy.LIST || strategy == PartitionStrategy.LIST_COLUMNS) {
            //===== validate List/ListColumns =====

            List<PartitionSpec> partSpecList = partitionInfo.getPartitionBy().getPartitions();
            for (int i = 0; i < partSpecList.size(); i++) {
                String partName = partSpecList.get(i).getName();
                MultiValuePartitionBoundSpec pSpec = (MultiValuePartitionBoundSpec) partSpecList.get(i).getBoundSpec();

                List<SearchDatumInfo> datums = pSpec.getMultiDatums();
                for (int j = 0; j < datums.size(); j++) {
                    SearchDatumInfo datum = datums.get(j);
                    int valLen = datum.getDatumInfo().length;
                    if (partColCnt != valLen) {
                        throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_INVALID_PARAMS,
                            String.format("the bound value[%s] of partition[%s] must match the partition columns",
                                pSpec, partName));
                    }
                }
            }

            List<PartitionSpec> sortedPartSpecList = partitionInfo.getPartitionBy().getOrderedPartitionSpec();
            for (int i = 0; i < sortedPartSpecList.size(); i++) {
                PartitionSpec partSpec = sortedPartSpecList.get(i);

                if (partSpec.getBoundSpec().containMaxValues()) {
                    throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_INVALID_PARAMS,
                        String.format("the bound value of 'maxvalue' of of partition[%s] are NOT allowed",
                            partSpec.getName()));
                }

                // Validate if boundInfos exists max values
                if (partSpec.getBoundSpec().isDefaultPartSpec()) {
                    throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_INVALID_PARAMS,
                        String.format("the partition[%s] is NOT support to be defined as default partition",
                            partSpec.getName()));
                }

            }
        } else {
            throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_INVALID_PARAMS, "Unsupported partition strategy");
        }
    }

    public static void validateAddPartition(PartitionInfo partitionInfo,
                                            List<PartitionSpec> existPartitionSpecs,
                                            PartitionSpec newPartitionSpec) {
        PartitionStrategy partitionStrategy = existPartitionSpecs.get(0).strategy;
        if (!newPartitionSpec.strategy.equals(partitionStrategy)) {
            throw new NotSupportException(
                "add " + newPartitionSpec.strategy + " to " + partitionStrategy + " partition table");
        }
        String newPartitionName = newPartitionSpec.getName();

        switch (partitionStrategy) {
        case RANGE:
        case RANGE_COLUMNS: {
            SearchDatumComparator comparator = partitionInfo.getPartitionBy().getPruningSpaceComparator();
            SearchDatumInfo newDatumInfo = newPartitionSpec.getBoundSpec().getSingleDatum();

            for (int i = 0; i < existPartitionSpecs.size(); i++) {
                PartitionSpec partitionSpec = existPartitionSpecs.get(i);
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
            Set<SearchDatumInfo> newPartValSet = new TreeSet<>(cmp);
            newPartValSet.addAll(newPartitionSpec.getBoundSpec().getMultiDatums());

            for (PartitionSpec partitionSpec : existPartitionSpecs) {
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
            throw new NotSupportException("add partition for " +
                partitionStrategy.toString() + " type");
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

    public static TableGroupConfig getTheBestTableGroupInfo(PartitionInfo partitionInfo, String tableGroupName,
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
        boolean isVectorStrategy =
            (strategy == PartitionStrategy.KEY || strategy == PartitionStrategy.RANGE_COLUMNS);

        TableGroupConfig targetTableGroupConfig = null;
        if (tableGroupName != null) {
            targetTableGroupConfig = tableGroupInfoManager.getTableGroupConfigByName(tableGroupName);
            if (targetTableGroupConfig == null) {
                throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_INVALID_PARAMS,
                    String.format("Failed to create partition table because the table group[%s] does NOT exists",
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
                    List<String> actualPartCols = getActualPartitionColumns(partitionInfo);
                    if (partitionEquals(partitionInfo, comparePartitionInfo, actualPartCols.size())) {
                        match = true;
                    }
                } else if (partitionInfo.equals(comparePartitionInfo)) {
                    match = true;
                }

                if (!match) {
                    throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_INVALID_PARAMS,
                        String.format(
                            "Failed to create partition table because the partition definition of the table mismatch the table group[%s]",
                            tableGroupName));
                }
            } else if (GeneralUtil.isEmpty(targetTableGroupConfig.getPartitionGroupRecords())) {
//                throw new TddlRuntimeException(ErrorCode.ERR_TABLE_GROUP_NOT_INIT,
//                    String.format(
//                        "the table group[%s] is not inited yet",
//                        tableGroupName));
            } else {
                if (partitionInfo.getPartitionBy().getPartitions().size() != targetTableGroupConfig
                    .getPartitionGroupRecords().size()) {
                    throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_INVALID_PARAMS,
                        String.format(
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
                            throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_INVALID_PARAMS,
                                String.format(
                                    "Failed to create partition table because the partition definition of the table mismatch the table group[%s]",
                                    tableGroupName));
                        }
                    }
                }
            }
        } else {
            int maxTableCount = -1;
            for (Map.Entry<Long, TableGroupConfig> entry : copyTableGroupInfo.entrySet()) {
                if (entry.getValue().getTableGroupRecord().tg_type == TableGroupRecord.TG_TYPE_OSS_TBL_TG) {
                    continue;
                }
                if (entry.getValue().getTableCount() > maxTableCount) {
                    if (partitionInfo.isBroadcastTable() || partitionInfo.isSingleTable()) {
                        maxTableCount = entry.getValue().getTableCount();
                        targetTableGroupConfig = entry.getValue();
                    } else if (entry.getValue().getTableCount() > 0) {
                        String tableName = entry.getValue().getTables().get(0).getLogTbRec().tableName;

                        PartitionInfo comparePartitionInfo = partitionInfoManager.getPartitionInfo(tableName);
                        /**
                         * Check if the partInfo of ddl is equals to the partInfo of the first table of the candidate table group
                         */
                        boolean match = false;
                        if (isVectorStrategy) {
                            List<String> actualPartCols = getActualPartitionColumns(partitionInfo);
                            if (partitionEquals(partitionInfo, comparePartitionInfo, actualPartCols.size())) {
                                match = true;
                            }
                        } else if (partitionInfo.equals(comparePartitionInfo)) {
                            match = true;
                        }
                        if (match) {
                            maxTableCount = entry.getValue().getTableCount();
                            targetTableGroupConfig = entry.getValue();
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

    public static List<String> getNextNPhyTableNames(PartitionInfo partitionInfo, int newTableCount,
                                                     int[] minPostfix) {
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
            for (PartitionSpec spec : partitionInfo.getPartitionBy().getPartitions()) {
                // ref to com.alibaba.polardbx.optimizer.partition.PartitionLocator.PHYSICAL_TABLENAME_PATTERN
                try {
                    curIndex = Integer.parseInt(spec.getLocation().getPhyTableName()
                        .substring(spec.getLocation().getPhyTableName().length() - 5));
                } catch (NumberFormatException e) {
                    curIndex = 0;
                }
                existingPostfix.add(curIndex);
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
            assert phyTableNames.size() == newTableCount;
            return phyTableNames;
        }
    }

    public static List<Pair<String, String>> getInvisiblePartitionPhysicalLocation(PartitionInfo partitionInfo) {
        List<Pair<String, String>> groupAndPhyTableList = new ArrayList<>();
        for (PartitionSpec partitionSpec : partitionInfo.getPartitionBy().getPartitions()) {
            PartitionLocation location = partitionSpec.getLocation();
            if (!location.isVisiable()) {
                groupAndPhyTableList.add(new Pair<>(location.groupKey, location.phyTableName));
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
                List<PartitionSpec> partitionSpecs = partitionInfo.getPartitionBy().getPartitions();
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
                Map<String, List<List<String>>> result =
                    topology.entrySet().stream()
                        .collect(
                            Collectors.toMap(Map.Entry::getKey,
                                x -> x.getValue().stream().map(ImmutableList::of).collect(Collectors.toList())
                            ));

                if (tableRule.isBroadcast()) {
                    Set<String> tables = topology.values().stream().findFirst().orElseThrow(() ->
                        new TddlRuntimeException(ErrorCode.ERR_TABLE_NO_RULE));
                    List<List<String>> tableList = ImmutableList.of(ImmutableList.copyOf(tables));

                    Map<String, Set<String>> common = RelUtils.getCommonTopology(schema, tableName);
                    common.forEach((k, v) -> result.put(k, tableList));
                }
                return result;
            }
        }
    }

    public static PartitionInfo updatePartitionInfoByOutDatePartitionRecords(Connection conn,
                                                                             PartitionInfo partitionInfo,
                                                                             TableInfoManager tableInfoManager) {
        List<PartitionGroupRecord> outdatedPartitionRecords =
            TableGroupUtils.getOutDatePartitionGroupsByTgId(conn, partitionInfo.getTableGroupId());
        partitionInfo = partitionInfo.copy();
        for (PartitionGroupRecord partitionGroupRecord : outdatedPartitionRecords) {
            PartitionSpec spec =
                partitionInfo.getPartitionBy().getPartitions().stream().filter(
                        o -> o.getName().equalsIgnoreCase(partitionGroupRecord.partition_name))
                    .findFirst().orElseThrow(
                        () ->
                            new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                                String.format("partition-group %d not found",
                                    partitionGroupRecord.partition_name)));
            assert spec != null;
            if (partitionInfo.getTableType() != PartitionTableType.BROADCAST_TABLE) {
                spec.getLocation().setGroupKey(
                    GroupInfoUtil.buildGroupNameFromPhysicalDb(partitionGroupRecord.phy_db));
            } else {
                spec.getLocation().setGroupKey(tableInfoManager.getDefaultDbIndex(partitionInfo.getTableSchema()));
            }

            spec.getLocation().setVisiable(false);
        }
        return partitionInfo;
    }

    public static void updatePartitionInfoByNewCommingPartitionRecords(Connection conn, PartitionInfo partitionInfo) {
        //set visible property for newPartitionInfo here
        List<PartitionGroupRecord> newComingPartitionRecords = TableGroupUtils
            .getAllUnVisiablePartitionGroupByGroupId(conn, partitionInfo.getTableGroupId());

        for (PartitionGroupRecord partitionGroupRecord : newComingPartitionRecords) {
            PartitionSpec spec =
                partitionInfo.getPartitionBy().getPartitions().stream().filter(
                        o -> o.getName().equalsIgnoreCase(partitionGroupRecord.partition_name))
                    .findFirst().orElseThrow(
                        () ->
                            new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                                String.format("partition-group %s not found",
                                    partitionGroupRecord.partition_name)));
            spec.getLocation().setGroupKey(
                GroupInfoUtil.buildGroupNameFromPhysicalDb(partitionGroupRecord.phy_db));
            spec.getLocation().setVisiable(false);
        }
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

        boolean isCharsetDiff = (charsetName == null && otherCharsetName != null)
            || (charsetName != null && otherCharsetName == null)
            || (charsetName != null && otherCharsetName != null && !charsetName.equals(otherCharsetName));
        if (isCharsetDiff) {
            return false;
        }

        CollationName collationName = partColMeta.getField().getDataType().getCollationName();
        CollationName otherCollationName = otherPartColMeta.getField().getDataType().getCollationName();
        boolean isCollationDiff = (collationName == null && otherCollationName != null)
            || (collationName != null && otherCollationName == null)
            || (collationName != null && otherCollationName != null && !collationName
            .equals(otherCollationName));
        if (isCollationDiff) {
            return false;
        }
        if (partColMeta.getDataType() == null) {
            if (otherPartColMeta.getDataType() != null) {
                return false;
            }
        } else if (!DataTypeUtil
            .equals(partColMeta.getDataType(), otherPartColMeta.getDataType(), true)) {
            return false;
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
            sourcePartitionInfo.getPartitionColumnsNotReorder().stream().map(String::toLowerCase)
                .collect(Collectors.toList());
        List<String> targetShardColumns =
            targetPartitionInfo.getPartitionColumnsNotReorder().stream().map(String::toLowerCase)
                .collect(Collectors.toList());

        // 分区键不同
        if (!sourceShardColumns.equals(targetShardColumns)) {
            return false;
        }

        return sourcePartitionInfo.equals(targetPartitionInfo);
    }

    public static void adjustPartitionPositionsForNewPartInfo(PartitionInfo newPartInfo) {
        // Reset the partition position for each new partition
        // FIXME @chegnbi , for range/range columns, the position of partitons must be same as the order of bound value!!
        List<PartitionSpec> newSpecs = newPartInfo.getPartitionBy().getPartitions();

        for (int i = 0; i < newSpecs.size(); i++) {
            PartitionSpec spec = newSpecs.get(i);
            long newPosi = i + 1;
            spec.setPosition(newPosi);
        }
    }

    public static List<String> getActualPartitionColumns(PartitionInfo partInfo) {
        return partInfo.getActualPartitionColumns();
    }

    /**
     * check the equality for tb1 and tb2 by their actual partition columns
     */
    public static boolean partitionEquals(PartitionInfo tb1, PartitionInfo tb2) {
        List<String> partColListTb1 = getActualPartitionColumns(tb1);
        List<String> partColListTb2 = getActualPartitionColumns(tb2);
        int partColCntTb1 = partColListTb1.size();
        int partColCntTb2 = partColListTb2.size();
        int maxPartColVal = partColCntTb1 > partColCntTb2 ? partColCntTb1 : partColCntTb2;
        return tb1.equals(tb2, maxPartColVal);
    }

    public static boolean partitionEquals(PartitionInfo tb1, PartitionInfo tb2, int nPartCol) {
        return tb1.equals(tb2, nPartCol);
    }

    public int checkIfActualPartitionColumnsChange(PartitionInfo beforeAlter, PartitionInfo afterAlter) {
        List<String> partColListBeforeAlter = getActualPartitionColumns(beforeAlter);
        List<String> partColListAlterAlter = getActualPartitionColumns(afterAlter);
        return partColListBeforeAlter.size() - partColListAlterAlter.size();
    }

    /**
     * If use the first prefixPartColCnt columns as multi-part-col key,
     * group all the logical tables by the definition of partInfo base on special the multi-part-col key
     *
     * <pre>
     *     For example:
     *  tg: t1,t2,t3,t4,t5,t6
     *  tg(partCols): c1,c2    ( tg use the first two part cols c1 and c2  ）
     *  t1(partCols): c1,c2,pk_int
     *  t2(partCols): c1,c2,pk_var （
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
        int prefixPartColCnt) {
        PartitionInfoManager partInfoMgr = OptimizerContext.getContext(schemaName).getPartitionInfoManager();
        List<PartitionInfo> partInfoList = new ArrayList<>();
        for (int i = 0; i < tableList.size(); i++) {
            PartitionInfo partInfo = partInfoMgr.getPartitionInfo(tableList.get(i));
            partInfoList.add(partInfo);
        }
        Map<Long, List<PartitionInfo>> outputPartInfoGrouping = new HashMap<>();
        Map<String, Long> outputTblNameToGrpIdMap = new TreeMap<>(CaseInsensitive.CASE_INSENSITIVE_ORDER);
        classifyTablesByPrefixPartitionColumns(partInfoList, prefixPartColCnt, outputPartInfoGrouping,
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

    public static void classifyTablesByPrefixPartitionColumns(List<PartitionInfo> partInfoListOfSameTg,
                                                              int prefixPartColCnt,
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
                boolean rs = partInfo.equals(partInfoOfFirstTbOfSameGrp, prefixPartColCnt);
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

    public static Pair<List<String>, List<String>> classifyTablesByPrefixPartitionColumns(
        String schemaName,
        int prefixPartColCnt,
        String tableGroupName,
        String baseTableName) {
        PartitionInfoManager partInfoMgr = OptimizerContext.getContext(schemaName).getPartitionInfoManager();
        TableGroupInfoManager tgMgr = OptimizerContext.getContext(schemaName).getTableGroupInfoManager();
        TableGroupConfig tgConfig = tgMgr.getTableGroupConfigByName(tableGroupName);

        PartitionInfo baseTblPartInfo = partInfoMgr.getPartitionInfo(baseTableName);
        List<String> targetTblList = new ArrayList<>();
        List<String> targetPartColList = new ArrayList<>();

        List<String> baseTblPartCols = baseTblPartInfo.getPartitionBy().getPartitionColumnNameList();
        if (baseTblPartCols.size() < prefixPartColCnt) {
            return null;
        }

        for (int i = 0; i < prefixPartColCnt; i++) {
            targetPartColList.add(baseTblPartCols.get(i));
        }
        List<TablePartRecordInfoContext> tblInfos = tgConfig.getTables();
        for (int i = 0; i < tblInfos.size(); i++) {
            TablePartRecordInfoContext tbInfo = tblInfos.get(i);
            String tbName = tbInfo.getTableName();
            if (tbName.equalsIgnoreCase(baseTableName)) {
                continue;
            }
            PartitionInfo partInfo = partInfoMgr.getPartitionInfo(tbName);

            boolean compRs = baseTblPartInfo.equals(partInfo, prefixPartColCnt);
            if (compRs) {
                targetTblList.add(tbName);
            }
        }
        Pair<List<String>, List<String>> result = new Pair<>(targetPartColList, targetTblList);
        return result;
    }

    public static List<ColumnMeta> getMaxPartColumnMetasInfoForTableGroup(String schemaName,
                                                                          String tableGroupName) {
        PartitionInfoManager partInfoMgr = OptimizerContext.getContext(schemaName).getPartitionInfoManager();
        TableGroupInfoManager tgMgr = OptimizerContext.getContext(schemaName).getTableGroupInfoManager();
        TableGroupConfig tgConfig = tgMgr.getTableGroupConfigByName(tableGroupName);
        List<TablePartRecordInfoContext> tblInfos = tgConfig.getTables();
        if (tblInfos.size() == 0) {
            return new ArrayList<>();
        }
        int minPartColCnt = Integer.MAX_VALUE;
        String minPartColTblName = "";
        List<PartitionInfo> partInfoList = new ArrayList<>();
        PartitionInfo firstPartInfo = null;
        for (int i = 0; i < tblInfos.size(); i++) {
            String logTbName = tblInfos.get(i).getTableName();
            PartitionInfo partInfo = partInfoMgr.getPartitionInfo(logTbName);
            if (i == 0) {
                firstPartInfo = partInfo;
            }
            partInfoList.add(partInfo);
            int partColCnt = partInfo.getPartitionBy().getPartitionColumnNameList().size();
            if (minPartColCnt > partColCnt) {
                minPartColCnt = partColCnt;
                minPartColTblName = logTbName;
            }
        }

        if (firstPartInfo != null) {
            PartitionStrategy strategy = firstPartInfo.getPartitionBy().getStrategy();
            if (strategy != PartitionStrategy.KEY && strategy != PartitionStrategy.RANGE_COLUMNS) {
                List<ColumnMeta> fullPartColMeta = firstPartInfo.getPartitionBy().getPartitionFieldList();
                return fullPartColMeta;
            }
        }

        Map<Long, List<PartitionInfo>> outputPartInfoGrouping = new HashMap<>();
        Map<String, Long> outputTblNameToGrpIdMap = new TreeMap<>(CaseInsensitive.CASE_INSENSITIVE_ORDER);
        int maxPartColCnt = minPartColCnt;
        for (int prefixPartColCnt = minPartColCnt; prefixPartColCnt >= 1; --prefixPartColCnt) {
            outputPartInfoGrouping.clear();
            outputTblNameToGrpIdMap.clear();
            PartitionInfoUtil.classifyTablesByPrefixPartitionColumns(partInfoList, prefixPartColCnt,
                outputPartInfoGrouping, outputTblNameToGrpIdMap);
            if (outputPartInfoGrouping.size() == 1) {
                maxPartColCnt = prefixPartColCnt;
                break;
            }
        }

        PartitionInfo minPartColPartInfo = partInfoMgr.getPartitionInfo(minPartColTblName);
        List<ColumnMeta> fullPartColMeta = minPartColPartInfo.getPartitionBy().getPartitionFieldList();
        List<ColumnMeta> maxPartColMetas = new ArrayList<>();
        for (int i = 0; i < maxPartColCnt; i++) {
            maxPartColMetas.add(fullPartColMeta.get(i));
        }
        return maxPartColMetas;
    }

    public static List<ColumnMeta> getActualPartColumnMetasInfoForTableGroup(String schemaName,
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
        int actualPartColCnt = PartitionInfoUtil.getActualPartitionColumns(partInfoOfFirstTbl).size();
        List<ColumnMeta> fullPartFldList = partInfoOfFirstTbl.getPartitionBy().getPartitionFieldList();
        List<ColumnMeta> prefixPartFldList = new ArrayList<>();
        for (int i = 0; i < actualPartColCnt; i++) {
            prefixPartFldList.add(fullPartFldList.get(i));
        }
        return prefixPartFldList;
    }

    public static List<ColumnMeta> getPrefixPartColumnMetasInfoForTableGroup(String schemaName,
                                                                             String tableGroupName,
                                                                             int prefixPartColCnt) {
        PartitionInfoManager partInfoMgr = OptimizerContext.getContext(schemaName).getPartitionInfoManager();
        TableGroupInfoManager tgMgr = OptimizerContext.getContext(schemaName).getTableGroupInfoManager();
        TableGroupConfig tgConfig = tgMgr.getTableGroupConfigByName(tableGroupName);
        List<TablePartRecordInfoContext> tblInfos = tgConfig.getTables();
        if (tblInfos.size() == 0) {
            return new ArrayList<>();
        }
        String firstTblName = tblInfos.get(0).getTableName();
        PartitionInfo partInfoOfFirstTbl = partInfoMgr.getPartitionInfo(firstTblName);
        List<ColumnMeta> fullPartFldList = partInfoOfFirstTbl.getPartitionBy().getPartitionFieldList();
        List<ColumnMeta> prefixPartFldList = new ArrayList<>();
        for (int i = 0; i < prefixPartColCnt; i++) {
            prefixPartFldList.add(fullPartFldList.get(i));
        }
        return prefixPartFldList;
    }

    /***
     * check if all logical tables are identical in partitioning
     *
     * @param schemaName
     * @param tgName
     * @param actualPartitionCols when -1, all partition columns involve
     * @return
     */
    public static boolean allTablesWithIdenticalPartitionColumns(String schemaName,
                                                                 String tgName,
                                                                 int actualPartitionCols) {

        PartitionInfoManager partInfoMgr = OptimizerContext.getContext(schemaName).getPartitionInfoManager();
        TableGroupInfoManager tgMgr = OptimizerContext.getContext(schemaName).getTableGroupInfoManager();
        TableGroupConfig tgConfig = tgMgr.getTableGroupConfigByName(tgName);

        List<TablePartRecordInfoContext> tblInfos = tgConfig.getTables();
        if (tblInfos.size() == 0) {
            return true;
        }

        String firstTblName = tblInfos.get(0).getTableName();
        PartitionInfo partInfoOfFirstTbl = partInfoMgr.getPartitionInfo(firstTblName);
        int targetPartCol = partInfoOfFirstTbl.getPartitionBy().getPartitionColumnNameList().size();
        List<String> tblNameList = new ArrayList<>();
        tblNameList.add(firstTblName);
        for (int i = 1; i < tblInfos.size(); i++) {
            tblNameList.add(tblInfos.get(i).getTableName());
            PartitionInfo partitionInfo = partInfoMgr.getPartitionInfo(tblInfos.get(i).getTableName());
            if (actualPartitionCols == PartitionInfoUtil.FULL_PART_COL_COUNT) {
                if (partitionInfo.getPartitionBy().getPartitionColumnNameList().size() != targetPartCol) {
                    return false;
                }
            } else if (partitionInfo.getPartitionBy().getPartitionColumnNameList().size() < actualPartitionCols) {
                return false;
            }
        }

        if (actualPartitionCols != PartitionInfoUtil.FULL_PART_COL_COUNT) {
            targetPartCol = actualPartitionCols;
        } else {
            targetPartCol = PartitionInfoUtil.FULL_PART_COL_COUNT;
        }
        List<List<String>> rs = classifyTablesByPrefixPartitionColumns(schemaName, tblNameList, targetPartCol);
        boolean allTheSame = rs.size() == 1;
        return allTheSame;
    }

    public static int getNewPrefixPartColCntBySqlPartitionAst(int fullPartColCnt,
                                                              int actualPartColCnt,
                                                              PartitionStrategy strategy,
                                                              List<SqlPartition> newPartitions) {
        int newPrefixPartColCnt = PartitionInfoUtil.FULL_PART_COL_COUNT;
        if (strategy == PartitionStrategy.KEY || strategy == PartitionStrategy.RANGE_COLUMNS) {
            int lastPartValColSize = -1;
            int valPartColSize;
            for (SqlPartition sqlPartition : newPartitions) {
                valPartColSize = sqlPartition.getValues().getItems().size();
                String partName = sqlPartition.getName().toString();
                newPrefixPartColCnt =
                    checkAndGetNewPrefixPartColCnt(fullPartColCnt, actualPartColCnt, lastPartValColSize, valPartColSize,
                        partName);
                if (lastPartValColSize == -1) {
                    lastPartValColSize = valPartColSize;
                } else {

                }
            }
        }
        return newPrefixPartColCnt;
    }

    public static int checkAndGetNewPrefixPartColCnt(int fullPartColCnt,
                                                     int actualPartColCnt,
                                                     int lastPartValColSize,
                                                     int valPartColSize,
                                                     String partName) {
        int newPrefixPartColCnt = PartitionInfoUtil.FULL_PART_COL_COUNT;
//        if (valPartColSize < actualPartColCnt) {
//            throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
//                String.format(
//                    "the column count[%s] of bound values of partition %s must match actual partition columns count[%s]",
//                    valPartColSize, partName == null ? "" : partName, actualPartColCnt));
//        }

        if (valPartColSize != lastPartValColSize && lastPartValColSize != -1) {
            throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                String.format("the column count[%s] of bound values of partition %s is not allowed to be different",
                    valPartColSize, partName));
        }

        if (valPartColSize > fullPartColCnt) {
            throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                String.format(
                    "the column count[%s] of bound values of partition %s is more than the full partition columns count[%s]",
                    valPartColSize, partName == null ? "" : partName,
                    fullPartColCnt));
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
}
