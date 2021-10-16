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

import com.alibaba.polardbx.common.charset.CollationName;
import com.alibaba.polardbx.common.exception.NotSupportException;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.druid.sql.ast.SQLExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.polardbx.druid.sql.dialect.mysql.parser.MySqlExprParser;
import com.alibaba.polardbx.druid.sql.parser.ByteString;
import com.alibaba.polardbx.druid.sql.parser.Lexer;
import com.alibaba.polardbx.druid.sql.parser.Token;
import com.alibaba.polardbx.gms.locality.LocalityDesc;
import com.alibaba.polardbx.gms.metadb.table.TableInfoManager;
import com.alibaba.polardbx.gms.partition.ExtraFieldJSON;
import com.alibaba.polardbx.gms.partition.TablePartitionRecord;
import com.alibaba.polardbx.gms.tablegroup.PartitionGroupRecord;
import com.alibaba.polardbx.gms.tablegroup.TableGroupConfig;
import com.alibaba.polardbx.gms.tablegroup.TableGroupLocation;
import com.alibaba.polardbx.gms.tablegroup.TableGroupRecord;
import com.alibaba.polardbx.gms.tablegroup.TableGroupUtils;
import com.alibaba.polardbx.gms.util.GroupInfoUtil;
import com.alibaba.polardbx.gms.util.PartitionNameUtil;
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
import com.alibaba.polardbx.optimizer.utils.RelUtils;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlAlterTableDropPartition;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlPartitionValueItem;
import org.apache.calcite.sql.util.SqlShuttle;
import org.apache.calcite.util.Pair;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;

import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Pattern;

import static com.alibaba.polardbx.common.ddl.newengine.DdlConstants.MAX_TABLE_NAME_LENGTH_MYSQL_ALLOWS;
import static com.alibaba.polardbx.common.ddl.newengine.DdlConstants.RANDOM_SUFFIX_LENGTH_OF_PHYSICAL_TABLE_NAME;
import static com.alibaba.polardbx.optimizer.partition.PartitionLocator.PHYSICAL_TABLENAME_PATTERN;

/**
 * @author chenghui.lch
 */
public class PartitionInfoUtil {

    static Pattern IS_NUMBER = Pattern.compile("^[\\d]*$");
    static String MAXVALUE = "MAXVALUE";

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
            partExprListStr += partExprStr;
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

        record.partDesc = partitionSpec.getBoundSpec().getPartitionBoundDescription();

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
        TableGroupRecord tableGroupRecord = new TableGroupRecord();
        tableGroupRecord.schema = partitionInfo.tableSchema;
        tableGroupRecord.meta_version = 0L;
        if (partitionInfo.getTableType() == PartitionTableType.SINGLE_TABLE) {
            if (partitionInfo.getTableGroupId() == TableGroupRecord.INVALID_TABLE_GROUP_ID) {
                tableGroupRecord.tg_type = TableGroupRecord.TG_TYPE_DEFAULT_SINGLE_TBL_TG;
            } else {
                tableGroupRecord.tg_type = TableGroupRecord.TG_TYPE_NON_DEFAULT_SINGLE_TBL_TG;
            }
        } else if (partitionInfo.getTableType() == PartitionTableType.BROADCAST_TABLE) {
            tableGroupRecord.tg_type = TableGroupRecord.TG_TYPE_BROADCAST_TBL_TG;
        } else {
            tableGroupRecord.tg_type = TableGroupRecord.TG_TYPE_PARTITION_TBL_TG;
        }
        return tableGroupRecord;
    }

    public static List<PartitionGroupRecord> prepareRecordForPartitionGroups(
        List<PartitionSpec> partitionSpecs) {
        List<PartitionGroupRecord> partitionGroupRecords = new ArrayList<>();
        int i = 0;
        for (PartitionSpec partitionSpec : partitionSpecs) {
            PartitionGroupRecord partitionGroupRecord = new PartitionGroupRecord();
            partitionGroupRecord.phy_db =
                GroupInfoUtil.buildPhysicalDbNameFromGroupName(partitionSpec.getLocation().getGroupKey());
            partitionGroupRecord.partition_name = partitionSpec.name;
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
        if (tblType == PartitionTableType.SINGLE_TABLE || tblType == PartitionTableType.BROADCAST_TABLE) {
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
            if (tblType == PartitionTableType.BROADCAST_TABLE) {
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
                if (tblType == PartitionTableType.BROADCAST_TABLE) {
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
            for (int i = 0; i < partitionSpecs.size(); i++) {
                final PartitionSpec partSpec = partitionSpecs.get(i);
                String partName = partSpec.getName();
                PartitionGroupRecord partitionGroupRecord = tableGroupConfig.getPartitionGroupRecords().get(i);
                if (!partitionGroupRecord.getPartition_name().equalsIgnoreCase(partSpec.getName())) {
                    partitionGroupRecord = tableGroupConfig.getPartitionGroupRecords().stream()
                        .filter(o -> o.partition_name.equalsIgnoreCase(partName)).findFirst().orElse(null);
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

        // validate partition columns data type
        validatePartitionColumns(partitionInfo);

        // validate partition name
        validatePartitionNames(partitionInfo);

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

    protected static void validatePartitionColumns(PartitionInfo partitionInfo) {
        PartitionStrategy strategy = partitionInfo.getPartitionBy().getStrategy();
        int partCol = partitionInfo.getPartitionBy().getPartitionColumnNameList().size();
        if (strategy == PartitionStrategy.HASH || strategy == PartitionStrategy.RANGE
            || strategy == PartitionStrategy.LIST) {
            if (partCol != 1) {
                throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_INVALID_PARAMS,
                    String.format("Not supported to use more than one partition columns on this strategy"));
            }

            ColumnMeta partFldCm = partitionInfo.getPartitionBy().getPartitionFieldList().get(0);
            DataType partFldDt = partFldCm.getField().getDataType();

            SqlOperator partIntOp = partitionInfo.getPartitionBy().getPartIntFuncOperator();
            if (partIntOp == null) {
                if (!DataTypeUtil.isUnderBigintType(partFldDt)) {
                    throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_INVALID_PARAMS, String
                        .format(
                            "Not supported to use the dataType[%s] as partition columns of table[%s] on this strategy[%s]",
                            partFldDt, partitionInfo.getTableName(), strategy.toString()));
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

        } else if (strategy == PartitionStrategy.KEY || strategy == PartitionStrategy.RANGE_COLUMNS
            || strategy == PartitionStrategy.LIST_COLUMNS) {

            SqlOperator partIntOp = partitionInfo.getPartitionBy().getPartIntFuncOperator();
            if (partIntOp != null) {
                throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_INVALID_PARAMS,
                    String.format("Partitioning function are not allowed"));
            }

            if (partCol == 0) {
                throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_INVALID_PARAMS,
                    String.format("empty partition columns are not allowed"));
            }

            List<ColumnMeta> partFldMetaList = partitionInfo.getPartitionBy().getPartitionFieldList();
            for (int i = 0; i < partFldMetaList.size(); i++) {
                DataType fldDataType = partFldMetaList.get(i).getDataType();
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
                    if (strategy == PartitionStrategy.RANGE_COLUMNS || strategy == PartitionStrategy.LIST_COLUMNS) {
                        throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_INVALID_PARAMS,
                            String.format("Field '%s' is of a not allowed type[%s] for this type of partitioning",
                                partFldMetaList.get(i).getName(),
                                partFldMetaList.get(i).getDataType().getStringSqlType()));
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
            PartitionNameUtil.validatePartName(name);
        }
    }

    protected static void validatePartitionSpecBounds(PartitionInfo partitionInfo, ExecutionContext ec) {

        PartitionStrategy strategy = partitionInfo.getPartitionBy().getStrategy();

        if (strategy == PartitionStrategy.RANGE
            || strategy == PartitionStrategy.RANGE_COLUMNS
            || strategy == PartitionStrategy.HASH
            || strategy == PartitionStrategy.KEY) {
            
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

                // Validate if boundInfos exists max values
                if (partSpec.getBoundSpec().containMaxValues()) {
//                    throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_INVALID_PARAMS,
//                        String.format("the bound of partition[%s] NOT support to contains 'maxvalue'",
//                            partSpec.getName()));
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
                    if (cmpRs < 1) {
                        // partSpec < lastPartSpec
                        throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_INVALID_PARAMS,
                            String.format("the bound of partition[%s] should be larger than the bound of partition[%s]",
                                partSpec.getName(), lastPartSpec.getName()));
                    }
                }
            }
        } else if (strategy == PartitionStrategy.LIST || strategy == PartitionStrategy.LIST_COLUMNS) {
            //===== validate List/ListColumns =====

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
        if (tblType == PartitionTableType.SINGLE_TABLE) {
            copyTableGroupInfo =
                tableGroupInfoManager.copyTableGroupConfigInfoFromCache(TableGroupRecord.TG_TYPE_DEFAULT_SINGLE_TBL_TG);
        } else if (tblType == PartitionTableType.BROADCAST_TABLE) {
            TableGroupConfig broadcastTgConf = tableGroupInfoManager.getBroadcastTableGroupConfig();
            copyTableGroupInfo = new HashMap<>();
            if (broadcastTgConf != null) {
                copyTableGroupInfo.put(broadcastTgConf.getTableGroupRecord().id, broadcastTgConf);
            }
        } else {
            copyTableGroupInfo = tableGroupInfoManager.copyTableGroupConfigInfoFromCache(null);
        }

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

                /**
                 * Check if the partInfo of ddl is equals to the partInfo of the first table of the candidate table group
                 */
                if (!partitionInfo.equals(partitionInfoManager.getPartitionInfo(tableName))) {
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
            int maxTableCount = 0;
            for (Map.Entry<Long, TableGroupConfig> entry : copyTableGroupInfo.entrySet()) {
                if (entry.getValue().getTableCount() > maxTableCount) {
                    String tableName = entry.getValue().getTables().get(0).getLogTbRec().tableName;

                    /**
                     * Check if the partInfo of ddl is equals to the partInfo of the first table of the candidate table group
                     */
                    if (partitionInfo.equals(partitionInfoManager.getPartitionInfo(tableName))) {
                        maxTableCount = entry.getValue().getTableCount();
                        targetTableGroupConfig = entry.getValue();
                    }
                }
            }

        }

        return targetTableGroupConfig;
    }

    /**
     * Generate n names of the added physical tables that must NOT be duplicated with any other physical tables
     */
    public static List<String> getNextNPhyTableNames(PartitionInfo partitionInfo, int n) {
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

    public static PartitionInfo updatePartitionInfoByOutDatePartitionRecords(PartitionInfo partitionInfo,
                                                                             TableInfoManager tableInfoManager) {
        List<PartitionGroupRecord> outdatedPartitionRecords =
            TableGroupUtils
                .getOutDatePartitionGroupsByTgId(partitionInfo.getTableGroupId());
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

    public static void updatePartitionInfoByNewCommingPartitionRecords(PartitionInfo partitionInfo) {
        //set visible property for newPartitionInfo here
        List<PartitionGroupRecord> newComingPartitionRecords = TableGroupUtils
            .getAllUnVisiablePartitionGroupByGroupId(partitionInfo.getTableGroupId());

        for (PartitionGroupRecord partitionGroupRecord : newComingPartitionRecords) {
            PartitionSpec spec =
                partitionInfo.getPartitionBy().getPartitions().stream().filter(
                    o -> o.getName().equalsIgnoreCase(partitionGroupRecord.partition_name))
                    .findFirst().orElseThrow(
                    () ->
                        new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                            String.format("partition-group %d not found",
                                partitionGroupRecord.partition_name)));
            spec.getLocation().setGroupKey(
                GroupInfoUtil.buildGroupNameFromPhysicalDb(partitionGroupRecord.phy_db));
            spec.getLocation().setVisiable(false);
        }
    }
}
