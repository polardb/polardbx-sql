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

package com.alibaba.polardbx.repo.mysql.handler;

import com.alibaba.polardbx.atom.TAtomDataSource;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.model.Group;
import com.alibaba.polardbx.common.model.Group.GroupType;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.common.ExecutorContext;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.handler.HandlerCommon;
import com.alibaba.polardbx.executor.handler.LogicalCheckLocalPartitionHandler;
import com.alibaba.polardbx.gms.engine.FileSystemGroup;
import com.alibaba.polardbx.gms.engine.FileSystemManager;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.gms.metadb.table.ColumnsRecord;
import com.alibaba.polardbx.gms.metadb.table.IndexStatus;
import com.alibaba.polardbx.gms.metadb.table.TableInfoManager;
import com.alibaba.polardbx.gms.metadb.table.TablesAccessor;
import com.alibaba.polardbx.gms.metadb.table.TablesRecord;
import com.alibaba.polardbx.gms.partition.TablePartitionAccessor;
import com.alibaba.polardbx.gms.partition.TablePartitionRecord;
import com.alibaba.polardbx.gms.tablegroup.PartitionGroupAccessor;
import com.alibaba.polardbx.gms.tablegroup.PartitionGroupRecord;
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.group.jdbc.TGroupDataSource;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.FileMeta;
import com.alibaba.polardbx.optimizer.config.table.GsiMetaManager;
import com.alibaba.polardbx.optimizer.config.table.OSSOrcFileMeta;
import com.alibaba.polardbx.optimizer.config.table.OrcMetaUtils;
import com.alibaba.polardbx.optimizer.config.table.PolarDBXOrcSchema;
import com.alibaba.polardbx.optimizer.config.table.SchemaManager;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.function.calc.scalar.CanAccessTable;
import com.alibaba.polardbx.optimizer.core.rel.dal.LogicalDal;
import com.alibaba.polardbx.optimizer.config.table.StripeColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.core.planner.rule.util.CBOUtil;
import com.alibaba.polardbx.optimizer.core.row.Row;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.partition.PartitionSpec;
import com.alibaba.polardbx.optimizer.partition.PartitionTableType;
import com.alibaba.polardbx.optimizer.rule.TddlRuleManager;
import com.alibaba.polardbx.optimizer.utils.RelUtils;
import com.alibaba.polardbx.repo.mysql.checktable.CheckTableUtil;
import com.alibaba.polardbx.repo.mysql.checktable.FieldDescription;
import com.alibaba.polardbx.repo.mysql.checktable.TableCheckResult;
import com.alibaba.polardbx.repo.mysql.checktable.TableDescription;
import com.alibaba.polardbx.repo.mysql.spi.MyRepository;
import com.alibaba.polardbx.rule.TableRule;
import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlCheckTable;
import org.apache.calcite.sql.SqlNode;
import org.apache.commons.lang.StringUtils;
import org.eclipse.jetty.util.StringUtil;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

/**
 * @author chenmo.cm
 */
public class LogicalCheckTableHandler extends HandlerCommon {

    private static final Logger logger = LoggerFactory.getLogger(LogicalCheckTableHandler.class);

    public LogicalCheckTableHandler(IRepository repo) {
        super(repo);
    }

    public String displayMode = "";

    static String statusOK = "OK";

    enum MsgType {
        status, error, info, note, warning
    }
    static String statusError = "error";

    @Override
    public Cursor handle(RelNode logicalPlan, ExecutionContext executionContext) {
        final LogicalDal dal = (LogicalDal) logicalPlan;
        final SqlCheckTable checkTable = (SqlCheckTable) dal.getNativeSqlNode();

        if (checkTable.isWithLocalPartitions()) {
            return new LogicalCheckLocalPartitionHandler(repo).handle(logicalPlan, executionContext);
        }
        displayMode = checkTable.getDisplayMode();

        List<String> tableNameList = new LinkedList<>();
        for (SqlNode tableName : checkTable.getTableNames()) {
            tableNameList.add(RelUtils.lastStringValue(tableName));
        }

        String appName = PlannerContext.getPlannerContext(logicalPlan).getSchemaName();
        ArrayResultCursor result = new ArrayResultCursor("checkTable");
        result.addColumn("Table", DataTypes.StringType);
        result.addColumn("Op", DataTypes.StringType);
        result.addColumn("Msg_type", DataTypes.StringType);
        result.addColumn("Msg_text", DataTypes.StringType);

        boolean isTableWithPrivileges = false;

        if (tableNameList.size() == 0) {
            if (DbInfoManager.getInstance().isNewPartitionDb(executionContext.getSchemaName())) {
                doCheckForPartitionGroup(executionContext.getSchemaName(), executionContext, result, tableNameList);
            }
        }
        for (int i = 0; i < tableNameList.size(); i++) {
            String table = tableNameList.get(i);
            isTableWithPrivileges = CanAccessTable.verifyPrivileges(
                executionContext.getSchemaName(),
                table,
                executionContext);
            if (isTableWithPrivileges) {
                if (DbInfoManager.getInstance().isNewPartitionDb(executionContext.getSchemaName())) {
                    if (CBOUtil.isOss(executionContext, table)) {
                        // check the existence of files of oss table
                        doCheckFileStorageTable(executionContext.getSchemaName(), table, executionContext, result);
                        continue;
                    }
                    doCheckForOnePartTable(executionContext.getSchemaName(), table, executionContext, result);
                    doCheckForOnePartTableGsi(executionContext.getSchemaName(), table, executionContext, result);
                    doCheckTableColumn(executionContext.getSchemaName(), table, executionContext, result);

                } else {
                    doCheckForOneTable(executionContext.getSchemaName(), appName, table, executionContext, result);
                }
            }
        }

        if ("error".equals(displayMode)) {
            ArrayResultCursor errorResult = new ArrayResultCursor("checkTable");
            errorResult.addColumn("Table", DataTypes.StringType);
            errorResult.addColumn("Op", DataTypes.StringType);
            errorResult.addColumn("Msg_type", DataTypes.StringType);
            errorResult.addColumn("Msg_text", DataTypes.StringType);

            List<Row> rows = result.getRows();
            for (Row row : rows) {
                if (!row.getString(3).equals(statusOK)) {
                    errorResult
                        .addRow(new Object[] {row.getString(0), row.getString(1), row.getString(2), row.getString(3)});
                }
            }
            return errorResult;
        }
        return result;

    }

    protected void doCheckTableColumn(String schemaName, String logicalTableName, ExecutionContext executionContext,
                                      ArrayResultCursor result) {
        String tableText = String.format("%s.%s:columns", schemaName, logicalTableName);
        String opText = "check";
        String statusText = "status";
        Boolean isBroadCast = false;
        TddlRuleManager tddlRuleManager = OptimizerContext.getContext(schemaName).getRuleManager();
        PartitionInfo logicalTablePartInfo =
            tddlRuleManager.getPartitionInfoManager().getPartitionInfo(logicalTableName);
        if (logicalTablePartInfo == null) {
            return;
        }
        boolean isSingleTable = logicalTablePartInfo.getTableType() == PartitionTableType.SINGLE_TABLE;
        if (isSingleTable) {
            return;
        }
        PartitionSpec firstLogicalTablePartSpec = logicalTablePartInfo.getPartitionBy().getPartitions().get(0);
        // referenceGroup: it's default values is the first partition.groupName
        String firstLogicalTablereferenceGroupName = firstLogicalTablePartSpec.getLocation().getGroupKey();
        // referenceTable: it default values is the first partition.phyTableName
        String firstLogicalTablereferenceTableName = firstLogicalTablePartSpec.getLocation().getPhyTableName();

        TableDescription firstLogicalTableDesc = CheckTableUtil.getTableDescription((MyRepository) this.repo,
            firstLogicalTablereferenceGroupName,
            firstLogicalTablereferenceTableName,
            false,
            schemaName);
        try (Connection connection = MetaDbUtil.getConnection()) {
            TableInfoManager tableInfoManager = new TableInfoManager();
            tableInfoManager.setConnection(connection);
            List<ColumnsRecord> columnsRecordList = tableInfoManager.queryColumns(schemaName, logicalTableName);

//        Map<String, FieldDescription> logicalTableMetaDbDesc = new LinkedHashMap<>();
            List<FieldDescription> logicalMetaDesc = new ArrayList<>();
            if (!columnsRecordList.isEmpty()) {
                for (ColumnsRecord columnsRecord : columnsRecordList) {
                    FieldDescription fieldDescription = new FieldDescription();
                    fieldDescription.setFieldDefault(columnsRecord.columnDefault);
                    fieldDescription.setFieldKey(columnsRecord.columnKey);
                    fieldDescription.setFieldName(columnsRecord.columnName);
                    fieldDescription.setFieldNull(columnsRecord.isNullable);
                    fieldDescription.setFieldType(columnsRecord.columnType);
                    fieldDescription.setFieldExtra(columnsRecord.extra);
//                logicalTableMetaDbDesc.put(columnsRecord.columnName, fieldDescription);
                    logicalMetaDesc.add(fieldDescription);
                }
            }

            if (firstLogicalTableDesc.isEmptyPartition()) {
                String msgContent =
                    String.format("Table '%s.%s' first partition doesn't exist", schemaName, logicalTableName);
                result.addRow(new Object[] {tableText, opText, "error", msgContent});
                return;
            }
            //TODO: compare covering column in gsi and logical table.

            TableCheckResult checkResult =
                CheckTableUtil.verifylogicalAndPhysicalMeta(firstLogicalTableDesc, logicalMetaDesc);
            if (!isCheckResultNormal(checkResult)) {
                statusText = "Error";
                outputFieldCheckResults(result, tableText, opText, statusText, checkResult, isBroadCast);
            } else {
                String msgContent = statusOK;
                result.addRow(new Object[] {tableText, opText, statusText, msgContent});
            }
        } catch (SQLException e) {
            logger.error(String.format(
                "error occurs while checking table column, schemaName: %s, tableName: %s", schemaName,
                logicalTableName), e);
            throw GeneralUtil.nestedException(e);
        }
    }

    /**
     * check all the files of oss tables
     */
    protected void doCheckFileStorageTable(String schemaName, String logicalTableName,
                                             ExecutionContext executionContext, ArrayResultCursor result) {
        TableMeta tableMeta = executionContext.getSchemaManager(schemaName).getTable(logicalTableName);
        String tableText = String.format("%s.%s", schemaName, logicalTableName);
        String opText = "check";

        PolarDBXOrcSchema orcSchema = OrcMetaUtils.buildPolarDBXOrcSchema(tableMeta);
        Set<String> expectBfColumns = new TreeSet<>(String::compareToIgnoreCase);
        expectBfColumns.addAll(orcSchema.getBfSchema().getFieldNames());
        // check files
        FileSystemGroup fileSystemGroup = FileSystemManager.getFileSystemGroup(tableMeta.getEngine());
        Preconditions.checkArgument(fileSystemGroup != null);
        for (List<FileMeta> fileMetas : tableMeta.getFlatFileMetas().values()) {
            for (FileMeta fileMeta : fileMetas) {
                Preconditions.checkArgument(fileMeta instanceof OSSOrcFileMeta);
                int stripeNum = 0;
                try {
                    // check the existence of file record in oss
                    if (!fileSystemGroup.exists(fileMeta.getFileName())) {
                        result.addRow(new Object[] {tableText, opText, MsgType.error.name(),
                            "File " + fileMeta.getFileName() + " doesn't exist"});
                        return;
                    }
                    OSSOrcFileMeta ossOrcFileMeta = (OSSOrcFileMeta) fileMeta;

                    for (ColumnMeta columnMeta : ossOrcFileMeta.getColumnMetas()) {
                        Map<Long, StripeColumnMeta> stripeColumnMetaMap =
                            ossOrcFileMeta.getStripeColumnMetas(columnMeta.getOriginColumnName());
                        boolean checkBf = expectBfColumns.contains(columnMeta.getName().toUpperCase());
                        stripeNum = (stripeNum == 0) ? stripeColumnMetaMap.size() : stripeNum;
                        // each column should have the same number of stripes
                        if (stripeNum != stripeColumnMetaMap.size()) {
                            String msgContent = String.format(
                                "Different stripe size of bloom filter for file %s, found size %d and %d"
                                , fileMeta.getFileName(), stripeNum, stripeColumnMetaMap.size());
                            result.addRow(new Object[] {tableText, opText, MsgType.error.name(), msgContent});
                            return;
                        }
                        // check each bloom filter
                        for (Map.Entry<Long, StripeColumnMeta> entry : stripeColumnMetaMap.entrySet()) {
                            StripeColumnMeta stripeColumnMeta = entry.getValue();
                            String path = stripeColumnMeta.getBloomFilterPath();
                            if (checkBf && StringUtil.isEmpty(path)) {
                                String msgContent = String.format(
                                    "Column %s of file %s should contain bloom filter for the %d-th stripe"
                                    , columnMeta.getName(), fileMeta.getFileName(),  entry.getKey());
                                result.addRow(new Object[] {tableText, opText, MsgType.error.name(), msgContent});
                                return;
                            }
                            // check the existence of bloom filter in oss
                            if (!StringUtil.isEmpty(path)) {
                                if (!fileSystemGroup.exists(path)) {
                                    result.addRow(new Object[] {tableText, opText, MsgType.error.name(),
                                        "Bloom filter " + path + " doesn't exist"});
                                    return;
                                }
                            }
                        }
                    }
                } catch (IOException e) {
                    throw GeneralUtil.nestedException(e);
                }
            }
        }
        result.addRow(new Object[] {tableText, opText, MsgType.status.name(), statusOK});
    }

    protected void doCheckForOnePartTableGsi(String schemaName, String logicalTableName,
                                             ExecutionContext executionContext, ArrayResultCursor result) {
        ExecutorContext executorContext = ExecutorContext.getContext(schemaName);
        if (null == executionContext) {
            throw new TddlRuntimeException(ErrorCode.ERR_UNKNOWN_DATABASE, schemaName);
        }
        GsiMetaManager metaManager = executorContext.getGsiManager().getGsiMetaManager();
        GsiMetaManager.GsiMetaBean meta = metaManager.getTableAndIndexMeta(logicalTableName, IndexStatus.ALL);
        if (meta.isEmpty()) {
            return;
        }

        for (GsiMetaManager.GsiTableMetaBean bean : meta.getTableMeta().values()) {
            if (bean.gsiMetaBean != null) {
                GsiMetaManager.GsiIndexMetaBean gsiMetaBean = bean.gsiMetaBean;
                doCheckForOnePartTable(schemaName, gsiMetaBean.indexTableName, executionContext, result,
                    logicalTableName);
                doCheckTableWithGsi(schemaName, gsiMetaBean.indexTableName, logicalTableName, executionContext, result);
            }
        }
    }

    protected void doCheckTableWithGsi(String schemaName, String gsiName, String logicalTableName,
                                       ExecutionContext executionContext, ArrayResultCursor result) {

        String tableText = String.format("%s.%s.%s:reference", schemaName, logicalTableName, gsiName);
        String opText = "check";
        String statusText = "status";
        //No broadcast and single table would reach here.
        Boolean isBroadCast = false;
        TddlRuleManager tddlRuleManager = OptimizerContext.getContext(schemaName).getRuleManager();

        PartitionInfo logicalTablePartInfo =
            tddlRuleManager.getPartitionInfoManager().getPartitionInfo(logicalTableName);
        Map<String, Set<String>> logicalTablePartTopology = logicalTablePartInfo.getTopology();
        PartitionSpec firstLogicalTablePartSpec = logicalTablePartInfo.getPartitionBy().getPartitions().get(0);
        // referenceGroup: it's default values is the first partition.groupName
        String firstLogicalTablereferenceGroupName = firstLogicalTablePartSpec.getLocation().getGroupKey();
        // referenceTable: it default values is the first partition.phyTableName
        String firstLogicalTablereferenceTableName = firstLogicalTablePartSpec.getLocation().getPhyTableName();

        TableDescription firstLogicalTableDesc = CheckTableUtil.getTableDescription((MyRepository) this.repo,
            firstLogicalTablereferenceGroupName,
            firstLogicalTablereferenceTableName,
            false,
            schemaName);

        PartitionInfo gsiPartInfo = tddlRuleManager.getPartitionInfoManager().getPartitionInfo(gsiName);
        Map<String, Set<String>> gsiPartTopology = gsiPartInfo.getTopology();
        PartitionSpec firstGsiPartSpec = gsiPartInfo.getPartitionBy().getPartitions().get(0);
        // referenceGroup: it's default values is the first partition.groupName
        String firstGsieferenceGroupName = firstGsiPartSpec.getLocation().getGroupKey();
        // referenceTable: it default values is the first partition.phyTableName
        String firstGsireferenceTableName = firstGsiPartSpec.getLocation().getPhyTableName();

        TableDescription firstGsiTableDesc = CheckTableUtil.getTableDescription((MyRepository) this.repo,
            firstGsieferenceGroupName,
            firstGsireferenceTableName,
            false,
            schemaName);

        if (firstGsiTableDesc.isEmptyPartition()) {
            String msgContent =
                String.format("Gsi '%s.%s.%s' first partition doesn't exist", schemaName, logicalTableName, gsiName);
            result.addRow(new Object[] {tableText, opText, "error", msgContent});
            return;
        }
        if (firstLogicalTableDesc.isEmptyPartition()) {
            String msgContent =
                String.format("Table '%s.%s' first partition doesn't exist", schemaName, logicalTableName);
            result.addRow(new Object[] {tableText, opText, "error", msgContent});
            return;
        }

        TableCheckResult checkResult = CheckTableUtil.verifyTableAndGsiMeta(firstLogicalTableDesc, firstGsiTableDesc);
        if (!isCheckResultNormal(checkResult)) {
            statusText = "Error";
            outputFieldCheckResults(result, tableText, opText, statusText, checkResult, isBroadCast);
        } else {
            String msgContent = statusOK;
            result.addRow(new Object[] {tableText, opText, statusText, msgContent});
        }

    }

    protected void doCheckForOneTable(String schemaName, String appName, String logicalTableName,
                                      ExecutionContext executionContext, ArrayResultCursor result) {

        // 获取表所对应的规则信息
        boolean hasTableRule = false;
        boolean isBroadcastTable = false;
        TddlRuleManager tddlRuleManager = OptimizerContext.getContext(schemaName).getRuleManager();
        TableRule tableRule = tddlRuleManager.getTableRule(logicalTableName);
        if (tableRule != null) {
            hasTableRule = true;
        }

        // =============获取库表的元数据=============
        String defaultDbIndex = tddlRuleManager.getDefaultDbIndex(logicalTableName);
        // 检查是否是分表
        if (!hasTableRule) {
            // 如果不是拆分表，检查默认库是否存在这个单表
            // 如果单表，则获取defaultDbIndex, 并获取单表的schema
            doCheckForSingleTable(appName, defaultDbIndex, logicalTableName, logicalTableName, result);

        } else {

            final String physicalTableName = tableRule.getTbNamePattern();
            String tableText = String.format("%s.%s", appName, logicalTableName);
            String opText = "check";
            String statusText = "Error";

            // 先获取拓扑列表
            List<Group> groupList = OptimizerContext.getContext(schemaName).getMatrix().getGroups();
            List<Group> jdbcGroupList = new ArrayList<Group>();
            for (Group group : groupList) {
                if (group.getType() == GroupType.MYSQL_JDBC) {
                    jdbcGroupList.add(group);
                }
            }

            // < groupName, < tableName, TableDescription > >
            Map<String, Map<String, TableDescription>> groupTableDescMaps =
                new HashMap<String, Map<String, TableDescription>>();

            // referenceGroup：各个分库在比较过程中的参考对象
            String referenceGroupName = null;

            // referenceTable: 各个分表在比较过程中的参考对象
            String referenceTableName = null;

            // 虽然是有规则，但是也有可能是单库单表
            // 例如, 它没有指定dbRuleArr或tbRuleArr等,
            // 而是将dbNamePattern或tbNamePattern的值写死
            boolean isSingleTable = false;
            String groupNameForSingleTable = null;
            String tableNameForSingleTable = null;
            Map<String, Set<String>> dbTbActualTopology = tableRule.getActualTopology();
            if (dbTbActualTopology.size() == 1) {
                Set<String> groupKeySet = dbTbActualTopology.keySet();
                Iterator<String> groupKeySetItor = groupKeySet.iterator();
                String groupKey = groupKeySetItor.next();
                Set<String> tableSet = dbTbActualTopology.get(groupKey);
                if (tableSet.size() == 1) {
                    isSingleTable = true;
                    Iterator<String> tableSetItor = tableSet.iterator();
                    String table = tableSetItor.next();
                    groupNameForSingleTable = groupKey;
                    tableNameForSingleTable = table;
                }
            }

            // 标识该表是否广播表
            isBroadcastTable = tableRule.isBroadcast();

            // We should check each group for broadcast table.
            if (isBroadcastTable) {
                referenceGroupName = defaultDbIndex;
                referenceTableName = physicalTableName;
                StringBuilder targetSql = new StringBuilder("describe ");
                targetSql.append(physicalTableName);

                // 首先获取各个group的广播表的
                for (Group group : jdbcGroupList) {
                    TableDescription tableDescription = CheckTableUtil.getTableDescription((MyRepository) this.repo,
                        group.getName(),
                        physicalTableName,
                        false,
                        schemaName);
                    Map<String, TableDescription> tableNameDescMaps = new HashMap<String, TableDescription>();
                    tableNameDescMaps.put(physicalTableName, tableDescription);
                    groupTableDescMaps.put(group.getName(), tableNameDescMaps);
                }

            } else {
                if (isSingleTable) {
                    // A single table only exists in the single group in PolarDB-X mode.
                    doCheckForSingleTable(appName,
                        groupNameForSingleTable,
                        logicalTableName,
                        tableNameForSingleTable,
                        result);
                    return;
                }

                // 逻辑来到这里，肯定是分库分表
                for (Map.Entry<String, Set<String>> tbTopologyInOneDb : dbTbActualTopology.entrySet()) {
                    String targetGroup = tbTopologyInOneDb.getKey();
                    Set<String> tableSet = tbTopologyInOneDb.getValue();
                    Iterator<String> tableSetItor = tableSet.iterator();
                    Map<String, TableDescription> tableNameDescMaps = new HashMap<String, TableDescription>();

                    if (StringUtils.isEmpty(referenceGroupName)) {
                        referenceGroupName = targetGroup;
                    }

                    while (tableSetItor.hasNext()) {

                        // 首先获取各个group的分表的description
                        String targetTable = tableSetItor.next();
                        TableDescription tableDescription = CheckTableUtil.getTableDescription((MyRepository) this.repo,
                            targetGroup,
                            targetTable,
                            false,
                            schemaName);

                        tableNameDescMaps.put(targetTable, tableDescription);

                        // 将 referenceGroupName 中的其中一个表作为参考表
                        if (targetGroup.equals(referenceGroupName)) {
                            // 随机获取
                            if (StringUtils.isEmpty(referenceTableName)) {
                                referenceTableName = targetTable;
                            }
                        }
                    }
                    groupTableDescMaps.put(targetGroup, tableNameDescMaps);
                }

            }

            // =============校验库表元数据=============

            // 1. 1检查各分库的各个表的存在性；
            boolean isStatusOK = true;
            List<TableCheckResult> abnormalTableCheckResultList = new ArrayList<TableCheckResult>();
            for (Map.Entry<String, Map<String, TableDescription>> groupTableItems : groupTableDescMaps.entrySet()) {
                Map<String, TableDescription> tableNameAndDescMap = groupTableItems.getValue();
                for (Map.Entry<String, TableDescription> tableDescItem : tableNameAndDescMap.entrySet()) {
                    TableDescription tableDesc = tableDescItem.getValue();
                    if (tableDesc.getFields() == null) {
                        TableCheckResult abnormalTable = new TableCheckResult();
                        abnormalTable.setTableDesc(tableDesc);
                        abnormalTable.setExist(false);
                        abnormalTable.setFieldCountTheSame(false);
                        abnormalTable.setFieldDescTheSame(false);
                        abnormalTableCheckResultList.add(abnormalTable);
                    }
                }
            }

            // 1.2 检查是否目标不存在
            if (abnormalTableCheckResultList.size() > 0) {
                TableCheckResult checkResult = abnormalTableCheckResultList.get(0);
                boolean isBroadcast = isBroadcastTable;
                for (int i = 0; i < abnormalTableCheckResultList.size(); i++) {
                    checkResult = abnormalTableCheckResultList.get(i);
                    outputExistCheckResults(result, tableText, opText, statusText, checkResult, isBroadcast);
                }
                isStatusOK = false;
                return;
            }

            // 2.1 根据参考库与参照表，检查各分库的各个分表的表定义是否一致
            Map<String, TableDescription> tableDescsOfReferGroup = groupTableDescMaps.get(referenceGroupName);
            TableDescription referTableDesc = tableDescsOfReferGroup.get(referenceTableName);
            for (Map.Entry<String, Map<String, TableDescription>> groupTableItems : groupTableDescMaps.entrySet()) {
                Map<String, TableDescription> tableNameAndDescMap = groupTableItems.getValue();
                for (Map.Entry<String, TableDescription> tableDescItem : tableNameAndDescMap.entrySet()) {
                    TableDescription tableDesc = tableDescItem.getValue();
                    TableCheckResult checkResult = CheckTableUtil.verifyTableMeta(referTableDesc, tableDesc);
                    if (!isCheckResultNormal(checkResult)) {
                        abnormalTableCheckResultList.add(checkResult);
                    }
                }
            }

            // 2.2 检查是否有分表的schema不一致; 如果不一致，则要报告哪个库的哪些表的哪个列不一致
            if (abnormalTableCheckResultList.size() > 0) {
                boolean isBroadcast = isBroadcastTable;
                for (int i = 0; i < abnormalTableCheckResultList.size(); i++) {
                    TableCheckResult checkResult = abnormalTableCheckResultList.get(i);
                    outputFieldCheckResults(result, tableText, opText, statusText, checkResult, isBroadcast);
                }
                isStatusOK = false;
            }

            if (isStatusOK) {
                statusText = "status";
                result.addRow(new Object[] {tableText, opText, statusText, "OK"});
            }
        }
    }

    protected void doCheckForPartitionGroup(String schemaName, ExecutionContext executionContext,
                                            ArrayResultCursor result, List<String> tableNameList) {
        String tableText = String.format("%s:partition_group", schemaName);
        String opText = "check";
        String statusText = "status";
        String msgText = statusOK;

        PartitionGroupAccessor partitionGroupAccessor = new PartitionGroupAccessor();
        try (Connection connection = MetaDbUtil.getConnection()) {

            partitionGroupAccessor.setConnection(connection);
            List<PartitionGroupRecord> partitionGroupRecordList = partitionGroupAccessor.getAllPartitionGroups();
            Set<Long> groupIds = new HashSet<Long>();
            partitionGroupRecordList.forEach(partitionGroupRecord -> groupIds.add(partitionGroupRecord.getId()));

            TablePartitionAccessor tablePartitionAccessor = new TablePartitionAccessor();
            tablePartitionAccessor.setConnection(connection);
            List<TablePartitionRecord> tablePartitionRecordList =
                tablePartitionAccessor.getTablePartitionsByDbNameLevel(schemaName, 1);
            List<TablePartitionRecord> unexpectedTablePartitionRecordList = tablePartitionRecordList.stream()
                .filter(tablePartitionRecord -> !groupIds.contains(tablePartitionRecord.groupId))
                .collect(Collectors.toList());

            if (unexpectedTablePartitionRecordList.size() == 0) {
                result.addRow(new Object[] {tableText, opText, statusText, msgText});
            } else {
                statusText = "Error";
                for (TablePartitionRecord tablePartitionRecord : unexpectedTablePartitionRecordList) {
                    tableText = String.format("%s.%s:partition_group", schemaName, tablePartitionRecord.tableName);
                    msgText = String.format("unexpected table group id for %s", tablePartitionRecord.phyTable);
                    result.addRow(new Object[] {tableText, opText, statusText, msgText});
                }
            }

            SchemaManager schemaManager = OptimizerContext.getContext(schemaName).getLatestSchemaManager();
            TablesAccessor tablesAccessor = new TablesAccessor();
            tablesAccessor.setConnection(connection);
            List<TablesRecord> tablesRecordList = tablesAccessor.query(schemaName);
            for (TablesRecord tablesRecord : tablesRecordList) {
                if (!schemaManager.getTable(tablesRecord.tableName).isGsi()) {
                    tableNameList.add(tablesRecord.tableName);
                }
            }
        } catch (SQLException e) {
            logger.error(String.format(
                "error occurs while checking partition group, schemaName: %s", schemaName), e);
            throw GeneralUtil.nestedException(e);
        }
    }

    protected void doCheckForOnePartTable(String schemaName, String logicalTableName,
                                          ExecutionContext executionContext, ArrayResultCursor result) {
        doCheckForOnePartTable(schemaName, logicalTableName, executionContext, result, null);
    }

    /**
     * do check for one partition table
     */
    protected void doCheckForOnePartTable(String schemaName, String logicalTableName,
                                          ExecutionContext executionContext, ArrayResultCursor result,
                                          String tableName) {
        TddlRuleManager tddlRuleManager = OptimizerContext.getContext(schemaName).getRuleManager();
        PartitionInfo partInfo = tddlRuleManager.getPartitionInfoManager().getPartitionInfo(logicalTableName);
        if (partInfo == null) {
            return;
        }
        boolean isSingleTable = partInfo.getTableType() == PartitionTableType.SINGLE_TABLE;
        boolean isBroadcastTable = partInfo.getTableType() == PartitionTableType.BROADCAST_TABLE;
        boolean isGsiTable = partInfo.getTableType() == PartitionTableType.GSI_TABLE;
        Map<String, Set<String>> partTblTopology = partInfo.getTopology();
        PartitionSpec firstPartSpec = partInfo.getPartitionBy().getPartitions().get(0);

        // =============获取库表的元数据=============
        if (isSingleTable) {
            doCheckForSingleTable(schemaName, firstPartSpec.getLocation().getGroupKey(), logicalTableName,
                firstPartSpec.getLocation().getPhyTableName(), result);
        } else {
            String tableText = String.format("%s.%s", schemaName, logicalTableName);
            if (isGsiTable) {
                tableText = String.format("%s.%s.%s", schemaName, tableName, logicalTableName);
            }
            String opText = "check";
            String statusText = "Error";

            // < groupName, < tableName, TableDescription > >
            Map<String, Map<String, TableDescription>> groupTableDescMaps =
                new HashMap<String, Map<String, TableDescription>>();

            // referenceGroup: it's default values is the first partition.groupName
            String referenceGroupName = firstPartSpec.getLocation().getGroupKey();

            // referenceTable: it default values is the first partition.phyTableName
            String referenceTableName = firstPartSpec.getLocation().getPhyTableName();

            Map<String, Set<String>> dbTbActualTopology = partTblTopology;

            // We should check each group for broadcast table or single
            if (isBroadcastTable) {
                // Fetch table descriptions from all group for broadcast table
                for (Map.Entry<String, Set<String>> tbTopologyInOneDb : dbTbActualTopology.entrySet()) {
                    String grpName = tbTopologyInOneDb.getKey();
                    String phyTbl = tbTopologyInOneDb.getValue().iterator().next();
                    TableDescription tableDescription = CheckTableUtil.getTableDescription((MyRepository) this.repo,
                        grpName,
                        phyTbl,
                        false,
                        schemaName);
                    Map<String, TableDescription> tableNameDescMaps = new HashMap<String, TableDescription>();
                    tableNameDescMaps.put(phyTbl, tableDescription);
                    groupTableDescMaps.put(grpName, tableNameDescMaps);
                }
            } else {
                // Fetch table descriptions from all group for partition table
                for (Map.Entry<String, Set<String>> tbTopologyInOneDb : dbTbActualTopology.entrySet()) {
                    String targetGroup = tbTopologyInOneDb.getKey();
                    Set<String> tableSet = tbTopologyInOneDb.getValue();
                    Iterator<String> tableSetItor = tableSet.iterator();
                    Map<String, TableDescription> tableNameDescMaps = new HashMap<String, TableDescription>();
                    while (tableSetItor.hasNext()) {
                        // 首先获取各个group的分表的description
                        String targetTable = tableSetItor.next();
                        TableDescription tableDescription = CheckTableUtil.getTableDescription((MyRepository) this.repo,
                            targetGroup,
                            targetTable,
                            false,
                            schemaName);
                        tableNameDescMaps.put(targetTable, tableDescription);
                    }
                    groupTableDescMaps.put(targetGroup, tableNameDescMaps);
                }
            }

            // =============校验库表元数据=============

            // 1. 1检查各分库的各个表的存在性；
            boolean isStatusOK = true;
            List<TableCheckResult> abnormalTableCheckResultList = new ArrayList<TableCheckResult>();
            for (Map.Entry<String, Map<String, TableDescription>> groupTableItems : groupTableDescMaps.entrySet()) {
                Map<String, TableDescription> tableNameAndDescMap = groupTableItems.getValue();
                for (Map.Entry<String, TableDescription> tableDescItem : tableNameAndDescMap.entrySet()) {
                    TableDescription tableDesc = tableDescItem.getValue();
                    if (tableDesc.getFields() == null) {
                        TableCheckResult abnormalTable = new TableCheckResult();
                        abnormalTable.setTableDesc(tableDesc);
                        abnormalTable.setExist(false);
                        abnormalTable.setFieldCountTheSame(false);
                        abnormalTable.setFieldDescTheSame(false);
                        abnormalTableCheckResultList.add(abnormalTable);
                    }
                }
            }

            // 1.2 检查是否目标不存在
            if (abnormalTableCheckResultList.size() > 0) {
                TableCheckResult checkResult = abnormalTableCheckResultList.get(0);
                boolean isBroadcast = isBroadcastTable;
                for (int i = 0; i < abnormalTableCheckResultList.size(); i++) {
                    checkResult = abnormalTableCheckResultList.get(i);
                    outputExistCheckResults(result, tableText, opText, statusText, checkResult, isBroadcast);
                }
                isStatusOK = false;
                return;
            }

            // 2.1 根据参考库与参照表，检查各分库的各个分表的表定义是否一致
            Map<String, TableDescription> tableDescsOfReferGroup = groupTableDescMaps.get(referenceGroupName);
            TableDescription referTableDesc = tableDescsOfReferGroup.get(referenceTableName);
            for (Map.Entry<String, Map<String, TableDescription>> groupTableItems : groupTableDescMaps.entrySet()) {
                Map<String, TableDescription> tableNameAndDescMap = groupTableItems.getValue();
                for (Map.Entry<String, TableDescription> tableDescItem : tableNameAndDescMap.entrySet()) {
                    TableDescription tableDesc = tableDescItem.getValue();
                    TableCheckResult checkResult = CheckTableUtil.verifyTableMeta(referTableDesc, tableDesc);
                    if (!isCheckResultNormal(checkResult)) {
                        abnormalTableCheckResultList.add(checkResult);
                    }
                }
            }

            // 2.2 检查是否有分表的schema不一致; 如果不一致，则要报告哪个库的哪些表的哪个列不一致
            if (abnormalTableCheckResultList.size() > 0) {
                boolean isBroadcast = isBroadcastTable;
                for (int i = 0; i < abnormalTableCheckResultList.size(); i++) {
                    TableCheckResult checkResult = abnormalTableCheckResultList.get(i);
                    outputFieldCheckResults(result, tableText, opText, statusText, checkResult, isBroadcast);
                }
                isStatusOK = false;
            }

            if (isStatusOK) {
                statusText = "status";
                result.addRow(new Object[] {tableText, opText, statusText, "OK"});
            }
        }
    }

    private void outputExistCheckResults(ArrayResultCursor result, String tableText, String opText, String statusText,
                                         TableCheckResult checkResult, boolean isBroadcast) {
        TableDescription tableDesc = checkResult.getTableDesc();
        String tblName = tableDesc.getTableName();
        String grpName = tableDesc.getGroupName();
        String msgContent = String.format("Table '%s.%s' doesn't exist", grpName, tblName);
        if (isBroadcast) {
            msgContent = "[broadcast] " + msgContent;
        }
        result.addRow(new Object[] {tableText, opText, statusText, msgContent});
    }

    private void outputFieldCheckResults(ArrayResultCursor result, String tableText, String opText, String statusText,
                                         TableCheckResult checkResult, boolean isBroadcast) {
        String grpName = checkResult.getTableDesc().getGroupName();
        String tlbName = checkResult.getTableDesc().getTableName();
        Map<String, FieldDescription> incorrectFields = checkResult.getAbnormalFieldDescMaps();
        StringBuilder incorrectFieldsMsgBuilder = new StringBuilder("");
        for (Map.Entry<String, FieldDescription> incorrectFieldItem : incorrectFields.entrySet()) {
            if (StringUtils.isNotEmpty(incorrectFieldsMsgBuilder.toString())) {
                incorrectFieldsMsgBuilder.append(", ");
            }
            incorrectFieldsMsgBuilder.append(incorrectFieldItem.getKey());
        }

        StringBuilder missingFieldsMsgBuilder = new StringBuilder("");
        Map<String, FieldDescription> missingFields = checkResult.getMissingFieldDescMaps();
        for (Map.Entry<String, FieldDescription> missingFieldItem : missingFields.entrySet()) {
            if (StringUtils.isNotEmpty(missingFieldsMsgBuilder.toString())) {
                missingFieldsMsgBuilder.append(", ");
            }
            missingFieldsMsgBuilder.append(missingFieldItem.getKey());
        }

        String incorrectFieldsMsg = incorrectFieldsMsgBuilder.toString();
        String missingFieldsMsg = missingFieldsMsgBuilder.toString();
        String msgContent = null;
        if (StringUtils.isNotEmpty(incorrectFieldsMsg) && StringUtils.isNotEmpty(missingFieldsMsg)) {
            msgContent = String.format("Table '%s.%s' find incorrect columns '%s', and find missing columns '%s'",
                grpName,
                tlbName,
                incorrectFieldsMsg,
                missingFieldsMsg);
        } else if (StringUtils.isNotEmpty(incorrectFieldsMsg)) {
            msgContent = String.format("Table '%s.%s' find incorrect columns '%s'",
                grpName,
                tlbName,
                incorrectFieldsMsg);
        } else if (StringUtils.isNotEmpty(missingFieldsMsg)) {
            msgContent = String.format("Table '%s.%s' find missing columns '%s'", grpName, tlbName, missingFieldsMsg);
        } else {
            msgContent = String.format("Table '%s.%s' is invaild", grpName, tlbName);
        }
        if (isBroadcast) {
            msgContent = "[broadcast] " + msgContent;
        }
        if (checkResult.isShadowTable()) {
            msgContent += ", please recreate the shadow table";
        } else {
            msgContent += ", please recreate table";
        }
        result.addRow(new Object[] {tableText, opText, statusText, msgContent});
    }

    protected boolean isCheckResultNormal(TableCheckResult checkResult) {

        if (checkResult.isExist() && checkResult.getUnexpectedFieldDescMaps().size() == 0
            && checkResult.getMissingFieldDescMaps().size() == 0
            && checkResult.getIncorrectFieldDescMaps().size() == 0) {
            return true;
        }
        return false;
    }

    protected void doCheckForSingleTable(String appName, String groupName,
                                         String logicalTableName,
                                         String physicalTableName,
                                         ArrayResultCursor result) {

        MyRepository myRepository = (MyRepository) this.repo;
        TGroupDataSource groupDataSource = (TGroupDataSource) myRepository.getDataSource(groupName);
        TAtomDataSource atomDataSource = CheckTableUtil.findMasterAtomForGroup(groupDataSource);
        StringBuilder targetSql = new StringBuilder("check table ");
        targetSql.append("`" + physicalTableName + "`");
        Connection conn = null;
        ResultSet rs = null;
        Throwable ex = null;
        try {
            conn = (Connection) atomDataSource.getConnection();
            rs = conn.createStatement().executeQuery(targetSql.toString());
            String tableText = String.format("%s.%s", appName, logicalTableName);
            if (rs.next()) {
                String opText = rs.getString(2);
                String statusText = rs.getString(3);
                String msgText = rs.getString(4);
                result.addRow(new Object[] {tableText, opText, statusText, msgText});
            }
        } catch (Throwable e) {
            // 打好相关的日志
            logger.error(e);
            ex = e;

        } finally {
            try {
                if (rs != null) {
                    rs.close();
                }
                if (conn != null) {
                    conn.close();
                }

            } catch (SQLException e) {
                logger.error(e);
            }

            if (ex != null) {
                GeneralUtil.nestedException(ex);
            }
        }
    }
}
