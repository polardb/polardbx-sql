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

import com.alibaba.polardbx.common.ddl.foreignkey.ForeignKeyData;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.model.Group;
import com.alibaba.polardbx.common.model.Group.GroupType;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.common.utils.version.InstanceVersion;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.executor.common.ExecutorContext;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.handler.HandlerCommon;
import com.alibaba.polardbx.executor.handler.LogicalCheckLocalPartitionHandler;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.gms.engine.FileSystemGroup;
import com.alibaba.polardbx.gms.engine.FileSystemManager;
import com.alibaba.polardbx.gms.metadb.record.RecordConverter;
import com.alibaba.polardbx.gms.metadb.table.ColumnsInfoSchemaRecord;
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
import com.alibaba.polardbx.optimizer.config.table.IndexMeta;
import com.alibaba.polardbx.optimizer.config.table.OSSOrcFileMeta;
import com.alibaba.polardbx.optimizer.config.table.OrcMetaUtils;
import com.alibaba.polardbx.optimizer.config.table.PolarDBXOrcSchema;
import com.alibaba.polardbx.optimizer.config.table.SchemaManager;
import com.alibaba.polardbx.optimizer.config.table.StripeColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.function.calc.scalar.CanAccessTable;
import com.alibaba.polardbx.optimizer.core.planner.rule.util.CBOUtil;
import com.alibaba.polardbx.optimizer.core.rel.dal.LogicalDal;
import com.alibaba.polardbx.optimizer.core.row.Row;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.partition.PartitionSpec;
import com.alibaba.polardbx.optimizer.partition.common.PartitionTableType;
import com.alibaba.polardbx.optimizer.rule.TddlRuleManager;
import com.alibaba.polardbx.repo.mysql.checktable.CheckTableUtil;
import com.alibaba.polardbx.repo.mysql.checktable.ColumnDiffResult;
import com.alibaba.polardbx.repo.mysql.checktable.FieldDescription;
import com.alibaba.polardbx.repo.mysql.checktable.TableCheckResult;
import com.alibaba.polardbx.repo.mysql.checktable.TableDescription;
import com.alibaba.polardbx.repo.mysql.spi.MyRepository;
import com.alibaba.polardbx.rule.TableRule;
import com.google.common.base.Preconditions;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlCheckTable;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.util.Pair;
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
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

import static com.alibaba.polardbx.gms.util.GroupInfoUtil.buildPhysicalDbNameFromGroupName;

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

        String appName = PlannerContext.getPlannerContext(logicalPlan).getSchemaName();
        List<Pair<String, String>> tableNameList = new LinkedList<>();
//        List<String> tableNameList = new LinkedList<>();
        for (SqlNode tableName : checkTable.getTableNames()) {
            if (tableName instanceof SqlIdentifier && ((SqlIdentifier) tableName).names.size() == 2) {
                List<String> names = ((SqlIdentifier) tableName).names;
                tableNameList.add(Pair.of(names.get(0), names.get(1)));
            } else {
                tableNameList.add(Pair.of(appName, tableName.toString()));
            }
        }

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
            String schemaName = tableNameList.get(i).getKey();
            String table = tableNameList.get(i).getValue();
            isTableWithPrivileges = CanAccessTable.verifyPrivileges(
                schemaName,
                table,
                executionContext);
            if (isTableWithPrivileges) {
                if (DbInfoManager.getInstance().isNewPartitionDb(schemaName)) {
                    if (CBOUtil.isOss(executionContext, table)) {
                        // check the existence of files of oss table
                        doCheckFileStorageTable(schemaName, table, executionContext, result);
                        continue;
                    }
                    doCheckForOnePartTableTopology(schemaName, table, executionContext, result);
                    doCheckTableColumn(schemaName, table, executionContext, result);
                    doCheckForOnePartTableLocalIndex(schemaName, table, executionContext, result);
                    doCheckForOnePartTableGsi(schemaName, table, executionContext, result);
                    doCheckForOnePartTableForeignKeys(schemaName, table, executionContext, result);

                } else {
                    doCheckForOneTable(schemaName, appName, table, executionContext, result);
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
        Boolean checkLogicalColumnOrder =
            executionContext.getParamManager().getBoolean(ConnectionParams.CHECK_LOGICAL_COLUMN_ORDER);
        String tableText = String.format("%s.%s:Columns", schemaName, logicalTableName);
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
//        if (isSingleTable) {
//            return;
//        }
        PartitionSpec firstPartSpec = logicalTablePartInfo.getPartitionBy().getPartitions().get(0);
        //PartitionSpec firstLogicalTablePartSpec = logicalTablePartInfo.getPartitionBy().getPartitions().get(0);
        if (logicalTablePartInfo.getPartitionBy().getSubPartitionBy() != null) {
            firstPartSpec = firstPartSpec.getSubPartitions().get(0);
        }
        String physicalGroupName = firstPartSpec.getLocation().getGroupKey();
        String physicalTableName = firstPartSpec.getLocation().getPhyTableName();

        TableDescription firstPhysicalTableDesc = CheckTableUtil.getTableDescription((MyRepository) this.repo,
            physicalGroupName,
            physicalTableName,
            false,
            schemaName);
        try (Connection connection = MetaDbUtil.getConnection()) {
            Boolean columnOrderSame = true;
            TableInfoManager tableInfoManager = new TableInfoManager();
            tableInfoManager.setConnection(connection);
            List<ColumnsRecord> logicalColumnsRecord = tableInfoManager.queryColumns(schemaName, logicalTableName);
            if (checkLogicalColumnOrder) {
                List<String> columnNames =
                    logicalColumnsRecord.stream().map(o -> o.columnName).collect(Collectors.toList());
                TGroupDataSource dataSource =
                    (TGroupDataSource) ExecutorContext.getContext(schemaName).getTopologyExecutor()
                        .getGroupExecutor(physicalGroupName).getDataSource();
                List<ColumnsInfoSchemaRecord> physicalColumnsInfo;
                Map<String, Map<String, Object>> columnsJdbcExtInfo;
                try (Connection phyDbConn = dataSource.getConnection()) {
                    String physicalDbName = buildPhysicalDbNameFromGroupName(dataSource.getDbGroupKey());
                    physicalColumnsInfo =
                        tableInfoManager.fetchColumnInfoSchema(physicalDbName, physicalTableName, columnNames,
                            phyDbConn);
                    columnsJdbcExtInfo = tableInfoManager.fetchColumnJdbcExtInfo(
                        physicalDbName, physicalTableName, dataSource);
                } catch (SQLException e) {
                    logger.error(String.format(
                        "error occurs while checking table column, schemaName: %s, tableName: %s", schemaName,
                        logicalTableName), e);
                    throw GeneralUtil.nestedException(e);
                }
                List<ColumnsRecord> physicalColumnsRecord =
                    RecordConverter.convertColumn(physicalColumnsInfo, columnsJdbcExtInfo, schemaName,
                        logicalTableName);
                ColumnDiffResult columnDiffResult =
                    ColumnDiffResult.diffPhysicalColumnAndLogicalColumnOrder(physicalColumnsRecord,
                        logicalColumnsRecord);

                if (columnDiffResult.diff()) {
                    statusText = "Error";
                    columnOrderSame = false;
                    List<Object[]> columnDiffRows = columnDiffResult.convertToRows(tableText, opText, statusText);
                    for (Object[] columDiffRow : columnDiffRows) {
                        result.addRow(columDiffRow);
                    }
                }
            }

            List<FieldDescription> logicalTableDesc = new ArrayList<>();
            for (ColumnsRecord columnsRecord : logicalColumnsRecord) {
                FieldDescription fieldDescription = new FieldDescription();
                fieldDescription.setFieldDefault(columnsRecord.columnDefault);
                fieldDescription.setFieldKey(columnsRecord.columnKey);
                fieldDescription.setFieldName(columnsRecord.columnName);
                fieldDescription.setFieldNull(columnsRecord.isNullable);
                fieldDescription.setFieldType(columnsRecord.columnType);
                fieldDescription.setFieldExtra(columnsRecord.extra);
                logicalTableDesc.add(fieldDescription);
            }

            if (firstPhysicalTableDesc.isEmptyPartition()) {
                String msgContent =
                    String.format("Table '%s.%s' first partition doesn't exist", schemaName, logicalTableName);
                result.addRow(new Object[] {tableText, opText, "error", msgContent});
                return;
            }
            TableCheckResult checkResult =
                CheckTableUtil.verifyLogicalAndPhysicalMeta(firstPhysicalTableDesc, logicalTableDesc);
            if (!isCheckResultNormal(checkResult) || !columnOrderSame) {
                statusText = "Error";
                if (!isCheckResultNormal(checkResult)) {
                    outputFieldCheckResults(result, tableText, opText, statusText, checkResult, isBroadCast);
                }
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
                    if (!fileSystemGroup.exists(fileMeta.getFileName(), false)) {
                        result.addRow(new Object[] {
                            tableText, opText, MsgType.error.name(),
                            "File " + fileMeta.getFileName() + " doesn't exist"});
                        return;
                    }
                    OSSOrcFileMeta ossOrcFileMeta = (OSSOrcFileMeta) fileMeta;

                    for (ColumnMeta columnMeta : tableMeta.getAllColumns()) {
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
                                    , columnMeta.getName(), fileMeta.getFileName(), entry.getKey());
                                result.addRow(new Object[] {tableText, opText, MsgType.error.name(), msgContent});
                                return;
                            }
                            // check the existence of bloom filter in oss
                            if (!StringUtil.isEmpty(path)) {
                                if (!fileSystemGroup.exists(path, false)) {
                                    result.addRow(new Object[] {
                                        tableText, opText, MsgType.error.name(),
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

    protected void doCheckForOnePartTableLocalIndex(String schemaName, String logicalTableName,
                                                    ExecutionContext executionContext, ArrayResultCursor result) {
        if (null == executionContext) {
            throw new TddlRuntimeException(ErrorCode.ERR_UNKNOWN_DATABASE, schemaName);
        }
        Map<String, IndexMeta> secondaryIndexMap =
            executionContext.getSchemaManager(schemaName).getTable(logicalTableName).getSecondaryIndexesMap();
        for (String indexName : secondaryIndexMap.keySet()) {
            IndexMeta indexMeta = secondaryIndexMap.get(indexName);
            doCheckForLocalIndexTopology(schemaName, logicalTableName, indexName, executionContext, result,
                indexMeta);
        }
    }

    protected void doCheckForLocalIndexTopology(String schemaName, String logicalTableName, String indexName,
                                                ExecutionContext executionContext, ArrayResultCursor result,
                                                IndexMeta indexMeta) {
        PartitionInfo partInfo =
            executionContext.getSchemaManager(schemaName).getTddlRuleManager().getPartitionInfoManager()
                .getPartitionInfo(logicalTableName);
        String tableText = String.format("%s.%s:Local Index", logicalTableName, indexName);
        String opText = "check";
        String statusText = "status";
        String msgText = statusOK;
        Boolean flag = true;
        if (partInfo == null) {
            return;
        }
        Map<String, Set<String>> partTblTopology = partInfo.getTopology();
        List<String> columns = indexMeta.getKeyColumns().stream().map(o -> o.getName()).collect(Collectors.toList());
        for (String groupName : partTblTopology.keySet()) {
            String phyDbName = buildPhysicalDbNameFromGroupName(groupName);
            List<String> phyTableLists =
                partTblTopology.get(groupName).stream().map(String::toLowerCase).collect(Collectors.toList());
            Map<String, List<String>> phyColumnsMap =
                CheckTableUtil.getTableIndexColumns(schemaName, groupName, phyTableLists, indexName,
                    phyDbName);
            for (String phyTable : phyTableLists) {
                if (InstanceVersion.isMYSQL80() && columns.isEmpty() && !phyColumnsMap.containsKey(phyTable)) {
                    //mysql 80 函数索引没有列名
                    continue;
                }
                if (!phyColumnsMap.containsKey(phyTable) || phyColumnsMap.get(phyTable).isEmpty()) {
                    flag = false;
                    msgText = String.format(
                        "index '%s' doesn't exist on group '%s' for physical table '%s'",
                        indexName, groupName, phyTable);
                    result.addRow(new Object[] {tableText, opText, MsgType.error.name(), msgText});
                } else {
                    List<String> phyColumns = phyColumnsMap.get(phyTable);
                    List<String> redundantColumns = phyColumns.stream().filter(o -> !columns.contains(o)).collect(
                        Collectors.toList());
                    List<String> lackColumns =
                        columns.stream().filter(o -> !phyColumns.contains(o)).collect(Collectors.toList());
                    if (!lackColumns.isEmpty() || !redundantColumns.isEmpty()) {
                        flag = false;
                        String msgText1 = String.format(
                            "index '%s' on group '%s' for physical table '%s' has redundant columns: '%s'. ",
                            indexName, groupName, phyTable, StringUtils.join(redundantColumns, ","));
                        String msgText2 = String.format(
                            "index '%s' on group '%s' for physical table '%s' lack columns: '%s'. ",
                            indexName, groupName, phyTable, StringUtils.join(lackColumns, ","));
                        msgText =
                            (redundantColumns.isEmpty() ? "" : msgText1) + (lackColumns.isEmpty() ? "" : msgText2);
                        result.addRow(new Object[] {tableText, opText, MsgType.error.name(), msgText});
                    }
                }
            }
        }
        if (flag) {
            result.addRow(new Object[] {tableText, opText, statusText, msgText});
        }
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
            if (bean.gsiMetaBean != null && !bean.gsiMetaBean.columnarIndex) {
                GsiMetaManager.GsiIndexMetaBean gsiMetaBean = bean.gsiMetaBean;
                doCheckForOnePartTableTopology(schemaName, gsiMetaBean.indexName, executionContext, result,
                    logicalTableName);
                doCheckTableGsiCoveringColumns(schemaName, gsiMetaBean.indexName, logicalTableName,
                    executionContext, result);
            }
        }
    }

    // compare physical meta between logical table and gsi.
    protected void doCheckTableGsiCoveringColumns(String schemaName, String gsiName, String logicalTableName,
                                                  ExecutionContext executionContext, ArrayResultCursor result) {

        String tableText = String.format("%s.%s.%s:Covering Columns", schemaName, logicalTableName, gsiName);
        String opText = "check";
        String statusText = "status";
        //No broadcast and single table would reach here.
        Boolean isBroadCast = false;
        TddlRuleManager tddlRuleManager = OptimizerContext.getContext(schemaName).getRuleManager();

        PartitionInfo logicalTablePartInfo =
            tddlRuleManager.getPartitionInfoManager().getPartitionInfo(logicalTableName);
        PartitionSpec firstPartSpec = logicalTablePartInfo.getPartitionBy().getPartitions().get(0);
        // referenceGroup: it's default values is the first partition.groupName
        if (logicalTablePartInfo.getPartitionBy().getSubPartitionBy() != null) {
            firstPartSpec = firstPartSpec.getSubPartitions().get(0);
        }
        String firstPhysicalGroupName = firstPartSpec.getLocation().getGroupKey();
        // referenceTable: it default values is the first partition.phyTableName
        String firstPhysicalTableName = firstPartSpec.getLocation().getPhyTableName();

        TableDescription firstLogicalTableDesc = CheckTableUtil.getTableDescription((MyRepository) this.repo,
            firstPhysicalGroupName,
            firstPhysicalTableName,
            false,
            schemaName);

        PartitionInfo gsiPartInfo = tddlRuleManager.getPartitionInfoManager().getPartitionInfo(gsiName);
        Map<String, Set<String>> gsiPartTopology = gsiPartInfo.getTopology();
        PartitionSpec firstGsiPartSpec = gsiPartInfo.getPartitionBy().getPartitions().get(0);
        if (gsiPartInfo.getPartitionBy().getSubPartitionBy() != null) {
            firstGsiPartSpec = firstGsiPartSpec.getSubPartitions().get(0);
        }
        // referenceGroup: it's default values is the first partition.groupName
        String firstGsiGroupName = firstGsiPartSpec.getLocation().getGroupKey();
        // referenceTable: it default values is the first partition.phyTableName
        String firstGsiTableName = firstGsiPartSpec.getLocation().getPhyTableName();

        TableDescription firstGsiTableDesc = CheckTableUtil.getTableDescription((MyRepository) this.repo,
            firstGsiGroupName,
            firstGsiTableName,
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
            doCheckForSingleTable(schemaName, defaultDbIndex, logicalTableName, logicalTableName, result);

        } else {

            final String physicalTableName = tableRule.getTbNamePattern();
            String tableText = String.format("%s.%s", schemaName, logicalTableName);
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

            // We should check each group for broadcast table
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
                    doCheckForSingleTable(schemaName,
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
                                            ArrayResultCursor result, List<Pair<String, String>> tableNameList) {
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
                tablePartitionAccessor.getTablePartitionsByDbNameLevel(schemaName,
                    TablePartitionRecord.PARTITION_LEVEL_PARTITION);
            if (GeneralUtil.isNotEmpty(tablePartitionRecordList) && tablePartitionRecordList.get(0).getNextLevel()
                != TablePartitionRecord.PARTITION_LEVEL_NO_NEXT_PARTITION) {
                tablePartitionRecordList = tablePartitionAccessor.getTablePartitionsByDbNameLevel(schemaName,
                    TablePartitionRecord.PARTITION_LEVEL_SUBPARTITION);
            }
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
                    tableNameList.add(Pair.of(tablesRecord.tableSchema, tablesRecord.tableName));
                }
            }
        } catch (SQLException e) {
            logger.error(String.format(
                "error occurs while checking partition group, schemaName: %s", schemaName), e);
            throw GeneralUtil.nestedException(e);
        }
    }

    protected void doCheckForOnePartTableTopology(String schemaName, String logicalTableName,
                                                  ExecutionContext executionContext, ArrayResultCursor result) {
        doCheckForOnePartTableTopology(schemaName, logicalTableName, executionContext, result, null);
    }

    /**
     * do check for one partition table
     */
    protected void doCheckForOnePartTableTopology(String schemaName, String logicalTableName,
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

        String tableText = String.format("%s.%s:Topology", schemaName, logicalTableName);
        if (isGsiTable) {
            tableText = String.format("%s.%s.%s:Topology", schemaName, tableName, logicalTableName);
        }
        String opText = "check";
        String statusText = "Error";

        // < groupName, < tableName, TableDescription > >
        Map<String, Map<String, TableDescription>> groupTableDescMaps =
            new HashMap<String, Map<String, TableDescription>>();

        if (partInfo.getPartitionBy().getSubPartitionBy() != null) {
            firstPartSpec = firstPartSpec.getSubPartitions().get(0);
        }

        // referenceGroup: it's default values is the first partition.groupName
        String referenceGroupName = firstPartSpec.getLocation().getGroupKey();

        // referenceTable: it default values is the first partition.phyTableName
        String referenceTableName = firstPartSpec.getLocation().getPhyTableName();

        Map<String, Set<String>> dbTbActualTopology = partTblTopology;

        List<TableCheckResult> abnormalTableCheckResultList = new ArrayList<TableCheckResult>();
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
                    try {
                        TableDescription tableDescription =
                            CheckTableUtil.getTableDescription((MyRepository) this.repo,
                                targetGroup,
                                targetTable,
                                false,
                                schemaName);
                        tableNameDescMaps.put(targetTable, tableDescription);
                    } catch (Exception e) {
                        TableCheckResult abnormalTable = new TableCheckResult();
                        abnormalTable.setTableDesc(new TableDescription(targetGroup, targetTable));
                        abnormalTable.setExist(false);
                        abnormalTable.setFieldCountTheSame(false);
                        abnormalTable.setFieldDescTheSame(false);
                        abnormalTableCheckResultList.add(abnormalTable);
                    }
                }
                groupTableDescMaps.put(targetGroup, tableNameDescMaps);
            }
        }

        // =============校验库表元数据=============

        // 1. 1检查各分库的各个表的存在性；
        boolean isStatusOK = true;
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
            result.addRow(new Object[] {tableText, opText, statusText, statusOK});
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

    protected void doCheckForSingleTable(String schemaName, String groupName,
                                         String logicalTableName,
                                         String physicalTableName,
                                         ArrayResultCursor result) {

        TGroupDataSource tGroupDataSource =
            (TGroupDataSource) ExecutorContext.getContext(schemaName).getTopologyExecutor()
                .getGroupExecutor(groupName).getDataSource();
        StringBuilder targetSql = new StringBuilder("check table ");
        targetSql.append("`" + physicalTableName + "`");
        Throwable ex = null;
        try (Connection conn = (Connection) tGroupDataSource.getConnection();
            ResultSet rs = conn.createStatement().executeQuery(targetSql.toString())) {
            String tableText = String.format("%s.%s", schemaName, logicalTableName);
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
            if (ex != null) {
                GeneralUtil.nestedException(ex);
            }
        }
    }

    protected void doCheckForOnePartTableForeignKeys(String schemaName, String logicalTableName,
                                                     ExecutionContext executionContext, ArrayResultCursor result) {
        if (null == executionContext) {
            throw new TddlRuntimeException(ErrorCode.ERR_UNKNOWN_DATABASE, schemaName);
        }
        Map<String, ForeignKeyData> foreignKeyData =
            executionContext.getSchemaManager(schemaName).getTable(logicalTableName).getForeignKeys();
        for (ForeignKeyData data : foreignKeyData.values()) {
            if (data.isPushDown()) {
                doCheckForForeignKeyTopology(schemaName, logicalTableName, executionContext, result, data);
            }
        }
    }

    protected void doCheckForForeignKeyTopology(String schemaName, String logicalTableName,
                                                ExecutionContext executionContext,
                                                ArrayResultCursor result, ForeignKeyData data) {
        PartitionInfo partInfo =
            executionContext.getSchemaManager(schemaName).getTddlRuleManager().getPartitionInfoManager()
                .getPartitionInfo(logicalTableName);
        String tableText = String.format("%s.%s.%s:Foreign Key", schemaName, logicalTableName, data.constraint);
        String opText = "check";
        String statusText = "status";
        String msgText = statusOK;
        boolean flag = true;
        if (partInfo == null) {
            return;
        }
        Map<String, Set<String>> partTblTopology = partInfo.getTopology();
        List<String> columns = data.columns;
        for (String groupName : partTblTopology.keySet()) {
            String phyDbName = buildPhysicalDbNameFromGroupName(groupName);
            List<String> phyTableLists =
                partTblTopology.get(groupName).stream().map(String::toLowerCase).collect(Collectors.toList());
            List<String> phyRefTable = new ArrayList<>();
            Map<String, List<String>> phyColumnsMap =
                CheckTableUtil.getTableForeignKeyColumns(schemaName, groupName, phyTableLists, data.constraint,
                    phyDbName, phyRefTable);

            if (phyRefTable.isEmpty()) {
                msgText = String.format(
                    "foreign key '%s' doesn't match any referencing physical table on group '%s'",
                    data.constraint, groupName);
                result.addRow(new Object[] {tableText, opText, MsgType.error.name(), msgText});
            }
            boolean allEqual = phyRefTable.stream().distinct().limit(2).count() <= 1;
            if (!allEqual || !phyRefTable.isEmpty() && !phyRefTable.get(0).startsWith(data.refTableName)) {
                msgText = String.format(
                    "foreign key '%s' doesn't match on group '%s' referencing physical table '%s'",
                    data.constraint, groupName, phyRefTable.get(0));
                result.addRow(new Object[] {tableText, opText, MsgType.error.name(), msgText});
            }
            for (String phyTable : phyTableLists) {
                if (!phyColumnsMap.containsKey(phyTable) || phyColumnsMap.get(phyTable).isEmpty()) {
                    flag = false;
                    msgText = String.format(
                        "foreign key '%s' doesn't exist on group '%s' for physical table '%s'",
                        data.constraint, groupName, phyTable);
                    result.addRow(new Object[] {tableText, opText, MsgType.error.name(), msgText});
                } else {
                    List<String> phyColumns = phyColumnsMap.get(phyTable);
                    List<String> redundantColumns =
                        phyColumns.stream().filter(o -> !columns.stream().anyMatch(o::equalsIgnoreCase)).collect(
                            Collectors.toList());
                    List<String> lackColumns =
                        columns.stream().filter(o -> !phyColumns.stream().anyMatch(o::equalsIgnoreCase))
                            .collect(Collectors.toList());
                    if (!lackColumns.isEmpty() || !redundantColumns.isEmpty()) {
                        flag = false;
                        String msgText1 = String.format(
                            "foreign key '%s' on group '%s' for physical table '%s' has redundant columns: '%s'. ",
                            data.constraint, groupName, phyTable, StringUtils.join(redundantColumns, ","));
                        String msgText2 = String.format(
                            "foreign key '%s' on group '%s' for physical table '%s' lack columns: '%s'. ",
                            data.constraint, groupName, phyTable, StringUtils.join(lackColumns, ","));
                        msgText =
                            (redundantColumns.isEmpty() ? "" : msgText1) + (lackColumns.isEmpty() ? "" : msgText2);
                        result.addRow(new Object[] {tableText, opText, MsgType.error.name(), msgText});
                    }
                }
            }
        }
        if (flag) {
            result.addRow(new Object[] {tableText, opText, statusText, msgText});
        }
    }
}
