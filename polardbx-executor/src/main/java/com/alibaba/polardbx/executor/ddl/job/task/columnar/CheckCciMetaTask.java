/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the License);
 * you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.polardbx.executor.ddl.job.task.columnar;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.annotation.JSONCreator;
import com.alibaba.polardbx.common.cdc.CdcDdlRecord;
import com.alibaba.polardbx.common.cdc.CdcManagerHelper;
import com.alibaba.polardbx.common.cdc.entity.DDLExtInfo;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.executor.ddl.job.task.cdc.CdcMarkUtil;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.executor.gsi.CheckerManager;
import com.alibaba.polardbx.executor.gsi.CheckerManager.CheckerReport;
import com.alibaba.polardbx.executor.gsi.CheckerManager.CheckerReportStatus;
import com.alibaba.polardbx.gms.metadb.table.ColumnarColumnEvolutionRecord;
import com.alibaba.polardbx.gms.metadb.table.ColumnarTableEvolutionRecord;
import com.alibaba.polardbx.gms.metadb.table.ColumnarTableMappingRecord;
import com.alibaba.polardbx.gms.metadb.table.ColumnarTableStatus;
import com.alibaba.polardbx.gms.metadb.table.ColumnsRecord;
import com.alibaba.polardbx.gms.metadb.table.IndexStatus;
import com.alibaba.polardbx.gms.metadb.table.IndexesRecord;
import com.alibaba.polardbx.gms.metadb.table.TableInfoManager;
import com.alibaba.polardbx.gms.metadb.table.TablesRecord;
import com.alibaba.polardbx.gms.partition.TablePartitionRecord;
import com.alibaba.polardbx.gms.tablegroup.PartitionGroupRecord;
import com.alibaba.polardbx.gms.tablegroup.TableGroupRecord;
import com.alibaba.polardbx.gms.util.TableGroupNameUtil;
import com.alibaba.polardbx.optimizer.config.table.GsiMetaManager;
import com.alibaba.polardbx.optimizer.config.table.SchemaManager;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.CheckCciPrepareData;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.gsi.CreateGlobalIndexPreparedData;
import com.alibaba.polardbx.optimizer.parse.FastsqlParser;
import com.alibaba.polardbx.optimizer.partition.common.PartitionTableType;
import com.cronutils.utils.Preconditions;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.sql.SqlCheckColumnarIndex.CheckCciExtraCmd;
import org.apache.calcite.sql.SqlCreateIndex;
import org.apache.calcite.sql.SqlCreateTable;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlIndexColumnName;
import org.apache.calcite.sql.SqlIndexDefinition;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.commons.lang3.tuple.Triple;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

@Getter
@TaskName(name = "CheckCciMetaTask")
public class CheckCciMetaTask extends CheckCciBaseTask {
    @JSONCreator
    public CheckCciMetaTask(String schemaName,
                            String tableName,
                            String indexName,
                            CheckCciExtraCmd extraCmd) {
        super(schemaName, tableName, indexName, extraCmd);
    }

    public static CheckCciMetaTask create(CheckCciPrepareData prepareData) {
        return new CheckCciMetaTask(
            prepareData.getSchemaName(),
            prepareData.getTableName(),
            prepareData.getIndexName(),
            prepareData.getExtraCmd()
        );
    }

    @Override
    protected void duringTransaction(Connection metaDbConnection, ExecutionContext executionContext) {
        final TableInfoManager tableInfoManager = new TableInfoManager();
        tableInfoManager.setConnection(metaDbConnection);
        final Blackboard blackboard = new Blackboard(metaDbConnection, executionContext);

        final List<CheckerReport> reports = new ArrayList<>(checkAndReport(blackboard));

        // Add finish record
        reports.add(
            createReportRecord(
                ReportErrorType.SUMMARY,
                CheckerReportStatus.FINISH,
                "metadata of columnar index checked"));

        // Add reports to metadb.checker_reports
        CheckerManager.insertReports(metaDbConnection, reports);
    }

    @NotNull
    private List<CheckerReport> checkAndReport(Blackboard blackboard) {

        // 1.Check primary and cci exists
        final List<CheckerReport> cciNotExists = checkCciTableAndIndex(blackboard);
        if (!cciNotExists.isEmpty()) {
            return cciNotExists;
        }

        // 2. Check primary and cci column identical (by default, all columnar index is clustered index)
        final List<CheckerReport> reports = new ArrayList<>(checkCciColumns(blackboard));

        // 3. Check columnar index related meta
        reports.addAll(checkCciTableAndColumnEvolution(blackboard));

        // 4. Check CDC mark
        reports.addAll(checkCdcDdlMark(blackboard));

        // 5. Check CCI partitioning (table partition, table group)
        reports.addAll(checkCciPartitioning(blackboard));

        // 6. Check CCI status in TableMetaManager
        reports.addAll(checkCciMemoryStatus(blackboard));

        return reports;
    }

    @NotNull
    private List<CheckerReport> checkCciMemoryStatus(Blackboard blackboard) {
        final List<CheckerReport> reports = new ArrayList<>();
        final SchemaManager sm = blackboard.ec.getSchemaManager(schemaName);
        if (null == sm) {
            reports.add(
                createReportRecord(
                    ReportErrorType.SCHEMA_NOT_EXISTS,
                    CheckerReportStatus.FOUND,
                    String.format("Database %s not exists", schemaName)));
            return reports;
        }

        final TableMeta primaryTable = sm.getTable(tableName);
        if (null == primaryTable) {
            reports.add(
                createReportRecord(
                    ReportErrorType.TABLE_NOT_EXISTS,
                    CheckerReportStatus.FOUND,
                    String.format("No primary table named '%s' in database %s", tableName, schemaName)));
            return reports;
        }

        final GsiMetaManager.GsiTableMetaBean gsiTableMetaBean = primaryTable.getGsiTableMetaBean();
        if (null == gsiTableMetaBean || GeneralUtil.isEmpty(gsiTableMetaBean.indexMap)) {
            reports.add(
                createReportRecord(
                    ReportErrorType.TABLE_NOT_EXISTS,
                    CheckerReportStatus.FOUND,
                    String.format("No index named '%s' in database %s", indexName, schemaName)));
            return reports;
        }

        final Optional<GsiMetaManager.GsiIndexMetaBean> cciBeanOptional = gsiTableMetaBean
            .indexMap
            .values()
            .stream()
            .filter(e -> TStringUtil.equalsIgnoreCase(e.indexName, indexName))
            .filter(cciBean -> {
                if (!cciBean.columnarIndex) {
                    reports.add(
                        createReportRecord(
                            ReportErrorType.UNEXPECTED_CACHED_INDEX_TYPE,
                            CheckerReportStatus.FOUND,
                            String.format("Unexpected cached index type %s", cciBean.indexType)));
                } else if (cciBean.indexStatus != IndexStatus.PUBLIC) {
                    reports.add(
                        createReportRecord(
                            ReportErrorType.UNEXPECTED_CACHED_INDEX_STATUS,
                            CheckerReportStatus.FOUND,
                            String.format("Unexpected cached index status %s", cciBean.indexStatus)));
                } else {
                    return true;
                }

                return false;
            })
            .findFirst();

        if (!cciBeanOptional.isPresent()) {
            reports.add(
                createReportRecord(
                    ReportErrorType.TABLE_NOT_EXISTS,
                    CheckerReportStatus.FOUND,
                    String.format("No public cci named '%s' in table meta cache", indexName)));
            return reports;
        }

        return reports;
    }

    /**
     * Check table group / table partition config
     */
    @NotNull
    private List<CheckerReport> checkCciPartitioning(Blackboard blackboard) {
        final List<CheckerReport> reports = new ArrayList<>();

        // Check table partition record exists and type is PartitionTableType.COLUMNAR_TABLE
        final List<TablePartitionRecord> partitionRecords = blackboard.queryTablePartition(schemaName, indexName);

        if (GeneralUtil.isEmpty(partitionRecords)) {
            reports.add(
                createReportRecord(
                    ReportErrorType.MISSING_COLUMNAR_TABLE_PARTITION_META,
                    CheckerReportStatus.FOUND,
                    String.format(
                        "Missing table partition record for index %s.%s.%s",
                        schemaName, tableName, indexName)));
        }

        final List<TablePartitionRecord> rootRecord = new ArrayList<>();
        final List<TablePartitionRecord> leafRecord = new ArrayList<>();

        partitionRecords.forEach(pr -> {
            if (pr.partLevel == 0) {
                rootRecord.add(pr);
            } else if (pr.partLevel > 0 && pr.nextLevel < 0) {
                leafRecord.add(pr);
                if (!TStringUtil.equals(pr.partEngine, TablePartitionRecord.PARTITION_ENGINE_COLUMNAR)) {
                    reports.add(
                        createReportRecord(
                            ReportErrorType.UNEXPECTED_COLUMNAR_COLUMNAR_TABLE_PARTITION_DEFINITION,
                            CheckerReportStatus.FOUND,
                            String.format(
                                "Unexpected partEngine value %s of table partition record %s of %s.%s.%s",
                                pr.partEngine, pr.id, schemaName, tableName, indexName)));
                }
            }

            if (pr.tblType != PartitionTableType.COLUMNAR_TABLE.getTableTypeIntValue()) {
                reports.add(
                    createReportRecord(
                        ReportErrorType.UNEXPECTED_COLUMNAR_COLUMNAR_TABLE_PARTITION_DEFINITION,
                        CheckerReportStatus.FOUND,
                        String.format(
                            "Unexpected tblType value %s of table partition record %s of %s.%s.%s",
                            pr.tblType, pr.id, schemaName, tableName, indexName)));
            }
        });

        Long tgId = -1L;
        if (rootRecord.size() != 1) {
            reports.add(
                createReportRecord(
                    ReportErrorType.MISSING_COLUMNAR_TABLE_LOGICAL_PARTITION_META,
                    CheckerReportStatus.FOUND,
                    String.format(
                        "Missing logical partition record for index %s.%s.%s",
                        schemaName, tableName, indexName)));
        } else {
            // Check table group exists and type is TableGroupRecord.TG_TYPE_COLUMNAR_TBL_TG
            tgId = rootRecord.get(0).getGroupId();
            final List<TableGroupRecord> tgRecords = blackboard.queryTableGroup(tgId);
            if (tgRecords.size() != 1) {
                reports.add(
                    createReportRecord(
                        ReportErrorType.MISSING_COLUMNAR_TABLE_GROUP_META,
                        CheckerReportStatus.FOUND,
                        String.format(
                            "Missing table group record for index %s.%s.%s",
                            schemaName, tableName, indexName)));
            } else {
                final TableGroupRecord actualTg = tgRecords.get(0);
                final int expectedTgType = TableGroupRecord.TG_TYPE_COLUMNAR_TBL_TG;
                final String expectedTgName = TableGroupNameUtil.autoBuildTableGroupName(tgId, expectedTgType);
                if (actualTg.tg_type != expectedTgType || !TStringUtil.equals(actualTg.tg_name, expectedTgName)) {
                    reports.add(
                        createReportRecord(
                            ReportErrorType.UNEXPECTED_COLUMNAR_COLUMNAR_TABLE_PARTITION_DEFINITION,
                            CheckerReportStatus.FOUND,
                            String.format(
                                "Unexpected tblType value %s / tgName value %s table partition record %s of %s.%s.%s",
                                actualTg.tg_type, actualTg.tg_name, actualTg, schemaName, tableName, indexName)));
                }
            }
        }

        if (leafRecord.isEmpty()) {
            reports.add(
                createReportRecord(
                    ReportErrorType.MISSING_COLUMNAR_TABLE_PHYSICAL_PARTITION_META,
                    CheckerReportStatus.FOUND,
                    String.format(
                        "Missing physical partition record for index %s.%s.%s",
                        schemaName, tableName, indexName)));
        } else if (tgId >= 0L) {
            // Check partition group config match with table partition config
            final List<PartitionGroupRecord> partitionGroupRecords = blackboard.queryPartitionGroupByTgId(tgId);
            reports.addAll(CheckerBuilder
                .keyListChecker(leafRecord, partitionGroupRecords)
                .withActualKeyGenerator(a -> a.groupId)
                .withExpectedKeyGenerator(e -> e.id)
                .withOrphanReporter(msgs -> createReportRecord(
                    ReportErrorType.ORPHAN_COLUMNAR_TABLE_PHYSICAL_PARTITION_META,
                    CheckerReportStatus.FOUND,
                    String.format(
                        "Orphan physical partition record with partition group of %s for index %s.%s.%s",
                        String.join(",", msgs), schemaName, tableName, indexName)))
                .withMissingReporter(msgs -> createReportRecord(
                    ReportErrorType.MISSING_COLUMNAR_TABLE_PHYSICAL_PARTITION_META,
                    CheckerReportStatus.FOUND,
                    String.format(
                        "Missing physical partition record of partition group %s for index %s.%s.%s",
                        String.join(",", msgs), schemaName, tableName, indexName)))
                .build()
                .check()
                .report());
        }

        return reports;
    }

    @NotNull
    private List<CheckerReport> checkCdcDdlMark(Blackboard blackboard) {
        final List<CheckerReport> reports = new ArrayList<>();

        final List<ColumnarTableMappingRecord> columnarTableMappingRecords =
            blackboard.queryColumnarTableMapping(schemaName, tableName, indexName);

        // Table mapping has been checked in #checkCciTableAndColumnEvolution
        final ColumnarTableMappingRecord columnarTableMapping = columnarTableMappingRecords.get(0);
        final long indexTableId = columnarTableMapping.tableId;
        final long latestVersionId = columnarTableMapping.latestVersionId;

        final ColumnarTableEvolutionRecord columnarTableEvolutionRecord =
            blackboard.queryColumnarTableEvolution(indexTableId, latestVersionId).get(0);
        final long ddlJobId = columnarTableEvolutionRecord.ddlJobId;

        final List<CdcDdlRecord> cdcDdlRecords = CdcManagerHelper.getInstance().queryDdlByJobId(ddlJobId);

        // For create table with cci, there will be ONLY ONE ddl mark record for CREATE TABLE.
        final List<CdcDdlRecord> filteredRecords = new ArrayList<>();
        cdcDdlRecords
            .stream()
            .filter(cdr -> TStringUtil.equalsIgnoreCase(cdr.getSqlKind(), SqlKind.CREATE_INDEX.name())
                || TStringUtil.equalsIgnoreCase(cdr.getSqlKind(), SqlKind.CREATE_TABLE.name()))
            .filter(cdr -> TStringUtil.containsIgnoreCase(cdr.getDdlSql(), "create clustered columnar")
                || TStringUtil.containsIgnoreCase(cdr.getExt(), "clustered columnar"))
            .forEach(filteredRecords::add);

        if (GeneralUtil.isEmpty(filteredRecords)) {
            reports.add(
                createReportRecord(
                    ReportErrorType.MISSING_CDC_MARK_CREATE_INDEX,
                    CheckerReportStatus.FOUND,
                    String.format(
                        "Missing CDC mark FOR index %s.%s.%s",
                        schemaName, tableName, indexName)));
        } else if (filteredRecords.size() > 1) {
            reports.add(
                createReportRecord(
                    ReportErrorType.DUPLICATED_CDC_MARK_CREATE_INDEX,
                    CheckerReportStatus.FOUND,
                    String.format(
                        "Duplicated CDC mark FOR index %s.%s.%s: %s",
                        schemaName, tableName, indexName, cdcDdlRecords.size())));
        }

        if (!reports.isEmpty()) {
            return reports;
        }

        // Get DDLExtInfo
        final CdcDdlRecord cdcDdlRecord = filteredRecords.get(0);
        final DDLExtInfo ddlExtInfo = JSONObject.parseObject(cdcDdlRecord.ext, DDLExtInfo.class);
        if (null == ddlExtInfo.getDdlId() || ddlExtInfo.getDdlId() <= 0) {
            reports.add(
                createReportRecord(
                    ReportErrorType.WRONG_CDC_MARK_DDL_ID,
                    CheckerReportStatus.FOUND,
                    String.format(
                        "Wrong cdc mark ddl_id: %s", ddlExtInfo.getDdlId())));
        }

        // Check CREATE INDEX / CREATE TABLE statement
        final String ddlSql = ddlExtInfo.getOriginalDdl();
        reports.addAll(checkCreateCciSql(ddlSql, latestVersionId, blackboard));

        return reports;
    }

    @NotNull
    private List<CheckerReport> checkCreateCciSql(String ddlSql, Long versionId, Blackboard blackboard) {
        final List<CheckerReport> reports = new ArrayList<>();

        final String hint = CdcMarkUtil.buildVersionIdHint(versionId);
        final boolean withHint = TStringUtil.contains(ddlSql, hint);
        if (!withHint) {
            reports.add(
                createReportRecord(
                    ReportErrorType.WRONG_CDC_MARK_STATEMENT,
                    CheckerReportStatus.FOUND,
                    String.format("Expect hint '%s' but get %s",
                        hint,
                        Optional
                            .ofNullable(ddlSql)
                            .orElse("NULL"))));
        }

        final ExecutionContext parserEc = blackboard.ec.copy();
        parserEc.setSchemaName(schemaName);
        final SqlNode sqlNode = new FastsqlParser().parse(ddlSql, parserEc).get(0);
        final boolean isCreateIndex = sqlNode instanceof SqlCreateIndex;
        final boolean isCreateTable = sqlNode instanceof SqlCreateTable;
        if (!(isCreateTable || isCreateIndex)) {
            reports.add(
                createReportRecord(
                    ReportErrorType.WRONG_CDC_MARK_STATEMENT,
                    CheckerReportStatus.FOUND,
                    String.format("Expect CREATE INDEX or CREATE TABLE but get %s",
                        Optional
                            .ofNullable(sqlNode)
                            .map(sn -> sn.getKind().name())
                            .orElse("NULL"))));
            return reports;
        }

        if (isCreateIndex) {
            reports.addAll(checkCreateIndex(ddlSql, blackboard, (SqlCreateIndex) sqlNode));
        }

        if (isCreateTable) {
            reports.addAll(checkCreateTable(ddlSql, blackboard, (SqlCreateTable) sqlNode));
        }

        return reports;
    }

    private List<CheckerReport> checkCreateTable(String ddlSql,
                                                 Blackboard blackboard,
                                                 final SqlCreateTable createTableWithCci) {
        final List<CheckerReport> reports = new ArrayList<>();

        final List<org.apache.calcite.util.Pair<SqlIdentifier, SqlIndexDefinition>> columnarKeys =
            createTableWithCci.getColumnarKeys();

        final List<SqlIndexDefinition> createCciList = Optional.ofNullable(columnarKeys)
            .map(m -> m
                .stream()
                .filter(p -> TStringUtil.startsWithIgnoreCase(this.indexName, p.left.getLastName()))
                .map(org.apache.calcite.util.Pair::getValue)
                .collect(Collectors.toList()))
            .orElse(null);

        final SqlIdentifier tableName = (SqlIdentifier) createTableWithCci.getName();
        if (GeneralUtil.isEmpty(createCciList)
            || !TStringUtil.equalsIgnoreCase(this.tableName, tableName.getLastName())) {
            reports.add(
                createReportRecord(
                    ReportErrorType.WRONG_CDC_MARK_STATEMENT,
                    CheckerReportStatus.FOUND,
                    String.format(
                        "Wrong table or index name found in statement: %s",
                        ddlSql)));
            return reports;
        }

        if (createCciList.size() > 1) {
            reports.add(
                createReportRecord(
                    ReportErrorType.WRONG_CDC_MARK_STATEMENT,
                    CheckerReportStatus.FOUND,
                    String.format(
                        "Multi cci definition found in statement: %s",
                        ddlSql)));
            return reports;
        }

        final SqlCreateIndex createCci = CreateGlobalIndexPreparedData.indexDefinition2CreateIndex(
            createCciList.get(0),
            null,
            null,
            null,
            null);

        return checkCreateCciDef(ddlSql, blackboard, createCci);
    }

    private List<CheckerReport> checkCreateIndex(String ddlSql, Blackboard blackboard, final SqlCreateIndex createCci) {
        final List<CheckerReport> reports = new ArrayList<>();

        final SqlIdentifier indexName = createCci.getIndexName();
        final SqlIdentifier tableName = createCci.getOriginTableName();
        if (!TStringUtil.startsWithIgnoreCase(this.indexName, indexName.getLastName())
            || !TStringUtil.equalsIgnoreCase(this.tableName, tableName.getLastName())) {
            reports.add(
                createReportRecord(
                    ReportErrorType.WRONG_CDC_MARK_STATEMENT,
                    CheckerReportStatus.FOUND,
                    String.format(
                        "Wrong table or index name found in statement: %s",
                        ddlSql)));
        }

        reports.addAll(checkCreateCciDef(ddlSql, blackboard, createCci));
        return reports;
    }

    private List<CheckerReport> checkCreateCciDef(String ddlSql, Blackboard blackboard, SqlCreateIndex createCci) {
        final List<CheckerReport> reports = new ArrayList<>();

        if (!createCci.createCci()
            || !createCci.createClusteredIndex()
            || !createCci.createGsi()) {
            reports.add(
                createReportRecord(
                    ReportErrorType.WRONG_CDC_MARK_STATEMENT,
                    CheckerReportStatus.FOUND,
                    String.format(
                        "Wrong index type found in statement: %s",
                        ddlSql)));
        }

        // Using user-input sql, it's possible that no partitioning clause exists
//        if (createCci.getPartitioning() == null
//            && createCci.getDbPartitionBy() == null) {
//            reports.add(
//                createReportRecord(
//                    ReportErrorType.WRONG_CDC_MARK_STATEMENT,
//                    CheckerReportStatus.FOUND,
//                    String.format(
//                        "Missing partitioning part in statement: %s",
//                        ddlSql)));
//        }

        final List<SqlIndexColumnName> indexColumns = createCci.getColumns();
        final List<SqlIndexColumnName> covering = createCci.getCovering();

        if (GeneralUtil.isEmpty(indexColumns)) {
            reports.add(
                createReportRecord(
                    ReportErrorType.WRONG_CDC_MARK_STATEMENT,
                    CheckerReportStatus.FOUND,
                    String.format(
                        "Missing sort key in statement: %s",
                        ddlSql)));
            return reports;
        }

        final Set<String> indexColumnSet = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        final Set<String> coveringColumnSet = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        final List<SqlIndexColumnName> actualColumnNames = new ArrayList<>(indexColumns);
        indexColumns.forEach(ic -> {
            indexColumnSet.add(ic.getColumnNameStr());
            actualColumnNames.add(ic);
        });
        if (null != covering) {
            covering.forEach(ic -> {
                coveringColumnSet.add(ic.getColumnNameStr());
                actualColumnNames.add(ic);
            });
        }

        final List<IndexesRecord> expectedIndexRecords = blackboard.queryIndexes(
            this.schemaName,
            this.tableName,
            this.indexName);
        final List<IndexesRecord> filteredExpectedIndexRecords = expectedIndexRecords
            .stream()
            .filter(ir -> !TStringUtil.equalsIgnoreCase(ir.comment, "COVERING")
                || coveringColumnSet.contains(ir.columnName))
            .collect(Collectors.toList());

        final List<CheckerReport> indexColumnReports = CheckerBuilder
            .stringKeyListChecker(actualColumnNames, filteredExpectedIndexRecords, true)
            .withActualKeyGenerator(SqlIndexColumnName::getColumnNameStr)
            .withExpectedKeyGenerator(e -> e.columnName)
            .withDefValidator((a, e) -> {
                final String cn = a.getColumnNameStr();
                final boolean validateIndexColumn = indexColumnSet.contains(cn)
                    && TStringUtil.equalsIgnoreCase(e.comment, "INDEX")
                    && e.isColumnar()
                    && e.isClustered();
                final boolean validateCoveringColumn = coveringColumnSet.contains(cn)
                    && TStringUtil.equalsIgnoreCase(e.comment, "COVERING");
                return validateIndexColumn || validateCoveringColumn;
            })
            .withOrphanReporter(msgs -> createReportRecord(
                ReportErrorType.WRONG_CDC_MARK_STATEMENT,
                CheckerReportStatus.FOUND,
                String.format(
                    "Orphan column %s found in statement: %s",
                    String.join(",", msgs),
                    ddlSql)))
            .withMissingReporter(msgs -> createReportRecord(
                ReportErrorType.WRONG_CDC_MARK_STATEMENT,
                CheckerReportStatus.FOUND,
                String.format(
                    "Missing column %s in statement: %s",
                    String.join(",", msgs),
                    ddlSql)))
            .withInvalidateDefReporter(msgs -> createReportRecord(
                ReportErrorType.WRONG_CDC_MARK_STATEMENT,
                CheckerReportStatus.FOUND,
                String.format(
                    "Unmatched definition of column %s found in statement: %s",
                    String.join(",", msgs),
                    ddlSql)))
            .build()
            .check()
            .report();

        reports.addAll(indexColumnReports);

        return reports;
    }

    /**
     * Check cci table and column evolution meta exists
     */
    @NotNull
    private List<CheckerReport> checkCciTableAndColumnEvolution(Blackboard blackboard) {
        final List<CheckerReport> reports = new ArrayList<>();

        final List<ColumnarTableMappingRecord> columnarTableMappingRecords =
            blackboard.queryColumnarTableMapping(schemaName, tableName, indexName);
        // Get table_id and check columnar table mapping record
        if (columnarTableMappingRecords.isEmpty()) {
            reports.add(
                createReportRecord(
                    ReportErrorType.MISSING_TABLE_MAPPING_META,
                    CheckerReportStatus.FOUND,
                    String.format(
                        "Missing table mapping meta of index %s.%s.%s",
                        schemaName, tableName, indexName)));
        } else if (columnarTableMappingRecords.size() > 1) {
            reports.add(
                createReportRecord(
                    ReportErrorType.DUPLICATED_TABLE_MAPPING_META,
                    CheckerReportStatus.FOUND,
                    String.format(
                        "Duplicated table mapping meta for index %s.%s.%s: %s",
                        schemaName,
                        tableName,
                        indexName,
                        columnarTableMappingRecords
                            .stream()
                            .map(ctmr -> String.valueOf(ctmr.tableId))
                            .collect(Collectors.joining()))));
        }

        if (!reports.isEmpty()) {
            return reports;
        }

        // Check table mapping status
        final ColumnarTableMappingRecord columnarTableMapping = columnarTableMappingRecords.get(0);
        final long indexTableId = columnarTableMapping.tableId;
        final long latestVersionId = columnarTableMapping.latestVersionId;
        if (ColumnarTableStatus.from(columnarTableMapping.status) != ColumnarTableStatus.PUBLIC) {
            reports.add(
                createReportRecord(
                    ReportErrorType.UNEXPECTED_COLUMNAR_TABLE_MAPPING_META,
                    CheckerReportStatus.FOUND,
                    String.format(
                        "Unexpected status %s of index %s.%s.%s",
                        columnarTableMapping.status, schemaName, tableName, indexName)));
        }

        final List<ColumnarTableEvolutionRecord> tableEvolutionRecords =
            blackboard.queryColumnarTableEvolution(indexTableId, latestVersionId);
        // Table evolution meta of column store
        if (tableEvolutionRecords.isEmpty()) {
            reports.add(
                createReportRecord(
                    ReportErrorType.MISSING_COLUMNAR_TABLE_EVOLUTION_META,
                    CheckerReportStatus.FOUND,
                    String.format(
                        "Missing columnar table evolution meta of index %s.%s.%s",
                        schemaName, tableName, indexName)));
            return reports;
        }

        // Check columnar column evolution records
        final List<ColumnarColumnEvolutionRecord> actualColumnarColumnEvoRecords =
            blackboard.queryColumnarColumnEvolution(tableEvolutionRecords.get(0).columns);
        if (actualColumnarColumnEvoRecords.isEmpty()) {
            reports.add(
                createReportRecord(
                    ReportErrorType.MISSING_COLUMNAR_COLUMN_EVOLUTION_META,
                    CheckerReportStatus.FOUND,
                    String.format(
                        "Missing columnar column evolution meta of index %s.%s.%s",
                        schemaName, tableName, indexName)));
            return reports;
        }

        // Column meta of row store (expected)
        final List<ColumnsRecord> expectedColumnRecords = blackboard.queryColumns(schemaName, indexName);
        // Column meta of column store (actual)
        // map<fieldId, list<id, columnsRecord>>
        final Map<Long, List<Pair<Long, ColumnsRecord>>> colEvoMap = new HashMap<>();
        // map<columnName, fieldId>
        final Map<String, Long> actualColumnNameFieldIdMap = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        actualColumnarColumnEvoRecords.forEach(ccer -> {
            actualColumnNameFieldIdMap.put(ccer.columnsRecord.columnName, ccer.fieldId);
            colEvoMap
                .computeIfAbsent(ccer.fieldId, k -> new ArrayList<>())
                .add(Pair.of(ccer.id, ccer.columnsRecord));
        });
        // map<fieldId, columnsRecord>
        final Map<Long, Pair<Long, ColumnsRecord>> actualColLatestMap = new HashMap<>();
        colEvoMap.forEach((fieldId, columnarRecordPair) ->
            columnarRecordPair
                .stream()
                .max(Comparator.comparing(Pair::getKey))
                .ifPresent(cr -> actualColLatestMap.put(fieldId, cr)));
        // map<columnName, columnsRecord>
        final ImmutableConcatMap<String, Long, Pair<Long, ColumnsRecord>> actualColumnRecordMap =
            new ImmutableConcatMap<>(actualColumnNameFieldIdMap, actualColLatestMap);

        // Check columnar column evolution records (reverseOrder)
        reports.addAll(checkColumnEvolutionRecords(expectedColumnRecords, actualColumnRecordMap));

        // Check columnar table evolution records
        reports.addAll(checkTableEvolutionRecord(tableEvolutionRecords.get(0), actualColLatestMap));

        return reports;
    }

    @NotNull
    private List<CheckerReport> checkColumnEvolutionRecords(List<ColumnsRecord> indexColumns,
                                                            Map<String, Pair<Long, ColumnsRecord>> expectedMap) {
        return CheckerBuilder
            .stringKeyListChecker(indexColumns, expectedMap, true)
            .withReverseOrderCheck()
            .withActualKeyGenerator(a -> a.columnName)
            .withExpectedKeyGenerator(e -> e.getValue().columnName)
            .withDefValidator((a, e) -> equalsColumnRecord(a, e.getValue()))
            .withOrdValidator((a, e) -> a.ordinalPosition == e.getValue().ordinalPosition)
            .withOrphanReporter(msgs -> createReportRecord(
                ReportErrorType.ORPHAN_COLUMNAR_COLUMN_EVOLUTION_META,
                CheckerReportStatus.FOUND,
                String.format(
                    "Orphan column evolution meta found for column: %s",
                    String.join(",", msgs))))
            .withMissingReporter(msgs -> createReportRecord(
                ReportErrorType.MISSING_COLUMNAR_COLUMN_EVOLUTION_META,
                CheckerReportStatus.FOUND,
                String.format(
                    "Missing column evolution meta for column: %s",
                    String.join(",", msgs))))
            .withInvalidateDefReporter(msgs -> createReportRecord(
                ReportErrorType.UNMATCHED_COLUMNAR_COLUMN_EVOLUTION_DEFINITION,
                CheckerReportStatus.FOUND,
                String.format(
                    "Unmatched column evolution definition found for column: %s",
                    String.join(",", msgs))))
            .withInvalidateOrdReporter(msgs -> createReportRecord(
                ReportErrorType.UNMATCHED_COLUMNAR_COLUMN_EVOLUTION_ORDER,
                CheckerReportStatus.FOUND,
                String.format(
                    "Unmatched column evolution order found for column: %s",
                    String.join(",", msgs))))
            .build()
            .check()
            .report();
    }

    @NotNull
    private List<CheckerReport> checkTableEvolutionRecord(ColumnarTableEvolutionRecord tableEvolutionRecord,
                                                          Map<Long, Pair<Long, ColumnsRecord>> colLatestMap) {
        return CheckerBuilder
            .listChecker(tableEvolutionRecord.columns, colLatestMap)
            .withActualKeyGenerator(Ord::getValue)
            .withExpectedKeyGenerator(Pair::getKey)
            .withOrdValidator((a, e) -> a.getKey() + 1 == e.getValue().ordinalPosition)
            .withMissingMsgFromExpectedGenerator(
                e -> String.format("%s[%s]", e.getValue().columnName, e.getValue().ordinalPosition))
            .withInvalidateOrdMsgGenerator((a, e) -> String.format("%s[%s](%s -> %s)",
                e.getValue().columnName,
                a.getValue(),
                e.getValue().ordinalPosition,
                a.getKey() + 1))
            .withOrphanReporter(msgs -> createReportRecord(
                ReportErrorType.ORPHAN_COLUMNAR_TABLE_EVOLUTION_FIELD_ID,
                CheckerReportStatus.FOUND,
                String.format(
                    "Orphan table evolution field id found for column: %s",
                    String.join(",", msgs))))
            .withMissingReporter(msgs -> createReportRecord(
                ReportErrorType.MISSING_COLUMNAR_TABLE_EVOLUTION_FIELD_ID,
                CheckerReportStatus.FOUND,
                String.format(
                    "Missing table evolution field id for column: %s",
                    String.join(",", msgs))))
            .withInvalidateOrdReporter(msgs -> createReportRecord(
                ReportErrorType.UNMATCHED_COLUMNAR_TABLE_EVOLUTION_ORDER,
                CheckerReportStatus.FOUND,
                String.format(
                    "Unmatched table evolution column order found for column: %s",
                    String.join(",", msgs))))
            .build()
            .check()
            .report();
    }

    /**
     * Check cci exists
     * & Check cci belongs to primary table
     * & Check cci table type
     * & Check cci index information
     */
    @NotNull
    private List<CheckerReport> checkCciTableAndIndex(Blackboard blackboard) {
        final List<CheckerReport> reports = new ArrayList<>();

        // Table or index not exists
        if (TStringUtil.isBlank(tableName)) {
            reports.add(
                createReportRecord(
                    ReportErrorType.TABLE_NOT_EXISTS,
                    CheckerReportStatus.FOUND,
                    String.format("No columnar index named '%s' in database %s", indexName, schemaName)));
            return reports;
        }

        final TablesRecord primaryTable = blackboard.queryTable(schemaName, tableName);
        final TablesRecord indexTable = blackboard.queryTable(schemaName, indexName);

        if (null != primaryTable && null != indexTable) {
            final List<IndexesRecord> indexesRecords = blackboard.queryIndexes(schemaName, tableName, indexName);
            if (null == indexesRecords) {
                reports.add(
                    createReportRecord(
                        ReportErrorType.MISSING_INDEX_META,
                        CheckerReportStatus.FOUND,
                        String.format(
                            "Missing table-index relationship between table %s and index %s",
                            tableName,
                            indexName)));
            } else {
                final List<ColumnsRecord> primaryColumns = blackboard.queryColumns(schemaName, tableName);

                final List<CheckerReport> indexRecordReports = CheckerBuilder
                    .stringKeyListChecker(indexesRecords, primaryColumns, true)
                    .withActualKeyGenerator(a -> a.columnName)
                    .withExpectedKeyGenerator(e -> e.columnName)
                    .withDefValidator((a, e) -> validateIndexRecord(e, a))
                    .withDistValidator()
                    .withOrphanReporter(msgs -> createReportRecord(
                        ReportErrorType.ORPHAN_INDEX_META,
                        CheckerReportStatus.FOUND,
                        String.format(
                            "Orphan index column meta found for column: %s",
                            String.join(",", msgs))))
                    .withDuplicatedReporter(msgs -> createReportRecord(
                        ReportErrorType.DUPLICATED_INDEX_META,
                        CheckerReportStatus.FOUND,
                        String.format(
                            "Duplicated index column meta found for column: %s",
                            String.join(",", msgs))))
                    .withInvalidateDefReporter(msgs -> createReportRecord(
                        ReportErrorType.INVALID_INDEX_META,
                        CheckerReportStatus.FOUND,
                        String.format(
                            "Invalid index column meta found for column: %s",
                            String.join(",", msgs))))
                    .withMissingReporter(msgs -> createReportRecord(
                        ReportErrorType.MISSING_INDEX_META,
                        CheckerReportStatus.FOUND,
                        String.format(
                            "Missing index column meta for column: %s",
                            String.join(",", msgs))))
                    .build()
                    .check()
                    .report();

                reports.addAll(indexRecordReports);
            }
        } else if (null == primaryTable) {
            // Primary table not exists
            reports.add(
                createReportRecord(
                    ReportErrorType.TABLE_NOT_EXISTS,
                    CheckerReportStatus.FOUND,
                    String.format("Primary table %s not exists in database %s", tableName, schemaName)));
        } else {
            // Index table not exists
            reports.add(
                createReportRecord(
                    ReportErrorType.TABLE_NOT_EXISTS,
                    CheckerReportStatus.FOUND,
                    String.format("Index table %s not exists in database %s", indexName, schemaName)));
        }

        if (null != indexTable) {
            if (!TStringUtil.equals(indexTable.tableType, "COLUMNAR TABLE")) {
                reports.add(
                    createReportRecord(
                        ReportErrorType.WRONG_TABLE_TYPE,
                        CheckerReportStatus.FOUND,
                        String.format("Unexpected type '%s' of index table %s", indexTable.tableType, indexName)));
            }
        }

        return reports;
    }

    /**
     * Check primary and cci column identical (by default, all columnar index is clustered index)
     */
    @NotNull
    private List<CheckerReport> checkCciColumns(Blackboard blackboard) {
        final List<ColumnsRecord> primaryColumns = blackboard.queryColumns(schemaName, tableName);
        final List<ColumnsRecord> indexColumns = blackboard.queryColumns(schemaName, indexName);

        // 1. Check index column not in primary table
        // 2. Check index column definition equals to primary table
        // 3. Check index column order equals to primary table
        // 4. Check primary column not in index table
        return CheckerBuilder
            .stringKeyListChecker(indexColumns, primaryColumns, true)
            .withActualKeyGenerator(a -> a.columnName)
            .withExpectedKeyGenerator(e -> e.columnName)
            .withDefValidator((a, e) -> equalsColumnRecord(e, a))
            .withOrdValidator((a, e) -> a.ordinalPosition == e.ordinalPosition)
            .withOrphanReporter(msgs -> createReportRecord(
                ReportErrorType.ORPHAN_COLUMN,
                CheckerReportStatus.FOUND,
                String.format(
                    "Orphan columns found for column: %s",
                    String.join(",", msgs))))
            .withInvalidateDefReporter(msgs -> createReportRecord(
                ReportErrorType.UNMATCHED_COLUMN_DEFINITION,
                CheckerReportStatus.FOUND,
                String.format(
                    "Unmatched column definition found for column: %s",
                    String.join(",", msgs))))
            .withInvalidateOrdReporter(msgs -> createReportRecord(
                ReportErrorType.UNMATCHED_COLUMN_ORDER,
                CheckerReportStatus.FOUND,
                String.format(
                    "Unmatched column order found for column: %s",
                    String.join(",", msgs))))
            .withMissingReporter(msg -> createReportRecord(
                ReportErrorType.MISSING_COLUMN,
                CheckerReportStatus.FOUND,
                String.format(
                    "Missing columns for column: %s",
                    String.join(",", msg))))
            .build()
            .check()
            .report();
    }

    private boolean equalsColumnRecord(ColumnsRecord left, ColumnsRecord right) {
        if (null == left || null == right) {
            return false;
        }

        return TStringUtil.equals(left.columnName, right.columnName)
            && TStringUtil.equals(left.columnType, right.columnType)
            && TStringUtil.equals(left.isNullable, right.isNullable)
            && TStringUtil.equals(left.columnDefault, right.columnDefault)
            && left.numericPrecision == right.numericPrecision
            && left.numericScale == right.numericScale
            && TStringUtil.equals(left.characterSetName, right.characterSetName)
            && TStringUtil.equals(left.collationName, right.collationName)
            && TStringUtil.equals(left.extra, right.extra);
    }

    private boolean validateIndexRecord(ColumnsRecord primaryColumnDef, IndexesRecord indexColumnDef) {
        if (null == primaryColumnDef || null == indexColumnDef) {
            return false;
        }

        /*
         * For information_schema.columns: The value is YES if NULL values can be stored in the column, NO if not.
         * For information_schema.statistics: Contains YES if the column may contain NULL values and '' if not.
         */
        final boolean equalNullable = TStringUtil.equals(primaryColumnDef.isNullable, indexColumnDef.nullable) || (
            TStringUtil.equals(primaryColumnDef.isNullable, "NO") && TStringUtil.isBlank(indexColumnDef.nullable));

        /*
         * 1. check column name
         * 2. check nullable
         * 3. columnar index cannot be unique
         * 4. for sort key (index_column_type == 0),
         *    flag should be 3 (IndexesRecord.FLAG_CLUSTERED | IndexesRecord.FLAG_COLUMNAR)
         * 5. for covering column (index_column_type == 1)
         *    flag should be 0
         */
        return TStringUtil.equals(primaryColumnDef.columnName, indexColumnDef.columnName)
            && TStringUtil.equals(primaryColumnDef.tableSchema, indexColumnDef.tableSchema)
            && TStringUtil.equals(primaryColumnDef.tableSchema, indexColumnDef.indexSchema)
            && TStringUtil.equals(primaryColumnDef.tableName, indexColumnDef.tableName)
            && equalNullable
            && indexColumnDef.nonUnique == 1
            && (indexColumnDef.indexColumnType == 1 || indexColumnDef.flag == 3);
    }

    private static class Blackboard {
        public final TableInfoManager tableInfoManager;
        public final ExecutionContext ec;

        private final Map<Pair<String, String>, TablesRecord> queryTableCache = new HashMap<>();
        private final Map<Pair<String, String>, List<ColumnsRecord>> queryColumnCache = new HashMap<>();
        private final Map<Triple<String, String, String>, List<ColumnarTableMappingRecord>>
            queryColumnarTableMappingCache = new HashMap<>();
        private final Map<Triple<String, String, String>, List<IndexesRecord>> queryIndexesCache = new HashMap<>();

        private final Map<Pair<Long, Long>, List<ColumnarTableEvolutionRecord>> queryColumnarTableEvolutionCache =
            new HashMap<>();
        private final Map<List<Long>, List<ColumnarColumnEvolutionRecord>>
            queryColumnarColumnEvolutionCache = new HashMap<>();
        private final Map<Pair<String, String>, List<TablePartitionRecord>> queryTablePartitionCache = new HashMap<>();
        private final Map<Long, List<TableGroupRecord>> queryTableGroupCache = new HashMap<>();
        private final Map<Long, List<PartitionGroupRecord>> queryPartitionGroupCache = new HashMap<>();

        private Blackboard(Connection metaDbConnection, ExecutionContext ec) {
            final TableInfoManager tableInfoManager = new TableInfoManager();
            tableInfoManager.setConnection(metaDbConnection);

            this.tableInfoManager = tableInfoManager;
            this.ec = ec;
        }

        public TablesRecord queryTable(String schemaName, String tableName) {
            return queryTableCache.computeIfAbsent(
                Pair.of(schemaName, tableName),
                k -> tableInfoManager.queryTable(k.getKey(), k.getValue(), false));
        }

        public List<ColumnsRecord> queryColumns(String schemaName, String tableName) {
            return queryColumnCache.computeIfAbsent(
                Pair.of(schemaName, tableName),
                k -> tableInfoManager.queryColumns(k.getKey(), k.getValue()));
        }

        public List<IndexesRecord> queryIndexes(String schemaName, String tableName, String indexName) {
            return queryIndexesCache.computeIfAbsent(
                Triple.of(schemaName, tableName, indexName),
                k -> tableInfoManager.queryIndexes(k.getLeft(), k.getMiddle(), k.getRight()));
        }

        public List<ColumnarTableMappingRecord> queryColumnarTableMapping(String schemaName,
                                                                          String tableName,
                                                                          String indexName) {
            return queryColumnarTableMappingCache.computeIfAbsent(
                Triple.of(schemaName, tableName, indexName),
                k -> tableInfoManager.queryColumnarTableMapping(k.getLeft(), k.getMiddle(), k.getRight()));
        }

        public List<ColumnarColumnEvolutionRecord> queryColumnarColumnEvolution(List<Long> fieldIdList) {
            return queryColumnarColumnEvolutionCache.computeIfAbsent(
                fieldIdList,
                tableInfoManager::queryColumnarColumnEvolution);
        }

        public List<ColumnarTableEvolutionRecord> queryColumnarTableEvolution(long indexTableId, long versionId) {
            return queryColumnarTableEvolutionCache.computeIfAbsent(
                Pair.of(indexTableId, versionId),
                p -> tableInfoManager.queryColumnarTableEvolution(p.getKey(), p.getValue()));
        }

        public List<TablePartitionRecord> queryTablePartition(@NotNull String schema, @NotNull String table) {
            return queryTablePartitionCache.computeIfAbsent(
                Pair.of(schema, table),
                k -> tableInfoManager.queryTablePartitions(k.getKey(), k.getValue(), false));
        }

        public List<TableGroupRecord> queryTableGroup(@NotNull Long id) {
            return queryTableGroupCache.computeIfAbsent(
                id,
                tableInfoManager::queryTableGroupById);
        }

        public List<PartitionGroupRecord> queryPartitionGroupByTgId(@NotNull Long tgId) {
            return queryPartitionGroupCache.computeIfAbsent(
                tgId,
                tableInfoManager::queryPartitionGroupByTgId);
        }
    }

    public enum ReportErrorType {
        UNKNOWN,
        SUMMARY,
        MISSING_COLUMN,
        UNMATCHED_COLUMN_ORDER,
        UNMATCHED_COLUMN_DEFINITION,
        ORPHAN_COLUMN,
        SCHEMA_NOT_EXISTS,
        TABLE_NOT_EXISTS,
        UNEXPECTED_CACHED_INDEX_STATUS,
        UNEXPECTED_CACHED_INDEX_TYPE,
        MISSING_INDEX_META,
        ORPHAN_INDEX_META,
        INVALID_INDEX_META,
        DUPLICATED_INDEX_META,
        WRONG_TABLE_TYPE,
        MISSING_TABLE_MAPPING_META,
        DUPLICATED_TABLE_MAPPING_META,
        MISSING_COLUMNAR_TABLE_EVOLUTION_META,
        UNMATCHED_COLUMNAR_TABLE_EVOLUTION_ORDER,
        MISSING_COLUMNAR_COLUMN_EVOLUTION_META,
        ORPHAN_COLUMNAR_COLUMN_EVOLUTION_META,
        UNMATCHED_COLUMNAR_COLUMN_EVOLUTION_DEFINITION,
        UNMATCHED_COLUMNAR_COLUMN_EVOLUTION_ORDER,
        MISSING_COLUMNAR_TABLE_EVOLUTION_FIELD_ID,
        ORPHAN_COLUMNAR_TABLE_EVOLUTION_FIELD_ID,
        MISSING_CDC_MARK_CREATE_INDEX,
        DUPLICATED_CDC_MARK_CREATE_INDEX,
        WRONG_CDC_MARK_DDL_ID,
        WRONG_CDC_MARK_STATEMENT,
        UNEXPECTED_COLUMNAR_TABLE_MAPPING_META,
        MISSING_COLUMNAR_TABLE_PARTITION_META,
        MISSING_COLUMNAR_TABLE_LOGICAL_PARTITION_META,
        MISSING_COLUMNAR_TABLE_PHYSICAL_PARTITION_META,
        ORPHAN_COLUMNAR_TABLE_PHYSICAL_PARTITION_META,
        UNEXPECTED_COLUMNAR_COLUMNAR_TABLE_PARTITION_DEFINITION,
        MISSING_COLUMNAR_TABLE_GROUP_META,
        UNEXPECTED_COLUMNAR_COLUMNAR_TABLE_GROUP_DEFINITION,
        ;

        public static ReportErrorType of(String value) {
            if (null == value) {
                return UNKNOWN;
            }
            try {
                return ReportErrorType.valueOf(value.toUpperCase());
            } catch (IllegalArgumentException ignored) {
                return UNKNOWN;
            }
        }
    }

    private interface ElementReporter<T, R> {
        String orphanMsgFromActual(T actual);

        String orphanMsgFromExpected(R expected);

        String invalidateDefMsg(T actual, R expected);

        String invalidateOrdMsg(T actual, R expected);

        String duplicatedMsg(T actual);

        String missingMsgFromExpected(R expected);

        String missingMsgFromActual(T actual);

        CheckerReport reportOrphan(List<String> msgs);

        CheckerReport reportInvalidateDef(List<String> msgs);

        CheckerReport reportInvalidateOrd(List<String> msgs);

        default CheckerReport reportDuplicated(List<String> msgs) {
            throw new UnsupportedOperationException();
        }

        CheckerReport reportMissing(List<String> msgs);
    }

    private interface ElementValidator<T, R> {
        boolean isInvalidateDef(@NotNull T actual, @NotNull R expected);

        default boolean isInvalidateOrd(@NotNull T actual, @NotNull R expected) {
            return false;
        }

        default boolean isDuplicated(@NotNull T actual) {
            return false;
        }

        @Nullable
        R findInExpected(@NotNull T actual);

        @Nullable
        T findInActual(@NotNull R expected);

        /**
         * @return false if actualNotInExpected() ? reportOrphan : reportMissing
         * </p>
         * true if actualNotInExpected() ? reportMissing : reportOrphan
         */
        default boolean reverseOrderCheck() {
            return false;
        }
    }

    private interface ElementChecker<T, R> extends ElementValidator<T, R>, ElementReporter<T, R> {
        @NotNull
        Iterable<T> getActualElements();

        @NotNull
        Iterable<R> getExpectedElements();

        @NotNull
        ElementChecker<T, R> check();

        @NotNull
        List<CheckerReport> report();
    }

    private static abstract class AbstractElementChecker<K, T, R>
        implements ElementChecker<T, R> {
        protected final Function<List<String>, CheckerReport> orphanReporter;
        protected final Function<List<String>, CheckerReport> invalidateDefReporter;
        protected final Function<List<String>, CheckerReport> invalidateOrdReporter;
        protected final Function<List<String>, CheckerReport> missingReporter;
        protected final Function<List<String>, CheckerReport> duplicatedReporter;

        protected final Function<T, String> orphanMsgFromActualGenerator;
        protected final Function<R, String> orphanMsgFromExpectedGenerator;
        protected final Function<R, String> missingMsgFromExpectedGenerator;
        protected final Function<T, String> missingMsgFromActualGenerator;
        protected final BiFunction<T, R, String> invalidateDefMsgGenerator;
        protected final BiFunction<T, R, String> invalidateOrdMsgGenerator;

        private final List<String> orphan = new ArrayList<>();
        private final List<String> invalidateDef = new ArrayList<>();
        private final List<String> invalidateOrd = new ArrayList<>();
        private final List<String> missing = new ArrayList<>();
        private final List<String> duplicated = new ArrayList<>();

        protected AbstractElementChecker(Function<List<String>, CheckerReport> orphanReporter,
                                         Function<List<String>, CheckerReport> invalidateDefReporter,
                                         Function<List<String>, CheckerReport> invalidateOrdReporter,
                                         Function<List<String>, CheckerReport> missingReporter,
                                         Function<List<String>, CheckerReport> duplicatedReporter,
                                         Function<T, String> orphanMsgFromActualGenerator,
                                         Function<R, String> orphanMsgFromExpectedGenerator,
                                         Function<R, String> missingMsgFromExpectedGenerator,
                                         Function<T, String> missingMsgFromActualGenerator,
                                         BiFunction<T, R, String> invalidateDefMsgGenerator,
                                         BiFunction<T, R, String> invalidateOrdMsgGenerator) {
            this.orphanReporter = orphanReporter;
            this.invalidateDefReporter = invalidateDefReporter;
            this.invalidateOrdReporter = invalidateOrdReporter;
            this.missingReporter = missingReporter;
            this.duplicatedReporter = duplicatedReporter;
            this.orphanMsgFromActualGenerator = orphanMsgFromActualGenerator;
            this.orphanMsgFromExpectedGenerator = orphanMsgFromExpectedGenerator;
            this.missingMsgFromExpectedGenerator = missingMsgFromExpectedGenerator;
            this.missingMsgFromActualGenerator = missingMsgFromActualGenerator;
            this.invalidateDefMsgGenerator = invalidateDefMsgGenerator;
            this.invalidateOrdMsgGenerator = invalidateOrdMsgGenerator;
        }

        @Override
        @NotNull
        public ElementChecker<T, R> check() {
            // Natural order check
            for (T actual : getActualElements()) {
                if (isDuplicated(actual)) {
                    duplicated.add(duplicatedMsg(actual));
                } else {
                    final R expected = findInExpected(actual);
                    if (null == expected) {
                        if (reverseOrderCheck()) {
                            missing.add(missingMsgFromActual(actual));
                        } else {
                            orphan.add(orphanMsgFromActual(actual));
                        }
                    } else if (isInvalidateDef(actual, expected)) {
                        invalidateDef.add(invalidateDefMsg(actual, expected));
                    } else if (isInvalidateOrd(actual, expected)) {
                        invalidateOrd.add(invalidateOrdMsg(actual, expected));
                    }
                }
            }

            // Reverse order check
            for (R expected : getExpectedElements()) {
                final T actual = findInActual(expected);
                if (null == actual) {
                    if (reverseOrderCheck()) {
                        orphan.add(orphanMsgFromExpected(expected));
                    } else {
                        missing.add(missingMsgFromExpected(expected));
                    }
                }
            }
            return this;
        }

        @Override
        @NotNull
        public List<CheckerReport> report() {
            final List<CheckerReport> reports = new ArrayList<>();

            if (!orphan.isEmpty()) {
                reports.add(reportOrphan(orphan));
            }

            if (!invalidateDef.isEmpty()) {
                reports.add(reportInvalidateDef(invalidateDef));
            }

            if (!invalidateOrd.isEmpty()) {
                reports.add(reportInvalidateOrd(invalidateOrd));
            }

            if (!duplicated.isEmpty()) {
                reports.add(reportDuplicated(duplicated));
            }

            if (!missing.isEmpty()) {
                reports.add(reportMissing(missing));
            }

            return reports;
        }

        private String unwrap(@NotNull K key) {
            if (key instanceof String) {
                return (String) key;
            } else {
                return key.toString();
            }
        }

        @NotNull
        public abstract K getKeyFromActual(T actual);

        @NotNull
        public abstract K getKeyFromExpected(R expected);

        @Override
        public String orphanMsgFromActual(T actual) {
            if (null != this.orphanMsgFromActualGenerator) {
                return this.orphanMsgFromActualGenerator.apply(actual);
            }
            return unwrap(getKeyFromActual(actual));
        }

        @Override
        public String orphanMsgFromExpected(R expected) {
            if (null != this.orphanMsgFromExpectedGenerator) {
                return this.orphanMsgFromExpectedGenerator.apply(expected);
            }
            return unwrap(getKeyFromExpected(expected));
        }

        @Override
        public String invalidateDefMsg(T actual, R expected) {
            if (null != this.invalidateDefMsgGenerator) {
                return this.invalidateDefMsgGenerator.apply(actual, expected);
            }
            return unwrap(getKeyFromActual(actual));
        }

        @Override
        public String invalidateOrdMsg(T actual, R expected) {
            if (null != this.invalidateOrdMsgGenerator) {
                return this.invalidateOrdMsgGenerator.apply(actual, expected);
            }
            return unwrap(getKeyFromActual(actual));
        }

        @Override
        public String duplicatedMsg(T actual) {
            return unwrap(getKeyFromActual(actual));
        }

        @Override
        public String missingMsgFromExpected(R expected) {
            if (null != this.missingMsgFromExpectedGenerator) {
                return this.missingMsgFromExpectedGenerator.apply(expected);
            }
            return unwrap(getKeyFromExpected(expected));
        }

        @Override
        public String missingMsgFromActual(T actual) {
            if (null != this.missingMsgFromActualGenerator) {
                return this.missingMsgFromActualGenerator.apply(actual);
            }
            return unwrap(getKeyFromActual(actual));
        }

        @Override
        public CheckerReport reportOrphan(List<String> msgs) {
            return orphanReporter.apply(msgs);
        }

        @Override
        public CheckerReport reportInvalidateDef(List<String> msgs) {
            return invalidateDefReporter.apply(msgs);
        }

        @Override
        public CheckerReport reportInvalidateOrd(List<String> msgs) {
            return invalidateOrdReporter.apply(msgs);
        }

        @Override
        public CheckerReport reportDuplicated(List<String> msgs) {
            return duplicatedReporter.apply(msgs);
        }

        @Override
        public CheckerReport reportMissing(List<String> msgs) {
            return missingReporter.apply(msgs);
        }

    }

    private static class ListElementChecker<K, T, R> extends AbstractElementChecker<K, T, R> {
        protected final Map<K, T> actualMap;
        protected final Map<K, R> expectedMap;
        protected final Function<T, K> actualKeyGenerator;
        protected final Function<R, K> expectedKeyGenerator;
        protected final BiFunction<K, R, T> actualFinder;
        protected final BiFunction<K, T, R> expectedFinder;
        protected final BiFunction<T, R, Boolean> defValidator;
        protected final BiFunction<T, R, Boolean> ordValidator;
        protected final Function<T, Boolean> distValidator;
        protected final boolean reverseOrderCheck;

        protected ListElementChecker(Map<K, T> actualMap,
                                     Map<K, R> expectedMap,
                                     Function<T, K> actualKeyGenerator,
                                     Function<R, K> expectedKeyGenerator,
                                     BiFunction<K, R, T> actualFinder,
                                     BiFunction<K, T, R> expectedFinder,
                                     BiFunction<T, R, Boolean> defValidator,
                                     BiFunction<T, R, Boolean> ordValidator,
                                     Function<T, Boolean> distValidator,
                                     Function<List<String>, CheckerReport> orphanReporter,
                                     Function<List<String>, CheckerReport> invalidateDefReporter,
                                     Function<List<String>, CheckerReport> invalidateOrdReporter,
                                     Function<List<String>, CheckerReport> missingReporter,
                                     Function<List<String>, CheckerReport> duplicatedReporter,
                                     Function<T, String> orphanMsgFromActualGenerator,
                                     Function<R, String> orphanMsgFromExpectedGenerator,
                                     Function<R, String> missingMsgFromExpectedGenerator,
                                     Function<T, String> missingMsgFromActualGenerator,
                                     BiFunction<T, R, String> invalidateDefMsgGenerator,
                                     BiFunction<T, R, String> invalidateOrdMsgGenerator,
                                     boolean reverseOrderCheck) {
            super(orphanReporter,
                invalidateDefReporter,
                invalidateOrdReporter,
                missingReporter,
                duplicatedReporter,
                orphanMsgFromActualGenerator,
                orphanMsgFromExpectedGenerator,
                missingMsgFromExpectedGenerator,
                missingMsgFromActualGenerator,
                invalidateDefMsgGenerator,
                invalidateOrdMsgGenerator);
            this.actualMap = actualMap;
            this.expectedMap = expectedMap;
            this.actualKeyGenerator = actualKeyGenerator;
            this.expectedKeyGenerator = expectedKeyGenerator;
            this.actualFinder = actualFinder;
            this.expectedFinder = expectedFinder;
            this.defValidator = defValidator;
            this.ordValidator = ordValidator;
            this.distValidator = distValidator;
            this.reverseOrderCheck = reverseOrderCheck;
        }

        @Override
        @NotNull
        public K getKeyFromActual(T actual) {
            return actualKeyGenerator.apply(actual);
        }

        @Override
        @NotNull
        public K getKeyFromExpected(R expected) {
            return expectedKeyGenerator.apply(expected);
        }

        @Override
        @Nullable
        public R findInExpected(@NotNull T actual) {
            if (null == expectedFinder) {
                return expectedMap.get(getKeyFromActual(actual));
            }
            return expectedFinder.apply(getKeyFromActual(actual), actual);
        }

        @Override
        @Nullable
        public T findInActual(@NotNull R expected) {
            if (null == actualFinder) {
                return actualMap.get(getKeyFromExpected(expected));
            }
            return actualFinder.apply(getKeyFromExpected(expected), expected);
        }

        @Override
        public boolean isInvalidateDef(@NotNull T actual, @NotNull R expected) {
            if (null == defValidator) {
                return false;
            }
            return !defValidator.apply(actual, expected);
        }

        @Override
        public boolean isInvalidateOrd(@NotNull T actual, @NotNull R expected) {
            if (null == ordValidator) {
                return false;
            }
            return !ordValidator.apply(actual, expected);
        }

        @Override
        public boolean isDuplicated(@NotNull T actual) {
            if (null == distValidator) {
                return false;
            }
            return !distValidator.apply(actual);
        }

        @Override
        public boolean reverseOrderCheck() {
            return reverseOrderCheck;
        }

        @Override
        public @NotNull Iterable<T> getActualElements() {
            return actualMap.values();
        }

        @Override
        public @NotNull Iterable<R> getExpectedElements() {
            return expectedMap.values();
        }
    }

    @RequiredArgsConstructor
    private static final class CheckerBuilder<K, T, R> {
        private final Iterable<T> actualList;
        private final Supplier<Map<K, T>> actualMapSupplier;
        private final Supplier<Map<K, R>> expectedMapSupplier;
        private final Supplier<Set<K>> setSupplier;
        private Function<T, K> actualKeyGenerator;
        private Function<R, K> expectedKeyGenerator;
        private BiFunction<K, R, T> actualFinder;
        private BiFunction<K, T, R> expectedFinder;
        private BiFunction<T, R, Boolean> defValidator;
        private BiFunction<T, R, Boolean> ordValidator;
        private Function<T, Boolean> distValidator;
        private Function<List<String>, CheckerReport> orphanReporter;
        private Function<List<String>, CheckerReport> invalidateDefReporter;
        private Function<List<String>, CheckerReport> invalidateOrdReporter;
        private Function<List<String>, CheckerReport> missingReporter;
        private Function<List<String>, CheckerReport> duplicatedReporter;

        private Function<T, String> orphanMsgFromActualGenerator;
        private Function<R, String> orphanMsgFromExpectedGenerator;
        private Function<R, String> missingMsgFromExpectedGenerator;
        private Function<T, String> missingMsgFromActualGenerator;
        private BiFunction<T, R, String> invalidateDefMsgGenerator;
        private BiFunction<T, R, String> invalidateOrdMsgGenerator;

        private Iterable<R> expectedList;
        private Map<K, T> actualMap;
        private Map<K, R> expectedMap;

        private boolean distCheck = false;
        private boolean reverseOrderCheck = false;

        public static <T, R> CheckerBuilder<String, T, R> stringKeyListChecker(@NotNull Iterable<T> actualList,
                                                                               @NotNull Iterable<R> expectedList,
                                                                               boolean caseInsensitive) {
            final CheckerBuilder<String, T, R> builder = new CheckerBuilder<>(
                actualList,
                () -> caseInsensitive ? new TreeMap<>(String.CASE_INSENSITIVE_ORDER) : new HashMap<>(),
                () -> caseInsensitive ? new TreeMap<>(String.CASE_INSENSITIVE_ORDER) : new HashMap<>(),
                () -> caseInsensitive ? new TreeSet<>(String.CASE_INSENSITIVE_ORDER) : new HashSet<>());
            return builder.withExpectedList(expectedList);
        }

        public static <T, R> CheckerBuilder<String, T, R> stringKeyListChecker(@NotNull Iterable<T> actualList,
                                                                               @NotNull Map<String, R> expectedMap,
                                                                               boolean caseInsensitive) {
            final CheckerBuilder<String, T, R> builder = new CheckerBuilder<>(
                actualList,
                () -> caseInsensitive ? new TreeMap<>(String.CASE_INSENSITIVE_ORDER) : new HashMap<>(),
                () -> caseInsensitive ? new TreeMap<>(String.CASE_INSENSITIVE_ORDER) : new HashMap<>(),
                () -> caseInsensitive ? new TreeSet<>(String.CASE_INSENSITIVE_ORDER) : new HashSet<>());
            return builder.withExpectedMap(expectedMap);
        }

        public static <K, T, R> CheckerBuilder<K, T, R> keyListChecker(@NotNull Iterable<T> actualList,
                                                                       @NotNull Map<K, R> expectedMap) {
            final CheckerBuilder<K, T, R> builder = new CheckerBuilder<>(
                actualList,
                HashMap::new,
                HashMap::new,
                HashSet::new);
            return builder.withExpectedMap(expectedMap);
        }

        public static <K, T, R> CheckerBuilder<K, T, R> keyListChecker(@NotNull Iterable<T> actualList,
                                                                       @NotNull Iterable<R> expectedList) {
            final CheckerBuilder<K, T, R> builder = new CheckerBuilder<>(
                actualList,
                HashMap::new,
                HashMap::new,
                HashSet::new);
            return builder.withExpectedList(expectedList);
        }

        public static <T, R> CheckerBuilder<T, Ord<T>, R> listChecker(@NotNull Iterable<T> actualList,
                                                                      @NotNull Map<T, R> expectedMap) {
            final CheckerBuilder<T, Ord<T>, R> builder = new CheckerBuilder<>(
                Ord.zip(actualList),
                HashMap::new,
                HashMap::new,
                HashSet::new);

            return builder.withExpectedMap(expectedMap);
        }

        public static <T, R> CheckerBuilder<Integer, Ord<T>, Ord<R>> listChecker(@NotNull Iterable<T> actualList,
                                                                                 @NotNull Iterable<R> expectedList) {
            final CheckerBuilder<Integer, Ord<T>, Ord<R>> builder = new CheckerBuilder<>(
                Ord.zip(actualList),
                HashMap::new,
                HashMap::new,
                HashSet::new);

            return builder
                .withExpectedList(Ord.zip(expectedList))
                .withActualKeyGenerator(Ord::getKey)
                .withExpectedKeyGenerator(Ord::getKey);
        }

        public ElementChecker<T, R> build() {
            Preconditions.checkNotNull(actualList);
            Preconditions.checkNotNull(actualKeyGenerator);
            Preconditions.checkNotNull(actualMapSupplier);
            Preconditions.checkNotNull(expectedKeyGenerator);

            if (GeneralUtil.isEmpty(expectedMap)) {
                Preconditions.checkNotNull(expectedList);
                Preconditions.checkNotNull(expectedMapSupplier);
                this.expectedMap = expectedMapSupplier.get();
                this.expectedList.forEach(e -> expectedMap.put(expectedKeyGenerator.apply(e), e));
            }

            this.actualMap = actualMapSupplier.get();
            this.actualList.forEach(a -> actualMap.put(actualKeyGenerator.apply(a), a));

            if (this.distCheck) {
                Preconditions.checkNotNull(setSupplier);
                final Set<K> actualSet = setSupplier.get();
                this.distValidator = (a) -> actualSet.add(actualKeyGenerator.apply(a));
            }

            return new ListElementChecker<>(
                actualMap,
                expectedMap,
                actualKeyGenerator,
                expectedKeyGenerator,
                actualFinder,
                expectedFinder,
                defValidator,
                ordValidator,
                distValidator,
                orphanReporter,
                invalidateDefReporter,
                invalidateOrdReporter,
                missingReporter,
                duplicatedReporter,
                orphanMsgFromActualGenerator,
                orphanMsgFromExpectedGenerator,
                missingMsgFromExpectedGenerator,
                missingMsgFromActualGenerator,
                invalidateDefMsgGenerator,
                invalidateOrdMsgGenerator,
                reverseOrderCheck);
        }

        public CheckerBuilder<K, T, R> withActualKeyGenerator(@NotNull Function<T, K> actualKeyGenerator) {
            this.actualKeyGenerator = actualKeyGenerator;
            return this;
        }

        public CheckerBuilder<K, T, R> withExpectedKeyGenerator(@NotNull Function<R, K> expectedKeyGenerator) {
            this.expectedKeyGenerator = expectedKeyGenerator;
            return this;
        }

        public CheckerBuilder<K, T, R> withActualFinder(BiFunction<K, R, T> actualFinder) {
            this.actualFinder = actualFinder;
            return this;
        }

        public CheckerBuilder<K, T, R> withExpectedFinder(BiFunction<K, T, R> expectedFinder) {
            this.expectedFinder = expectedFinder;
            return this;
        }

        public CheckerBuilder<K, T, R> withExpectedList(@NotNull Iterable<R> expectedList) {
            this.expectedList = expectedList;
            return this;
        }

        public CheckerBuilder<K, T, R> withExpectedMap(@NotNull Map<K, R> expectedMap) {
            this.expectedMap = expectedMap;
            return this;
        }

        public CheckerBuilder<K, T, R> withDefValidator(BiFunction<T, R, Boolean> defValidator) {
            this.defValidator = defValidator;
            return this;
        }

        public CheckerBuilder<K, T, R> withOrdValidator(BiFunction<T, R, Boolean> ordValidator) {
            this.ordValidator = ordValidator;
            return this;
        }

        public CheckerBuilder<K, T, R> withDistValidator() {
            this.distCheck = true;
            return this;
        }

        public CheckerBuilder<K, T, R> withReverseOrderCheck() {
            this.reverseOrderCheck = true;
            return this;
        }

        public CheckerBuilder<K, T, R> withOrphanReporter(
            Function<List<String>, CheckerReport> orphanReporter) {
            this.orphanReporter = orphanReporter;
            return this;
        }

        public CheckerBuilder<K, T, R> withInvalidateDefReporter(
            Function<List<String>, CheckerReport> invalidateDefReporter) {
            this.invalidateDefReporter = invalidateDefReporter;
            return this;
        }

        public CheckerBuilder<K, T, R> withInvalidateOrdReporter(
            Function<List<String>, CheckerReport> invalidateOrdReporter) {
            this.invalidateOrdReporter = invalidateOrdReporter;
            return this;
        }

        public CheckerBuilder<K, T, R> withMissingReporter(
            Function<List<String>, CheckerReport> missingReporter) {
            this.missingReporter = missingReporter;
            return this;
        }

        public CheckerBuilder<K, T, R> withDuplicatedReporter(
            Function<List<String>, CheckerReport> duplicatedReporter) {
            this.duplicatedReporter = duplicatedReporter;
            return this;
        }

        public CheckerBuilder<K, T, R> withOrphanMsgFromActualGenerator(
            Function<T, String> orphanMsgFromActualGenerator) {
            this.orphanMsgFromActualGenerator = orphanMsgFromActualGenerator;
            return this;
        }

        public CheckerBuilder<K, T, R> withOrphanMsgFromExpectedGenerator(
            Function<R, String> orphanMsgFromExpectedGenerator) {
            this.orphanMsgFromExpectedGenerator = orphanMsgFromExpectedGenerator;
            return this;
        }

        public CheckerBuilder<K, T, R> withMissingMsgFromExpectedGenerator(
            Function<R, String> missingMsgFromExpectedGenerator) {
            this.missingMsgFromExpectedGenerator = missingMsgFromExpectedGenerator;
            return this;
        }

        public CheckerBuilder<K, T, R> withMissingMsgFromActualGenerator(
            Function<T, String> missingMsgFromActualGenerator) {
            this.missingMsgFromActualGenerator = missingMsgFromActualGenerator;
            return this;
        }

        public CheckerBuilder<K, T, R> withInvalidateDefMsgGenerator(
            BiFunction<T, R, String> invalidateDefMsgGenerator) {
            this.invalidateDefMsgGenerator = invalidateDefMsgGenerator;
            return this;
        }

        public CheckerBuilder<K, T, R> withInvalidateOrdMsgGenerator(
            BiFunction<T, R, String> invalidateOrdMsgGenerator) {
            this.invalidateOrdMsgGenerator = invalidateOrdMsgGenerator;
            return this;
        }
    }

    @RequiredArgsConstructor
    private static class ImmutableConcatMap<K, R, V> extends HashMap<K, V> {
        private final Map<K, R> left;
        private final Map<R, V> right;

        @Override
        public V get(Object key) {
            return Optional
                .ofNullable(left.get(key))
                .map(right::get)
                .orElse(null);
        }

        @Override
        public Set<K> keySet() {
            return keySet();
        }

        @Override
        public Collection<V> values() {
            return right.values();
        }

        @Override
        public boolean isEmpty() {
            return left.isEmpty() || right.isEmpty();
        }

        @Override
        public int size() {
            return Math.min(left.size(), right.size());
        }

        @Override
        public boolean containsKey(Object key) {
            return left.containsKey(key) && right.containsKey(left.get(key));
        }

        @Override
        public V put(K key, V value) {
            throw new UnsupportedOperationException("put is not supported");
        }

        @Override
        public void putAll(Map<? extends K, ? extends V> m) {
            throw new UnsupportedOperationException("putAll is not supported");
        }

        @Override
        public V remove(Object key) {
            throw new UnsupportedOperationException("remove is not supported");
        }

        @Override
        public void clear() {
            throw new UnsupportedOperationException("clear is not supported");
        }

        @Override
        public Set<Entry<K, V>> entrySet() {
            throw new UnsupportedOperationException("entrySet is not supported");
        }

        @Override
        public boolean containsValue(Object value) {
            throw new UnsupportedOperationException("containsValue is not supported");
        }
    }

    @Override
    public String remark() {
        return String.format("|CheckCci(%s.%s)", tableName, indexName);
    }
}
