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

package com.alibaba.polardbx.executor.gms;

import com.alibaba.druid.pool.GetConnectionTimeoutException;
import com.alibaba.druid.proxy.jdbc.ResultSetMetaDataProxy;
import com.alibaba.polardbx.common.charset.CharsetName;
import com.alibaba.polardbx.common.charset.CollationName;
import com.alibaba.polardbx.common.exception.NotSupportException;
import com.alibaba.polardbx.common.exception.TddlNestableRuntimeException;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.model.lifecycle.AbstractLifecycle;
import com.alibaba.polardbx.common.utils.CaseInsensitive;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.common.StorageInfoManager;
import com.alibaba.polardbx.executor.mdl.MdlContext;
import com.alibaba.polardbx.executor.mdl.MdlDuration;
import com.alibaba.polardbx.executor.mdl.MdlKey;
import com.alibaba.polardbx.executor.mdl.MdlManager;
import com.alibaba.polardbx.executor.mdl.MdlRequest;
import com.alibaba.polardbx.executor.mdl.MdlTicket;
import com.alibaba.polardbx.executor.mdl.MdlType;
import com.alibaba.polardbx.gms.metadb.MetaDbDataSource;
import com.alibaba.polardbx.gms.metadb.table.ColumnsRecord;
import com.alibaba.polardbx.gms.metadb.table.IndexStatus;
import com.alibaba.polardbx.gms.metadb.table.IndexesRecord;
import com.alibaba.polardbx.gms.metadb.table.TableInfoManager;
import com.alibaba.polardbx.gms.metadb.table.TableStatus;
import com.alibaba.polardbx.gms.metadb.table.TablesExtRecord;
import com.alibaba.polardbx.gms.metadb.table.TablesRecord;
import com.alibaba.polardbx.gms.partition.TableLocalPartitionRecord;
import com.alibaba.polardbx.gms.partition.TablePartRecordInfoContext;
import com.alibaba.polardbx.gms.partition.TablePartitionRecord;
import com.alibaba.polardbx.gms.tablegroup.ComplexTaskOutlineRecord;
import com.alibaba.polardbx.gms.tablegroup.TableGroupConfig;
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.ComplexTaskMetaManager;
import com.alibaba.polardbx.optimizer.config.table.Field;
import com.alibaba.polardbx.optimizer.config.table.GsiMetaManager;
import com.alibaba.polardbx.optimizer.config.table.IndexColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.IndexMeta;
import com.alibaba.polardbx.optimizer.config.table.IndexType;
import com.alibaba.polardbx.optimizer.config.table.Relationship;
import com.alibaba.polardbx.optimizer.config.table.SchemaManager;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.core.TddlRelDataTypeSystemImpl;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypeUtil;
import com.alibaba.polardbx.optimizer.exception.TableNotFoundException;
import com.alibaba.polardbx.optimizer.parse.TableMetaParser;
import com.alibaba.polardbx.optimizer.partition.LocalPartitionDefinitionInfo;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.partition.PartitionInfoManager;
import com.alibaba.polardbx.optimizer.partition.PartitionInfoUtil;
import com.alibaba.polardbx.optimizer.planmanager.PlanManager;
import com.alibaba.polardbx.optimizer.rule.TddlRuleManager;
import com.alibaba.polardbx.optimizer.sql.sql2rel.TddlSqlToRelConverter;
import com.alibaba.polardbx.optimizer.tablegroup.TableGroupInfoManager;
import com.alibaba.polardbx.rpc.client.XSession;
import com.alibaba.polardbx.rpc.compatible.XResultSet;
import com.alibaba.polardbx.rpc.compatible.XResultSetMetaData;
import com.alibaba.polardbx.rpc.pool.XConnection;
import com.alibaba.polardbx.rpc.result.XResult;
import com.alibaba.polardbx.statistics.SQLRecorderLogger;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.mysql.cj.polarx.protobuf.PolarxResultset;
import lombok.val;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.exception.ExceptionUtils;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.alibaba.polardbx.common.constants.SequenceAttribute.AUTO_SEQ_PREFIX;

/**
 * @author mengshi.sunmengshi
 */
public class GmsTableMetaManager extends AbstractLifecycle implements SchemaManager {

    private final static Logger logger = LoggerFactory.getLogger(GmsTableMetaManager.class);

    private final String schemaName;
    private final String appName;
    private final TddlRuleManager rule;
    private final StorageInfoManager storage;
    /**
     * !!!!!!!NOTE !!!!!!
     * all tableNames should convert to lowercase
     */
    private Map<String, TableMeta> latestTables = null;
    private boolean expired;

    public GmsTableMetaManager(String schemaName, String appName, TddlRuleManager rule, StorageInfoManager storage) {
        this.schemaName = schemaName;
        this.appName = appName;
        this.rule = rule;
        this.storage = storage;
    }

    public GmsTableMetaManager(GmsTableMetaManager old, String tableName, TddlRuleManager rule) {
        this.schemaName = old.schemaName;
        this.appName = old.appName;
        this.rule = rule;
        this.latestTables = new HashMap<>(old.latestTables);
        this.storage = old.storage;
        loadAndCacheTableMeta(tableName.toLowerCase());
    }

    /**
     * Load multiple tables in transaction
     */
    public GmsTableMetaManager(GmsTableMetaManager old, List<String> tableNames, TddlRuleManager rule) {
        this.schemaName = old.schemaName;
        this.appName = old.appName;
        this.rule = rule;
        this.latestTables = new HashMap<>(old.latestTables);
        this.storage = old.storage;
        loadAndCacheTableMeta(tableNames);
    }

    public static TableMeta fetchTableMeta(Connection metaDbConn,
                                           String schemaName,
                                           String logicalTableName,
                                           TddlRuleManager rule,
                                           StorageInfoManager storage,
                                           boolean fetchPrimaryTableMetaOnly,
                                           boolean includeInvisiableInfo) {
        if (metaDbConn != null) {
            return fetchTableMeta(metaDbConn, schemaName, Arrays.asList(logicalTableName), rule, storage,
                fetchPrimaryTableMetaOnly, includeInvisiableInfo).get(logicalTableName);
        } else {
            try (Connection conn = MetaDbUtil.getConnection()) {
                return fetchTableMeta(conn, schemaName, Arrays.asList(logicalTableName), rule, storage,
                    fetchPrimaryTableMetaOnly, includeInvisiableInfo).get(logicalTableName);
            } catch (SQLException e) {
                throw new TddlRuntimeException(ErrorCode.ERR_GMS_GENERIC, "fetch tablemeta failed", e);
            }
        }
    }

    public static Map<String, TableMeta> fetchTableMeta(Connection metaDbConn,
                                                        String schemaName,
                                                        List<String> logicalTableNameList,
                                                        TddlRuleManager rule,
                                                        StorageInfoManager storage,
                                                        boolean fetchPrimaryTableMetaOnly,
                                                        boolean includeInvisiableInfo) {
        TableInfoManager tableInfoManager = new TableInfoManager();
        tableInfoManager.setConnection(metaDbConn);

        boolean locked = false;
        Map<String, TableMeta> metaMap = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);

        for (String logicalTableName : GeneralUtil.emptyIfNull(logicalTableNameList)) {

            TableMeta meta = null;
            String origTableName = logicalTableName;

            TablesRecord tableRecord = tableInfoManager.queryTable(schemaName, logicalTableName, false);

            if (tableRecord == null) {
                // Check if there is an ongoing RENAME TABLE operation, so search with new table name.
                tableRecord = tableInfoManager.queryTable(schemaName, logicalTableName, true);

                // Use original table name to find column and index meta.
                if (tableRecord != null) {
                    origTableName = tableRecord.tableName;
                }
            }

            if (tableRecord != null) {
                List<ColumnsRecord> columnsRecords;
                List<IndexesRecord> indexesRecords;
                if (includeInvisiableInfo) {
                    columnsRecords =
                        tableInfoManager.queryColumns(schemaName, origTableName);
                    indexesRecords =
                        tableInfoManager.queryIndexes(schemaName, origTableName);
                } else {
                    columnsRecords =
                        tableInfoManager.queryVisibleColumns(schemaName, origTableName);
                    indexesRecords =
                        tableInfoManager.queryVisibleIndexes(schemaName, origTableName);
                }
                meta = buildTableMeta(tableRecord, columnsRecords, indexesRecords, logicalTableName);

                if (meta != null && !fetchPrimaryTableMetaOnly) {

                    meta.setSchemaName(schemaName);
                    DataSource dataSource = MetaDbDataSource.getInstance().getDataSource();
                    final GsiMetaManager gsiMetaManager =
                        new GsiMetaManager(dataSource, schemaName);
                    meta.setGsiTableMetaBean(
                        gsiMetaManager.getTableMeta(origTableName, IndexStatus.ALL));
                    meta.setComplexTaskTableMetaBean(
                        ComplexTaskMetaManager.getComplexTaskTableMetaBean(metaDbConn, schemaName, origTableName));
                    boolean isNewPartDb = DbInfoManager.getInstance().isNewPartitionDb(schemaName);
                    if (isNewPartDb) {
                        loadNewestPartitionInfo(metaDbConn,
                            schemaName, logicalTableName, origTableName, rule,
                            tableInfoManager, meta);
                    }
                    // Get auto partition mark.jjjjj
                    final TablesExtRecord extRecord =
                        tableInfoManager.queryTableExt(schemaName, origTableName, false);
                    if (extRecord != null) {
                        meta.setAutoPartition(extRecord.isAutoPartition());
                        // Load lock flag.
                        locked = extRecord.isLocked();
                    }

                    // Auto partition flag for new partition table.
                    if (meta.getPartitionInfo() != null) {
                        meta.setAutoPartition(
                            (meta.getPartitionInfo().getPartFlags() & TablePartitionRecord.FLAG_AUTO_PARTITION)
                                != 0);
                        // Load lock flag.
                        locked = (meta.getPartitionInfo().getPartFlags() & TablePartitionRecord.FLAG_LOCK) != 0;
                    }
                }
            }

            metaMap.put(logicalTableName, meta);
        }

        if (locked) {
            throw new RuntimeException("Table `" + logicalTableNameList + "` has been locked by logical meta lock.");
        }
        return metaMap;
    }

    @Override
    protected void doInit() {
        if (latestTables != null) {
            return;
        }

        synchronized (this) {
            if (latestTables != null) {
                return;
            } else {
                latestTables = new HashMap<>();
                List<TableMeta> tableMetas = fetchTableMetas();
                for (TableMeta meta : tableMetas) {
                    meta.setSchemaName(schemaName);
                    latestTables.put(meta.getTableName().toLowerCase(), meta);
                }
                latestTables.put(DUAL, buildDualTable());

            }
        }
    }

    protected TableMeta buildDualTable() {
        IndexMeta index = new IndexMeta(SchemaManager.DUAL,
            new ArrayList<ColumnMeta>(),
            new ArrayList<ColumnMeta>(),
            IndexType.NONE,
            Relationship.NONE,
            false,
            true,
            true,
            "");

        TableMeta dual = new TableMeta(DUAL, new ArrayList<ColumnMeta>(), index, new ArrayList<IndexMeta>(), true,
            TableStatus.PUBLIC, 0, 0);
        dual.setSchemaName(schemaName);
        return dual;
    }

    /**
     * Default implementation of schema change
     * 1. Non-preemptive, which may cause deadlock when multiple table changes happens
     * 2. Allow two concurrent version exists, which is not safe for single-versioned TableRule & PartitionInfoManager
     */
    public void tonewversion(String tableName) {
        tonewversionImpl(Arrays.asList(tableName), false, null, null, null, true);
    }

    public void tonewversion(String tableName,
                             boolean preemptive, Long initWait, Long interval, TimeUnit timeUnit,
                             boolean allowTwoVersion) {
        tonewversionImpl(Arrays.asList(tableName), preemptive, initWait, interval, timeUnit, allowTwoVersion);
    }

    /**
     * Change multiple tables' meta to new version transactional
     */
    @Override
    public void toNewVersionInTrx(List<String> tableNameList,
                                  boolean preemptive, long initWait, long interval, TimeUnit timeUnit,
                                  boolean allowTwoVersion) {
        tonewversionImpl(tableNameList, preemptive, initWait, interval, timeUnit, allowTwoVersion);
    }

    /**
     * Default implementation of batched schema change
     * 1. Be preemptive to avoid deadlock within multiple tables
     */
    @Override
    public void toNewVersionInTrx(List<String> tableNameList, boolean allowTwoVersion) {
        toNewVersionInTrx(tableNameList, true, 15, 15, TimeUnit.SECONDS, allowTwoVersion);
    }

    /**
     * Change all tables in the same table-group
     */
    @Override
    public void toNewVersionForTableGroup(String tableName, boolean allowTwoVersion) {
        boolean isPartDb = DbInfoManager.getInstance().isNewPartitionDb(schemaName);
        GmsTableMetaManager gtm =
            (GmsTableMetaManager) OptimizerContext.getContext(schemaName).getLatestSchemaManager();
        if (!isPartDb) {
            tonewversion(tableName);
        } else {
            final TableGroupInfoManager tgm = OptimizerContext.getContext(schemaName).getTableGroupInfoManager();
            final TableMeta tableMeta = gtm.getTableWithNull(tableName);

            if (tableMeta == null || tableMeta.getPartitionInfo() == null) {
                tonewversion(tableName);
                return;
            }

            long tableGroupId = tableMeta.getPartitionInfo().getTableGroupId();
            TableGroupConfig tgConfig = tgm.getTableGroupConfigById(tableGroupId);
            if (tgConfig != null) {
                List<String> tableNames =
                    GeneralUtil.emptyIfNull(tgConfig.getTables()).stream()
                        .map(TablePartRecordInfoContext::getTableName)
                        .collect(Collectors.toList());

                toNewVersionInTrx(tableNames, allowTwoVersion);
            }
        }

    }

    public static TableMeta buildTableMeta(TablesRecord tableRecord, List<ColumnsRecord> columnsRecords,
                                           List<IndexesRecord> indexesRecords,
                                           String tableName) {
        if (columnsRecords == null || tableRecord == null) {
            return null;
        }
        List<ColumnMeta> allColumnsOrderByDefined = new ArrayList<>();
        Map<String, ColumnMeta> columnMetaMap = new TreeMap<>(CaseInsensitive.CASE_INSENSITIVE_ORDER);
        List<IndexMeta> secondaryIndexMetas = new ArrayList<>();
        boolean hasPrimaryKey;
        List<String> primaryKeys;
        // Get charset and collation in level of table.
        String tableCollation = GeneralUtil.coalesce(tableRecord.tableCollation, CharsetName.DEFAULT_COLLATION);
        String tableCharacterSet = Optional.ofNullable(tableCollation)
            .map(CollationName::getCharsetOf)
            .map(Enum::name)
            .orElse(CharsetName.DEFAULT_CHARACTER_SET);

        try {
            for (ColumnsRecord record : columnsRecords) {
                String columnName = record.columnName;
                String extra = record.extra;
                String columnDefault = record.columnDefault;
                int precision = (int) record.numericPrecision;
                int scale = (int) record.numericScale;
                int datetimePrecision = (int) record.datetimePrecision;
                long length = record.fieldLength;

                // for datetime / timestamp / time
                SqlTypeName sqlTypeName = SqlTypeName.getNameForJdbcType(record.jdbcType);
                if (sqlTypeName != null && SqlTypeName.DATETIME_TYPES.contains(sqlTypeName)) {
                    scale = datetimePrecision;
                    precision = TddlRelDataTypeSystemImpl.getInstance().getMaxPrecision(sqlTypeName);
                }
                if (precision == 0) {
                    precision = (int) record.characterMaximumLength;
                }
                int status = record.status;
                long flag = record.flag;

                boolean autoIncrement = TStringUtil.equalsIgnoreCase(record.extra, "auto_increment");

                boolean nullable = "YES".equalsIgnoreCase(record.isNullable);

                String typeName = record.jdbcTypeName;
                if (TStringUtil.startsWithIgnoreCase(record.columnType, "enum(")) {
                    typeName = record.columnType;
                }

                // Fix length for char & varchar.
                if (record.jdbcType == Types.VARCHAR || record.jdbcType == Types.CHAR) {
                    length = record.characterMaximumLength;
                }

                RelDataType calciteDataType =
                    DataTypeUtil.jdbcTypeToRelDataType(record.jdbcType, typeName, precision, scale, length, nullable);

                // handle character types
                if (SqlTypeUtil.isCharacter(calciteDataType)) {
                    String columnCharacterSet = record.characterSetName;
                    String columnCollation = record.collationName;

                    String characterSet = GeneralUtil.coalesce(columnCharacterSet, tableCharacterSet);
                    String collation = columnCollation != null ? columnCollation :
                        Optional.ofNullable(columnCharacterSet)
                            .map(CharsetName::of)
                            .map(CharsetName::getDefaultCollationName)
                            .map(Enum::name)
                            .orElse(tableCollation);

                    calciteDataType =
                        DataTypeUtil.getCharacterTypeWithCharsetAndCollation(calciteDataType, characterSet, collation);
                }

                Field field =
                    new Field(tableName, columnName, record.collationName, extra, columnDefault, calciteDataType,
                        autoIncrement, false);

                ColumnMeta columnMeta =
                    new ColumnMeta(tableName, columnName, null, field, TableStatus.convert(status), flag);

                allColumnsOrderByDefined.add(columnMeta);

                columnMetaMap.put(columnMeta.getName(), columnMeta);
            }

            try {
                if (TStringUtil.startsWithIgnoreCase(tableName, "information_schema.")) {
                    hasPrimaryKey = true;
                    primaryKeys = new ArrayList<>();
                    if (indexesRecords.size() > 0) {
                        primaryKeys.add(indexesRecords.get(0).columnName);
                    }
                } else {
                    primaryKeys = extractPrimaryKeys(indexesRecords);
                    if (primaryKeys.size() == 0) {
                        if (indexesRecords.size() > 0) {
                            primaryKeys.add(indexesRecords.get(0).columnName);
                        }
                        hasPrimaryKey = false;
                    } else {
                        hasPrimaryKey = true;
                    }

                    Map<String, SecondaryIndexMeta> localIndexMetaMap = new HashMap<>();
                    for (IndexesRecord record : indexesRecords) {
                        String indexName = record.indexName;
                        if ("PRIMARY".equalsIgnoreCase(indexName)
                            || IndexesRecord.GLOBAL_INDEX == record.indexLocation) {
                            continue;
                        }
                        SecondaryIndexMeta meta;
                        if ((meta = localIndexMetaMap.get(indexName)) == null) {
                            meta = new SecondaryIndexMeta();
                            meta.name = indexName;
                            meta.keys = new ArrayList<>();
                            meta.keySubParts = new ArrayList<>();
                            meta.values = primaryKeys;
                            meta.unique = record.nonUnique == 0;
                            localIndexMetaMap.put(indexName, meta);
                        }
                        meta.keys.add(record.columnName);
                        meta.keySubParts.add(record.subPart);
                    }
                    for (SecondaryIndexMeta meta : localIndexMetaMap.values()) {
                        secondaryIndexMetas.add(convertFromSecondaryIndexMeta(meta, columnMetaMap, tableName, true));
                    }
                }
            } catch (Exception ex) {
                throw ex;
            }
        } catch (Exception ex) {
            logger.error("fetch schema error", ex);
            return null;
        }

        List<String> primaryValues = new ArrayList<String>(allColumnsOrderByDefined.size() - primaryKeys.size());
        for (ColumnMeta column : allColumnsOrderByDefined) {
            boolean c = false;
            for (String s : primaryKeys) {
                if (column.getName().equalsIgnoreCase(s)) {
                    c = true;
                    break;
                }
            }
            if (!c) {
                primaryValues.add(column.getName());
            }
        }

        IndexMeta primaryKeyMeta = buildPrimaryIndexMeta(tableName,
            columnMetaMap,
            true,
            primaryKeys,
            primaryValues);

        TableMeta res = new TableMeta(tableName,
            allColumnsOrderByDefined,
            primaryKeyMeta,
            secondaryIndexMetas,
            hasPrimaryKey, TableStatus.convert(tableRecord.status), tableRecord.version, tableRecord.flag);
        res.setId(tableRecord.id);
        return res;
    }

    private static List<String> extractPrimaryKeys(List<IndexesRecord> indexesRecords) {
        List<String> primaryKeys = new ArrayList<>();
        for (IndexesRecord record : indexesRecords) {
            if (TStringUtil.equalsIgnoreCase(record.indexName, "PRIMARY")) {
                primaryKeys.add(record.columnName);
            }
        }
        return primaryKeys;
    }

    private static IndexMeta buildPrimaryIndexMeta(String tableName, Map<String, ColumnMeta> columnMetas,
                                                   boolean strongConsistent, List<String> primaryKeys,
                                                   List<String> primaryValues) {
        if (primaryKeys == null) {
            primaryKeys = new ArrayList<>();
        }

        if (primaryValues == null) {
            primaryValues = new ArrayList<>();
        }

        return new IndexMeta(tableName,
            toColumnMeta(primaryKeys, columnMetas, tableName),
            toColumnMeta(primaryValues, columnMetas, tableName),
            IndexType.BTREE,
            Relationship.NONE,
            strongConsistent,
            true,
            true,
            "PRIMARY");
    }

    private static List<ColumnMeta> toColumnMeta(List<String> columns, Map<String, ColumnMeta> columnMetas,
                                                 String tableName) {
        List<ColumnMeta> metas = Lists.newArrayList();
        for (String cname : columns) {
            if (!columnMetas.containsKey(cname)) {
                throw new RuntimeException("column " + cname + " is not a column of table " + tableName);
            }
            metas.add(columnMetas.get(cname));
        }
        return metas;
    }

    private static List<IndexColumnMeta> toColumnMetaExt(List<String> columns, List<Long> keySubParts,
                                                         Map<String, ColumnMeta> columnMetas, String tableName) {
        List<IndexColumnMeta> metas = Lists.newArrayList();
        int idx = 0;
        for (String cname : columns) {
            if (!columnMetas.containsKey(cname)) {
                throw new RuntimeException("column " + cname + " is not a column of table " + tableName);
            }
            final Long subParts = keySubParts.get(idx);
            metas.add(new IndexColumnMeta(columnMetas.get(cname), null == subParts ? 0 : subParts));
            ++idx;
        }
        return metas;
    }

    @Override
    public TableMeta getTable(String tableName) {
        tableName = tableName.toLowerCase();
        TableMeta table = latestTables.get(tableName);
        if (table == null) {
            throw new TableNotFoundException(ErrorCode.ERR_TABLE_NOT_EXIST, tableName);
        }
        return table;
    }

    @Override
    public void putTable(String tableName, TableMeta tableMeta) {
        throw new UnsupportedOperationException();
    }

    /**
     * return published tables only
     */
    public Collection<TableMeta> getAllTables() {
        return latestTables.values();
    }

    @Override
    public void reload(String tableName) {
        logger.error("unsupported");
    }

    @Override
    public void invalidate(String tableName) {
    }

    @Override
    public void invalidateAll() {
    }

    @Override
    public String toString() {
        return latestTables.toString();
    }

    private List<TableMeta> fetchTableMetas() {
        List<TableMeta> tableMetas = new ArrayList<>();
        TableInfoManager tableInfoManager = new TableInfoManager();
        boolean isNewPartitionDb = DbInfoManager.getInstance().isNewPartitionDb(schemaName);
        try (Connection metaDbConn = MetaDbUtil.getConnection()) {
            tableInfoManager.setConnection(metaDbConn);
            List<TablesRecord> tablesRecords = tableInfoManager.queryTables(schemaName);
            final Map<String, List<TablePartitionRecord>> tablePartitionMap =
                new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
            final Map<String, List<TablePartitionRecord>> tablePartitionMapFromDelta =
                new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
            final Map<String, List<TableLocalPartitionRecord>> tableLocalPartitionMap =
                new TreeMap<>(String.CASE_INSENSITIVE_ORDER);

            if (isNewPartitionDb) {
                tablePartitionMap.putAll(tableInfoManager.queryTablePartitions(schemaName, false).stream()
                    .collect(Collectors.groupingBy(TablePartitionRecord::getTableName)));
                tablePartitionMapFromDelta.putAll(tableInfoManager.queryTablePartitions(schemaName, true).stream()
                    .collect(Collectors.groupingBy(TablePartitionRecord::getTableName)));
                tableLocalPartitionMap.putAll(tableInfoManager.getLocalPartitionRecordBySchema(schemaName).stream()
                    .collect(Collectors.groupingBy(TableLocalPartitionRecord::getTableName)));
            }

            Map<String, List<ColumnsRecord>> allColumns = tableInfoManager.queryVisibleColumns(schemaName);
            Map<String, List<IndexesRecord>> allIndexes = tableInfoManager.queryVisibleIndexes(schemaName);
            Map<String, TablesExtRecord> extRecords = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
            extRecords.putAll(tableInfoManager.queryTableExts(schemaName).stream().collect(
                Collectors.toMap(TablesExtRecord::getTableName, r -> r)));

            DataSource dataSource = MetaDbDataSource.getInstance().getDataSource();
            final GsiMetaManager gsiMetaManager =
                new GsiMetaManager(dataSource, schemaName);

            List<GsiMetaManager.IndexRecord> allIndexRecords = gsiMetaManager.getIndexRecords(schemaName);

            // tableName->List<IndexRecord>
            Map<String, List<GsiMetaManager.IndexRecord>> indexRecordsTableMap =
                new TreeMap<>(String.CASE_INSENSITIVE_ORDER);

            // indexName->List<IndexRecord>
            Map<String, List<GsiMetaManager.IndexRecord>> indexRecordsIndexMap =
                new TreeMap<>(String.CASE_INSENSITIVE_ORDER);

            allIndexRecords.forEach(r -> {
                indexRecordsTableMap.computeIfAbsent(r.getTableName(), k -> new ArrayList<>()).add(r);
                indexRecordsIndexMap.computeIfAbsent(r.getIndexName(), k -> new ArrayList<>()).add(r);
            });

            Map<String, List<ComplexTaskOutlineRecord>> complexTaskRecordsMap =
                ComplexTaskMetaManager.getUnFinishTasksBySchName(schemaName);

            for (TablesRecord tableRecord : tablesRecords) {
                String origTableName = tableRecord.tableName;
                List<ColumnsRecord> columnsRecords =
                    allColumns.get(origTableName);
                List<IndexesRecord> indexesRecords = allIndexes.get(origTableName);
                if (indexesRecords == null) {
                    indexesRecords = Collections.emptyList();
                }

                TableMeta meta = buildTableMeta(tableRecord, columnsRecords, indexesRecords, tableRecord.tableName);
                boolean locked = false;
                if (meta != null) {
                    meta.setGsiTableMetaBean(
                        gsiMetaManager.initTableMeta(origTableName, indexRecordsTableMap.get(origTableName),
                            indexRecordsIndexMap.get(origTableName)));

                    meta.setComplexTaskTableMetaBean(ComplexTaskMetaManager
                        .getComplexTaskTableMetaBean(schemaName, origTableName,
                            complexTaskRecordsMap.get(origTableName)));

                    if (isNewPartitionDb) {
                        loadNewestPartitionInfo(metaDbConn, schemaName, origTableName, origTableName, rule,
                            tableInfoManager, meta,
                            tablePartitionMap.get(origTableName), tablePartitionMapFromDelta.get(origTableName),
                            tableLocalPartitionMap.get(origTableName));
                    }
                    // Get auto partition mark.
                    final TablesExtRecord extRecord =
                        extRecords.get(origTableName);
                    if (extRecord != null) {
                        meta.setAutoPartition(extRecord.isAutoPartition());
                        // Load lock flag.
                        locked = extRecord.isLocked();
                    }

                    // Auto partition flag for new partition table.
                    if (meta.getPartitionInfo() != null) {
                        meta.setAutoPartition(
                            (meta.getPartitionInfo().getPartFlags() & TablePartitionRecord.FLAG_AUTO_PARTITION) != 0);
                        // Load lock flag.
                        locked = (meta.getPartitionInfo().getPartFlags() & TablePartitionRecord.FLAG_LOCK) != 0;
                    }

                } else {
                    logger.error(
                        "Table `" + origTableName + "` build meta error.");
                    continue;
                }
                if (locked) {
                    logger.warn("Table `" + origTableName + "` has been locked by logical meta lock.");
                } else {
                    tableMetas.add(meta);
                }

                tableMetas.add(meta);
            }
        } catch (SQLException e) {
            throw new RuntimeException(
                "Schema `" + schemaName + "` build meta error.");
        } finally {
            tableInfoManager.setConnection(null);
        }

        return tableMetas;
    }

    private static void logParitionInfo(TableMeta tableMeta) {
        PartitionInfo partitionInfo = tableMeta.getPartitionInfo();
        PartitionInfo newPartitionInfo = tableMeta.getNewPartitionInfo();
        if (partitionInfo != null) {
            SQLRecorderLogger.ddlMetaLogger.info(MessageFormat.format(
                "curPartitionInfo:{0}",
                partitionInfo.getDigest(tableMeta.getVersion())));
        }
        if (newPartitionInfo != null) {
            SQLRecorderLogger.ddlMetaLogger.info(MessageFormat.format(
                "newPartitionInfo:{0}",
                newPartitionInfo.getDigest(tableMeta.getVersion())));
        }
    }

    private static void loadNewestPartitionInfo(Connection conn,
                                                String schemaName,
                                                String logicalTableName,
                                                String origTableName,
                                                TddlRuleManager ruleMgr,
                                                TableInfoManager tblInfoMgr,
                                                TableMeta logTblMeta) {
        // init the partitionInfo firstly
        logTblMeta.initPartitionInfo(conn, schemaName, logicalTableName, ruleMgr);
        logTblMeta.setLocalPartitionDefinitionInfo(
            LocalPartitionDefinitionInfo.from(tblInfoMgr.getLocalPartitionRecord(schemaName, logicalTableName))
        );
        // get the partitionInfo secondly
        PartitionInfo curPartitionInfo = ruleMgr.getPartitionInfoManager().getPartitionInfo(origTableName);
        // set the partitionInfo at the last step
        logTblMeta.setPartitionInfo(curPartitionInfo);

        SQLRecorderLogger.ddlMetaLogger.info(logTblMeta.getComplexTaskTableMetaBean().getDigest());
        if (!logTblMeta.getComplexTaskTableMetaBean().allPartIsPublic()) {
            if (curPartitionInfo != null) {
                PartitionInfo newPartitionInfo =
                    ruleMgr.getPartitionInfoManager().getPartitionInfoFromDeltaTable(conn, origTableName);
                logTblMeta.initPartitionInfo(conn, schemaName, logicalTableName, ruleMgr);
                PartitionInfoUtil.updatePartitionInfoByNewCommingPartitionRecords(conn, newPartitionInfo);
                if (logTblMeta.getComplexTaskTableMetaBean().isNeedSwitchDatasource()) {
                    curPartitionInfo = PartitionInfoUtil
                        .updatePartitionInfoByOutDatePartitionRecords(conn, curPartitionInfo, tblInfoMgr);
                    newPartitionInfo = newPartitionInfo.copy();
                    logTblMeta.setNewPartitionInfo(curPartitionInfo);
                    logTblMeta.setPartitionInfo(newPartitionInfo);
                } else {
                    logTblMeta.setNewPartitionInfo(newPartitionInfo);
                    logTblMeta.setPartitionInfo(curPartitionInfo);
                }
            }
        }

        logParitionInfo(logTblMeta);
    }

    private static void loadNewestPartitionInfo(Connection metaDbConnect,
                                                String schemaName,
                                                String logicalTableName,
                                                String origTableName,
                                                TddlRuleManager ruleMgr,
                                                TableInfoManager tblInfoMgr,
                                                TableMeta logTblMeta,
                                                List<TablePartitionRecord> tablePartitionRecords,
                                                List<TablePartitionRecord> tablePartitionRecordsFromDelta,
                                                List<TableLocalPartitionRecord> tableLocalPartitionRecords) {
        // init the partitionInfo firstly
        logTblMeta.initPartitionInfo(schemaName, logicalTableName, ruleMgr, tablePartitionRecords,
            tablePartitionRecordsFromDelta);
        if (CollectionUtils.isNotEmpty(tableLocalPartitionRecords)) {
            assert tableLocalPartitionRecords.size() == 1;
            logTblMeta.setLocalPartitionDefinitionInfo(
                LocalPartitionDefinitionInfo.from(tableLocalPartitionRecords.get(0))
            );
        }
        // get the partitionInfo secondly
        PartitionInfo curPartitionInfo = ruleMgr.getPartitionInfoManager().getPartitionInfo(origTableName);
        // set the partitionInfo at the last step
        logTblMeta.setPartitionInfo(curPartitionInfo);

        SQLRecorderLogger.ddlMetaLogger.info(logTblMeta.getComplexTaskTableMetaBean().getDigest());
        if (!logTblMeta.getComplexTaskTableMetaBean().allPartIsPublic()) {
            if (curPartitionInfo != null) {
                PartitionInfo newPartitionInfo =
                    ruleMgr.getPartitionInfoManager().getPartitionInfoFromDeltaTable(origTableName);
                logTblMeta.initPartitionInfo(schemaName, logicalTableName, ruleMgr, tablePartitionRecords,
                    tablePartitionRecordsFromDelta);
                if (logTblMeta.getComplexTaskTableMetaBean().isNeedSwitchDatasource()) {
                    curPartitionInfo = PartitionInfoUtil
                        .updatePartitionInfoByOutDatePartitionRecords(metaDbConnect, curPartitionInfo, tblInfoMgr);
                    newPartitionInfo = newPartitionInfo.copy();
                    logTblMeta.setNewPartitionInfo(curPartitionInfo);
                    logTblMeta.setPartitionInfo(newPartitionInfo);
                } else {
                    PartitionInfoUtil.updatePartitionInfoByNewCommingPartitionRecords(metaDbConnect, newPartitionInfo);
                    logTblMeta.setNewPartitionInfo(newPartitionInfo);
                    logTblMeta.setPartitionInfo(curPartitionInfo);
                }
            }
        }
        logParitionInfo(logTblMeta);
    }

    protected void loadAndCacheTableMeta(List<String> tableNames) {
        try (Connection metaDbConn = MetaDbUtil.getConnection()) {
            metaDbConn.setAutoCommit(false);
            metaDbConn.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);

            Map<String, TableMeta> metaMap =
                fetchTableMeta(metaDbConn, schemaName, tableNames, rule, storage, false, false);

            for (val entry : metaMap.entrySet()) {
                String tableName = entry.getKey().toLowerCase();
                TableMeta meta = entry.getValue();

                if (meta == null) {
                    latestTables.remove(tableName);
                } else {
                    //create/alter table
                    meta.setSchemaName(schemaName);
                    latestTables.put(tableName, meta);
                    if (meta.getGsiTableMetaBean() != null && !meta.getGsiTableMetaBean().indexMap.isEmpty()) {
                        for (GsiMetaManager.GsiIndexMetaBean index : meta.getGsiTableMetaBean().indexMap.values()) {
                            String indexName = index.indexName.toLowerCase();
                            TableMeta indexTableMeta =
                                fetchTableMeta(metaDbConn, schemaName, indexName, rule, storage, false, false);
                            if (indexTableMeta == null) {
                                latestTables.remove(indexName);
                            } else {
                                boolean isNewPartition = DbInfoManager.getInstance().isNewPartitionDb(schemaName);
                                indexTableMeta.setSchemaName(schemaName);
                                latestTables.put(indexName, indexTableMeta);
                                indexTableMeta.getGsiTableMetaBean().gsiMetaBean = index;
                                if (!isNewPartition) {
                                    TableRuleManager.reload(schemaName, index.indexName);
                                }
                            }
                        }
                    }
                }
            }

            metaDbConn.setAutoCommit(true);
        } catch (SQLException e) {
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_GENERIC, "fail to fetch table metas: " + tableNames, e);
        }
    }

    protected void loadAndCacheTableMeta(String tableName) {
        loadAndCacheTableMeta(Arrays.asList(tableName));
    }

    /**
     * Steps:
     * 1. Check meta version of table, decide whether loading new TableMeta if necessary
     * 2. Invalidate existed cache, such as SequenceCache and TableRule
     * 3. Step up the new SchemaManager
     * 4. Acquire MDL
     * 5. Expire Old SchemaManager
     * 6. Invalidate Plan Cache
     * 7. Release MDL
     *
     * @param allowTwoVersion if two versions of schema exist at the same time
     */
    private void tonewversionImpl(List<String> tableNameList,
                                  boolean preemptive, Long initWait, Long interval, TimeUnit timeUnit,
                                  boolean allowTwoVersion) {
        synchronized (OptimizerContext.getContext(schemaName)) {
            boolean isNewPartitionDb = DbInfoManager.getInstance().isNewPartitionDb(schemaName);
            GmsTableMetaManager oldSchemaManager =
                (GmsTableMetaManager) OptimizerContext.getContext(schemaName).getLatestSchemaManager();
            GmsTableMetaManager newSchemaManager;
            Map<String, Long> staleTables = new HashMap<>();

            for (String tableName : tableNameList) {
                TableMeta currentMeta = oldSchemaManager.getTableWithNull(tableName);
                long version = checkTableVersion(tableName);

                if (version != -1 && currentMeta != null && currentMeta.getVersion() >= version) {
                    SQLRecorderLogger.ddlLogger.info(MessageFormat.format(
                        "{0}.{1} meta version change to {2} ignored, current version {3}", schemaName, tableName,
                        version,
                        currentMeta.getVersion()));
                    continue;
                }

                // Invalidate various cache
                SequenceCacheManager.invalidate(schemaName, AUTO_SEQ_PREFIX + tableName);
                if (!isNewPartitionDb) {
                    if (version == -1) {
                        TableRuleManager.invalidate(schemaName, tableName);
                    } else {
                        TableRuleManager.reload(schemaName, tableName);
                    }
                } else {
                    if (version == -1) {
                        PartitionInfoManager.invalidate(schemaName, tableName);
                    } else {
                        PartitionInfoManager.reload(schemaName, tableName);
                    }
                }
                staleTables.put(tableName, version);
            }

            if (staleTables.isEmpty()) {
                return;
            }

            // Load new TableMeta
            {
                Map.Entry<String, Long> firstTable = staleTables.entrySet().iterator().next();
                String tableName = firstTable.getKey();
                TableMeta currentMeta = oldSchemaManager.getTableWithNull(tableName);
                long oldVersion = currentMeta == null ? 0 : currentMeta.getVersion();
                long newVersion = firstTable.getValue();

                // TODO(moyi) unify these two code path
                if (tableNameList.size() > 1) {
                    newSchemaManager = new GmsTableMetaManager(oldSchemaManager, tableNameList, rule);
                } else {
                    newSchemaManager = new GmsTableMetaManager(oldSchemaManager, tableName, rule);
                }

                newSchemaManager.init();

                SQLRecorderLogger.ddlMetaLogger.info("allowTwoVersion1:" + String.valueOf(allowTwoVersion));
                if (allowTwoVersion) {
                    OptimizerContext.getContext(schemaName).setSchemaManager(newSchemaManager);
                }
                SQLRecorderLogger.ddlMetaLogger.info(MessageFormat.format(
                    "{0} reload table metas for [{1}]: since meta version of table {2} change from {3} to {4}",
                    String.valueOf(System.identityHashCode(newSchemaManager)), tableNameList, tableName, oldVersion,
                    newVersion));

            }

            // Insert mdl barrier
            {
                mdlCriticalSection(preemptive, initWait, interval, timeUnit, oldSchemaManager, staleTables.keySet(),
                    (x) -> {
                        oldSchemaManager.expire();
                        if (!allowTwoVersion) {
                            OptimizerContext.getContext(schemaName).setSchemaManager(newSchemaManager);
                            SQLRecorderLogger.ddlMetaLogger
                                .info("newSchemaManager:" + System.identityHashCode(newSchemaManager));

                        }
                        return null;
                    });
            }
        }
    }

    /**
     * Insert an MDL barrier for tables to clear cross status transaction.
     */
    private void mdlCriticalSection(boolean preemptive, Long initWait, Long interval, TimeUnit timeUnit,
                                    GmsTableMetaManager oldSchemaManager, Collection<String> tableNameList,
                                    Function<Void, Void> duringBarrier) {
        final MdlContext context;
        if (preemptive) {
            context = MdlManager.addContext(schemaName, initWait, interval, timeUnit);
        } else {
            context = MdlManager.addContext(schemaName, false);
        }
        SQLRecorderLogger.ddlLogger.warn(MessageFormat.format(
            "Mdl {0}  {1}.addContext({2})", Thread.currentThread().getName(), this.hashCode(), schemaName));

        try {
            long startMillis = System.currentTimeMillis();
            List<MdlTicket> tickets = new ArrayList<>();
            // sort by table name to avoid deadlock
            List<String> lockedTables =
                tableNameList.stream()
                    .map(oldSchemaManager::getTableWithNull)
                    .filter(Objects::nonNull)
                    .map(TableMeta::getDigest)
                    .sorted()
                    .distinct()
                    .collect(Collectors.toList());

            SQLRecorderLogger.ddlLogger.info(MessageFormat.format(
                "[{0} {1}] Mdl write lock try to acquired table[{2}]",
                Thread.currentThread().getName(), this.hashCode(), lockedTables));

            for (String tableName : lockedTables) {
                MdlTicket ticket = context.acquireLock(
                    new MdlRequest(1L,
                        MdlKey.getTableKeyWithLowerTableName(schemaName, tableName),
                        MdlType.MDL_EXCLUSIVE,
                        MdlDuration.MDL_TRANSACTION));

                tickets.add(ticket);
            }
            long elapsedMillis = System.currentTimeMillis() - startMillis;
            SQLRecorderLogger.ddlLogger.info(MessageFormat.format(
                "[{0} {1}] Mdl write lock acquired table[{2}] cost {3}ms",
                Thread.currentThread().getName(), this.hashCode(), lockedTables, elapsedMillis));

            if (duringBarrier != null) {
                duringBarrier.apply(null);
            }

            // invalid plan cache when old table meta is not in using
            // Note: Invalidate plan cache is still necessary,
            // because non-multi-write plan for simple table may be cached.
            for (String schemaName : OptimizerContext.getActiveSchemaNames()) {
                final OptimizerContext optimizerContext = OptimizerContext.getContext(schemaName);
                if (optimizerContext != null) {
                    final PlanManager planManager = optimizerContext.getPlanManager();
                    if (planManager != null) {
                        planManager.invalidateCache();
                    }
                }
            }

            for (MdlTicket ticket : tickets) {
                context.releaseLock(1L, ticket);
            }

            elapsedMillis = System.currentTimeMillis() - startMillis;
            SQLRecorderLogger.ddlLogger.info(MessageFormat.format(
                "[{0} {1}] Mdl write lock release table[{2}] cost {3}ms",
                Thread.currentThread().getName(), this.hashCode(), lockedTables, elapsedMillis));
        } finally {
            context.releaseAllTransactionalLocks();
            MdlManager.removeContext(context);
        }
    }

    private long checkTableVersion(String tableName) {
        long version = -1;

        try (Connection metaDbConn = MetaDbUtil.getConnection()) {
            PreparedStatement stmt =
                metaDbConn.prepareStatement("select version from tables where table_schema=? and table_name=?");
            stmt.setString(1, schemaName);
            stmt.setString(2, tableName);
            ResultSet rs = stmt.executeQuery();
            if (rs.next()) {
                version = rs.getLong(1);
            }
        } catch (SQLException e) {
            throw new TddlNestableRuntimeException(e);
        }
        return version;
    }

    @Override
    public String getSchemaName() {
        return schemaName;
    }

    @Override
    public void expire() {
        this.expired = true;
    }

    @Override
    public boolean isExpired() {
        return expired;
    }

    @Override
    public GsiMetaManager.GsiMetaBean getGsi(String primaryOrIndexTableName,
                                             EnumSet<IndexStatus> statusSet) {
        DataSource dataSource = MetaDbDataSource.getInstance().getDataSource();
        final GsiMetaManager gsiMetaManager =
            new GsiMetaManager(dataSource, schemaName);
        return gsiMetaManager.getTableAndIndexMeta(primaryOrIndexTableName, statusSet);
    }

    @Override
    public Set<String> guessGsi(String unwrappedName) {
        DataSource dataSource = MetaDbDataSource.getInstance().getDataSource();
        final GsiMetaManager gsiMetaManager =
            new GsiMetaManager(dataSource, schemaName);
        final GsiMetaManager.GsiMetaBean meta = gsiMetaManager.getAllGsiMetaBean(schemaName);

        final Set<String> gsi = new HashSet<>();
        for (GsiMetaManager.GsiTableMetaBean bean : meta.getTableMeta().values()) {
            if (bean.gsiMetaBean != null && TddlSqlToRelConverter.unwrapGsiName(bean.gsiMetaBean.indexName)
                .equalsIgnoreCase(unwrappedName)) {
                gsi.add(bean.gsiMetaBean.indexName);
            }
        }
        return gsi;
    }

    private static class SecondaryIndexMeta {
        String name;
        Boolean unique;
        List<String> keys;
        List<Long> keySubParts;
        List<String> values;
    }

    @Override
    public TableMeta getTableMetaFromConnection(String tableName, Connection conn) {
        try {
            Map<String, String> collationTypes = fetchCollationType(conn, tableName);
            Map<String, String> specialTypes = fetchColumnType(conn, tableName);
            Map<String, String> defaultInfo = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
            Map<String, String> extraInfo = fetchColumnExtraAndDefault(conn, tableName, defaultInfo);
            return fetchTableMeta(conn, tableName, tableName, collationTypes, specialTypes, extraInfo, defaultInfo);
        } catch (Exception e) {
            return null;
        } finally {
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                    logger.warn("", e);
                }
            }
        }
    }

    /**
     * 20160429 方物 增加获取表collaction meta信息
     */
    private static Map<String, String> fetchCollationType(Connection conn, String actualTableName) {
        Map<String, String> collationType = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        Statement stmt = null;
        ResultSet rs = null;

        try {
            stmt = conn.createStatement();
            rs = stmt.executeQuery("SHOW FULL COLUMNS FROM `" + actualTableName + "`");
            while (rs.next()) {
                String field = rs.getString("Field");
                String collation = rs.getString("Collation");
                collationType.put(field, collation);
            }
        } catch (Exception e) {
            Throwables.propagate(e);
        } finally {
            try {
                if (rs != null) {
                    rs.close();
                }
                if (stmt != null) {
                    stmt.close();
                }
            } catch (SQLException e) {
                logger.warn("", e);
            }
        }

        return collationType;
    }

    private static Map<String, String> fetchColumnExtraAndDefault(Connection conn, String actualTableName,
                                                                  Map<String, String> defaultInfo) {
        Map<String, String> columnExtra = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        Statement stmt = null;
        ResultSet rs = null;

        try {
            stmt = conn.createStatement();
            rs = stmt.executeQuery("DESC `" + actualTableName + "`");
            while (rs.next()) {
                String field = rs.getString("Field");
                String extra = rs.getString("Extra");
                String defalutStr = rs.getString("Default");
                if (extra != null) {
                    columnExtra.put(field, extra);
                }
                if (defalutStr != null) {
                    defaultInfo.put(field, defalutStr);
                }
            }
        } catch (Exception e) {
            Throwables.propagate(e);
        } finally {
            try {
                if (rs != null) {
                    rs.close();
                }
                if (stmt != null) {
                    stmt.close();
                }
            } catch (SQLException e) {
                logger.warn("", e);
            }
        }

        return columnExtra;
    }

    private static Map<String, String> fetchColumnType(Connection conn, String actualTableName) {
        Map<String, String> specialType = new TreeMap(String.CASE_INSENSITIVE_ORDER);
        Statement stmt = null;
        ResultSet rs = null;
        try {
            stmt = conn.createStatement();
            rs = stmt.executeQuery("desc `" + actualTableName + "`");
            while (rs.next()) {
                String field = rs.getString("Field");
                String type = rs.getString("Type");

                if (TStringUtil.startsWithIgnoreCase(type, "enum(")) {
                    specialType.put(field, type);
                }
            }
        } catch (Exception e) {
            Throwables.propagate(e);
        } finally {
            try {
                if (rs != null) {
                    rs.close();
                }
                if (stmt != null) {
                    stmt.close();
                }
            } catch (SQLException e) {
                logger.warn("", e);
            }
        }

        return specialType;
    }

    private static TableMeta fetchTableMeta(Connection conn, String actualTableName, String logicalTableName,
                                            Map<String, String> collationType, Map<String, String> specialType,
                                            Map<String, String> extraInfo, Map<String, String> defaultInfo) {
        Statement stmt = null;
        ResultSet rs = null;
        TableMeta meta = null;

        try {
            try {
                stmt = conn.createStatement();
                rs = stmt.executeQuery("select * from `" + actualTableName + "` where 1 = 2");
                ResultSetMetaData rsmd = rs.getMetaData();
                DatabaseMetaData dbmd = conn.isWrapperFor(XConnection.class) ? null : conn.getMetaData();
                meta = resultSetMetaToSchema(rsmd,
                    dbmd,
                    specialType,
                    collationType,
                    extraInfo,
                    defaultInfo,
                    logicalTableName,
                    actualTableName);
            } catch (Exception e) {
                if (e instanceof SQLException) {
                    if ("42000".equals(((SQLException) e).getSQLState())) {
                        try {
                            rs = stmt.executeQuery("select * from `" + actualTableName + "` where rownum<=2");
                            ResultSetMetaData rsmd = rs.getMetaData();
                            DatabaseMetaData dbmd =
                                conn.isWrapperFor(XConnection.class) ? null : conn.getMetaData();
                            return resultSetMetaToSchema(rsmd,
                                dbmd,
                                specialType,
                                collationType,
                                extraInfo,
                                defaultInfo,
                                logicalTableName,
                                actualTableName);
                        } catch (SQLException e1) {
                            logger.warn(e);
                        }
                    }
                }
                logger.error("schema of " + logicalTableName + " cannot be fetched", e);
                Throwables.propagate(e);
            } finally {
                try {
                    if (rs != null) {
                        rs.close();
                    }
                    if (stmt != null) {
                        stmt.close();
                    }
                } catch (SQLException e) {
                    logger.warn("", e);
                }
            }
        } finally {
            //ignore
        }

        return meta;
    }

    public static TableMeta resultSetMetaToSchema(ResultSetMetaData rsmd, DatabaseMetaData dbmd,
                                                  Map<String, String> specialType, Map<String, String> collationType,
                                                  Map<String, String> extraInfo, Map<String, String> defaultInfo,
                                                  String logicalTableName, String actualTableName) {

        return resultSetMetaToTableMeta(rsmd,
            dbmd,
            specialType,
            collationType,
            extraInfo,
            defaultInfo,
            logicalTableName,
            actualTableName);
    }

    private static final java.lang.reflect.Field MYSQL_RSMD_FIELDS;

    static {
        try {
            MYSQL_RSMD_FIELDS = com.mysql.jdbc.ResultSetMetaData.class.getDeclaredField("fields");
            MYSQL_RSMD_FIELDS.setAccessible(true);
        } catch (SecurityException | NoSuchFieldException e) {
            throw GeneralUtil.nestedException(e);
        }
    }

    private static TableMeta resultSetMetaToTableMeta(ResultSetMetaData rsmd, DatabaseMetaData dbmd,
                                                      Map<String, String> specialType,
                                                      Map<String, String> collationType,
                                                      Map<String, String> extraInfo,
                                                      Map<String, String> defaultInfo,
                                                      String tableName, String actualTableName) {

        List<ColumnMeta> allColumnsOrderByDefined = new ArrayList<>();
        List<IndexMeta> secondaryIndexMetas = new ArrayList<>();
        Map<String, ColumnMeta> columnMetaMap = new TreeMap<>(CaseInsensitive.CASE_INSENSITIVE_ORDER);
        boolean hasPrimaryKey;
        List<String> primaryKeys = new ArrayList<>();

        try {
            com.mysql.jdbc.Field[] fields = null;
            if (rsmd instanceof ResultSetMetaDataProxy) {
                rsmd = rsmd.unwrap(com.mysql.jdbc.ResultSetMetaData.class);
            }
            if (rsmd instanceof com.mysql.jdbc.ResultSetMetaData) {
                fields = (com.mysql.jdbc.Field[]) MYSQL_RSMD_FIELDS.get(rsmd);
            }

            if (rsmd instanceof XResultSetMetaData) {
                // X connection.

                // Column.
                for (int i = 0; i < rsmd.getColumnCount(); ++i) {
                    final PolarxResultset.ColumnMetaData metaData =
                        ((XResultSetMetaData) rsmd).getResult().getMetaData().get(i);
                    String extra = extraInfo.get(rsmd.getColumnName(i + 1));
                    String defaultStr = defaultInfo.get(rsmd.getColumnName(i + 1));
                    ColumnMeta columnMeta = TableMetaParser.buildColumnMeta(metaData,
                        XSession.toJavaEncoding(
                            ((XResultSetMetaData) rsmd).getResult().getSession()
                                .getResultMetaEncodingMySQL()),
                        extra, defaultStr);
                    allColumnsOrderByDefined.add(columnMeta);
                    columnMetaMap.put(columnMeta.getName(), columnMeta);
                }

                // PK and index.
                try {
                    if (TStringUtil.startsWithIgnoreCase(actualTableName, "information_schema.")) {
                        hasPrimaryKey = true;
                        primaryKeys.add(rsmd.getColumnName(1));
                    } else {
                        final XResult result = ((XResultSetMetaData) rsmd).getResult();
                        final XConnection connection = result.getConnection();

                        // Consume all request before send new one.
                        while (result.next() != null) {
                            ;
                        }

                        final XResult keyResult = connection.execQuery("SHOW KEYS FROM `" + actualTableName + '`');
                        final XResultSet pkrs = new XResultSet(keyResult);
                        TreeMap<Integer, String> treeMap = new TreeMap<>();
                        while (pkrs.next()) {
                            if (pkrs.getString("Key_name").equalsIgnoreCase("PRIMARY")) {
                                treeMap.put(pkrs.getInt("Seq_in_index"), pkrs.getString("Column_name"));
                            }
                        }

                        for (String v : treeMap.values()) {
                            primaryKeys.add(v);
                        }

                        if (primaryKeys.size() == 0) {
                            primaryKeys.add(rsmd.getColumnName(1));
                            hasPrimaryKey = false;
                        } else {
                            hasPrimaryKey = true;
                        }

                        final XResult indexResult =
                            connection.execQuery("SHOW INDEX FROM `" + actualTableName + '`');
                        final XResultSet sirs = new XResultSet(indexResult);
                        Map<String, SecondaryIndexMeta> secondaryIndexMetaMap = new HashMap<>();
                        while (sirs.next()) {
                            String indexName = sirs.getString("Key_name");
                            if (indexName.equalsIgnoreCase("PRIMARY")) {
                                continue;
                            }
                            SecondaryIndexMeta meta;
                            if ((meta = secondaryIndexMetaMap.get(indexName)) == null) {
                                meta = new SecondaryIndexMeta();
                                meta.name = indexName;
                                meta.keys = new ArrayList<>();
                                meta.keySubParts = new ArrayList<>();
                                meta.values = primaryKeys;
                                meta.unique = sirs.getInt("Non_unique") == 0;
                                secondaryIndexMetaMap.put(indexName, meta);
                            }
                            meta.keys.add(sirs.getString("Column_name"));
                            meta.keySubParts.add(sirs.getLong("Sub_part"));
                        }
                        for (SecondaryIndexMeta meta : secondaryIndexMetaMap.values()) {
                            secondaryIndexMetas
                                .add(convertFromSecondaryIndexMeta(meta, columnMetaMap, tableName, true));
                        }

                    }
                } catch (Exception ex) {
                    propagateIfGetConnectionFailed(ex);
                    throw ex;
                }
            } else {
                throw new NotSupportException("jdbc");
            }
        } catch (Exception ex) {
            logger.error("fetch schema error", ex);
            return null;
        }

        List<String> primaryValues = new ArrayList<String>(allColumnsOrderByDefined.size() - primaryKeys.size());
        for (ColumnMeta column : allColumnsOrderByDefined) {
            boolean c = false;
            for (String s : primaryKeys) {
                if (column.getName().equalsIgnoreCase(s)) {
                    c = true;
                    break;
                }
            }
            if (!c) {
                primaryValues.add(column.getName());
            }
        }
        IndexMeta primaryKeyMeta = buildPrimaryIndexMeta(tableName,
            columnMetaMap,
            true,
            primaryKeys,
            primaryValues);
        return new TableMeta(tableName,
            allColumnsOrderByDefined,
            primaryKeyMeta,
            secondaryIndexMetas,
            hasPrimaryKey, TableStatus.PUBLIC, 0, 0);

    }

    private static void propagateIfGetConnectionFailed(Throwable t) {
        String message = null;
        List<Throwable> ths = ExceptionUtils.getThrowableList(t);
        for (int i = ths.size() - 1; i >= 0; i--) {
            Throwable e = ths.get(i);
            if (e instanceof GetConnectionTimeoutException) {
                if (e.getCause() != null) {
                    message = e.getCause().getMessage();
                } else {
                    message = e.getMessage();
                }
                throw new TddlRuntimeException(ErrorCode.ERR_ATOM_GET_CONNECTION_FAILED_UNKNOWN_REASON, e, message);
            }
        }
    }

    private static IndexMeta convertFromSecondaryIndexMeta(SecondaryIndexMeta secondaryIndexMeta,
                                                           Map<String, ColumnMeta> columnMetas, String tableName,
                                                           boolean strongConsistent) {

        return new IndexMeta(tableName,
            toColumnMetaExt(secondaryIndexMeta.keys, secondaryIndexMeta.keySubParts, columnMetas, tableName),
            toColumnMeta(secondaryIndexMeta.values, columnMetas, tableName),
            IndexType.NONE,
            Relationship.NONE,
            strongConsistent,
            secondaryIndexMeta.unique,
            secondaryIndexMeta.name);
    }

    @Override
    public Map<String, TableMeta> getCache() {
        return latestTables;
    }

    @Override
    public TddlRuleManager getTddlRuleManager() {
        return rule;
    }
}
