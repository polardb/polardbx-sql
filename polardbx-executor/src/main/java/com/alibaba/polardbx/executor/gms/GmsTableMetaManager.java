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
import com.alibaba.polardbx.gms.partition.TablePartitionRecord;
import com.alibaba.polardbx.gms.tablegroup.ComplexTaskOutlineRecord;
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
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.partition.PartitionInfoManager;
import com.alibaba.polardbx.optimizer.partition.PartitionInfoUtil;
import com.alibaba.polardbx.optimizer.planmanager.PlanManager;
import com.alibaba.polardbx.optimizer.rule.TddlRuleManager;
import com.alibaba.polardbx.optimizer.sql.sql2rel.TddlSqlToRelConverter;
import com.alibaba.polardbx.rpc.client.XSession;
import com.alibaba.polardbx.rpc.compatible.XResultSet;
import com.alibaba.polardbx.rpc.compatible.XResultSetMetaData;
import com.alibaba.polardbx.rpc.pool.XConnection;
import com.alibaba.polardbx.rpc.result.XResult;
import com.alibaba.polardbx.statistics.SQLRecorderLogger;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.mysql.cj.polarx.protobuf.PolarxResultset;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeUtil;
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
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
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
        tableName = tableName.toLowerCase();
        loadAndCacheTableMeta(tableName);
    }

    protected void loadAndCacheTableMeta(String tableName) {
        TableMeta meta = fetchTableMeta(schemaName, tableName, rule, false, false);

        boolean locked = false;
        if (meta == null) {// drop table
            latestTables.remove(tableName);
        } else {
            //create/alter table
            meta.setSchemaName(schemaName);
            latestTables.put(tableName, meta);
            if (meta.getGsiTableMetaBean() != null && !meta.getGsiTableMetaBean().indexMap.isEmpty()) {
                for (GsiMetaManager.GsiIndexMetaBean index : meta.getGsiTableMetaBean().indexMap.values()) {
                    String indexName = index.indexName.toLowerCase();
                    TableMeta indexTableMeta =
                        fetchTableMeta(schemaName, indexName, rule, false, false);
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
/*
            TableInfoManager tableInfoManager = new TableInfoManager();
            try {
                try (Connection metaDbConn = MetaDbUtil.getConnection()) {
                    tableInfoManager.setConnection(metaDbConn);

                    // Load meta for scaleout
                    ScaleOutMetaManager scaleOutMetaManager = new ScaleOutMetaManager(schemaName);
                    Map<String, List<ScaleOutMetaManager.MoveTableRecord>> moveTableRecordsMap =
                        scaleOutMetaManager.getTableRecordsBySchName(schemaName);
                    List<ScaleOutMetaManager.MoveTableRecord> moveTableRecords = moveTableRecordsMap.get(tableName);
                    meta.setScaleOutTableMetaBean(
                        ScaleOutPlanUtil.getScaleOutTableMetaBean(schemaName, tableName, rule, moveTableRecords));

                    // Load meta for partitionInfo
                    loadNewestPartitionInfo(schemaName, tableName, tableName, rule, tableInfoManager, meta);

                    // Get auto partition mark.
                    final TablesExtRecord extRecord =
                        tableInfoManager.queryTableExt(schemaName, tableName, false);
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

                    if (locked) {
                        throw new RuntimeException("Table `" + tableName + "` has been locked by logical meta lock.");
                    }

                } catch (Throwable ex) {
                    throw GeneralUtil.nestedException(ex);
                }
            } finally {
                tableInfoManager.setConnection(null);
            }
*/
        }
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
            TableStatus.PUBLIC, 0);
        dual.setSchemaName(schemaName);
        return dual;
    }

    public void tonewversion(String tableName, long initWait, long interval, TimeUnit timeUnit) {
        tonewversion(tableName, true, initWait, interval, timeUnit);
    }

    public void tonewversion(String tableName) {
        tonewversion(tableName, false, null, null, null);
    }

    public void tonewversion(String tableName, boolean preemptive, Long initWait, Long interval, TimeUnit timeUnit) {
        synchronized (OptimizerContext.getContext(schemaName)) {
            GmsTableMetaManager oldSchemaManager =
                (GmsTableMetaManager) OptimizerContext.getContext(schemaName).getLatestSchemaManager();
            TableMeta currentMeta = oldSchemaManager.getTableWithNull(tableName);

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
            boolean isNewPartitionDb = DbInfoManager.getInstance().isNewPartitionDb(schemaName);

            if (version == -1) {
                if (!isNewPartitionDb) {
                    TableRuleManager.invalidate(schemaName, tableName);
                } else {
                    PartitionInfoManager.invalidate(schemaName, tableName);
                }
                String seqName = AUTO_SEQ_PREFIX + tableName;
                SequenceCacheManager.invalidate(schemaName, seqName);
            } else if (currentMeta != null && currentMeta.getVersion() >= version) {
                SQLRecorderLogger.ddlLogger.info(MessageFormat.format(
                    "{0}.{1} meta version change to {2} ignored, current version {3}", schemaName, tableName, version,
                    currentMeta.getVersion()));
                return;
            }

            SchemaManager newSchemaManager =
                new GmsTableMetaManager(oldSchemaManager, tableName, rule);
            newSchemaManager.init();

            String seqName = AUTO_SEQ_PREFIX + tableName;

            if (version != -1) {
                SequenceCacheManager.invalidate(schemaName, seqName);
                if (!isNewPartitionDb) {
                    TableRuleManager.reload(schemaName, tableName);
                }
            }
            OptimizerContext.getContext(schemaName).setSchemaManager(newSchemaManager);

            SQLRecorderLogger.ddlLogger.info(MessageFormat.format(
                "{0}.{1} meta version change to {2}", schemaName, tableName, version));

            if (currentMeta == null) {
                oldSchemaManager.expire();
                return;
            }

            // Lock and unlock MDL on primary table to clear cross status transaction.
            final MdlContext context;
            if (preemptive) {
                context = MdlManager.addContext(schemaName, initWait, interval, timeUnit);
            } else {
                context = MdlManager.addContext(schemaName, false);
            }
            SQLRecorderLogger.ddlLogger.warn(MessageFormat.format(
                "{0}  {1}.addContext({2})", Thread.currentThread().getName(),
                this.hashCode(), schemaName));

            try {
                final MdlTicket ticket;
                ticket = context.acquireLock(new MdlRequest(1L,
                    MdlKey
                        .getTableKeyWithLowerTableName(schemaName, currentMeta.getDigest()),
                    MdlType.MDL_EXCLUSIVE,
                    MdlDuration.MDL_TRANSACTION));

                oldSchemaManager.expire();

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

                SQLRecorderLogger.ddlLogger.warn(MessageFormat.format(
                    "[Mdl write lock acquired table[{0}]]",
                    tableName));
                context.releaseLock(1L, ticket);
            } finally {
                context.releaseAllTransactionalLocks();
                MdlManager.removeContext(context);
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
            hasPrimaryKey, TableStatus.convert(tableRecord.status), tableRecord.version);
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
        try (Connection metaDbConn = MetaDbUtil.getConnection()) {
            tableInfoManager.setConnection(metaDbConn);
            List<TablesRecord> tablesRecords = tableInfoManager.queryTables(schemaName);
            Map<String, List<ColumnsRecord>> allColumns = tableInfoManager.queryVisibleColumns(schemaName);
            Map<String, List<IndexesRecord>> allIndexes = tableInfoManager.queryVisibleIndexes(schemaName);
            Map<String, TablesExtRecord> extRecords = tableInfoManager.queryTableExts(schemaName).stream().collect(
                Collectors.toMap(TablesExtRecord::getTableName, r -> r));
            DataSource dataSource = MetaDbDataSource.getInstance().getDataSource();
            final GsiMetaManager gsiMetaManager =
                new GsiMetaManager(dataSource, schemaName);

            List<GsiMetaManager.IndexRecord> allIndexRecords = gsiMetaManager.getIndexRecords(schemaName);

            // tableName->List<IndexRecord>
            Map<String, List<GsiMetaManager.IndexRecord>> indexRecordsTableMap = new HashMap<>();

            // indexName->List<IndexRecord>
            Map<String, List<GsiMetaManager.IndexRecord>> indexRecordsIndexMap = new HashMap<>();

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
                    boolean isNewPartitionDb = DbInfoManager.getInstance().isNewPartitionDb(schemaName);

                    if (isNewPartitionDb) {
                        loadNewestPartitionInfo(schemaName, origTableName, origTableName, rule, tableInfoManager, meta);
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

    public static TableMeta fetchTableMeta(String schemaName,
                                           String logicalTableName,
                                           TddlRuleManager rule,
                                           boolean fetchPrimaryTableMetaOnly,
                                           boolean includeInvisiableInfo) {
        TableInfoManager tableInfoManager = new TableInfoManager();

        boolean locked = false;
        TableMeta meta = null;
        try (Connection metaDbConn = MetaDbUtil.getConnection()) {
            String origTableName = logicalTableName;

            tableInfoManager.setConnection(metaDbConn);

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
                        ComplexTaskMetaManager.getComplexTaskTableMetaBean(schemaName, origTableName));
                    boolean isNewPartDb = DbInfoManager.getInstance().isNewPartitionDb(schemaName);
                    if (isNewPartDb) {
                        loadNewestPartitionInfo(schemaName, logicalTableName, origTableName, rule, tableInfoManager,
                            meta);
                    }
                    // Get auto partition mark.
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
                            (meta.getPartitionInfo().getPartFlags() & TablePartitionRecord.FLAG_AUTO_PARTITION) != 0);
                        // Load lock flag.
                        locked = (meta.getPartitionInfo().getPartFlags() & TablePartitionRecord.FLAG_LOCK) != 0;
                    }
                }
            }
        } catch (Exception e) {
            logger.error("schema of " + logicalTableName + " cannot be fetched", e);
        } finally {
            tableInfoManager.setConnection(null);
        }

        if (locked) {
            throw new RuntimeException("Table `" + logicalTableName + "` has been locked by logical meta lock.");
        }
        return meta;
    }

    private static void loadNewestPartitionInfo(String schemaName,
                                                String logicalTableName,
                                                String origTableName,
                                                TddlRuleManager ruleMgr,
                                                TableInfoManager tblInfoMgr,
                                                TableMeta logTblMeta) {
        // init the partitionInfo firstly
        logTblMeta.initPartitionInfo(schemaName, logicalTableName, ruleMgr);
        // get the partitionInfo secondly
        PartitionInfo curPartitionInfo = ruleMgr.getPartitionInfoManager().getPartitionInfo(origTableName);
        // set the partitionInfo at the last step
        logTblMeta.setPartitionInfo(curPartitionInfo);

        if (!logTblMeta.getComplexTaskTableMetaBean().allPartIsPublic()) {
            if (curPartitionInfo != null) {
                PartitionInfo newPartitionInfo =
                    ruleMgr.getPartitionInfoManager().getPartitionInfoFromDeltaTable(origTableName);
                logTblMeta.initPartitionInfo(schemaName, logicalTableName, ruleMgr);
                if (logTblMeta.getComplexTaskTableMetaBean().isNeedSwitchDatasource()) {
                    curPartitionInfo = PartitionInfoUtil
                        .updatePartitionInfoByOutDatePartitionRecords(curPartitionInfo, tblInfoMgr);
                    newPartitionInfo = newPartitionInfo.copy();
                    logTblMeta.setNewPartitionInfo(curPartitionInfo);
                    logTblMeta.setPartitionInfo(newPartitionInfo);
                } else {
                    PartitionInfoUtil.updatePartitionInfoByNewCommingPartitionRecords(newPartitionInfo);
                    logTblMeta.setNewPartitionInfo(newPartitionInfo);
                    logTblMeta.setPartitionInfo(curPartitionInfo);
                }
            }
        }
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
            hasPrimaryKey, TableStatus.PUBLIC, 0);

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
