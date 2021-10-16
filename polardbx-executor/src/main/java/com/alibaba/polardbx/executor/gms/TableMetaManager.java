/*package com.alibaba.polardbx.executor.gms;

import com.google.common.collect.Lists;
import CharsetName;
import CollationName;
import TddlRuntimeException;
import ErrorCode;
import CaseInsensitive;
import GeneralUtil;
import TStringUtil;
import Logger;
import LoggerFactory;
import com.alibaba.polardbx.executor.common.ExecutorContext;
import com.alibaba.polardbx.executor.ddl.engine.AsyncDDLManager;
import MetaDbDataSource;
import ColumnsRecord;
import IndexStatus;
import IndexesRecord;
import TableInfoManager;
import TableStatus;
import TablesExtRecord;
import TablesRecord;
import PartitionGroupRecord;
import TableGroupOutlineRecord;
import MetaDbUtil;
import OptimizerContext;
import ColumnMeta;
import Field;
import GsiMetaManager;
import IndexMeta;
import IndexType;
import Relationship;
import RepoSchemaManager;
import ScaleOutPlanUtil;
import TableGroupTaskMetaManager;
import TableMeta;
import DataTypeUtil;
import PartitionInfo;
import PartitionSpec;
import AlterTableGroupSnapShotUtils;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.type.SqlTypeUtil;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;

public class TableMetaManager extends RepoSchemaManager {

    private final static Logger LOGGER = LoggerFactory.getLogger(TableMetaManager.class);
    private final String schemaName;
    private final String appName;

    public TableMetaManager(String schemaName, String appName) {
        this.schemaName = schemaName;
        this.appName = appName;
    }

    @Override
    protected void doInit() {
        super.doInit();
    }

    @Override
    public TableMeta getTable0(String logicalTableName, String actualTableName) {
        checkOngoingDDL(logicalTableName, null);
        TableMeta ts = fetchTableMeta(this.schemaName, this.appName, logicalTableName, false);
        return ts;
    }

    @Override
    public void checkOngoingDDL(String logicalTableName, TableMeta tableMeta) {
        boolean objectHidden = Optional.ofNullable(tableMeta)
            .map(meta -> AsyncDDLManager.getInstance().isObjectHidden(schemaName, schemaName, logicalTableName, meta))
            .orElse(AsyncDDLManager.getInstance().isObjectHiddenForTableMeta(schemaName, schemaName, logicalTableName));
        if (objectHidden) {
            throw new TddlRuntimeException(ErrorCode.ERR_UNKNOWN_TABLE, schemaName, logicalTableName);
        }
    }

    public static TableMeta fetchTableMeta(String schemaName, String appName, String logicalTableName,
                                           boolean fetchPrimaryTableMetaOnly) {
        TableInfoManager tableInfoManager = new TableInfoManager();

        boolean locked = false;
        TableMeta meta = null;
        try (Connection metaDbConn = MetaDbUtil.getConnection()) {
            String origTableName = logicalTableName;

            tableInfoManager.setConnection(metaDbConn);

            TablesRecord tableRecord = tableInfoManager.queryVisibleTable(schemaName, logicalTableName, false);

            if (tableRecord == null) {
                // Check if there is an ongoing RENAME TABLE operation, so search with new table name.
                tableRecord = tableInfoManager.queryVisibleTable(schemaName, logicalTableName, true);

                // Use original table name to find column and index meta.
                if (tableRecord != null) {
                    origTableName = tableRecord.tableName;
                }
            }

            if (tableRecord != null) {
                List<ColumnsRecord> columnsRecords = tableInfoManager.queryVisibleColumns(schemaName, origTableName);
                List<IndexesRecord> indexesRecords = tableInfoManager.queryVisibleIndexes(schemaName, origTableName);
                meta = buildTableMeta(tableRecord, columnsRecords, indexesRecords, logicalTableName);

                if (meta != null && !fetchPrimaryTableMetaOnly) {

                    meta.setSchemaName(schemaName);
                    DataSource dataSource = MetaDbDataSource.getInstance().getDataSource();
                    final boolean lowerCaseTableNames =
                        ExecutorContext.getContext(schemaName).getStorageInfoManager().isLowerCaseTableNames();
                    final GsiMetaManager gsiMetaManager =
                        new GsiMetaManager(dataSource, appName, schemaName, lowerCaseTableNames);
                    meta.setGsiTableMetaBean(
                        gsiMetaManager.getTableMeta(origTableName, IndexStatus.ALL));
                    meta.setScaleOutTableMetaBean(ScaleOutPlanUtil.getScaleOutTableMetaBean(schemaName, origTableName));

                    PartitionInfo curPartitionInfo =
                        OptimizerContext.getContext(schemaName).getPartitionInfoManager()
                            .getPartitionInfo(origTableName);
                    meta.initPartitionInfo(logicalTableName);
                    meta.setPartitionInfo(curPartitionInfo);

                    final TableGroupOutlineRecord tableGroupOutlineRecord =
                        TableGroupTaskMetaManager.getInstance()
                            .getTableGroupOutlineBySchemaAndTable(schemaName, origTableName);

                    meta.setTableGroupOutlineRecord(tableGroupOutlineRecord);
                    if (tableGroupOutlineRecord != null
                        && tableGroupOutlineRecord.getStatus() != TableGroupTaskMetaManager.TableGroupTaskStatus.PUBLIC
                        .getValue()) {
                        final TableGroupOutlineRecord parentTableGroupOutlineRecord =
                            TableGroupTaskMetaManager.getInstance()
                                .getTableGroupOutlineByJobId(tableGroupOutlineRecord.getParent_job_id());
                        if (curPartitionInfo != null) {
                            PartitionInfo newPartitionInfo =
                                OptimizerContext.getContext(schemaName).getPartitionInfoManager()
                                    .getPartitionInfoFromDeltaTable(origTableName);
                            meta.initPartitionInfo(logicalTableName);
                            if (TableGroupTaskMetaManager.TableGroupTaskStatus.from(tableGroupOutlineRecord.getStatus())
                                .isNeedSwitchDatasource()) {
                                List<PartitionGroupRecord> outdatedPartitionRecords =
                                    AlterTableGroupSnapShotUtils
                                        .getOutDatedPartitionGroupRecordsByJsonString(curPartitionInfo,
                                        parentTableGroupOutlineRecord.getPartition_info());
                                curPartitionInfo = curPartitionInfo.copy();
                                for (PartitionGroupRecord partitionGroupRecord : outdatedPartitionRecords) {
                                    PartitionSpec spec =
                                        curPartitionInfo.getPartitionBy().getPartitions().stream().filter(
                                            o -> o.getLocation().getPartitionGroupId().longValue()
                                                == partitionGroupRecord.id
                                                .longValue()).findFirst().orElse(null);
                                    assert spec != null;
                                    spec.getLocation().setVisiable(false);
                                }
                                newPartitionInfo = newPartitionInfo.copy();
                                meta.setNewPartitionInfo(curPartitionInfo);
                                meta.setPartitionInfo(newPartitionInfo);
                            } else {
                                //set visible property for newPartitionInfo here
                                List<PartitionGroupRecord> newComingPartitionRecords =
                                    AlterTableGroupSnapShotUtils
                                        .getNewComingPartitionGroupRecordsByJsonString(curPartitionInfo,
                                            parentTableGroupOutlineRecord.getPartition_info());
                                for (PartitionGroupRecord partitionGroupRecord : newComingPartitionRecords) {
                                    PartitionSpec spec =
                                        newPartitionInfo.getPartitionBy().getPartitions().stream().filter(
                                            o -> o.getLocation().getPartitionGroupId().longValue()
                                                == partitionGroupRecord.id
                                                .longValue()).findFirst().orElse(null);
                                    assert spec != null;
                                    spec.getLocation().setVisiable(false);
                                }
                                meta.setNewPartitionInfo(newPartitionInfo);
                                meta.setPartitionInfo(curPartitionInfo);
                            }
                        }
                    }

                    // Get auto partition mark.
                    final TablesExtRecord extRecord = tableInfoManager.queryTableExt(schemaName, origTableName, false);
                    if (extRecord != null) {
                        meta.setAutoPartition(extRecord.isAutoPartition());
                        // Load lock flag.
                        locked = extRecord.isLocked();
                    }
                }
            }
        } catch (Exception e) {
            LOGGER.error("schema of " + logicalTableName + " cannot be fetched", e);
        } finally {
            tableInfoManager.setConnection(null);
        }

        if (locked) {
            throw new RuntimeException("Table `" + logicalTableName + "` has been locked by logical meta lock.");
        }
        return meta;
    }

    @Override
    public TableMeta getTableMetaFromConnection(String tableName, Connection conn) {
        try {
            return fetchTableMeta(this.schemaName, this.appName, tableName, false);
        } catch (Exception e) {
            return null;
        } finally {
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                    LOGGER.warn("", e);
                }
            }
        }
    }

    public static TableMeta buildTableMeta(TablesRecord tableRecord, List<ColumnsRecord> columnsRecords,
                                           List<IndexesRecord> indexesRecords,
                                           String tableName) {
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
                long length = record.fieldLength;
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
                            meta.values = primaryKeys;
                            meta.unique = record.nonUnique == 0;
                            localIndexMetaMap.put(indexName, meta);
                        }
                        meta.keys.add(record.columnName);
                    }
                    for (SecondaryIndexMeta meta : localIndexMetaMap.values()) {
                        secondaryIndexMetas.add(convertFromSecondaryIndexMeta(meta, columnMetaMap, tableName, true));
                    }
                }
            } catch (Exception ex) {
                throw ex;
            }
        } catch (Exception ex) {
            LOGGER.error("fetch schema error", ex);
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
            hasPrimaryKey);
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

    private static class SecondaryIndexMeta {
        String name;
        Boolean unique;
        List<String> keys;
        List<String> values;
    }

    private static IndexMeta convertFromSecondaryIndexMeta(SecondaryIndexMeta secondaryIndexMeta,
                                                           Map<String, ColumnMeta> columnMetas, String tableName,
                                                           boolean strongConsistent) {

        return new IndexMeta(tableName,
            toColumnMeta(secondaryIndexMeta.keys, columnMetas, tableName),
            toColumnMeta(secondaryIndexMeta.values, columnMetas, tableName),
            IndexType.BTREE,
            Relationship.NONE,
            strongConsistent,
            false,
            secondaryIndexMeta.unique,
            secondaryIndexMeta.name);
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

    @Override
    public GsiMetaManager.GsiMetaBean getGsi(String primaryOrIndexTableName,
                                             EnumSet<IndexStatus> statusSet) {
        DataSource dataSource = MetaDbDataSource.getInstance().getDataSource();
        final boolean lowerCaseTableNames =
            ExecutorContext.getContext(schemaName).getStorageInfoManager().isLowerCaseTableNames();
        final GsiMetaManager gsiMetaManager = new GsiMetaManager(dataSource, appName, schemaName, lowerCaseTableNames);
        return gsiMetaManager.getTableAndIndexMeta(primaryOrIndexTableName, statusSet);
    }

}
*/
