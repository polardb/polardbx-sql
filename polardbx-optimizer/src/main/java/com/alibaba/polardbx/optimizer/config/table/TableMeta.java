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

package com.alibaba.polardbx.optimizer.config.table;

import com.alibaba.polardbx.optimizer.config.table.statistic.StatisticManager;
import com.alibaba.polardbx.optimizer.partition.LocalPartitionDefinitionInfo;
import com.alibaba.polardbx.optimizer.config.table.statistic.StatisticResult;
import com.alibaba.polardbx.gms.metadb.table.ColumnStatus;
import com.alibaba.polardbx.optimizer.tablegroup.TableGroupVersionManager;
import com.alibaba.polardbx.optimizer.utils.SchemaVersionManager;
import com.google.common.collect.ImmutableList;
import com.alibaba.polardbx.common.Engine;
import com.alibaba.polardbx.common.TddlConstants;
import com.alibaba.polardbx.common.utils.CaseInsensitive;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.gms.metadb.table.TableStatus;
import com.alibaba.polardbx.gms.metadb.table.TablesRecord;
import com.alibaba.polardbx.gms.partition.TablePartitionRecord;
import com.alibaba.polardbx.gms.tablegroup.ComplexTaskOutlineRecord;
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.schema.MetaDbSchema;
import com.alibaba.polardbx.optimizer.config.table.GsiMetaManager.GsiIndexMetaBean;
import com.alibaba.polardbx.optimizer.core.TddlOperatorTable;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypeUtil;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.partition.LocalPartitionDefinitionInfo;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.rule.TddlRuleManager;
import com.alibaba.polardbx.optimizer.sql.sql2rel.TddlSqlToRelConverter;
import com.alibaba.polardbx.optimizer.utils.CalciteUtils;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.taobao.tddl.common.utils.TddlToStringStyle;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.schema.Statistics;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.Wrapper;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql2rel.InitializerContext;
import org.apache.calcite.sql2rel.InitializerExpressionFactory;
import org.apache.calcite.sql2rel.NullInitializerExpressionFactory;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.NlsString;
import org.apache.commons.lang.builder.ToStringBuilder;

import java.io.Serializable;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Collectors;

/**
 * 一个table的描述，包含主键信息/字段信息/索引信息等，暂时不考虑外键/约束键，目前没意义
 *
 * @author whisper
 */
public class TableMeta implements Serializable, Cloneable, Table, Wrapper {

    private static final long serialVersionUID = 5168519373619656091L;
    private String digest;
    /**
     * the table group version of table
     */
    private List<String> tableGroupDigest = null;

    /**
     * the schema version of table
     */
    private List<String> schemaDigest = null;

    // id in metadb
    private long id;

    private String schemaName = null;

    /**
     * 表名
     */
    private final String tableName;

    private final TableStatus status;

    private final long version;

    private Engine engine;

    private final long flag;

    /**
     * 主键索引描述
     */
    private final Map<String, IndexMeta> primaryIndexes = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);

    /**
     * 二级索引描述
     */
    private final Map<String, IndexMeta> secondaryIndexes =
        new TreeMap<>(String.CASE_INSENSITIVE_ORDER);

    private final Map<String, ColumnMeta> primaryKeys =
        new TreeMap<>(CaseInsensitive.CASE_INSENSITIVE_ORDER);
    private final Map<String, ColumnMeta> columns =
        new TreeMap<>(CaseInsensitive.CASE_INSENSITIVE_ORDER);
    private final Map<String, ColumnMeta> allColumns =
        new TreeMap<>(CaseInsensitive.CASE_INSENSITIVE_ORDER);
    private final List<ColumnMeta> allColumnsOrderByDefined = new ArrayList<>();

    private boolean hasPrimaryKey = true;

    private TableColumnMeta tableColumnMeta = null;

    private List<ColumnMeta> autoUpdateColumns = null;
    private GsiMetaManager.GsiTableMetaBean gsiTableMetaBean = null;
    private Map<String, GsiIndexMetaBean> gsiPublished = null;

    private ComplexTaskOutlineRecord complexTaskOutlineRecord = null;
    private ComplexTaskMetaManager.ComplexTaskTableMetaBean complexTaskTableMetaBean = null;

    private final InitializerExpressionFactory initializerExpressionFactory =
        new TableMetaInitializerExpressionFactory();

    private volatile boolean isAutoPartition = false;

    private volatile PartitionInfo partitionInfo = null;

    // when split/merge/move the table, this entry will save the new partitionInfo temporarily
    private volatile PartitionInfo newPartitionInfo = null;

    // for oss engine
    private Map<String, Map<String, List<FileMeta>>> fileMetaSet = null;
    private Map<String, List<FileMeta>> flatFileMetas = null;

    private volatile TableFilesMeta tableFilesMeta = null;

    private volatile LocalPartitionDefinitionInfo localPartitionDefinitionInfo;

    public TableMeta(String schemaName, String tableName, List<ColumnMeta> allColumnsOrderByDefined,
                     IndexMeta primaryIndex,
                     List<IndexMeta> secondaryIndexes, boolean hasPrimaryKey, TableStatus status, long version,
                     long flag) {
        this.schemaName = schemaName;
        this.tableName = tableName;
        this.hasPrimaryKey = hasPrimaryKey;
        if (hasPrimaryKey && primaryIndex != null) {
            this.primaryIndexes.put(primaryIndex.getPhysicalIndexName(), primaryIndex);
            for (ColumnMeta c : primaryIndex.getKeyColumns()) {
                c.getField().setPrimary(true);
                this.primaryKeys.put(c.getName(), c);
            }
        }

        if (secondaryIndexes != null) {
            for (IndexMeta one : secondaryIndexes) {
                this.secondaryIndexes.put(one.getPhysicalIndexName(), one);
            }
        }

        for (ColumnMeta column : allColumnsOrderByDefined) {
            this.allColumns.put(column.getName(), column);
        }
        this.allColumnsOrderByDefined.addAll(allColumnsOrderByDefined);
        this.status = status;
        this.version = version;
        this.flag = flag;
        this.digest = tableName + "#version:" + version;
    }

    public Engine getEngine() {
        return engine;
    }

    public void setEngine(Engine engine) {
        this.engine = engine;
    }

    public long getId() {
        return this.id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public TableStatus getStatus() {
        return this.status;
    }

    public long getVersion() {
        return version;
    }

    public long getFlag() {
        return flag;
    }

    public boolean requireLogicalColumnOrder() {
        return (flag & TablesRecord.FLAG_LOGICAL_COLUMN_ORDER) != 0L;
    }

    public String getSchemaName() {
        return schemaName;
    }

    public void setSchemaName(String schemaName) {
        this.digest = schemaName + "." + tableName + "#version:" + version;
        this.schemaName = schemaName;
    }

    public IndexMeta getPrimaryIndex() {
        if (!hasPrimaryKey) {
            return null;
        }
        return primaryIndexes.isEmpty() ? null : primaryIndexes.values().iterator().next();
    }

    private boolean indexContainsMultiWriteTargetColumn(IndexMeta indexMeta, ColumnMeta multiWriteTargetColumnMeta) {
        if (multiWriteTargetColumnMeta == null) {
            return false;
        }
        return indexMeta.getKeyColumns().stream()
            .anyMatch(cm -> cm.getName().equalsIgnoreCase(multiWriteTargetColumnMeta.getName()));
    }

    public List<IndexMeta> getSecondaryIndexes() {
        ColumnMeta multiWriteTargetColumnMeta = getColumnMultiWriteTargetColumnMeta();
        return secondaryIndexes.values().stream()
            .filter(im -> !indexContainsMultiWriteTargetColumn(im, multiWriteTargetColumnMeta))
            .collect(Collectors.toList());
    }

    public List<IndexMeta> getUniqueIndexes(boolean includingPrimaryIndex) {
        ArrayList<IndexMeta> uniqueIndexes = new ArrayList<>();

        if (hasPrimaryKey && includingPrimaryIndex) {
            uniqueIndexes.add(getPrimaryIndex());
        }

        ColumnMeta multiWriteTargetColumnMeta = getColumnMultiWriteTargetColumnMeta();
        for (IndexMeta indexMeta : getSecondaryIndexes()) {
            if (!indexMeta.isPrimaryKeyIndex() && indexMeta.isUniqueIndex() && !indexContainsMultiWriteTargetColumn(
                indexMeta, multiWriteTargetColumnMeta)) {
                uniqueIndexes.add(indexMeta);
            }
        }
        return uniqueIndexes;
    }

    public Map<String, IndexMeta> getSecondaryIndexesMap() {
        Map<String, IndexMeta> result = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        ColumnMeta multiWriteTargetColumnMeta = getColumnMultiWriteTargetColumnMeta();
        for (Entry<String, IndexMeta> entry : secondaryIndexes.entrySet()) {
            if (!indexContainsMultiWriteTargetColumn(entry.getValue(), multiWriteTargetColumnMeta)) {
                result.put(entry.getKey(), entry.getValue());
            }
        }
        return result;
    }

    public Collection<ColumnMeta> getPrimaryKey() {
        return primaryKeys.values();
    }

    public Collection<ColumnMeta> getGsiImplicitPrimaryKey() {
        final IndexMeta pk = secondaryIndexes.get(TddlConstants.UGSI_PK_INDEX_NAME);
        if (null == pk) {
            return new ArrayList<>();
        }
        return pk.getKeyColumns();
    }

    public Collection<ColumnMeta> getColumns() {
        return columns.values();
    }

    public Map<String, ColumnMeta> getPrimaryKeyMap() {
        return this.primaryKeys;
    }

    // Get all column ignore the status.
    public List<ColumnMeta> getPhysicalColumns() {
        return allColumnsOrderByDefined;
    }

    public List<ColumnMeta> getAllColumns() {   //兼容以前
        return allColumnsOrderByDefined.stream().filter(column -> column.getStatus() == ColumnStatus.PUBLIC
            || column.getStatus() == ColumnStatus.MULTI_WRITE_SOURCE).collect(Collectors.toList());
    }

    public List<ColumnMeta> getWriteColumns() {  //可写的columns
        return allColumnsOrderByDefined.stream()
            .filter(column -> column.getStatus() == ColumnStatus.PUBLIC
                || column.getStatus() == ColumnStatus.MULTI_WRITE_SOURCE
                || column.getStatus() == ColumnStatus.WRITE_ONLY || column.getStatus() == ColumnStatus.WRITE_REORG)
            .collect(Collectors.toList());
    }

    public List<ColumnMeta> getReadColumns() {   //可读的columns
        return allColumnsOrderByDefined.stream()
            .filter(column -> column.getStatus() == ColumnStatus.PUBLIC
                || column.getStatus() == ColumnStatus.MULTI_WRITE_SOURCE)
            .collect(Collectors.toList());
    }

    public ColumnMeta getColumnMultiWriteSourceColumnMeta() {
        for (ColumnMeta columnMeta : allColumnsOrderByDefined) {
            if (columnMeta.getStatus() == ColumnStatus.MULTI_WRITE_SOURCE) {
                return columnMeta;
            }
        }
        return null;
    }

    public ColumnMeta getColumnMultiWriteTargetColumnMeta() {
        for (ColumnMeta columnMeta : allColumnsOrderByDefined) {
            if (columnMeta.getStatus() == ColumnStatus.MULTI_WRITE_TARGET) {
                return columnMeta;
            }
        }
        return null;
    }

    public IndexMeta getIndexMeta(String indexName) {
        IndexMeta retMeta = primaryIndexes.get(indexName);
        if (retMeta != null) {
            return retMeta;
        }
        retMeta = secondaryIndexes.get(indexName);
        return retMeta;
    }

    public List<IndexMeta> getIndexes() {
        List<IndexMeta> indexes = new ArrayList<IndexMeta>();
        IndexMeta index = this.getPrimaryIndex();
        if (index != null) {
            indexes.add(this.getPrimaryIndex());
        }
        indexes.addAll(this.getSecondaryIndexes());
        return indexes;
    }

    public Set<String> getLocalIndexNames() {
        Set<String> indexes = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        indexes.addAll(
            this.getAllIndexes().stream().map(IndexMeta::getPhysicalIndexName).collect(Collectors.toList()));
        return indexes;
    }

    public List<IndexMeta> getAllIndexes() {
        List<IndexMeta> indexes = new ArrayList<IndexMeta>();
        IndexMeta index = this.getPrimaryIndex();
        if (index != null) {
            indexes.add(this.getPrimaryIndex());
        }
        indexes.addAll(secondaryIndexes.values());
        return indexes;
    }

    public ColumnMeta getColumn(String name) {
        if (name.contains(".")) {
            return allColumns.get(name.split("\\.")[1]); // 避免转义
        }
        return allColumns.get(name);
    }

    public ColumnMeta getColumnIgnoreCase(String name) {
        if (name.contains(".")) {
            name = name.split("\\.")[1]; // 避免转义
        }
        for (Entry<String, ColumnMeta> column : allColumns.entrySet()) {
            if (column.getKey().equalsIgnoreCase(name)) {
                return column.getValue();
            }
        }
        return null;
    }

    @Override
    public String toString() {
        return ToStringBuilder.reflectionToString(this, TddlToStringStyle.DEFAULT_STYLE);
    }

    public boolean isHasPrimaryKey() {
        return hasPrimaryKey;
    }

    public boolean hasGsiImplicitPrimaryKey() {
        return isGsi() && secondaryIndexes.containsKey(TddlConstants.UGSI_PK_INDEX_NAME);
    }

    public void setHasPrimaryKey(boolean hasPrimaryKey) {
        this.hasPrimaryKey = hasPrimaryKey;
    }

    public boolean isAutoPartition() {
        return isAutoPartition;
    }

    public void setAutoPartition(boolean autoPartition) {
        isAutoPartition = autoPartition;
    }

    public ColumnMeta getAutoIncrementColumn() {
        for (ColumnMeta column : getAllColumns()) {
            if (column.isAutoIncrement()) {
                return column;
            }
        }
        return null;
    }

    public double getRowCount() {
        if (MetaDbSchema.NAME.equalsIgnoreCase(schemaName)) {
            return 100;
        }
        if (ConfigDataMode.isFastMock()) {
            return 10;
        }
        StatisticResult statisticResult = StatisticManager.getInstance().getRowCount(schemaName, tableName);
        long rowCount = statisticResult.getLongValue();
        return rowCount <= 0 ? 1 : rowCount;
    }

    public RelDataTypeField getRowTypeIgnoreCase(String colName, RelDataTypeFactory typeFactory) {
        if (colName.contains(".")) {
            colName = colName.split("\\.")[1]; // 避免转义
        }
        for (int i = 0; i < allColumnsOrderByDefined.size(); i++) {
            ColumnMeta columnMeta = allColumnsOrderByDefined.get(i);
            if (colName.equalsIgnoreCase(columnMeta.getName())) {
                RelDataType relDataType = columnMeta.getField().getRelType();
                return new RelDataTypeFieldImpl(columnMeta.getName(), i, relDataType);
            }
        }
        return null;
    }

    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        return CalciteUtils.switchRowType(getAllColumns(), typeFactory);
    }

    @Override
    public Statistic getStatistic() {
        return Statistics.of(getRowCount(), ImmutableList.<ImmutableBitSet>of());
    }

    @Override
    public Schema.TableType getJdbcTableType() {
        return Schema.TableType.TABLE;
    }

    @Override
    public boolean isRolledUp(String column) {
        return false;
    }

    @Override
    public boolean rolledUpColumnValidInsideAgg(String column, SqlCall call, SqlNode parent,
                                                CalciteConnectionConfig config) {
        return false;
    }

    public String getTableName() {
        return tableName;
    }

    public List<String> getAutoIncrementColumns() {
        List<String> autoIncrementColumns = new ArrayList<>();
        for (Entry<String, ColumnMeta> entry : allColumns.entrySet()) {
            ColumnMeta meta = entry.getValue();
            if (meta.isAutoIncrement()) {
                autoIncrementColumns.add(entry.getKey());
            }
        }
        return autoIncrementColumns;
    }

    public List<ColumnMeta> getAutoUpdateColumns() {
        if (autoUpdateColumns == null) {
            synchronized (this) {
                if (autoUpdateColumns == null) {
                    autoUpdateColumns = new ArrayList<>();
                    for (ColumnMeta column : allColumnsOrderByDefined) {
                        if (TStringUtil.containsIgnoreCase(column.getField().getExtra(), "on update")) {
                            autoUpdateColumns.add(column);
                        }
                    }
                }
            }
        }
        return autoUpdateColumns;
    }

    public TableColumnMeta getTableColumnMeta() {
        return tableColumnMeta;
    }

    public void setTableColumnMeta(TableColumnMeta tableColumnMeta) {
        this.tableColumnMeta = tableColumnMeta;
    }

    public GsiMetaManager.GsiTableMetaBean getGsiTableMetaBean() {
        return gsiTableMetaBean;
    }

    public Map<String, GsiIndexMetaBean> getGsiPublished() {
        return gsiPublished;
    }

    public void setGsiTableMetaBean(GsiMetaManager.GsiTableMetaBean gsiTableMetaBean) {
        this.gsiTableMetaBean = gsiTableMetaBean;
        if (null != gsiTableMetaBean && gsiTableMetaBean.tableType.isPrimary()) {
            this.gsiPublished = gsiTableMetaBean.indexMap.entrySet()
                .stream()
                .filter(e -> e.getValue().indexStatus.isPublished())
                .collect(Collectors.toMap(Entry::getKey, Entry::getValue));
        }
    }

    public boolean withGsi() {
        return null != getGsiTableMetaBean() && getGsiTableMetaBean().tableType != GsiMetaManager.TableType.GSI
            && GeneralUtil.isNotEmpty(getGsiTableMetaBean().indexMap);
    }

    public boolean hasGsi(String indexName) {
        List<String> gsiNames = new ArrayList<>();
        getGsiTableMetaBean().indexMap.forEach((key, value) -> {
            gsiNames.add(TddlSqlToRelConverter.unwrapGsiName(key));
        });
        return gsiNames.contains(indexName);
    }

    public boolean withGsi(String indexName) {
        return withGsi() && hasGsi(indexName);
    }

    public boolean hasGsiIgnoreCase(String indexName) {
        Set<String> gsiNames = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        getGsiTableMetaBean().indexMap.forEach((key, value) -> {
            gsiNames.add(TddlSqlToRelConverter.unwrapGsiName(key));
        });
        return gsiNames.contains(indexName);
    }

    public boolean withClustered() {
        return withGsi() && getGsiTableMetaBean().indexMap.values().stream().anyMatch(bean -> bean.clusteredIndex);
    }

    public boolean withPublishedGsi() {
        return GeneralUtil.isNotEmpty(this.gsiPublished);
    }

    public boolean withPublishedGsi(String index) {
        return withPublishedGsi() && getGsiPublished().containsKey(index);
    }

    public boolean isGsi() {
        if (partitionInfo != null && partitionInfo.isGsi()) {
            return true;
        }
        return null != getGsiTableMetaBean() && getGsiTableMetaBean().tableType == GsiMetaManager.TableType.GSI;
    }

    public boolean isClustered() {
        return isGsi() && getGsiTableMetaBean().gsiMetaBean.clusteredIndex;
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof TableMeta) {
            return tableName.equals(((TableMeta) o).getTableName())
                && primaryIndexes.size() == ((TableMeta) o).primaryIndexes.size()
                && secondaryIndexes.size() == ((TableMeta) o).secondaryIndexes.size()
                && primaryKeys.size() == ((TableMeta) o).primaryKeys.size()
                && columns.size() == ((TableMeta) o).columns.size() && allColumns.size() == ((TableMeta) o).allColumns
                .size()
                && allColumnsOrderByDefined.size() == ((TableMeta) o).allColumnsOrderByDefined.size()
                && hasPrimaryKey == ((TableMeta) o).hasPrimaryKey
                && autoUpdateColumns == ((TableMeta) o).autoUpdateColumns
                && gsiTableMetaBean == ((TableMeta) o).gsiTableMetaBean
                && status == ((TableMeta) o).status
                && version == ((TableMeta) o).version
                && isAutoPartition == ((TableMeta) o).isAutoPartition;
        }
        return false;
    }

    public ComplexTaskOutlineRecord getComplexTaskOutlineRecord() {
        return complexTaskOutlineRecord;
    }

    public void setComplexTaskOutlineRecord(ComplexTaskOutlineRecord complexTaskOutlineRecord) {
        this.complexTaskOutlineRecord = complexTaskOutlineRecord;
    }

    public ComplexTaskMetaManager.ComplexTaskTableMetaBean getComplexTaskTableMetaBean() {
        return complexTaskTableMetaBean;
    }

    public void setComplexTaskTableMetaBean(
        ComplexTaskMetaManager.ComplexTaskTableMetaBean complexTaskTableMetaBean) {
        this.complexTaskTableMetaBean = complexTaskTableMetaBean;
    }

    public PartitionInfo getNewPartitionInfo() {
        return newPartitionInfo;
    }

    public void setNewPartitionInfo(PartitionInfo newPartitionInfo) {
        this.newPartitionInfo = newPartitionInfo;
    }

    public PartitionInfo getPartitionInfo() {
        return partitionInfo;
    }

    public void setPartitionInfo(PartitionInfo partitionInfo) {
        this.partitionInfo = partitionInfo;
    }

    public LocalPartitionDefinitionInfo getLocalPartitionDefinitionInfo() {
        return this.localPartitionDefinitionInfo;
    }

    public void setLocalPartitionDefinitionInfo(final LocalPartitionDefinitionInfo localPartitionDefinitionInfo) {
        this.localPartitionDefinitionInfo = localPartitionDefinitionInfo;
    }

    public String getDigest() {
        return this.digest;
    }

    public String getTableGroupDigest(Long trxId) {
        return this.tableGroupDigest.get((int) (trxId % TableGroupVersionManager.segmentLockSize));
    }

    public String getSchemaDigest(Long trxId) {
        return this.schemaDigest.get((int) (trxId % SchemaVersionManager.segmentLockSize));
    }

    public List<String> getTableGroupDigestList() {
        return this.tableGroupDigest;
    }

    public void setTableGroupDigestList(List<String> tableGroupDigest) {
        this.tableGroupDigest = tableGroupDigest;
    }

    public List<String> getSchemaDigestList() {
        return this.schemaDigest;
    }

    public void setSchemaDigestList(List<String> schemaDigest) {
        this.schemaDigest = schemaDigest;
    }

    @Override
    public <C> C unwrap(Class<C> aClass) {
        if (aClass.isInstance(initializerExpressionFactory)) {
            return aClass.cast(initializerExpressionFactory);
        } else if (aClass.isInstance(this)) {
            return aClass.cast(this);
        }
        return null;
    }

    private class TableMetaInitializerExpressionFactory extends NullInitializerExpressionFactory {
        @Override
        public RexNode newColumnDefaultValue(RelOptTable table, int iColumn, InitializerContext context) {
            final RelDataTypeField relDataTypeField = table.getRowType().getFieldList().get(iColumn);
            final String columnName = relDataTypeField.getName();
            final Field field = getColumn(columnName).getField();

            final DataType columnDataType = field.getDataType();
            final String columnDefaultStr = field.getDefault();
            final String columnExtraStr = field.getExtra();

            final RexBuilder rexBuilder = context.getRexBuilder();

            if (null == columnDefaultStr) {
                if (!field.isNullable()) {
                    // Column has no default value
                    return null;
                } else {
                    // Default value is NULL
                    return rexBuilder.makeLiteral(columnDefaultStr, relDataTypeField.getType(), false);
                }
            }

            if (TStringUtil.containsIgnoreCase(columnDefaultStr, "CURRENT_TIMESTAMP")) {
                // Let MySQL handle time precision
                return rexBuilder.makeCall(TddlOperatorTable.CURRENT_TIMESTAMP, ImmutableList.of());
            }

            if (DataTypeUtil.isNumberSqlType(columnDataType)) {
                final Object converted = columnDataType.convertFrom(columnDefaultStr);
                return rexBuilder.makeLiteral(converted, relDataTypeField.getType(), false);
            }

            if (DataTypeUtil.isStringType(columnDataType)) {
                return rexBuilder.makeLiteral(columnDefaultStr, relDataTypeField.getType(), false);
            }

            if (DataTypeUtil.isDateType(columnDataType) || DataTypeUtil
                .equalsSemantically(columnDataType, DataTypes.BinaryType)) {
                final NlsString valueStr = new NlsString(columnDefaultStr, null, null);
                return rexBuilder.makeCharLiteral(valueStr);
            }

            // Return null for unsupported data type
            return null;
        }

        @Override
        public RexNode newImplicitDefaultValue(RelOptTable table, int iColumn, InitializerContext context) {
            final RelDataTypeField relDataTypeField = table.getRowType().getFieldList().get(iColumn);
            final String columnName = relDataTypeField.getName();
            final Field field = getColumn(columnName).getField();
            final DataType columnDataType = field.getDataType();

            final RexBuilder rexBuilder = context.getRexBuilder();

            if (DataTypeUtil.isNumberSqlType(columnDataType)) {
                final Object converted = columnDataType.convertFrom("0");
                return rexBuilder.makeLiteral(converted, relDataTypeField.getType(), false);
            }

            if (DataTypeUtil.isStringType(columnDataType)) {
                return rexBuilder.makeLiteral("", relDataTypeField.getType(), false);
            }

            return super.newImplicitDefaultValue(table, iColumn, context);
        }
    }

    public void initPartitionInfo(String schemaName, String tableName, TddlRuleManager rule) {
        initPartitionInfo(null, schemaName, tableName, rule);
    }

    public void initPartitionInfo(Connection conn, String schemaName, String tableName, TddlRuleManager rule) {
        rule.getPartitionInfoManager().reloadPartitionInfo(conn, schemaName, tableName);
        this.partitionInfo = rule.getPartitionInfoManager().getPartitionInfo(tableName);
    }

    public Map<String, Map<String, List<FileMeta>>> getFileMetaSet() {
        return fileMetaSet;
    }

    public void setFileMetaSet(Map<String, Map<String, List<FileMeta>>> fileMetaSet) {
        // only for file-store engine table
        Preconditions.checkArgument(Engine.isFileStore(this.getEngine()));
        this.fileMetaSet = fileMetaSet;
        // build flat map for physical table - file meta
        Map<String, List<FileMeta>> flatFileMetas = new HashMap<>();
        for (Entry<String, Map<String, List<FileMeta>>> phySchemas : this.fileMetaSet.entrySet()) {
            for (Entry<String, List<FileMeta>> phyTables : phySchemas.getValue().entrySet()) {
                flatFileMetas.put(phyTables.getKey(), phyTables.getValue());
            }
        }
        this.flatFileMetas = flatFileMetas;
    }

    public Map<String, List<FileMeta>> getFlatFileMetas() {
        return flatFileMetas;
    }

    public void initPartitionInfo(String schemaName, String tableName, TddlRuleManager rule,
                                  List<TablePartitionRecord> tablePartitionRecords,
                                  List<TablePartitionRecord> tablePartitionRecordsFromDelta) {
        rule.getPartitionInfoManager()
            .reloadPartitionInfo(schemaName, tableName, this, tablePartitionRecords, tablePartitionRecordsFromDelta);
        this.partitionInfo = rule.getPartitionInfoManager().getPartitionInfo(tableName);
    }

    public Map<String, Set<String>> getLatestTopology() {
        boolean isNewPart = DbInfoManager.getInstance().isNewPartitionDb(schemaName);
        if (isNewPart) {
            return OptimizerContext.getContext(schemaName).getPartitionInfoManager().getPartitionInfo(tableName)
                .getTopology();
        } else {
            return OptimizerContext.getContext(schemaName).getRuleManager().getTableRule(tableName).getActualTopology();
        }
    }

    /**
     * get the field id of a column
     *
     * @param column column name
     * @return the same colum name if the table is an old file storage table
     */
    public String getColumnFieldId(String column) {
        return tableFilesMeta.columnMapping.get(column.toLowerCase());
    }

}
