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

package com.alibaba.polardbx.optimizer.config.table.statistic;

import com.alibaba.polardbx.common.datatype.RowValue;
import com.alibaba.polardbx.common.utils.LoggerUtil;
import com.alibaba.polardbx.common.datatype.RowValue;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.time.core.TimeStorage;
import com.alibaba.polardbx.gms.metadb.table.IndexStatus;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.config.meta.DrdsRelMdSelectivity;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.GsiMetaManager;
import com.alibaba.polardbx.optimizer.config.table.IndexMeta;
import com.alibaba.polardbx.optimizer.config.table.SchemaManager;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.config.table.statistic.inf.StatisticResultSource;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypeUtil;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.exception.TableNotFoundException;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.partition.PartitionInfoManager;
import com.alibaba.polardbx.optimizer.rule.TddlRuleManager;
import com.alibaba.polardbx.rule.TableRule;
import io.airlift.slice.Slice;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.commons.lang.StringUtils;
import org.jetbrains.annotations.Nullable;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.alibaba.polardbx.optimizer.config.table.statistic.StatisticTrace.MAX_DIGEST_SIZE;

public class StatisticUtils {

    private static final Logger logger = LoggerUtil.statisticsLogger;
    public static final int DEFAULT_SAMPLE_SIZE = 100000;
    public static final int DEFAULT_SAMPLE_SIZE_ = 80000;
    public static final int DATA_MAX_LEN = 128;

    public static DataType decodeDataType(String type) {
        DataType datatype;
        switch (type) {
        case "unsigned_int":
            datatype = DataTypes.ULongType;
            break;
        case "Int":
            datatype = DataTypes.LongType;
            break;
        case "Double":
            datatype = DataTypes.DoubleType;
            break;
        case "String":
            datatype = DataTypes.StringType;
            break;
        case "Year":
            datatype = DataTypes.YearType;
            break;
        case "Datetime":
            datatype = DataTypes.DatetimeType;
            break;
        case "Date":
            datatype = DataTypes.DateType;
            break;
        case "Time":
            datatype = DataTypes.TimeType;
            break;
        case "Timestamp":
            datatype = DataTypes.TimestampType;
            break;
        case "Boolean":
            datatype = DataTypes.BooleanType;
            break;
        default:
            datatype = DataTypes.StringType;
        }
        return datatype;
    }

    /**
     * convert date/time/timestamp type data to long
     * used for statistic
     * if obj convert to null, return -1
     */
    public static long packDateTypeToLong(DataType dataType, Object obj) {
        if (DataTypeUtil.equalsSemantically(DataTypes.TimestampType, dataType) || DataTypeUtil.equalsSemantically(
            DataTypes.DatetimeType, dataType)) {
            Timestamp timestamp = (Timestamp) dataType.convertFrom(obj);
            return Optional.ofNullable(timestamp).map(TimeStorage::packDatetime).orElse(-1L);
        } else if (DataTypeUtil.equalsSemantically(DataTypes.DateType, dataType)) {
            Date date = (Date) dataType.convertFrom(obj);
            return Optional.ofNullable(date).map(TimeStorage::packDate).orElse(-1L);
        } else if (DataTypeUtil.equalsSemantically(DataTypes.TimeType, dataType)) {
            Time time = (Time) dataType.convertFrom(obj);
            return Optional.ofNullable(time).map(TimeStorage::packTime).orElse(-1L);
        }
        throw new IllegalStateException("Unexpected value: " + dataType);
    }

    public static String encodeDataType(DataType dataType) {
        String type;
        switch (dataType.getSqlType()) {
        case Types.TINYINT:
        case Types.SMALLINT:
        case Types.INTEGER:
        case Types.BIGINT:
        case DataType.MEDIUMINT_SQL_TYPE:
            if (dataType.isUnsigned()) {
                type = "unsigned_int";
            } else {
                type = "Int";
            }
            break;
        case Types.FLOAT:
        case Types.REAL:
        case Types.DOUBLE:
        case Types.NUMERIC:
        case Types.DECIMAL:
            type = "Double";
            break;
        case Types.CHAR:
        case Types.VARCHAR:
        case Types.LONGVARCHAR:
        case Types.NCHAR:
        case Types.NVARCHAR:
        case Types.LONGNVARCHAR:
            type = "String";
            break;
        case Types.DATE:
            type = "Date";
            break;
        case DataType.DATETIME_SQL_TYPE:
            type = "Datetime";
            break;
        case DataType.YEAR_SQL_TYPE:
            type = "Year";
            break;
        case Types.TIME:
            type = "Time";
            break;
        case Types.TIMESTAMP:
        case Types.TIME_WITH_TIMEZONE:
        case Types.TIMESTAMP_WITH_TIMEZONE:
            type = "Timestamp";
            break;
        case Types.BIT:
        case Types.BLOB:
        case Types.CLOB:
        case Types.BINARY:
        case Types.VARBINARY:
        case Types.LONGVARBINARY:
            type = "String"; // FIXME to support byte type histogram
            break;
        case Types.BOOLEAN:
            type = "Boolean";
            break;
        default:
            type = "String";
            break;
        }
        return type;
    }

    public static boolean isBinaryOrJsonColumn(ColumnMeta columnMeta) {
        switch (columnMeta.getDataType().getSqlType()) {
        case Types.BIT:
        case Types.BLOB:
        case Types.CLOB:
        case Types.BINARY:
        case Types.VARBINARY:
        case Types.LONGVARBINARY:
        case DataType.JSON_SQL_TYPE:
            return true;
        default:
            return false;
        }
    }

    public static boolean isStringColumn(ColumnMeta columnMeta) {
        switch (columnMeta.getDataType().getSqlType()) {
        case Types.BIT:
        case Types.BLOB:
        case Types.CLOB:
        case Types.BINARY:
        case Types.VARBINARY:
        case Types.CHAR:
        case Types.VARCHAR:
        case Types.LONGVARCHAR:
        case Types.LONGNVARCHAR:
        case Types.LONGVARBINARY:
        case DataType.JSON_SQL_TYPE:
            return true;
        default:
            return false;
        }
    }

    public static List<ColumnMeta> getColumnMetas(boolean onlyAnalyzeColumnWithIndex, String schemaName,
                                                  String logicalTableName) {
        TableMeta tableMeta;
        try {
            OptimizerContext optimizerContext = OptimizerContext.getContext(schemaName);
            // schema might be not exists
            if (optimizerContext == null || optimizerContext.getLatestSchemaManager() == null) {
                return null;
            }
            tableMeta = optimizerContext.getLatestSchemaManager().getTableWithNull(logicalTableName);
        } catch (Throwable e) {
            logger.error(e.getMessage(), e);
            return null;
        }

        if (tableMeta == null) {
            logger.info("no tableMeta for schemaName = " + schemaName + ", logicalTableName = " + logicalTableName);
            return null;
        }

        return tableMeta.getAllColumns().stream().filter(x -> !isBinaryOrJsonColumn(x)).collect(Collectors.toList());
    }

    /**
     * Checks if the specified column name matches any unique key in the given table.
     *
     * @param schema The database schema name.
     * @param logicalTableName The logical table name.
     * @param columnName The column names to check. These column names should be combined by ','
     * @return true if a matching unique key is found; otherwise, returns false.
     */
    public static boolean doesColumnNameMatchAnyUniqueKey(String schema, String logicalTableName, String columnName) {
        if (StringUtils.isEmpty(schema) || StringUtils.isEmpty(logicalTableName) || StringUtils.isEmpty(columnName)) {
            return false;
        }
        if (OptimizerContext.getContext(schema) == null) {
            return false;
        }
        String[] cols = columnName.split(",");
        return areColumnsUniqueInTable(schema, logicalTableName, cols);
    }

    /**
     * order firstly, then join the values
     */
    public static String buildColumnsName(Collection<String> cols) {
        String[] orderStr = cols.toArray(new String[0]);
        Arrays.sort(orderStr);
        return String.join(",", orderStr);
    }

    /**
     * order firstly, then join the values
     */
    public static String buildColumnsName(List<String> cols, int index) {
        String[] orderStr = Arrays.copyOf(cols.toArray(new String[0]), index);
        Arrays.sort(orderStr);
        return String.join(",", orderStr).toLowerCase();
    }

    public static String buildSketchKey(String schemaName, String tableName, String columnNames) {
        return (schemaName + ":" + tableName + ":" + columnNames).toLowerCase();
    }

    public static PlannerContext getPlannerContextFromRelNode(RelNode rel) {
        if (rel == null || rel.getCluster() == null || rel.getCluster().getPlanner() == null
            || rel.getCluster().getPlanner().getContext() == null) {
            return null;
        }
        PlannerContext plannerContext = rel.getCluster().getPlanner().getContext().unwrap(PlannerContext.class);
        if (plannerContext == null) {
            return null;
        }
        return plannerContext;
    }

    public static StatisticTrace buildTrace(String catalogTarget, String action, Object value,
                                            StatisticResultSource source, long modifyTime, String desc) {
        return new StatisticTrace(catalogTarget, action, value, desc, source, modifyTime);
    }

    public static String digestForStatisticTrace(Object o) {
        StringBuilder sb = new StringBuilder();
        digest(o, sb);
        return sb.toString();
    }

    public static void digest(Object value, StringBuilder sb) {
        if (value == null) {
            sb.append("null");
            return;
        }
        if (sb.length() > MAX_DIGEST_SIZE) {
            return;
        }
        if (value instanceof Collection) {
            digest((Collection) value, sb);
        } else if (value instanceof RowValue) {
            digest((RowValue) value, sb);
        } else if (value instanceof Slice) {
            sb.append(((Slice) value).toStringUtf8());
        } else {
            sb.append(value);
        }

        if (sb.length() > MAX_DIGEST_SIZE) {
            sb.setLength(MAX_DIGEST_SIZE);
            sb.append("...");
        }
    }

    public static void digest(Collection values, StringBuilder sb) {
        if (values == null) {
            return;
        }
        if (sb.length() > MAX_DIGEST_SIZE) {
            return;
        }
        for (Object o : values) {
            digest(o, sb);
        }
    }

    public static void digest(RowValue values, StringBuilder sb) {
        if (sb.length() > MAX_DIGEST_SIZE) {
            return;
        }
        int originLength = sb.length();
        for (Object o : values.getValues()) {
            if (o == null) {
                continue;
            }
            sb.append(o).append(",");

            if (sb.length() > MAX_DIGEST_SIZE) {
                sb.setLength(MAX_DIGEST_SIZE);
                sb.append("...");
                break;
            }
        }
        if (sb.length() > originLength) {
            sb.setLength(sb.length() - 1);
        }
    }

    public static String skewKey(Collection<String> columns) {
        if (columns == null || columns.size() == 0) {
            return null;
        }
        columns = columns.stream().map(String::toLowerCase).sorted().collect(Collectors.toList());
        return String.join("_", columns);
    }

    public static boolean areColumnsUniqueInTable(String schema, String table, String[] columns) {
        if (TStringUtil.isEmpty(schema) || TStringUtil.isEmpty(table) || columns == null) {
            return false;
        }
        OptimizerContext oc = OptimizerContext.getContext(schema);
        if (oc == null) {
            return false;
        }
        SchemaManager schemaManager = oc.getLatestSchemaManager();
        TableMeta tableMeta;
        try {
            tableMeta = schemaManager.getTable(table);
        } catch (TableNotFoundException tableNotFoundException) {
            return false;
        }
        if (tableMeta == null) {
            return false;
        }
        ImmutableBitSet.Builder colMask = ImmutableBitSet.builder();

        for (String column : columns) {
            if (StringUtils.isEmpty(column)) {
                // ignore empty column
                continue;
            }
            column = column.trim();
            RelDataTypeField field = tableMeta.getRowTypeIgnoreCase(column, null);
            if (field == null) {
                return false;
            }
            colMask.set(field.getIndex());
        }

        Boolean isColumnsUnique = isColumnsUnique(tableMeta, colMask.build(), schemaManager);
        return Boolean.TRUE.equals(isColumnsUnique);
    }

    @Nullable
    public static Boolean isColumnsUnique(TableMeta tableMeta, ImmutableBitSet columns, SchemaManager sm) {
        if (tableMeta == null || columns == null || sm == null) {
            return null;
        }
        if (tableMeta.isColumnar()) {
            GsiMetaManager.GsiTableMetaBean tableMetaBean = tableMeta.getGsiTableMetaBean();
            if (tableMetaBean != null
                && tableMetaBean.gsiMetaBean != null
                && tableMetaBean.gsiMetaBean.tableName != null
                && tableMetaBean.gsiMetaBean.indexStatus == IndexStatus.PUBLIC) {
                tableMeta = sm.getTableWithNull(tableMeta.getGsiTableMetaBean().gsiMetaBean.tableName);
            }
        }
        if (tableMeta == null) {
            return false;
        }

        if (DrdsRelMdSelectivity.isPrimaryKeyAutoIncrement(tableMeta) && columns.cardinality() > 0) {
            IndexMeta pk = tableMeta.getPrimaryIndex();
            if (pk != null) {
                List<ColumnMeta> pkColumns = pk.getKeyColumns();
                if (pkColumns != null && pkColumns.size() > 0) {
                    int pkIndex = DrdsRelMdSelectivity.getColumnIndex(tableMeta, pkColumns.get(0));
                    if (columns.contains(ImmutableBitSet.of(pkIndex))) {
                        return true;
                    }
                }

            }

        }

        TddlRuleManager tddlRuleManager = sm.getTddlRuleManager();
        if (tddlRuleManager == null) {
            return null;
        }
        PartitionInfoManager partitionInfoManager = tddlRuleManager.getPartitionInfoManager();
        List<String> dbKeysAndTbKeys = new ArrayList<>();
        if (partitionInfoManager != null &&
            partitionInfoManager.isNewPartDbTable(tableMeta.getTableName())) {
            PartitionInfo partitionInfo = partitionInfoManager.getPartitionInfo(tableMeta.getTableName());
            if (partitionInfo != null) {
                dbKeysAndTbKeys.addAll(partitionInfo.getPartitionColumns());
            }
        } else {
            TableRule tableRule = tddlRuleManager.getTableRule(tableMeta.getTableName());
            if (tableRule != null && tddlRuleManager.isShard(tableMeta.getTableName())) {
                dbKeysAndTbKeys.addAll(tableRule.getDbPartitionKeys());
                dbKeysAndTbKeys.addAll(tableRule.getTbPartitionKeys());
            }
        }

        List<IndexMeta> ukList = tableMeta.getUniqueIndexes(true);
        if (ukList == null) {
            return null;
        }
        for (IndexMeta uk : ukList) {
            if (uk == null || uk.getKeyColumns() == null) {
                continue;
            }
            List<String> ukColumns =
                uk.getKeyColumns().stream().map(columnMeta -> columnMeta.getName()).collect(Collectors.toList());
            if (columnCoverUkCoverSk(tableMeta, columns, ukColumns, dbKeysAndTbKeys)) {
                return true;
            }
        }

        // gsi
        Map<String, GsiMetaManager.GsiIndexMetaBean> gsiIndexMetaBeanMap = tableMeta.getGsiPublished();
        if (gsiIndexMetaBeanMap != null) {
            for (Map.Entry<String, GsiMetaManager.GsiIndexMetaBean> entry : gsiIndexMetaBeanMap.entrySet()) {
                GsiMetaManager.GsiIndexMetaBean gsiIndexMetaBean = entry.getValue();
                if (gsiIndexMetaBean != null && !gsiIndexMetaBean.nonUnique) {
                    List<String> ukColumns =
                        gsiIndexMetaBean.indexColumns.stream().map(x -> x.columnName).collect(Collectors.toList());
                    if (columnCoverUkCoverSk(tableMeta, columns, ukColumns, new ArrayList<>())) {
                        return true;
                    }
                }
            }
        }

        return false;
    }

    private static boolean columnCoverUkCoverSk(TableMeta tableMeta, ImmutableBitSet columns, List<String> ukColumns,
                                                List<String> dbKeysAndTbKeys) {
        if (tableMeta == null || columns == null || ukColumns == null) {
            return false;
        }
        int[] keyIndexs = new int[ukColumns.size()];
        for (int i = 0; i < ukColumns.size(); i++) {
            keyIndexs[i] = DrdsRelMdSelectivity.getColumnIndex(tableMeta,
                tableMeta.getColumnIgnoreCase(ukColumns.get(i)));
        }
        // uk covered by columns, otherwise bail out
        if (!columns.contains(ImmutableBitSet.of(keyIndexs))) {
            return false;
        }

        /**
         * all dbKeys and tbKeys covered by unique key, example:
         * pk = c1, dbkey = c1, tbkey = c2 return false
         * pk = c1, dbkey = c2, tbkey = c1 return false
         * pk = c1、c2, dbkey = c1, tbkey = c1 return true
         * pk = c1、c2 , dbkey = c1, tbkey = c3 return false
         */

        if (dbKeysAndTbKeys == null || dbKeysAndTbKeys.isEmpty()) {
            return true;
        }
        for (String key : dbKeysAndTbKeys) {
            boolean found = false;
            for (String columnName : ukColumns) {
                if (columnName.equalsIgnoreCase(key)) {
                    found = true;
                    break;
                }
            }
            if (!found) {
                return false;
            }
        }
        return true;
    }
}