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

import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.GsiMetaManager;
import com.alibaba.polardbx.optimizer.config.table.IndexMeta;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.core.datatype.AbstractDataType;
import com.alibaba.polardbx.optimizer.core.datatype.Calculator;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.google.common.util.concurrent.RateLimiter;

import java.sql.Types;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

public class StatisticUtils {

    private static final Logger logger = LoggerFactory.getLogger("statistics");
    public static final int DEFAULT_SAMPLE_SIZE = 100000;

    public static void logInfo(String schemaName, String msg) {
        logger.info(msg);
        OptimizerContext oc = OptimizerContext.getContext(schemaName);
        if (oc != null) {
            oc.getStatisticManager().getStatisticLogInfo().add("msg");
        }
    }

    public static void logDebug(String schemaName, String msg) {
        logger.debug(msg);
    }

    public static DataType decodeDataType(String type) {
        DataType datatype;
        switch (type) {
        case "Int":
            datatype = DataTypes.LongType;
            break;
        case "Double":
            datatype = DataTypes.DoubleType;
            break;
        case "String":
            datatype = DataTypes.StringType;
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

    public static String encodeDataType(DataType dataType) {
        String type;
        switch (dataType.getSqlType()) {
        case Types.TINYINT:
        case Types.SMALLINT:
        case Types.INTEGER:
        case Types.BIGINT:
        case DataType.MEDIUMINT_SQL_TYPE:
        case DataType.YEAR_SQL_TYPE:
            type = "Int";
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
        case Types.TIME:
            type = "Time";
            break;
        case Types.TIMESTAMP:
        case Types.TIME_WITH_TIMEZONE:
        case Types.TIMESTAMP_WITH_TIMEZONE:
        case DataType.DATETIME_SQL_TYPE:
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

    public static List<ColumnMeta> getColumnMetas(boolean onlyAnalyzeColumnWithIndex, String schemaName,
                                                  String logicalTableName) {
        TableMeta tableMeta;
        try {
            tableMeta =
                OptimizerContext.getContext(schemaName).getLatestSchemaManager().getTable(logicalTableName);
        } catch (Throwable e) {
            logger.error(e.getMessage());
            return null;
        }

        if (tableMeta == null) {
            logger.error("no tableMeta for schemaName = " + schemaName + ", logicalTableName = " + logicalTableName);
            return null;
        }

        List<ColumnMeta> analyzeColumnList;
        if (onlyAnalyzeColumnWithIndex) {
            Map<String, ColumnMeta> columnMetaMap = new HashMap<>();
            for (IndexMeta indexMeta : tableMeta.getIndexes()) {
                for (ColumnMeta columnMeta : indexMeta.getKeyColumns()) {
                    if (!isBinaryOrJsonColumn(columnMeta)) {
                        columnMetaMap.put(columnMeta.getName().toLowerCase(), columnMeta);
                    }
                }
            }

            // gsi
            Map<String, GsiMetaManager.GsiIndexMetaBean> gsiIndexMetaBeanMap = tableMeta.getGsiPublished();
            if (gsiIndexMetaBeanMap != null) {
                for (GsiMetaManager.GsiIndexMetaBean gsiIndexMetaBean : gsiIndexMetaBeanMap.values()) {
                    for (GsiMetaManager.GsiIndexColumnMetaBean gsiIndexColumnMetaBean : gsiIndexMetaBean.indexColumns) {
                        ColumnMeta columnMeta = tableMeta.getColumn(gsiIndexColumnMetaBean.columnName);
                        columnMetaMap.put(columnMeta.getName().toLowerCase(), columnMeta);
                    }

                }
            }

            analyzeColumnList = columnMetaMap.entrySet().stream().map(x -> x.getValue()).collect(Collectors.toList());
        } else {
            analyzeColumnList =
                tableMeta.getAllColumns().stream().filter(x -> !isBinaryOrJsonColumn(x)).collect(Collectors.toList());
        }
        return analyzeColumnList;
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
        return String.join(",", orderStr);
    }

    public static String buildSketchKey(String schemaName, String tableName, String columnNames) {
        return (schemaName + ":" + tableName + ":" + columnNames).toLowerCase();
    }
}
