package com.alibaba.polardbx.executor.statistic;

import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.pl.ProcedureManager;
import com.alibaba.polardbx.executor.pl.StoredFunctionManager;
import com.alibaba.polardbx.gms.config.impl.InstConfUtil;
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.gms.topology.SystemDbHelper;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.GlobalIndexMeta;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.config.table.statistic.Histogram;
import com.alibaba.polardbx.optimizer.config.table.statistic.StatisticManager;
import com.alibaba.polardbx.optimizer.core.expression.JavaFunctionManager;
import com.alibaba.polardbx.optimizer.core.planner.PlanCache;
import org.glassfish.jersey.internal.guava.Sets;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.alibaba.polardbx.optimizer.config.table.statistic.StatisticUtils.DEFAULT_SAMPLE_SIZE;
import static com.alibaba.polardbx.optimizer.config.table.statistic.StatisticUtils.buildColumnsName;
import static com.alibaba.polardbx.optimizer.config.table.statistic.StatisticUtils.isBinaryOrJsonColumn;

/**
 * @author jilong.ljl
 */
public enum RealStatsLogType {
    /**
     * real-time information
     */
    TABLE_NUM {
        @Override
        public String getLog() {
            return "" + tableCount();
        }
    },
    COLUMN_NEED_SAMPLE_STATS_SIZE {
        @Override
        public String getLog() {
            return "" + calculateColumnsForSamplingStats();
        }
    },
    COLUMN_HAS_FRESH_SAMPLE_STATS_SIZE {
        @Override
        public String getLog() {
            return "" + countFreshSampleStatsColumns();
        }
    },
    COLUMN_NEED_HLL_STATS_SIZE {
        @Override
        public String getLog() {
            return "" + calculateColumnsForHyperLoglogStats();
        }
    },
    COLUMN_HAS_FRESH_HLL_STATS_SIZE {
        @Override
        public String getLog() {
            return "" + countFreshHyperLogLogStatsColumns();
        }
    },
    PLAN_CACHE_SIZE {
        @Override
        public String getLog() {
            return "" + planCacheSize();
        }
    },
    STATISTIC_TABLE_NUM {
        @Override
        public String getLog() {
            return "" + tableInStatsCount();
        }
    },
    PROCEDURE_NUM {
        @Override
        public String getLog() {
            return "" + procedureCount();
        }
    },
    SQL_UDF_NUM {
        @Override
        public String getLog() {
            return "" + sqlUdfCount();
        }
    },
    JAVA_UDF_NUM {
        @Override
        public String getLog() {
            return "" + javaUdfCount();
        }
    };

    public abstract String getLog();

    // util method

    protected static long planCacheSize() {
        return PlanCache.getInstance().getCacheKeyCount();
    }

    protected static long procedureCount() {
        return ProcedureManager.getInstance().getProcedureSize();
    }

    protected static long sqlUdfCount() {
        return StoredFunctionManager.getInstance().getUdfCount();
    }

    protected static long javaUdfCount() {
        return JavaFunctionManager.getInstance().getFuncNum();
    }

    public static long countFreshHyperLogLogStatsColumns() {
        long columnCount = 0;

        // Retrieve list of databases
        List<String> databaseNames = DbInfoManager.getInstance().getDbList();

        int expiredTime = InstConfUtil.getInt(ConnectionParams.STATISTIC_NDV_SKETCH_EXPIRE_TIME);
        long current = System.currentTimeMillis();

        // Iterate over each database.
        for (String dbName : databaseNames) {
            OptimizerContext context = OptimizerContext.getContext(dbName);

            // Skip if no context exists for this database.
            if (context == null) {
                continue;
            }

            List<TableMeta> tables = context.getLatestSchemaManager().getAllUserTables();

            // Iterate over each table within the current database.
            for (TableMeta tableMeta : tables) {

                String tableName = tableMeta.getTableName();
                StatisticManager.CacheLine c = StatisticManager.getInstance().getCacheLine(dbName, tableName);

                if (c.getRowCount() <= DEFAULT_SAMPLE_SIZE) {
                    continue;
                }

                Map<String, Map<String, List<String>>> indexColsMap = GlobalIndexMeta.getTableIndexMap(tableMeta, null);
                Set<String> colDoneSet = Sets.newHashSet();
                for (Map.Entry<String, Map<String, List<String>>> entry : indexColsMap.entrySet()) {
                    // index key -> columns
                    Map<String, List<String>> indexColumnMap = entry.getValue();

                    for (List<String> cols : indexColumnMap.values()) {
                        for (int i = 0; i <= cols.size() - 1; i++) {
                            // Sequentially counting the occurrences of prefix combinations for a prefix index
                            String colsName = buildColumnsName(cols, i + 1);
                            colDoneSet.add(colsName);
                        }
                    }
                }

                for (String cols : colDoneSet) {
                    long lastUpdate = StatisticManager.getInstance().getSds().ndvModifyTime(dbName, tableName, cols);
                    if (current - lastUpdate <= expiredTime) {
                        columnCount++;
                    } else {
                        Logger logger = LoggerFactory.getLogger("STATISTICS");
                        logger.warn("ndv sketch has expired:" + dbName + "," + tableName + "," + cols);
                    }
                }
            }
        }
        return columnCount;
    }

    public static long calculateColumnsForHyperLoglogStats() {
        long columnCount = 0;

        // Retrieve list of databases
        List<String> databaseNames = DbInfoManager.getInstance().getDbList();

        // Iterate over each database.
        for (String dbName : databaseNames) {
            OptimizerContext context = OptimizerContext.getContext(dbName);

            // Skip if no context exists for this database.
            if (context == null) {
                continue;
            }

            List<TableMeta> tables = context.getLatestSchemaManager().getAllUserTables();

            // Iterate over each table within the current database.
            for (TableMeta tableMeta : tables) {
                String tableName = tableMeta.getTableName();
                StatisticManager.CacheLine c = StatisticManager.getInstance().getCacheLine(dbName, tableName);

                if (c.getRowCount() <= DEFAULT_SAMPLE_SIZE) {
                    continue;
                }

                Map<String, Map<String, List<String>>> indexColsMap = GlobalIndexMeta.getTableIndexMap(tableMeta, null);
                Set<String> colDoneSet = Sets.newHashSet();
                for (Map.Entry<String, Map<String, List<String>>> entry : indexColsMap.entrySet()) {
                    // index key -> columns
                    Map<String, List<String>> indexColumnMap = entry.getValue();

                    for (List<String> cols : indexColumnMap.values()) {
                        for (int i = 0; i <= cols.size() - 1; i++) {
                            // Sequentially counting the occurrences of prefix combinations for a prefix index
                            String colsName = buildColumnsName(cols, i + 1);
                            colDoneSet.add(colsName);
                        }
                    }
                }
                columnCount += colDoneSet.size();
            }
        }
        return columnCount;
    }

    /**
     * Calculates the total number of non-binary and non-JSON type columns across all databases,
     * which might require sampling statistics.
     *
     * @return The total count of columns requiring sampling statistics.
     */
    public static long calculateColumnsForSamplingStats() {
        long columnCount = 0;

        // Retrieve list of databases
        List<String> databaseNames = DbInfoManager.getInstance().getDbList();

        // Iterate over each database.
        for (String dbName : databaseNames) {
            OptimizerContext context = OptimizerContext.getContext(dbName);

            // Skip if no context exists for this database.
            if (context == null) {
                continue;
            }

            if (SystemDbHelper.isDBBuildIn(dbName)) {
                continue;
            }

            List<TableMeta> tables = context.getLatestSchemaManager().getAllUserTables();

            // Iterate over each table within the current database.
            for (TableMeta tableMeta : tables) {
                Collection<ColumnMeta> columns = tableMeta.getAllColumns();

                // Iterate over each column within the current table.
                for (ColumnMeta columnMeta : columns) {
                    // Increment counter only if the column is neither binary nor JSON type.
                    if (!isBinaryOrJsonColumn(columnMeta)) {
                        columnCount++;
                    }
                }
            }
        }
        // Return the total count of columns needing sampling stats.
        return columnCount;
    }

    /**
     * Counts the total number of columns having fresh sample statistics across all database schemas.
     *
     * @return Total number of columns with fresh statistics.
     */
    public static long countFreshSampleStatsColumns() {
        long totalSize = 0L;
        List<String> databaseSchemas = DbInfoManager.getInstance().getDbList();

        for (String schema : databaseSchemas) {
            OptimizerContext context = OptimizerContext.getContext(schema);
            if (context == null) {
                continue;
            }
            if (SystemDbHelper.isDBBuildIn(schema)) {
                continue;
            }

            List<TableMeta> tables = context.getLatestSchemaManager().getAllUserTables();

            for (TableMeta tableMeta : tables) {
                StatisticManager.CacheLine cacheLine =
                    StatisticManager.getInstance().getCacheLine(schema, tableMeta.getTableName());
                // Check if cache line exists and hasn't expired.
                if (cacheLine != null && !cacheLine.hasExpire()) {
                    Collection<ColumnMeta> columns = tableMeta.getAllColumns();

                    if (cacheLine.getRowCount() == 0L) {
                        totalSize += countColumnsWithoutBinaryOrJson(columns);
                    } else { // Handle cases where row count is greater than zero.
                        totalSize += countColumnsWithNotExpiredStatistics(cacheLine, columns);
                    }
                }
            }
        }

        return totalSize;
    }

    /**
     * Counts columns without binary or JSON types.
     *
     * @param columns Collection of ColumnMeta objects representing columns.
     * @return Count of columns excluding binary or JSON types.
     */
    public static long countColumnsWithoutBinaryOrJson(Collection<ColumnMeta> columns) {
        long count = 0;
        for (ColumnMeta columnMeta : columns) {
            if (!isBinaryOrJsonColumn(columnMeta)) {
                count++;
            }
        }
        return count;
    }

    /**
     * Counts columns with statistical information that is not expired.
     *
     * @param cacheLine Cache line containing statistical information.
     * @param columns Collection of ColumnMeta objects representing columns.
     * @return Count of columns lacking complete statistical data.
     */
    public static long countColumnsWithNotExpiredStatistics(StatisticManager.CacheLine cacheLine,
                                                            Collection<ColumnMeta> columns) {
        long count = 0;
        for (ColumnMeta columnMeta : columns) {
            if (!isBinaryOrJsonColumn(columnMeta)) {
                if (!hasMissingStatistics(cacheLine, columnMeta.getName())) {
                    count++;
                }
            }
        }
        return count;
    }

    /**
     * Checks if there are missing statistics for a given column name.
     *
     * @param cacheLine Cache line containing statistical information.
     * @param columnName Name of the column to check.
     * @return True if statistics are missing, false otherwise.
     */
    public static boolean hasMissingStatistics(StatisticManager.CacheLine cacheLine, String columnName) {
        String lowerCaseColName = columnName.toLowerCase();
        boolean isTopnNull = cacheLine.getTopN(lowerCaseColName) == null;
        Histogram histogram =
            cacheLine.getHistogramMap() == null ? null : cacheLine.getHistogramMap().get(lowerCaseColName);
        boolean isHistogramNull = histogram == null || histogram.getBuckets().isEmpty();
        long nullNum = 0L;
        if (cacheLine.getNullCountMap().containsKey(lowerCaseColName)) {
            nullNum = cacheLine.getNullCountMap().get(lowerCaseColName);
        }
        return (cacheLine.getRowCount() - nullNum) > 0 && isTopnNull && isHistogramNull;
    }

    protected static int tableCount() {
        int count = 0;
        for (String schema : DbInfoManager.getInstance().getDbList()) {
            if (SystemDbHelper.isDBBuildIn(schema)) {
                continue;
            }
            if (!OptimizerContext.getActiveSchemaNames().contains(schema)) {
                continue;
            }
            OptimizerContext optimizerContext = OptimizerContext.getContext(schema);
            if (optimizerContext == null) {
                continue;
            }
            count += optimizerContext.getLatestSchemaManager().getAllUserTables().size();
        }
        return count;
    }

    protected static int tableInStatsCount() {
        return StatisticManager.getInstance().getStatisticCache().values().stream().mapToInt(Map::size).sum();
    }
}
