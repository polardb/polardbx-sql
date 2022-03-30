package com.alibaba.polardbx.optimizer.config.table.statistic;

import com.alibaba.polardbx.common.properties.ParamManager;
import com.alibaba.polardbx.optimizer.config.table.statistic.inf.SystemTableColumnStatistic;
import com.alibaba.polardbx.optimizer.config.table.statistic.inf.SystemTableTableStatistic;

import java.util.Collection;
import java.util.List;
import java.util.Map;

public interface StatisticDataSource {
    void init();

    Collection<SystemTableTableStatistic.Row> loadAllTableStatistic(long sinceTime);

    Collection<SystemTableColumnStatistic.Row> loadAllColumnStatistic(long sinceTime);

    ParamManager acquireStatisticConfig();

    void renameTable(String oldTableName, String newTableName);

    void removeLogicalTableColumnList(String logicalTableName, List<String> columnNameList);

    void removeLogicalTableList(List<String> logicalTableNameList);

    // ndv api start

    /**
     * reload ndv info by name from meta source
     */
    void reloadNDVbyTableName(String tableName);

    /**
     * reload all ndv info from meta source
     */
    Map<? extends String, ? extends Long> loadAllCardinality();

    /**
     * update ndv info by table name and column name from meta source if needed
     */
    void updateColumnCardinality(String tableName, String columnName);

    /**
     * force rebuilt ndv info by table name and column name from meta source
     */
    void rebuildColumnCardinality(String tableName, String columnName);

    /**
     * get current ndv value from cache
     */
    Map<? extends String, ? extends Long> syncCardinality();

    // ndv api end
}
