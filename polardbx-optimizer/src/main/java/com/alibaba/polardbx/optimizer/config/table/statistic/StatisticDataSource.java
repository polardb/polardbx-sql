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

import com.alibaba.polardbx.optimizer.config.table.statistic.inf.SystemTableColumnStatistic;
import com.alibaba.polardbx.optimizer.config.table.statistic.inf.SystemTableTableStatistic;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadPoolExecutor;

public interface StatisticDataSource {
    void init();

    Collection<SystemTableTableStatistic.Row> loadAllTableStatistic(long sinceTime);

    Collection<SystemTableColumnStatistic.Row> loadAllColumnStatistic(long sinceTime);

    void renameTable(String schema, String oldTableName, String newTableName);

    void removeLogicalTableColumnList(String schema, String logicalTableName, List<String> columnNameList);

    void removeLogicalTableList(String schema, List<String> logicalTableNameList);

    boolean sampleColumns(String schema, String logicalTableName);

    // ndv api start

    /**
     * reload ndv info by name from meta source
     */
    void reloadNDVbyTableName(String schema, String tableName);

    /**
     * remove ndv info by table name from meta source
     */
    void removeNdvLogicalTable(String schema, String logicalTableName);

    /**
     * reload all ndv info from meta source
     */
    Map<? extends String, ? extends Long> loadAllCardinality();

    /**
     * update ndv info by table name and column name from meta source if needed
     */
    void updateColumnCardinality(String schema, String tableName, String columnName, ExecutionContext ec,
                                 ThreadPoolExecutor sketchHllExecutor)
        throws SQLException;

    /**
     * force rebuilt ndv info by table name and column name from meta source
     */
    void rebuildColumnCardinality(String schema, String tableName, String columnName, ExecutionContext ec,
                                  ThreadPoolExecutor sketchHllExecutor)
        throws SQLException;

    /**
     * get current ndv value from cache
     */
    Map<? extends String, ? extends Long> syncCardinality();

    // ndv api end

    void batchReplace(final List<SystemTableTableStatistic.Row> rowList);

    void batchReplace(final ArrayList<SystemTableColumnStatistic.Row> rowList);

    String scheduleJobs();

    long ndvModifyTime(String schema, String tableName, String columnNames);

    void clearCache();
}
