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

import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.optimizer.config.table.statistic.inf.NDVSketchService;
import com.alibaba.polardbx.optimizer.config.table.statistic.inf.SystemTableColumnStatistic;
import com.alibaba.polardbx.optimizer.config.table.statistic.inf.SystemTableNDVSketchStatistic;
import com.alibaba.polardbx.optimizer.config.table.statistic.inf.SystemTableTableStatistic;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadPoolExecutor;

public class StatisticDataTableSource implements StatisticDataSource {
    private SystemTableTableStatistic systemTableTableStatistic;

    private SystemTableColumnStatistic systemTableColumnStatistic;

    private SystemTableNDVSketchStatistic ndvSketchStatistic;

    private NDVSketchService ndvSketch;

    public StatisticDataTableSource(SystemTableTableStatistic systemTableTableStatistic,
                                    SystemTableColumnStatistic systemTableColumnStatistic,
                                    SystemTableNDVSketchStatistic ndvSketchStatistic,
                                    NDVSketchService ndvSketch
    ) {
        this.systemTableTableStatistic = systemTableTableStatistic;
        this.systemTableColumnStatistic = systemTableColumnStatistic;
        this.ndvSketchStatistic = ndvSketchStatistic;
        this.ndvSketch = ndvSketch;
    }

    @Override
    public void init() {
        systemTableTableStatistic.createTableIfNotExist();
        systemTableColumnStatistic.createTableIfNotExist();
        if (ndvSketchStatistic != null) {
            ndvSketchStatistic.createTableIfNotExist();
        }
    }

    @Override
    public Collection<SystemTableTableStatistic.Row> loadAllTableStatistic(long sinceTime) {
        return systemTableTableStatistic.selectAll(sinceTime);
    }

    @Override
    public Collection<SystemTableColumnStatistic.Row> loadAllColumnStatistic(long sinceTime) {
        return systemTableColumnStatistic.selectAll(sinceTime);
    }

    @Override
    public Map<? extends String, ? extends Long> loadAllCardinality() {
        ndvSketch.parse(ndvSketchStatistic.loadAll());
        return ndvSketch.getCardinalityMap();
    }

    @Override
    public Map<? extends String, ? extends Long> syncCardinality() {
        return ndvSketch.getCardinalityMap();
    }

    @Override
    public void batchReplace(List<SystemTableTableStatistic.Row> rowList) {
        systemTableTableStatistic.batchReplace(rowList);
    }

    @Override
    public void batchReplace(ArrayList<SystemTableColumnStatistic.Row> rowList) {
        systemTableColumnStatistic.batchReplace(rowList);
    }

    @Override
    public String scheduleJobs() {
        return ndvSketch.scheduleJobs();
    }

    @Override
    public void reloadNDVbyTableName(String schema, String tableName) {
        ndvSketch.parse(ndvSketchStatistic.loadByTableName(schema, tableName));
    }

    @Override
    public void renameTable(String schema, String oldTableName, String newTableName) {
        systemTableTableStatistic.renameTable(schema, oldTableName, newTableName);
        systemTableColumnStatistic.renameTable(schema, oldTableName, newTableName);
        ndvSketchStatistic.updateTableName(schema, oldTableName, newTableName);
        ndvSketch.remove(schema, oldTableName);
    }

    @Override
    public void removeLogicalTableColumnList(String schema, String logicalTableName, List<String> columnNameList) {
        systemTableColumnStatistic.removeLogicalTableColumnList(schema, logicalTableName, columnNameList);
    }

    @Override
    public void removeLogicalTableList(String schema, List<String> logicalTableNameList) {
        systemTableTableStatistic.removeLogicalTableList(schema, logicalTableNameList);
        systemTableColumnStatistic.removeLogicalTableList(schema, logicalTableNameList);
        logicalTableNameList.forEach(table -> ndvSketchStatistic.deleteByTableName(schema, table));
        logicalTableNameList.forEach(table -> ndvSketch.remove(schema, table));
    }

    @Override
    public boolean sampleColumns(String schema, String logicalTableName) {
        return ndvSketch.sampleColumns(schema, logicalTableName);
    }

    @Override
    public void removeNdvLogicalTable(String schema, String logicalTableName) {
        ndvSketchStatistic.deleteByTableName(schema, logicalTableName);
        ndvSketch.remove(schema, logicalTableName);
    }

    @Override
    public void updateColumnCardinality(String schema, String tableName, String columnName, ExecutionContext ec,
                                        ThreadPoolExecutor sketchHllExecutor)
        throws Exception {
        ndvSketch.updateAllShardParts(schema, tableName, columnName, ec, sketchHllExecutor);
    }

    @Override
    public void rebuildColumnCardinality(String schema, String tableName, String columnNames, ExecutionContext ec,
                                         ThreadPoolExecutor sketchHllExecutor)
        throws Exception {
        ndvSketch.reBuildShardParts(schema, tableName, columnNames, ec, sketchHllExecutor);
    }

    @Override
    public long ndvModifyTime(String schema, String tableName, String columnNames) {
        return ndvSketch.modifyTime(schema, tableName, columnNames);
    }

    @Override
    public void clearCache() {
        ndvSketch.cleanCache();
    }
}
