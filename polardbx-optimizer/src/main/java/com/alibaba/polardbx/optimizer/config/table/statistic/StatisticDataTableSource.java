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

import com.alibaba.polardbx.common.properties.ParamManager;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.optimizer.config.table.statistic.inf.NDVSketchService;
import com.alibaba.polardbx.optimizer.config.table.statistic.inf.SystemTableColumnStatistic;
import com.alibaba.polardbx.optimizer.config.table.statistic.inf.SystemTableNDVSketchStatistic;
import com.alibaba.polardbx.optimizer.config.table.statistic.inf.SystemTableTableStatistic;

import java.sql.SQLException;
import java.util.Collection;
import java.util.List;
import java.util.Map;

public class StatisticDataTableSource implements StatisticDataSource {
    private SystemTableTableStatistic systemTableTableStatistic;

    private SystemTableColumnStatistic systemTableColumnStatistic;

    private SystemTableNDVSketchStatistic ndvSketchStatistic;

    /**
     * TDataSource connection properties manager
     */
    private final ParamManager paramManager;

    private NDVSketchService ndvSketch;

    private String schemaName;

    public StatisticDataTableSource(String schemaName,
                                    SystemTableTableStatistic systemTableTableStatistic,
                                    SystemTableColumnStatistic systemTableColumnStatistic,
                                    SystemTableNDVSketchStatistic ndvSketchStatistic,
                                    NDVSketchService ndvSketch,
                                    Map<String, Object> connectionProperties
    ) {
        this.schemaName = schemaName;
        this.systemTableTableStatistic = systemTableTableStatistic;
        this.systemTableColumnStatistic = systemTableColumnStatistic;
        this.ndvSketchStatistic = ndvSketchStatistic;
        this.ndvSketch = ndvSketch;
        this.paramManager = new ParamManager(connectionProperties);
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
        ndvSketch.parse(ndvSketchStatistic.loadAll(schemaName));
        return ndvSketch.getCardinalityMap();
    }

    @Override
    public Map<? extends String, ? extends Long> syncCardinality() {
        return ndvSketch.getCardinalityMap();
    }

    @Override
    public void reloadNDVbyTableName(String tableName) {
        ndvSketch.parse(ndvSketchStatistic.loadByTableName(schemaName, tableName));
    }

    @Override
    public ParamManager acquireStatisticConfig() {
        return paramManager;
    }

    @Override
    public void renameTable(String oldTableName, String newTableName) {
        systemTableTableStatistic.renameTable(oldTableName, newTableName);
        systemTableColumnStatistic.renameTable(oldTableName, newTableName);
        ndvSketchStatistic.updateTableName(schemaName, oldTableName, newTableName);
        ndvSketch.remove(oldTableName);
    }

    @Override
    public void removeLogicalTableColumnList(String logicalTableName, List<String> columnNameList) {
        systemTableColumnStatistic.removeLogicalTableColumnList(logicalTableName, columnNameList);
    }

    @Override
    public void removeLogicalTableList(List<String> logicalTableNameList) {
        systemTableTableStatistic.removeLogicalTableList(logicalTableNameList);
        systemTableColumnStatistic.removeLogicalTableList(logicalTableNameList);
        logicalTableNameList.forEach(table -> ndvSketchStatistic.deleteByTableName(schemaName, table));
        logicalTableNameList.forEach(table -> ndvSketch.remove(table));
    }

    @Override
    public void updateColumnCardinality(String tableName, String columnName) {
        try {
            ndvSketch.updateAllShardParts(tableName, columnName);
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
    }

    @Override
    public void rebuildColumnCardinality(String tableName, String columnNames) {
        try {
            ndvSketch.reBuildShardParts(tableName, columnNames);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

}
