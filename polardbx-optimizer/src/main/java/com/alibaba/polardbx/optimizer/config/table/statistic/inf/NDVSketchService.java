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

package com.alibaba.polardbx.optimizer.config.table.statistic.inf;

import com.alibaba.polardbx.optimizer.config.table.statistic.StatisticResult;

import java.sql.SQLException;
import java.util.Map;

/**
 * ndv sketch inf
 */
public interface NDVSketchService {
    /**
     * 更新已存在的分片的 ndv sketch 信息, 如果不存在则直接返回,如果已存在判断是否需要更新
     */
    boolean updateStockShardParts(String tableName, String columnName) throws SQLException;

    /**
     * 重建全部分片的 ndv sketch 信息
     */
    void reBuildShardParts(String tableName, String columnName) throws SQLException;

    /**
     * 更新所有分片的 ndv sketch 信息,如果不存在重建.如果已存在则判断是否需要更新
     */
    void updateAllShardParts(String tableName, String columnName) throws SQLException;

    /**
     * 加载组装 meta db 中的数据
     */
    void parse(SystemTableNDVSketchStatistic.SketchRow[] sketchRows);

    /**
     * remove by table
     */
    void remove(String tableName);

    /**
     * remove by table, column
     */
    void remove(String tableName, String column);

    /**
     * cal the ndv value
     */
    StatisticResult getCardinality(String tableName, String columnName);

    Map<? extends String, ? extends Long> getCardinalityMap();
}
