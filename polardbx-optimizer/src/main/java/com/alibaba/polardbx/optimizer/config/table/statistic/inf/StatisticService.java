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

import com.alibaba.polardbx.optimizer.config.table.statistic.AutoAnalyzeTask;
import com.alibaba.polardbx.optimizer.config.table.statistic.Histogram;
import com.alibaba.polardbx.optimizer.config.table.statistic.StatisticLogInfo;
import com.alibaba.polardbx.optimizer.config.table.statistic.StatisticResult;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.function.calc.scalar.filter.Row;

import java.util.List;
import java.util.Set;

public interface StatisticService {
    StatisticResult getRowCount(String logicalTableName);

    void setRowCount(String logicalTableName, long rowCount);

    StatisticResult getCardinality(String logicalTableName, String columnName);

    StatisticResult getFrequency(String logicalTableName, String columnName, String value);

    /**
     * get frequency with row value.
     */
    StatisticResult getFrequency(String logicalTableName, String columnName, Row.RowValue value);

    StatisticResult getNullCount(String logicalTableName, String columnName);

    StatisticResult getRangeCount(String logicalTableName, String columnName, Object lower,
                                         boolean lowerInclusive,
                                         Object upper, boolean upperInclusive);

    void addUpdateRowCount(String logicalTableName, long affectRow);

    DataType getDataType(String tableName, String columnName);

    @Deprecated
    StatisticLogInfo getStatisticLogInfo();

    @Deprecated
    AutoAnalyzeTask getAutoAnalyzeTask();

    void renameTable(String oldLogicalTableName, String newLogicalTableName);

    void removeLogicalColumnList(String logicalTableName, List<String> columnNameList);

    Set<String> getTableNamesCollected();

    void sampleTable(String logicalTableName);
}
