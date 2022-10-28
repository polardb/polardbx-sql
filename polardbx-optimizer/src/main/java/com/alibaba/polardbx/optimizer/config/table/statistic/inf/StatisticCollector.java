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

import com.alibaba.polardbx.common.properties.ParamManager;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.partition.PartitionLocation;
import org.apache.calcite.rel.ddl.AlterTableGroupSetPartitionsLocality;

import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public abstract class StatisticCollector implements Runnable {

    public abstract int getInitialDelay();

    public abstract int getPeriod();

    public abstract TimeUnit getTimeUnit();

    /**
     * fast collect all table's row count
     */
    public abstract void collectRowCount();

    /**
     * fast collect all table's columns cardinality
     */
    public abstract void collectCardinality();

    /**
     * fast collect a table's row count
     */
    public abstract void collectRowCount(String logicalTableNmae);

    /**
     * collect the tables
     */
    public abstract void collectTables();

    /**
     * analyze a table's columns
     */
    public abstract boolean analyzeColumns(String logicalTableName, List<ColumnMeta> columnMetaList);

    /**
     * just sample
     */
    public abstract boolean sampleColumns(String logicalTableName, List<ColumnMeta> columnMetaList);

    public abstract List<List<Object>> mockSampleColumns(String logicalTableName, List<ColumnMeta> columnMetaList,
                                              ParamManager userParamManager);

    public abstract List<List<Object>> samplePartitionColumns(String logicalTableName, String partitionName, List<ColumnMeta> columnMetaList, PartitionLocation partitionLocation,
                                              ParamManager userParamManager);

    public abstract boolean forceAnalyzeColumns(String logicalTableName, List<ColumnMeta> columnMetaList);

    /**
     * get all the tables needed to be collected
     */
    public abstract Set<String> getNeedCollectorTables();
}
