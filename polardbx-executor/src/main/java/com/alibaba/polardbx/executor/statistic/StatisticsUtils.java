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

package com.alibaba.polardbx.executor.statistic;

import com.alibaba.polardbx.common.properties.DynamicConfig;
import com.alibaba.polardbx.executor.common.ExecutorContext;
import com.alibaba.polardbx.executor.spi.ITransactionManager;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.utils.ITransaction;
import com.alibaba.polardbx.optimizer.utils.OptimizerHelper;
import com.alibaba.polardbx.stats.AccumulateStatistics;
import com.alibaba.polardbx.stats.CurrentTransactionStatistics;
import com.alibaba.polardbx.stats.SchemaTransactionStatistics;
import com.alibaba.polardbx.stats.MatrixStatistics;
import com.alibaba.polardbx.stats.TransStatsColumn;

import java.util.Collection;
import java.util.List;
import java.util.Optional;

import static com.alibaba.polardbx.stats.TransStatsColumn.COLUMNS;
import static com.alibaba.polardbx.stats.TransStatsColumn.NAME;
import static com.alibaba.polardbx.stats.TransStatsColumn.NUM_COLUMN;

/**
 * @author yaozhili
 */
public class StatisticsUtils {
    public static Object[] getStats(String schema) {
        final Object[] results = new Object[NUM_COLUMN];

        results[COLUMNS.get(NAME).index] = schema;
        Optional.ofNullable(OptimizerContext.getTransStat(schema)).ifPresent(s -> getResultList(results, s, schema));

        return results;
    }

    public static Object[] getInstanceStats() {
        final Object[] results = new Object[NUM_COLUMN];

        results[COLUMNS.get(NAME).index] = "polardbx";
        getResultList(results, MatrixStatistics.instanceTransactionStats, null);

        return results;
    }

    private static void getResultList(Object[] resultList, SchemaTransactionStatistics s, String schema) {
        final CurrentTransactionStatistics currentStat = new CurrentTransactionStatistics();
        long currentTime = System.nanoTime();

        if (null == schema) {
            for (String schema0 : OptimizerHelper.getServerConfigManager().getLoadedSchemas()) {
                updateCurrentStatistics(currentStat, currentTime, schema0);
            }
        } else {
            updateCurrentStatistics(currentStat, currentTime, schema);
        }

        for (TransStatsColumn.ColumnDef column : COLUMNS.values()) {
            if (column.currentStat) {
                column.dataGenerator.accept(currentStat, resultList);
            } else {
                column.dataGenerator.accept(s, resultList);
            }
        }
    }

    private static void updateCurrentStatistics(CurrentTransactionStatistics currentStat,
                                                long currentTime,
                                                String schema) {
        final ITransactionManager tm = ExecutorContext.getContext(schema).getTransactionManager();
        Collection<ITransaction> transactions = tm.getTransactions().values();
        long threshold = DynamicConfig.getInstance().getSlowTransThreshold();
        for (ITransaction tran : transactions) {
            long durationTimeMs = (currentTime - tran.getStat().startTime) / 1000000;
            if (tran.isBegun() && durationTimeMs > threshold) {
                tran.updateCurrentStatistics(currentStat, durationTimeMs);
            }
        }
    }

    /**
     * Merge different stats and generate an approximate accumulated stats.
     */
    public static Object[] mergeStats(List<Object[]> stats) {
        // For finished transactions.
        final AccumulateStatistics acc0 = new AccumulateStatistics();
        // For currently running transactions.
        final AccumulateStatistics acc1 = new AccumulateStatistics();

        // Accumulate stats.
        for (Object[] stat : stats) {
            for (TransStatsColumn.ColumnDef column : COLUMNS.values()) {
                if (column.currentStat) {
                    column.dataAccumulator.accept(acc1, stat);
                } else {
                    column.dataAccumulator.accept(acc0, stat);
                }
            }
        }

        // Extract to one stat.
        final Object[] results = new Object[NUM_COLUMN];
        for (TransStatsColumn.ColumnDef column : COLUMNS.values()) {
            if (column.currentStat) {
                column.dataExtractor.accept(acc1, results);
            } else {
                column.dataExtractor.accept(acc0, results);
            }
        }

        return results;
    }

}
