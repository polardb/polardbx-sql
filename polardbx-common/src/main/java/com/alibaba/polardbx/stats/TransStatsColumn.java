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

package com.alibaba.polardbx.stats;

import com.alibaba.polardbx.common.properties.DynamicConfig;
import com.google.common.collect.ImmutableMap;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

import static com.alibaba.polardbx.stats.TransactionStatisticsSummary.DURATION_TIME;
import static com.alibaba.polardbx.stats.TransactionStatisticsSummary.HISTOGRAM_BUCKET_SIZE;
import static com.alibaba.polardbx.stats.TransactionStatisticsSummary.Workspace.avg;
import static com.alibaba.polardbx.stats.TransactionStatisticsSummary.Workspace.calculateHistogram;
import static com.alibaba.polardbx.stats.TransactionStatisticsSummary.Workspace.histogramToString;
import static com.alibaba.polardbx.stats.TransactionStatisticsSummary.Workspace.max;
import static com.alibaba.polardbx.stats.TransactionStatisticsSummary.Workspace.p95;

/**
 * @author yaozhili
 */
public class TransStatsColumn {
    /**
     * Column name.
     */
    public static final String NAME = "NAME";
    public static final String TRANS_COUNT_XA = "TRANS_COUNT_XA";
    public static final String TRANS_COUNT_XA_RW = "TRANS_COUNT_XA_RW";
    public static final String TRANS_COUNT_XA_RO = "TRANS_COUNT_XA_RO";
    public static final String TRANS_COUNT_TSO = "TRANS_COUNT_TSO";
    public static final String TRANS_COUNT_TSO_RW = "TRANS_COUNT_TSO_RW";
    public static final String TRANS_COUNT_TSO_RO = "TRANS_COUNT_TSO_RO";
    public static final String TRANS_COUNT_CROSS_GROUP = "TRANS_COUNT_CROSS_GROUP";
    public static final String LOCAL_DEADLOCK_COUNT = "LOCAL_DEADLOCK_COUNT";
    public static final String GLOBAL_DEADLOCK_COUNT = "GLOBAL_DEADLOCK_COUNT";
    public static final String MDL_DEADLOCK_COUNT = "MDL_DEADLOCK_COUNT";
    public static final String COMMIT_ERROR_COUNT = "COMMIT_ERROR_COUNT";
    public static final String ROLLBACK_ERROR_COUNT = "ROLLBACK_ERROR_COUNT";
    public static final String ROLLBACK_COUNT = "ROLLBACK_COUNT";
    public static final String RECOVER_COMMIT_BRANCH_COUNT = "RECOVER_COMMIT_BRANCH_COUNT";
    public static final String RECOVER_ROLLBACK_BRANCH_COUNT = "RECOVER_ROLLBACK_BRANCH_COUNT";
    public static final String SLOW_TRANS_COUNT = "SLOW_TRANS_COUNT";
    public static final String SLOW_TRANS_COUNT_RW = "SLOW_TRANS_COUNT_RW";
    public static final String SLOW_TRANS_COUNT_RO = "SLOW_TRANS_COUNT_RO";
    public static final String SLOW_TRANS_TPS = "SLOW_TRANS_TPS";
    public static final String SLOW_TRANS_TPS_RW = "SLOW_TRANS_TPS_RW";
    public static final String SLOW_TRANS_TPS_RO = "SLOW_TRANS_TPS_RO";
    public static final String SLOW_TRANS_TIME_AVG = "SLOW_TRANS_TIME_AVG";
    public static final String SLOW_TRANS_TIME_AVG_RW = "SLOW_TRANS_TIME_AVG_RW";
    public static final String SLOW_TRANS_TIME_AVG_RO = "SLOW_TRANS_TIME_AVG_RO";
    public static final String SLOW_TRANS_TIME_95P = "SLOW_TRANS_TIME_95P";
    public static final String SLOW_TRANS_TIME_95P_RW = "SLOW_TRANS_TIME_95P_RW";
    public static final String SLOW_TRANS_TIME_95P_RO = "SLOW_TRANS_TIME_95P_RO";
    public static final String SLOW_TRANS_TIME_MAX = "SLOW_TRANS_TIME_MAX";
    public static final String SLOW_TRANS_TIME_MAX_RW = "SLOW_TRANS_TIME_MAX_RW";
    public static final String SLOW_TRANS_TIME_MAX_RO = "SLOW_TRANS_TIME_MAX_RO";
    public static final String SLOW_TRANS_TIME_HISTO = "SLOW_TRANS_TIME_HISTO";
    public static final String CUR_SLOW_TRANS_COUNT = "CUR_SLOW_TRANS_COUNT";
    public static final String CUR_SLOW_TRANS_COUNT_RW = "CUR_SLOW_TRANS_COUNT_RW";
    public static final String CUR_SLOW_TRANS_COUNT_RO = "CUR_SLOW_TRANS_COUNT_RO";
    public static final String CUR_SLOW_TRANS_TIME_AVG = "CUR_SLOW_TRANS_TIME_AVG";
    public static final String CUR_SLOW_TRANS_TIME_AVG_RW = "CUR_SLOW_TRANS_TIME_AVG_RW";
    public static final String CUR_SLOW_TRANS_TIME_AVG_RO = "CUR_SLOW_TRANS_TIME_AVG_RO";
    public static final String CUR_SLOW_TRANS_TIME_95P = "CUR_SLOW_TRANS_TIME_95P";
    public static final String CUR_SLOW_TRANS_TIME_95P_RW = "CUR_SLOW_TRANS_TIME_95P_RW";
    public static final String CUR_SLOW_TRANS_TIME_95P_RO = "CUR_SLOW_TRANS_TIME_95P_RO";
    public static final String CUR_SLOW_TRANS_TIME_MAX = "CUR_SLOW_TRANS_TIME_MAX";
    public static final String CUR_SLOW_TRANS_TIME_MAX_RW = "CUR_SLOW_TRANS_TIME_MAX_RW";
    public static final String CUR_SLOW_TRANS_TIME_MAX_RO = "CUR_SLOW_TRANS_TIME_MAX_RO";
    public static final String CUR_SLOW_TRANS_TIME_HISTO = "CUR_SLOW_TRANS_TIME_HISTO";

    public static final Map<String, ColumnDef> COLUMNS;
    /**
     * Column sorted by index.
     */
    public static final List<ColumnDef> SORTED_COLUMNS;

    public static final int NUM_COLUMN;

    public static class ColumnDef {
        public final String name;

        /**
         * Index of the column presented in the final result.
         * It is also the index of some intermediate result array.
         * Start form 0.
         */
        public final int index;

        public enum ColumnType {
            STRING, LONG, DOUBLE
        }

        public final ColumnType columnType;

        /**
         * Whether this column metric represents a currently running transaction.
         */
        public final boolean currentStat;

        /**
         * How to generate the corresponding column data from ITransactionStatistics.
         */
        public final BiConsumer<ITransactionStatistics, Object[]> dataGenerator;

        /**
         * How to accumulate the corresponding column data into AccumulateStatistics.
         */
        public final BiConsumer<AccumulateStatistics, Object[]> dataAccumulator;

        /**
         * How to extract the corresponding column data from AccumulateStatistics.
         */
        public final BiConsumer<AccumulateStatistics, Object[]> dataExtractor;

        public ColumnDef(String columnName, int index, ColumnType columnType, boolean currentStat,
                         BiConsumer<ITransactionStatistics, Object[]> dataGenerator,
                         BiConsumer<AccumulateStatistics, Object[]> dataAccumulator,
                         BiConsumer<AccumulateStatistics, Object[]> dataExtractor) {
            this.name = columnName;
            this.index = index;
            this.columnType = columnType;
            this.currentStat = currentStat;
            this.dataGenerator = dataGenerator;
            this.dataAccumulator = dataAccumulator;
            this.dataExtractor = dataExtractor;
        }

    }

    static {
        int i = 0;
        COLUMNS = new ImmutableMap.Builder<String, ColumnDef>()
            .put(NAME,
                new ColumnDef(NAME, i++,
                    ColumnDef.ColumnType.STRING,
                    false,
                    (stat, data) -> { /* do nothing */ },
                    (stat, data) -> { /* do nothing */ },
                    (stat, data) -> { /* do nothing */ }
                ))

            .put(TRANS_COUNT_XA,
                new ColumnDef(TRANS_COUNT_XA, i++,
                    ColumnDef.ColumnType.LONG,
                    false,
                    (stat, data) -> data[indexOf(TRANS_COUNT_XA)] = ((SchemaTransactionStatistics) stat).countXA.get(),
                    (stat, data) -> stat.countXA += (Long) data[indexOf(TRANS_COUNT_XA)],
                    (stat, data) -> data[indexOf(TRANS_COUNT_XA)] = stat.countXA
                ))

            .put(TRANS_COUNT_XA_RW,
                new ColumnDef(TRANS_COUNT_XA_RW, i++,
                    ColumnDef.ColumnType.LONG,
                    false,
                    (stat, data) -> data[indexOf(TRANS_COUNT_XA_RW)] =
                        ((SchemaTransactionStatistics) stat).countXARW.get(),
                    (stat, data) -> stat.countXARW += (Long) data[indexOf(TRANS_COUNT_XA_RW)],
                    (stat, data) -> data[indexOf(TRANS_COUNT_XA_RW)] = stat.countXARW
                ))

            .put(TRANS_COUNT_XA_RO,
                new ColumnDef(TRANS_COUNT_XA_RO, i++,
                    ColumnDef.ColumnType.LONG,
                    false,
                    (stat, data) -> data[indexOf(TRANS_COUNT_XA_RO)] =
                        ((SchemaTransactionStatistics) stat).countXA.get()
                            - ((SchemaTransactionStatistics) stat).countXARW.get(),
                    (stat, data) -> stat.countXARO += (Long) data[indexOf(TRANS_COUNT_XA_RO)],
                    (stat, data) -> data[indexOf(TRANS_COUNT_XA_RO)] = stat.countXARO
                ))

            .put(TRANS_COUNT_TSO,
                new ColumnDef(TRANS_COUNT_TSO, i++,
                    ColumnDef.ColumnType.LONG,
                    false,
                    (stat, data) -> data[indexOf(TRANS_COUNT_TSO)] =
                        ((SchemaTransactionStatistics) stat).countTSO.get(),
                    (stat, data) -> stat.countTSO += (Long) data[indexOf(TRANS_COUNT_TSO)],
                    (stat, data) -> data[indexOf(TRANS_COUNT_TSO)] = stat.countTSO
                ))

            .put(TRANS_COUNT_TSO_RW,
                new ColumnDef(TRANS_COUNT_TSO_RW, i++,
                    ColumnDef.ColumnType.LONG,
                    false,
                    (stat, data) -> data[indexOf(TRANS_COUNT_TSO_RW)] =
                        ((SchemaTransactionStatistics) stat).countTSORW.get(),
                    (stat, data) -> stat.countTSORW += (Long) data[indexOf(TRANS_COUNT_TSO_RW)],
                    (stat, data) -> data[indexOf(TRANS_COUNT_TSO_RW)] = stat.countTSORW
                ))

            .put(TRANS_COUNT_TSO_RO,
                new ColumnDef(TRANS_COUNT_TSO_RO, i++,
                    ColumnDef.ColumnType.LONG,
                    false,
                    (stat, data) -> data[indexOf(TRANS_COUNT_TSO_RO)] =
                        ((SchemaTransactionStatistics) stat).countTSO.get()
                            - ((SchemaTransactionStatistics) stat).countTSORW.get(),
                    (stat, data) -> stat.countTSORO += (Long) data[indexOf(TRANS_COUNT_TSO_RO)],
                    (stat, data) -> data[indexOf(TRANS_COUNT_TSO_RO)] = stat.countTSORO
                ))

            .put(TRANS_COUNT_CROSS_GROUP,
                new ColumnDef(TRANS_COUNT_CROSS_GROUP, i++,
                    ColumnDef.ColumnType.LONG,
                    false,
                    (stat, data) -> data[indexOf(TRANS_COUNT_CROSS_GROUP)] =
                        ((SchemaTransactionStatistics) stat).countCrossGroup.get(),
                    (stat, data) -> stat.countCrossGroup += (Long) data[indexOf(TRANS_COUNT_CROSS_GROUP)],
                    (stat, data) -> data[indexOf(TRANS_COUNT_CROSS_GROUP)] = stat.countCrossGroup
                ))

            .put(LOCAL_DEADLOCK_COUNT,
                new ColumnDef(LOCAL_DEADLOCK_COUNT, i++,
                    ColumnDef.ColumnType.LONG,
                    false,
                    (stat, data) -> data[indexOf(LOCAL_DEADLOCK_COUNT)] =
                        ((SchemaTransactionStatistics) stat).countLocalDeadlock.get(),
                    (stat, data) -> stat.countLocalDeadlock += (Long) data[indexOf(LOCAL_DEADLOCK_COUNT)],
                    (stat, data) -> data[indexOf(LOCAL_DEADLOCK_COUNT)] = stat.countLocalDeadlock
                ))

            .put(GLOBAL_DEADLOCK_COUNT,
                new ColumnDef(GLOBAL_DEADLOCK_COUNT, i++,
                    ColumnDef.ColumnType.LONG,
                    false,
                    (stat, data) -> data[indexOf(GLOBAL_DEADLOCK_COUNT)] =
                        ((SchemaTransactionStatistics) stat).countGlobalDeadlock.get(),
                    (stat, data) -> stat.countGlobalDeadlock += (Long) data[indexOf(GLOBAL_DEADLOCK_COUNT)],
                    (stat, data) -> data[indexOf(GLOBAL_DEADLOCK_COUNT)] = stat.countGlobalDeadlock
                ))

            .put(MDL_DEADLOCK_COUNT,
                new ColumnDef(MDL_DEADLOCK_COUNT, i++,
                    ColumnDef.ColumnType.LONG,
                    false,
                    (stat, data) -> data[indexOf(MDL_DEADLOCK_COUNT)] =
                        ((SchemaTransactionStatistics) stat).countMdlDeadlock.get(),
                    (stat, data) -> stat.countMdlDeadlock += (Long) data[indexOf(MDL_DEADLOCK_COUNT)],
                    (stat, data) -> data[indexOf(MDL_DEADLOCK_COUNT)] = stat.countMdlDeadlock
                ))

            .put(COMMIT_ERROR_COUNT,
                new ColumnDef(COMMIT_ERROR_COUNT, i++,
                    ColumnDef.ColumnType.LONG,
                    false,
                    (stat, data) -> data[indexOf(COMMIT_ERROR_COUNT)] =
                        ((SchemaTransactionStatistics) stat).countCommitError.get(),
                    (stat, data) -> stat.countCommitError += (Long) data[indexOf(COMMIT_ERROR_COUNT)],
                    (stat, data) -> data[indexOf(COMMIT_ERROR_COUNT)] = stat.countCommitError
                ))

            .put(ROLLBACK_ERROR_COUNT,
                new ColumnDef(ROLLBACK_ERROR_COUNT, i++,
                    ColumnDef.ColumnType.LONG,
                    false,
                    (stat, data) -> data[indexOf(ROLLBACK_ERROR_COUNT)] =
                        ((SchemaTransactionStatistics) stat).countRollbackError.get(),
                    (stat, data) -> stat.countRollbackError += (Long) data[indexOf(ROLLBACK_ERROR_COUNT)],
                    (stat, data) -> data[indexOf(ROLLBACK_ERROR_COUNT)] = stat.countRollbackError
                ))

            .put(ROLLBACK_COUNT,
                new ColumnDef(ROLLBACK_COUNT, i++,
                    ColumnDef.ColumnType.LONG,
                    false,
                    (stat, data) -> data[indexOf(ROLLBACK_COUNT)] =
                        ((SchemaTransactionStatistics) stat).countRollback.get(),
                    (stat, data) -> stat.countRollback += (Long) data[indexOf(ROLLBACK_COUNT)],
                    (stat, data) -> data[indexOf(ROLLBACK_COUNT)] = stat.countRollback
                ))

            .put(RECOVER_COMMIT_BRANCH_COUNT,
                new ColumnDef(RECOVER_COMMIT_BRANCH_COUNT, i++,
                    ColumnDef.ColumnType.LONG,
                    false,
                    (stat, data) -> data[indexOf(RECOVER_COMMIT_BRANCH_COUNT)] =
                        ((SchemaTransactionStatistics) stat).countRecoverCommit.get(),
                    (stat, data) -> stat.countRecoverCommit += (Long) data[indexOf(RECOVER_COMMIT_BRANCH_COUNT)],
                    (stat, data) -> data[indexOf(RECOVER_COMMIT_BRANCH_COUNT)] = stat.countRecoverCommit
                ))

            .put(RECOVER_ROLLBACK_BRANCH_COUNT,
                new ColumnDef(RECOVER_ROLLBACK_BRANCH_COUNT, i++,
                    ColumnDef.ColumnType.LONG,
                    false,
                    (stat, data) -> data[indexOf(RECOVER_ROLLBACK_BRANCH_COUNT)] =
                        ((SchemaTransactionStatistics) stat).countRecoverRollback.get(),
                    (stat, data) -> stat.countRecoverRollback += (Long) data[indexOf(RECOVER_ROLLBACK_BRANCH_COUNT)],
                    (stat, data) -> data[indexOf(RECOVER_ROLLBACK_BRANCH_COUNT)] = stat.countRecoverRollback
                ))

            .put(SLOW_TRANS_COUNT,
                new ColumnDef(SLOW_TRANS_COUNT, i++,
                    ColumnDef.ColumnType.LONG,
                    false,
                    (stat, data) -> data[indexOf(SLOW_TRANS_COUNT)] =
                        ((SchemaTransactionStatistics) stat).summary.summary.count,
                    (stat, data) -> stat.countSlow += (Long) data[indexOf(SLOW_TRANS_COUNT)],
                    (stat, data) -> data[indexOf(SLOW_TRANS_COUNT)] = stat.countSlow
                ))

            .put(SLOW_TRANS_COUNT_RW,
                new ColumnDef(SLOW_TRANS_COUNT_RW, i++,
                    ColumnDef.ColumnType.LONG,
                    false,
                    (stat, data) -> data[indexOf(SLOW_TRANS_COUNT_RW)] =
                        ((SchemaTransactionStatistics) stat).summary.summaryRW.count,
                    (stat, data) -> stat.countSlowRW += (Long) data[indexOf(SLOW_TRANS_COUNT_RW)],
                    (stat, data) -> data[indexOf(SLOW_TRANS_COUNT_RW)] = stat.countSlowRW
                ))

            .put(SLOW_TRANS_COUNT_RO,
                new ColumnDef(SLOW_TRANS_COUNT_RO, i++,
                    ColumnDef.ColumnType.LONG,
                    false,
                    (stat, data) -> data[indexOf(SLOW_TRANS_COUNT_RO)] =
                        ((SchemaTransactionStatistics) stat).summary.summaryRO.count,
                    (stat, data) -> stat.countSlowRO += (Long) data[indexOf(SLOW_TRANS_COUNT_RO)],
                    (stat, data) -> data[indexOf(SLOW_TRANS_COUNT_RO)] = stat.countSlowRO
                ))

            .put(SLOW_TRANS_TPS,
                new ColumnDef(SLOW_TRANS_TPS, i++,
                    ColumnDef.ColumnType.DOUBLE,
                    false,
                    (stat, data) -> data[indexOf(SLOW_TRANS_TPS)] =
                        ((SchemaTransactionStatistics) stat).summary.summary.tps,
                    (stat, data) -> stat.tpsSlow += (Double) data[indexOf(SLOW_TRANS_TPS)],
                    (stat, data) -> data[indexOf(SLOW_TRANS_TPS)] = stat.tpsSlow
                ))

            .put(SLOW_TRANS_TPS_RW,
                new ColumnDef(SLOW_TRANS_TPS_RW, i++,
                    ColumnDef.ColumnType.DOUBLE,
                    false,
                    (stat, data) -> data[indexOf(SLOW_TRANS_TPS_RW)] =
                        ((SchemaTransactionStatistics) stat).summary.summaryRW.tps,
                    (stat, data) -> stat.tpsSlowRW += (Double) data[indexOf(SLOW_TRANS_TPS_RW)],
                    (stat, data) -> data[indexOf(SLOW_TRANS_TPS_RW)] = stat.tpsSlowRW
                ))

            .put(SLOW_TRANS_TPS_RO,
                new ColumnDef(SLOW_TRANS_TPS_RO, i++,
                    ColumnDef.ColumnType.DOUBLE,
                    false,
                    (stat, data) -> data[indexOf(SLOW_TRANS_TPS_RO)] =
                        ((SchemaTransactionStatistics) stat).summary.summaryRO.tps,
                    (stat, data) -> stat.tpsSlowRO += (Double) data[indexOf(SLOW_TRANS_TPS_RO)],
                    (stat, data) -> data[indexOf(SLOW_TRANS_TPS_RO)] = stat.tpsSlowRO
                ))

            .put(SLOW_TRANS_TIME_AVG,
                new ColumnDef(SLOW_TRANS_TIME_AVG, i++,
                    ColumnDef.ColumnType.LONG,
                    false,
                    // Convert time in millisecond.
                    (stat, data) -> data[indexOf(SLOW_TRANS_TIME_AVG)] =
                        ((SchemaTransactionStatistics) stat).summary.summary.avg[DURATION_TIME] / 1000,
                    (stat, data) -> stat.sumTime +=
                        (Long) data[indexOf(SLOW_TRANS_TIME_AVG)] * (Long) data[indexOf(SLOW_TRANS_COUNT)],
                    (stat, data) -> data[indexOf(SLOW_TRANS_TIME_AVG)]
                        = stat.sumTime / Math.max(1, stat.countSlow)
                ))

            .put(SLOW_TRANS_TIME_AVG_RW,
                new ColumnDef(SLOW_TRANS_TIME_AVG_RW, i++,
                    ColumnDef.ColumnType.LONG,
                    false,
                    // Convert time in millisecond.
                    (stat, data) -> data[indexOf(SLOW_TRANS_TIME_AVG_RW)] =
                        ((SchemaTransactionStatistics) stat).summary.summaryRW.avg[DURATION_TIME] / 1000,
                    (stat, data) -> stat.sumTimeRW +=
                        (Long) data[indexOf(SLOW_TRANS_TIME_AVG_RW)] * (Long) data[indexOf(SLOW_TRANS_COUNT_RW)],
                    (stat, data) -> data[indexOf(SLOW_TRANS_TIME_AVG_RW)]
                        = stat.sumTimeRW / Math.max(1, stat.countSlowRW)
                ))

            .put(SLOW_TRANS_TIME_AVG_RO,
                new ColumnDef(SLOW_TRANS_TIME_AVG_RO, i++,
                    ColumnDef.ColumnType.LONG,
                    false,
                    // Convert time in millisecond.
                    (stat, data) -> data[indexOf(SLOW_TRANS_TIME_AVG_RO)] =
                        ((SchemaTransactionStatistics) stat).summary.summaryRO.avg[DURATION_TIME] / 1000,
                    (stat, data) -> stat.sumTimeRO +=
                        (Long) data[indexOf(SLOW_TRANS_TIME_AVG_RO)] * (Long) data[indexOf(SLOW_TRANS_COUNT_RO)],
                    (stat, data) -> data[indexOf(SLOW_TRANS_TIME_AVG_RO)]
                        = stat.sumTimeRO / Math.max(1, stat.countSlowRO)
                ))

            .put(SLOW_TRANS_TIME_95P,
                new ColumnDef(SLOW_TRANS_TIME_95P, i++,
                    ColumnDef.ColumnType.LONG,
                    false,
                    // Convert time in millisecond.
                    (stat, data) -> data[indexOf(SLOW_TRANS_TIME_95P)] =
                        ((SchemaTransactionStatistics) stat).summary.summary.p95[DURATION_TIME] / 1000,
                    (stat, data) -> stat.sumP95Time +=
                        (Long) data[indexOf(SLOW_TRANS_TIME_95P)]
                            * (Long) data[indexOf(SLOW_TRANS_COUNT)],
                    (stat, data) -> data[indexOf(SLOW_TRANS_TIME_95P)]
                        = stat.sumP95Time / Math.max(1, stat.countSlow)
                ))

            .put(SLOW_TRANS_TIME_95P_RW,
                new ColumnDef(SLOW_TRANS_TIME_95P_RW, i++,
                    ColumnDef.ColumnType.LONG,
                    false,
                    // Convert time in millisecond.
                    (stat, data) -> data[indexOf(SLOW_TRANS_TIME_95P_RW)] =
                        ((SchemaTransactionStatistics) stat).summary.summaryRW.p95[DURATION_TIME] / 1000,
                    (stat, data) -> stat.sumP95TimeRW +=
                        (Long) data[indexOf(SLOW_TRANS_TIME_95P_RW)]
                            * (Long) data[indexOf(SLOW_TRANS_COUNT_RW)],
                    (stat, data) -> data[indexOf(SLOW_TRANS_TIME_95P_RW)]
                        = stat.sumP95TimeRW / Math.max(1, stat.countSlowRW)
                ))

            .put(SLOW_TRANS_TIME_95P_RO,
                new ColumnDef(SLOW_TRANS_TIME_95P_RO, i++,
                    ColumnDef.ColumnType.LONG,
                    false,
                    // Convert time in millisecond.
                    (stat, data) -> data[indexOf(SLOW_TRANS_TIME_95P_RO)] =
                        ((SchemaTransactionStatistics) stat).summary.summaryRO.p95[DURATION_TIME] / 1000,
                    (stat, data) -> stat.sumP95TimeRO +=
                        (Long) data[indexOf(SLOW_TRANS_TIME_95P_RO)]
                            * (Long) data[indexOf(SLOW_TRANS_COUNT_RO)],
                    (stat, data) -> data[indexOf(SLOW_TRANS_TIME_95P_RO)]
                        = stat.sumP95TimeRO / Math.max(1, stat.countSlowRO)
                ))

            .put(SLOW_TRANS_TIME_MAX,
                new ColumnDef(SLOW_TRANS_TIME_MAX, i++,
                    ColumnDef.ColumnType.LONG,
                    false,
                    // Convert time in millisecond.
                    (stat, data) -> data[indexOf(SLOW_TRANS_TIME_MAX)] =
                        ((SchemaTransactionStatistics) stat).summary.summary.max[DURATION_TIME] / 1000,
                    (stat, data) -> stat.maxTime =
                        Math.max(stat.maxTime, (Long) data[indexOf(SLOW_TRANS_TIME_MAX)]),
                    (stat, data) -> data[indexOf(SLOW_TRANS_TIME_MAX)] = stat.maxTime
                ))

            .put(SLOW_TRANS_TIME_MAX_RW,
                new ColumnDef(SLOW_TRANS_TIME_MAX_RW, i++,
                    ColumnDef.ColumnType.LONG,
                    false,
                    // Convert time in millisecond.
                    (stat, data) -> data[indexOf(SLOW_TRANS_TIME_MAX_RW)] =
                        ((SchemaTransactionStatistics) stat).summary.summaryRW.max[DURATION_TIME] / 1000,
                    (stat, data) -> stat.maxTimeRW =
                        Math.max(stat.maxTimeRW, (Long) data[indexOf(SLOW_TRANS_TIME_MAX_RW)]),
                    (stat, data) -> data[indexOf(SLOW_TRANS_TIME_MAX_RW)] = stat.maxTimeRW
                ))

            .put(SLOW_TRANS_TIME_MAX_RO,
                new ColumnDef(SLOW_TRANS_TIME_MAX_RO, i++,
                    ColumnDef.ColumnType.LONG,
                    false,
                    // Convert time in millisecond.
                    (stat, data) -> data[indexOf(SLOW_TRANS_TIME_MAX_RO)] =
                        ((SchemaTransactionStatistics) stat).summary.summaryRO.max[DURATION_TIME] / 1000,
                    (stat, data) -> stat.maxTimeRO =
                        Math.max(stat.maxTimeRO, (Long) data[indexOf(SLOW_TRANS_TIME_MAX_RO)]),
                    (stat, data) -> data[indexOf(SLOW_TRANS_TIME_MAX_RO)] = stat.maxTimeRO
                ))

            .put(SLOW_TRANS_TIME_HISTO,
                new ColumnDef(SLOW_TRANS_TIME_HISTO, i++,
                    ColumnDef.ColumnType.STRING,
                    false,
                    (stat, data) -> data[indexOf(SLOW_TRANS_TIME_HISTO)] =
                        histogramToString(((SchemaTransactionStatistics) stat).summary.histogram,
                            ((SchemaTransactionStatistics) stat).summary.SLOW_TRANS_THRESHOLD),
                    (stat, data) -> processHistogram(stat.histogram, (String) data[indexOf(SLOW_TRANS_TIME_HISTO)]),
                    (stat, data) -> data[indexOf(SLOW_TRANS_TIME_HISTO)] =
                        histogramToString(stat.histogram, stat.threshold)
                ))

            .put(CUR_SLOW_TRANS_COUNT,
                new ColumnDef(CUR_SLOW_TRANS_COUNT, i++,
                    ColumnDef.ColumnType.LONG,
                    true,
                    (stat, data) -> data[indexOf(CUR_SLOW_TRANS_COUNT)] =
                        ((CurrentTransactionStatistics) stat).countSlow,
                    (stat, data) -> stat.countSlow += (Long) data[indexOf(CUR_SLOW_TRANS_COUNT)],
                    (stat, data) -> data[indexOf(CUR_SLOW_TRANS_COUNT)] = stat.countSlow
                ))

            .put(CUR_SLOW_TRANS_COUNT_RW,
                new ColumnDef(CUR_SLOW_TRANS_COUNT_RW, i++,
                    ColumnDef.ColumnType.LONG,
                    true,
                    (stat, data) -> data[indexOf(CUR_SLOW_TRANS_COUNT_RW)] =
                        ((CurrentTransactionStatistics) stat).countSlowRW,
                    (stat, data) -> stat.countSlowRW += (Long) data[indexOf(CUR_SLOW_TRANS_COUNT_RW)],
                    (stat, data) -> data[indexOf(CUR_SLOW_TRANS_COUNT_RW)] = stat.countSlowRW
                ))

            .put(CUR_SLOW_TRANS_COUNT_RO,
                new ColumnDef(CUR_SLOW_TRANS_COUNT_RO, i++,
                    ColumnDef.ColumnType.LONG,
                    true,
                    (stat, data) -> data[indexOf(CUR_SLOW_TRANS_COUNT_RO)] =
                        ((CurrentTransactionStatistics) stat).countSlowRO,
                    (stat, data) -> stat.countSlowRO += (Long) data[indexOf(CUR_SLOW_TRANS_COUNT_RO)],
                    (stat, data) -> data[indexOf(CUR_SLOW_TRANS_COUNT_RO)] = stat.countSlowRO
                ))

            .put(CUR_SLOW_TRANS_TIME_AVG,
                new ColumnDef(CUR_SLOW_TRANS_TIME_AVG, i++,
                    ColumnDef.ColumnType.LONG,
                    true,
                    (stat, data) -> data[indexOf(CUR_SLOW_TRANS_TIME_AVG)] =
                        avg(((CurrentTransactionStatistics) stat).durationTime),
                    (stat, data) -> stat.sumTime +=
                        (Long) data[indexOf(CUR_SLOW_TRANS_TIME_AVG)]
                            * (Long) data[indexOf(CUR_SLOW_TRANS_COUNT)],
                    (stat, data) -> data[indexOf(CUR_SLOW_TRANS_TIME_AVG)]
                        = stat.sumTime / Math.max(1, stat.countSlow)
                ))

            .put(CUR_SLOW_TRANS_TIME_AVG_RW,
                new ColumnDef(CUR_SLOW_TRANS_TIME_AVG_RW, i++,
                    ColumnDef.ColumnType.LONG,
                    true,
                    (stat, data) -> data[indexOf(CUR_SLOW_TRANS_TIME_AVG_RW)] =
                        avg(((CurrentTransactionStatistics) stat).durationTimeRW),
                    (stat, data) -> stat.sumTimeRW +=
                        (Long) data[indexOf(CUR_SLOW_TRANS_TIME_AVG_RW)]
                            * (Long) data[indexOf(CUR_SLOW_TRANS_COUNT_RW)],
                    (stat, data) -> data[indexOf(CUR_SLOW_TRANS_TIME_AVG_RW)] =
                        stat.sumTimeRW / Math.max(1, stat.countSlowRW)
                ))

            .put(CUR_SLOW_TRANS_TIME_AVG_RO,
                new ColumnDef(CUR_SLOW_TRANS_TIME_AVG_RO, i++,
                    ColumnDef.ColumnType.LONG,
                    true,
                    (stat, data) -> data[indexOf(CUR_SLOW_TRANS_TIME_AVG_RO)] =
                        avg(((CurrentTransactionStatistics) stat).durationTimeRO),
                    (stat, data) -> stat.sumTimeRO +=
                        (Long) data[indexOf(CUR_SLOW_TRANS_TIME_AVG_RO)]
                            * (Long) data[indexOf(CUR_SLOW_TRANS_COUNT_RO)],
                    (stat, data) -> data[indexOf(CUR_SLOW_TRANS_TIME_AVG_RO)] =
                        stat.sumTimeRO / Math.max(1, stat.countSlowRO)
                ))

            .put(CUR_SLOW_TRANS_TIME_95P,
                new ColumnDef(CUR_SLOW_TRANS_TIME_95P, i++,
                    ColumnDef.ColumnType.LONG,
                    true,
                    (stat, data) -> data[indexOf(CUR_SLOW_TRANS_TIME_95P)] =
                        p95(((CurrentTransactionStatistics) stat).durationTime),
                    (stat, data) -> stat.sumP95Time +=
                        (Long) data[indexOf(CUR_SLOW_TRANS_TIME_95P)]
                            * (Long) data[indexOf(CUR_SLOW_TRANS_COUNT)],
                    (stat, data) -> data[indexOf(CUR_SLOW_TRANS_TIME_95P)]
                        = stat.sumP95Time / Math.max(1, stat.countSlow)
                ))

            .put(CUR_SLOW_TRANS_TIME_95P_RW,
                new ColumnDef(CUR_SLOW_TRANS_TIME_95P_RW, i++,
                    ColumnDef.ColumnType.LONG,
                    true,
                    (stat, data) -> data[indexOf(CUR_SLOW_TRANS_TIME_95P_RW)] =
                        p95(((CurrentTransactionStatistics) stat).durationTimeRW),
                    (stat, data) -> stat.sumP95TimeRW +=
                        (Long) data[indexOf(CUR_SLOW_TRANS_TIME_95P_RW)]
                            * (Long) data[indexOf(CUR_SLOW_TRANS_COUNT_RW)],
                    (stat, data) -> data[indexOf(CUR_SLOW_TRANS_TIME_95P_RW)]
                        = stat.sumP95TimeRW / Math.max(1, stat.countSlowRW)
                ))

            .put(CUR_SLOW_TRANS_TIME_95P_RO,
                new ColumnDef(CUR_SLOW_TRANS_TIME_95P_RO, i++,
                    ColumnDef.ColumnType.LONG,
                    true,
                    (stat, data) -> data[indexOf(CUR_SLOW_TRANS_TIME_95P_RO)] =
                        p95(((CurrentTransactionStatistics) stat).durationTimeRO),
                    (stat, data) -> stat.sumP95TimeRO +=
                        (Long) data[indexOf(CUR_SLOW_TRANS_TIME_95P_RO)]
                            * (Long) data[indexOf(CUR_SLOW_TRANS_COUNT_RO)],
                    (stat, data) -> data[indexOf(CUR_SLOW_TRANS_TIME_95P_RO)]
                        = stat.sumP95TimeRO / Math.max(1, stat.countSlowRO)
                ))

            .put(CUR_SLOW_TRANS_TIME_MAX,
                new ColumnDef(CUR_SLOW_TRANS_TIME_MAX, i++,
                    ColumnDef.ColumnType.LONG,
                    true,
                    (stat, data) -> data[indexOf(CUR_SLOW_TRANS_TIME_MAX)] =
                        max(((CurrentTransactionStatistics) stat).durationTime),
                    (stat, data) -> stat.maxTime =
                        Math.max(stat.maxTime, (Long) data[indexOf(CUR_SLOW_TRANS_TIME_MAX)]),
                    (stat, data) -> data[indexOf(CUR_SLOW_TRANS_TIME_MAX)] = stat.maxTime
                ))

            .put(CUR_SLOW_TRANS_TIME_MAX_RW,
                new ColumnDef(CUR_SLOW_TRANS_TIME_MAX_RW, i++,
                    ColumnDef.ColumnType.LONG,
                    true,
                    (stat, data) -> data[indexOf(CUR_SLOW_TRANS_TIME_MAX_RW)] =
                        max(((CurrentTransactionStatistics) stat).durationTimeRW),
                    (stat, data) -> stat.maxTimeRW =
                        Math.max(stat.maxTimeRW, (Long) data[indexOf(CUR_SLOW_TRANS_TIME_MAX_RW)]),
                    (stat, data) -> data[indexOf(CUR_SLOW_TRANS_TIME_MAX_RW)] = stat.maxTimeRW
                ))

            .put(CUR_SLOW_TRANS_TIME_MAX_RO,
                new ColumnDef(CUR_SLOW_TRANS_TIME_MAX_RO, i++,
                    ColumnDef.ColumnType.LONG,
                    true,
                    (stat, data) -> data[indexOf(CUR_SLOW_TRANS_TIME_MAX_RO)] =
                        max(((CurrentTransactionStatistics) stat).durationTimeRO),
                    (stat, data) -> stat.maxTimeRO =
                        Math.max(stat.maxTimeRO, (Long) data[indexOf(CUR_SLOW_TRANS_TIME_MAX_RO)]),
                    (stat, data) -> data[indexOf(CUR_SLOW_TRANS_TIME_MAX_RO)] = stat.maxTimeRO))

            .put(CUR_SLOW_TRANS_TIME_HISTO,
                new ColumnDef(CUR_SLOW_TRANS_TIME_HISTO, i++,
                    ColumnDef.ColumnType.STRING,
                    true,
                    (stat, data) -> data[indexOf(CUR_SLOW_TRANS_TIME_HISTO)] = generateHistogram(
                        ((CurrentTransactionStatistics) stat).durationTime,
                        DynamicConfig.getInstance().getSlowTransThreshold()),
                    (stat, data) -> processHistogram(stat.histogram, (String) data[indexOf(CUR_SLOW_TRANS_TIME_HISTO)]),
                    (stat, data) -> data[indexOf(CUR_SLOW_TRANS_TIME_HISTO)] =
                        histogramToString(stat.histogram, stat.threshold)
                ))

            .build();

        SORTED_COLUMNS =
            COLUMNS.values().stream().sorted(Comparator.comparingInt(o -> o.index)).collect(Collectors.toList());

        NUM_COLUMN = SORTED_COLUMNS.size();
    }

    private static void processHistogram(double[] histogram, String histogramStr) {
        final String[] split = histogramStr.split("#");
        assert split.length == HISTOGRAM_BUCKET_SIZE + 1;
        for (int i = 1; i < split.length; i++) {
            histogram[i - 1] += Double.parseDouble(split[i]);
        }
    }

    private static String generateHistogram(List<Long> list, long threshold) {
        final double[] histogram = new double[HISTOGRAM_BUCKET_SIZE];
        for (Long i : list) {
            calculateHistogram(histogram, threshold, i);
        }
        return histogramToString(histogram, threshold);
    }

    public static int indexOf(String columnName) {
        return COLUMNS.get(columnName).index;
    }
}
