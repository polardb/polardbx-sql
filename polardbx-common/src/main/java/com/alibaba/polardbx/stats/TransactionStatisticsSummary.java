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

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;

/**
 * @author yaozhili
 */
public class TransactionStatisticsSummary {
    /**
     * Only consider the following metrics:
     */
    public static int METRIC_COUNT = 0;
    public static final int DURATION_TIME = METRIC_COUNT++;
    public static final int ACTIVE_TIME = METRIC_COUNT++;
    public static final int IDLE_TIME = METRIC_COUNT++;
    public static final int MDL_WAIT_TIME = METRIC_COUNT++;
    public static final int GET_TSO_TIME = METRIC_COUNT++;
    public static final int PREPARE_TIME = METRIC_COUNT++;
    public static final int TRX_LOG_TIME = METRIC_COUNT++;
    public static final int COMMIT_TIME = METRIC_COUNT++;
    public static final int ROLLBACK_TIME = METRIC_COUNT++;
    public static final int READ_TIME = METRIC_COUNT++;
    public static final int READ_RETURN_ROWS = METRIC_COUNT++;
    public static final int WRITE_TIME = METRIC_COUNT++;
    public static final int WRITE_AFFECT_ROWS = METRIC_COUNT++;
    public static final int SQL_COUNT = METRIC_COUNT++;
    public static final int RW_SQL_COUNT = METRIC_COUNT++;

    public final static int HISTOGRAM_BUCKET_SIZE = 5;

    public final Summary summary = new Summary();
    public final Summary summaryRW = new Summary();
    public final Summary summaryRO = new Summary();

    /**
     * Summary all slow transactions finish in [left, right] in millisecond.
     */
    public long left;
    public long right;

    public long startTimeMs = Long.MAX_VALUE;
    public long finishTimeMs = 0;

    /**
     * Histogram of transaction duration time.
     */
    public final double[] histogram = new double[HISTOGRAM_BUCKET_SIZE];

    public final long SLOW_TRANS_THRESHOLD = DynamicConfig.getInstance().getSlowTransThreshold();

    public void init(long l, long r) {
        summary.init();
        summaryRW.init();
        summaryRO.init();
        left = l;
        right = r;
    }

    public void offer(TransactionStatistics stat) {
        startTimeMs = Math.min(startTimeMs, stat.startTimeInMs);
        finishTimeMs = Math.max(finishTimeMs, stat.finishTimeInMs);

        summary.offer(stat);
        if (stat.readOnly) {
            summaryRO.offer(stat);
        } else {
            summaryRW.offer(stat);
        }
    }

    public void calculate() {
        int diffInSecond = Math.max(1, (int) ((right - left) / 1000));

        Workspace.processHistogram(histogram, summary.workspace, SLOW_TRANS_THRESHOLD);
        for (int i = 0; i < HISTOGRAM_BUCKET_SIZE; i++) {
            histogram[i] = histogram[i] / diffInSecond;
        }

        summary.calculate(diffInSecond);
        summaryRW.calculate(diffInSecond);
        summaryRO.calculate(diffInSecond);
    }

    public static class Summary {
        public final long[] avg = new long[METRIC_COUNT];
        public final long[] max = new long[METRIC_COUNT];
        public final long[] p95 = new long[METRIC_COUNT];
        public double tps = 0;
        public long count = 0;

        private boolean calculated = false;

        private Workspace workspace = null;

        public void init() {
            workspace = new Workspace();
        }

        public void offer(TransactionStatistics stat) {
            if (null == workspace) {
                return;
            }
            workspace.offer(stat);
            count++;
        }

        public void calculate(int diffInSecond) {
            if (null == workspace || 0 == count || calculated) {
                workspace = null;
                return;
            }
            calculated = true;

            workspace.generateSummary(avg, max, p95);
            workspace = null;
            tps = ((double) (count)) / diffInSecond;
        }
    }

    public static class Workspace {
        final List<List<Long>> cache = new ArrayList<>();

        public Workspace() {
            for (int i = 0; i < METRIC_COUNT; i++) {
                cache.add(new ArrayList<>());
            }
        }

        public void offer(TransactionStatistics stat) {
            // Convert time unit to millisecond.
            cache.get(DURATION_TIME).add(stat.durationTime / 1000);
            cache.get(ACTIVE_TIME).add(stat.activeTime / 1000);
            cache.get(IDLE_TIME).add(stat.idleTime / 1000);
            cache.get(MDL_WAIT_TIME).add(stat.mdlWaitTime / 1000);
            cache.get(GET_TSO_TIME).add(stat.getTsoTime / 1000);
            cache.get(PREPARE_TIME).add(stat.prepareTime / 1000);
            cache.get(COMMIT_TIME).add(stat.commitTime / 1000);
            cache.get(TRX_LOG_TIME).add(stat.trxLogTime / 1000);
            cache.get(ROLLBACK_TIME).add(stat.rollbackTime / 1000);
            cache.get(READ_TIME).add(stat.readTime / 1000);
            cache.get(READ_RETURN_ROWS).add(stat.readReturnRows);
            cache.get(WRITE_TIME).add(stat.writeTime / 1000);
            cache.get(WRITE_AFFECT_ROWS).add(stat.writeAffectRows);
            cache.get(SQL_COUNT).add(stat.sqlCount);
            cache.get(RW_SQL_COUNT).add(stat.rwSqlCount);
        }

        public void generateSummary(long[] avg, long[] max, long[] p95) {
            cache.forEach(s -> s.sort(null));

            for (int i = 0; i < METRIC_COUNT; i++) {
                avg[i] = avg(cache.get(i));
                max[i] = max(cache.get(i));
                p95[i] = p95(cache.get(i));
            }
        }

        public static void processHistogram(double[] histogram, Workspace workspace, long threshold) {
            if (null == workspace) {
                return;
            }

            List<Long> durationTimeList = workspace.cache.get(DURATION_TIME);
            for (Long durationTime : durationTimeList) {
                // Convert duration time to millisecond.
                calculateHistogram(histogram, threshold, durationTime / 1000);
            }
        }

        public static void calculateHistogram(double[] histogram, long threshold, long durationTime) {
            double diffInSecond = (double) (durationTime - threshold) / 1000;
            // Bucket width: 4s, 8s, 16s, 32s, otherwise.
            int index = (int) (Math.log(diffInSecond) / Math.log(2)) - 1;
            index = Math.min(HISTOGRAM_BUCKET_SIZE - 1, Math.max(0, index));
            histogram[index] = histogram[index] + 1;
        }

        public static String histogramToString(double[] histogram, long threshold) {
            final StringBuilder sb = new StringBuilder();
            sb.append(threshold / 1000);
            DecimalFormat df = new DecimalFormat("#.###");
            for (Double i : histogram) {
                sb.append("#").append(df.format(i));
            }
            return sb.toString();
        }

        public static long avg(List<Long> list) {
            return (long) (list.stream().mapToLong(Long::longValue).average().orElse(0));
        }

        public static long max(List<Long> list) {
            return list.isEmpty() ? 0 : list.get(list.size() - 1);
        }

        public static long p95(List<Long> list) {
            return list.isEmpty() ? 0 : list.get((int) ((list.size() - 1) * 0.95));
        }
    }

}
