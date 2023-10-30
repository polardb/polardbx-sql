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

import static com.alibaba.polardbx.stats.TransactionStatisticsSummary.HISTOGRAM_BUCKET_SIZE;

/**
 * @author yaozhili
 */
public class AccumulateStatistics {
    public long countXA = 0L;
    public long countXARW = 0L;
    public long countXARO = 0L;
    public long countTSO = 0L;
    public long countTSORW = 0L;
    public long countTSORO = 0L;
    public long countCrossGroup = 0L;
    public long countLocalDeadlock = 0L;
    public long countGlobalDeadlock = 0L;
    public long countMdlDeadlock = 0L;
    public long countCommitError = 0L;
    public long countRollbackError = 0L;
    public long countRollback = 0L;
    public long countRecoverCommit = 0L;
    public long countRecoverRollback = 0L;
    public long countSlow = 0L;
    public long countSlowRW = 0L;
    public long countSlowRO = 0L;
    public double tpsSlow = 0D;
    public double tpsSlowRW = 0D;
    public double tpsSlowRO = 0D;
    public long sumTime = 0L;
    public long sumTimeRW = 0L;
    public long sumTimeRO = 0L;
    public long sumP95Time = 0L;
    public long sumP95TimeRW = 0L;
    public long sumP95TimeRO = 0L;
    public long maxTime = 0L;
    public long maxTimeRW = 0L;
    public long maxTimeRO = 0L;
    public double[] histogram = new double[HISTOGRAM_BUCKET_SIZE];
    public long threshold = DynamicConfig.getInstance().getSlowTransThreshold();
}
