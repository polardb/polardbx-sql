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

package com.alibaba.polardbx.transaction.async;

import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.utils.OptimizerHelper;
import com.alibaba.polardbx.stats.SchemaTransactionStatistics;
import com.alibaba.polardbx.stats.MatrixStatistics;
import com.alibaba.polardbx.stats.TransactionStatistics;
import com.alibaba.polardbx.stats.TransactionStatisticsSummary;

import java.util.Optional;

/**
 * @author yaozhili
 */
public class TransactionStatisticsTask implements Runnable {

    private long lastCollectTimeMs = 0;

    @Override
    public void run() {
        long currentTimeMs = System.currentTimeMillis();

        try {
            final TransactionStatisticsSummary instanceSummary = new TransactionStatisticsSummary();
            instanceSummary.init(lastCollectTimeMs == 0 ? currentTimeMs - 5000 : lastCollectTimeMs, currentTimeMs);

            final SchemaTransactionStatistics instanceStat = new SchemaTransactionStatistics();

            for (String schema : OptimizerHelper.getServerConfigManager().getLoadedSchemas()) {
                Optional.ofNullable(OptimizerContext.getTransStat(schema)).ifPresent(s -> {
                    updateInstanceStatistics(instanceStat, s);

                    final TransactionStatisticsSummary summary = new TransactionStatisticsSummary();

                    summary.init(lastCollectTimeMs == 0 ? currentTimeMs - 5000 : lastCollectTimeMs, currentTimeMs);

                    TransactionStatistics stat;
                    while (true) {
                        stat = s.slowTransStats.poll();

                        if (null == stat) {
                            // Empty queue.
                            break;
                        }

                        MatrixStatistics.cachedTransactionStatsCount.decrementAndGet();

                        summary.offer(stat);
                        instanceSummary.offer(stat);

                        if (stat.finishTimeInMs >= currentTimeMs) {
                            // End of this round.
                            break;
                        }
                    }

                    summary.calculate();

                    s.summary = summary;
                });
            }

            instanceSummary.calculate();
            instanceStat.summary = instanceSummary;
            MatrixStatistics.instanceTransactionStats = instanceStat;
        } finally {
            lastCollectTimeMs = currentTimeMs;
        }
    }

    private static void updateInstanceStatistics(SchemaTransactionStatistics instanceStat,
                                                 SchemaTransactionStatistics s) {
        instanceStat.countXA.addAndGet(s.countXA.get());
        instanceStat.countXARW.addAndGet(s.countXARW.get());
        instanceStat.countTSO.addAndGet(s.countTSO.get());
        instanceStat.countTSORW.addAndGet(s.countTSORW.get());
        instanceStat.countCrossGroup.addAndGet(s.countCrossGroup.get());
        instanceStat.countLocalDeadlock.addAndGet(s.countLocalDeadlock.get());
        instanceStat.countGlobalDeadlock.addAndGet(s.countGlobalDeadlock.get());
        instanceStat.countMdlDeadlock.addAndGet(s.countMdlDeadlock.get());
        instanceStat.countCommitError.addAndGet(s.countCommitError.get());
        instanceStat.countRollbackError.addAndGet(s.countRollbackError.get());
        instanceStat.countRollback.addAndGet(s.countRollback.get());
        instanceStat.countRecoverCommit.addAndGet(s.countRecoverCommit.get());
        instanceStat.countRecoverRollback.addAndGet(s.countRecoverRollback.get());
    }
}
