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

package com.alibaba.polardbx.optimizer.optimizeralert;

import com.alibaba.polardbx.common.model.lifecycle.AbstractLifecycle;
import com.alibaba.polardbx.common.properties.DynamicConfig;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.optimizeralert.statisticalert.StatisticAlertLoggerBaseImpl;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

import java.util.List;
import java.util.Map;

public class OptimizerAlertManager extends AbstractLifecycle {
    private static final OptimizerAlertManager INSTANCE = new OptimizerAlertManager();

    private Map<OptimizerAlertType, OptimizerAlertLogger> optimizerAlertLoggers;

    public static OptimizerAlertManager getInstance() {
        if (!INSTANCE.isInited()) {
            synchronized (INSTANCE) {
                if (!INSTANCE.isInited()) {
                    INSTANCE.init();
                }
            }
        }
        return INSTANCE;
    }

    @Override
    protected void doInit() {
        super.doInit();

        optimizerAlertLoggers = OptimizerAlertManager.createLogger();
    }

    @Override
    protected void doDestroy() {
        super.doDestroy();
        optimizerAlertLoggers = null;
    }

    /**
     * core entry to log alert
     *
     * @param optimizerAlertType type of alerts
     * @param ec Context need to print log
     * @return true if logTrace if printed
     */
    public boolean log(OptimizerAlertType optimizerAlertType, ExecutionContext ec) {
        return log(optimizerAlertType, ec, null);
    }

    /**
     * core entry to log alert
     *
     * @param optimizerAlertType type of alerts
     * @param ec Context need to print log
     * @param extra extra info to print log
     * @return true if logTrace if printed
     */
    public boolean log(OptimizerAlertType optimizerAlertType, ExecutionContext ec, Object extra) {
        return log(optimizerAlertType, ec, extra, null);
    }

    public boolean log(OptimizerAlertType optimizerAlertType, ExecutionContext ec, Object extra, Map<String, Object> extraMap) {
        if (!DynamicConfig.getInstance().optimizerAlert()) {
            return false;
        }
        OptimizerAlertLogger logger = optimizerAlertLoggers.get(optimizerAlertType);
        if (logger == null) {
            return false;
        }
        logger.inc();

        if (!DynamicConfig.getInstance().optimizerAlertLog()) {
            return false;
        }
        return logger.logDetail(ec, extra, extraMap);
    }

    synchronized public List<Pair<OptimizerAlertType, Long>> collectByScheduleJob() {
        List<Pair<OptimizerAlertType, Long>> result = Lists.newArrayList();
        for (OptimizerAlertLogger logger : optimizerAlertLoggers.values()) {
            Pair<OptimizerAlertType, Long> collect = logger.collectByScheduleJob();
            if (collect != null) {
                result.add(collect);
            }
        }
        return result;
    }

    public List<Pair<OptimizerAlertType, Long>> collectByView() {
        List<Pair<OptimizerAlertType, Long>> result = Lists.newArrayList();
        for (OptimizerAlertLogger logger : optimizerAlertLoggers.values()) {
            result.add(logger.collectByView());
        }
        return result;
    }

    private static Map<OptimizerAlertType, OptimizerAlertLogger> createLogger() {
        ImmutableMap.Builder<OptimizerAlertType, OptimizerAlertLogger>
            optimizerAlertLoggers = ImmutableMap.builder();

        optimizerAlertLoggers.put(OptimizerAlertType.BKA_TOO_MUCH, new OptimizerAlertLoggerBKAImpl());
        optimizerAlertLoggers.put(OptimizerAlertType.TP_SLOW, new OptimizerAlertLoggerTpImpl());
        optimizerAlertLoggers.put(OptimizerAlertType.SELECTIVITY_ERR, new OptimizerAlertLoggerSelectivityImpl());
        optimizerAlertLoggers.put(OptimizerAlertType.SPM_ERR, new OptimizerAlertLoggerSpmImpl());
        optimizerAlertLoggers.put(OptimizerAlertType.PRUNING_SLOW, new OptimizerAlertLoggerPruningSlowImpl());

        // for statistic
        optimizerAlertLoggers.put(OptimizerAlertType.STATISTIC_MISS, new StatisticAlertLoggerBaseImpl(OptimizerAlertType.STATISTIC_MISS));
        optimizerAlertLoggers.put(OptimizerAlertType.STATISTIC_JOB_INTERRUPT, new StatisticAlertLoggerBaseImpl(OptimizerAlertType.STATISTIC_JOB_INTERRUPT));
//        optimizerAlertLoggers.put(OptimizerAlertType.STATISTIC_INCONSISTENT,
//            new OptimizerAlertLoggerStatisticInconsistentImpl());
        optimizerAlertLoggers.put(OptimizerAlertType.STATISTIC_SAMPLE_FAIL, new StatisticAlertLoggerBaseImpl(OptimizerAlertType.STATISTIC_SAMPLE_FAIL));
        optimizerAlertLoggers.put(OptimizerAlertType.STATISTIC_HLL_FAIL, new StatisticAlertLoggerBaseImpl(OptimizerAlertType.STATISTIC_HLL_FAIL));
        optimizerAlertLoggers.put(OptimizerAlertType.STATISTIC_COLLECT_ROWCOUNT_FAIL, new StatisticAlertLoggerBaseImpl(OptimizerAlertType.STATISTIC_COLLECT_ROWCOUNT_FAIL));
        optimizerAlertLoggers.put(OptimizerAlertType.STATISTIC_PERSIST_FAIL, new StatisticAlertLoggerBaseImpl(OptimizerAlertType.STATISTIC_PERSIST_FAIL));
        optimizerAlertLoggers.put(OptimizerAlertType.STATISTIC_SYNC_FAIL, new StatisticAlertLoggerBaseImpl(OptimizerAlertType.STATISTIC_SYNC_FAIL));
        optimizerAlertLoggers.put(OptimizerAlertType.STATISTIC_SCHEDULE_JOB_INFORMATION_TABLES_FAIL, new StatisticAlertLoggerBaseImpl(OptimizerAlertType.STATISTIC_SCHEDULE_JOB_INFORMATION_TABLES_FAIL));
        optimizerAlertLoggers.put(OptimizerAlertType.STATISTIC_SCHEDULE_JOB_SAMPLE_FAIL, new StatisticAlertLoggerBaseImpl(OptimizerAlertType.STATISTIC_SCHEDULE_JOB_SAMPLE_FAIL));
        optimizerAlertLoggers.put(OptimizerAlertType.STATISTIC_SCHEDULE_JOB_HLL_FAIL, new StatisticAlertLoggerBaseImpl(OptimizerAlertType.STATISTIC_SCHEDULE_JOB_HLL_FAIL));

        return optimizerAlertLoggers.build();
    }
}
