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
     * @return true if logTrace if printed
     */
    public boolean log(OptimizerAlertType optimizerAlertType, ExecutionContext ec) {
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
        return logger.logDetail(ec);
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
        optimizerAlertLoggers.put(OptimizerAlertType.XPLAN_SLOW, new OptimizerAlertLoggerXplanImpl());
        optimizerAlertLoggers.put(OptimizerAlertType.PLAN_CACHE_FULL, new OptimizerAlertLoggerPlanCacheImpl());
        optimizerAlertLoggers.put(OptimizerAlertType.TP_SLOW, new OptimizerAlertLoggerTpImpl());
        optimizerAlertLoggers.put(OptimizerAlertType.STATISTIC_MISS, new OptimizerAlertLoggerStatisticImpl());

        return optimizerAlertLoggers.build();
    }
}
