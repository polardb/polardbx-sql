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

import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.properties.ConnectionProperties;
import com.alibaba.polardbx.common.properties.DynamicConfig;
import com.alibaba.polardbx.common.properties.ParamManager;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.transaction.utils.ParamValidationUtils;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TimerTask;

import static com.alibaba.polardbx.common.constants.ServerVariables.MODIFIABLE_DEADLOCK_DETECTION_PARAM;
import static com.alibaba.polardbx.common.constants.ServerVariables.MODIFIABLE_TRANSACTION_STATISTICS_PARAM;

/**
 * @author wuzhe
 */
public class TransactionStatisticsTaskWrapper extends BaseTimerTaskWrapper {

    public TransactionStatisticsTaskWrapper(Map<String, Object> properties,
                                            AsyncTaskQueue asyncTaskQueue) {
        super(properties, asyncTaskQueue);
        // Enable an active task when this timer task wrapper is created.
        resetTask();
    }

    @Override
    protected Set<String> getParamsDef() {
        return MODIFIABLE_TRANSACTION_STATISTICS_PARAM;
    }

    @Override
    public TimerTask createTask(Map<String, String> newParam) {
        if (ConfigDataMode.isFastMock()) {
            return null;
        }

        final boolean enable = DynamicConfig.getInstance().isEnableTransactionStatistics();
        if (!enable) {
            return null;
        }

        final long interval = Long.parseLong(newParam.get(ConnectionProperties.TRANSACTION_STATISTICS_TASK_INTERVAL));

        return asyncTaskQueue
            .scheduleTransactionStatisticsTask(interval, getTask(new TransactionStatisticsTask()));
    }

    @Override
    protected Map<String, String> getNewParams() {
        final Map<String, String> newParam = new HashMap<>(2);
        newParam.put(ConnectionProperties.TRANSACTION_STATISTICS_TASK_INTERVAL,
            String.valueOf(DynamicConfig.getInstance().getTransactionStatisticsTaskInterval()));
        newParam.put(ConnectionProperties.ENABLE_TRANSACTION_STATISTICS,
            String.valueOf(DynamicConfig.getInstance().isEnableTransactionStatistics()));
        return newParam;
    }

    @Override
    protected void validateParams(Map<String, String> newParams) {
        for (Map.Entry<String, String> keyAndVal : newParams.entrySet()) {
            final String key = keyAndVal.getKey();
            final String val = keyAndVal.getValue();
            ParamValidationUtils.validateTransactionStatisticsParam(key, val);
        }
    }
}
