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
import com.alibaba.polardbx.common.properties.ParamManager;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.transaction.utils.ParamValidationUtils;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TimerTask;

import static com.alibaba.polardbx.common.constants.ServerVariables.MODIFIABLE_TRX_IDLE_TIMEOUT_PARAM;

/**
 * @author yaozhili
 */
public class TransactionIdleTimeoutTaskWrapper extends BaseTimerTaskWrapper {

    public TransactionIdleTimeoutTaskWrapper(Map<String, Object> properties, AsyncTaskQueue asyncTaskQueue) {
        super(properties, asyncTaskQueue);
        // Enable an active task when this timer task wrapper is created.
        resetTask();
    }

    @Override
    public void resetTask() {
        if (ConfigDataMode.isFastMock()) {
            cancel();
            return;
        }

        // 1. Get new parameters.
        final Map<String, String> newParams = getNewParams();

        // 2. Validate parameters.
        validateParams(newParams);

        // 3. If new parameters are identical to the current running ones, ignore the reset.
        final Map<String, String> currentParam = getCurrentParam();
        if (null != currentParam &&
            ParamValidationUtils.isIdentical(newParams, currentParam, MODIFIABLE_TRX_IDLE_TIMEOUT_PARAM)) {
            return;
        }

        // 4. Reset the timer task.
        innerReset(newParams);
    }

    @Override
    Set<String> getParamsDef() {
        return MODIFIABLE_TRX_IDLE_TIMEOUT_PARAM;
    }

    @Override
    public TimerTask createTask(Map<String, String> newParam) {
        if (ConfigDataMode.isFastMock()) {
            return null;
        }
        final Boolean enable =
            GeneralUtil.convertStringToBoolean(newParam.get(ConnectionProperties.ENABLE_TRX_IDLE_TIMEOUT_TASK));
        if (null == enable || !enable) {
            return null;
        }

        final int interval = Integer.parseInt(newParam.get(ConnectionProperties.TRX_IDLE_TIMEOUT_TASK_INTERVAL));

        return asyncTaskQueue
            .scheduleTransactionIdleTimeoutTask(interval, getTask(new TransactionIdleTimeoutTask()));
    }

    @Override
    protected Map<String, String> getNewParams() {
        final Map<String, String> newParam = new HashMap<>(2);
        ParamManager paramManager = new ParamManager(properties);
        newParam.put(ConnectionProperties.ENABLE_TRX_IDLE_TIMEOUT_TASK,
            paramManager.get(ConnectionParams.ENABLE_TRX_IDLE_TIMEOUT_TASK));
        newParam.put(ConnectionProperties.TRX_IDLE_TIMEOUT_TASK_INTERVAL,
            paramManager.get(ConnectionParams.TRX_IDLE_TIMEOUT_TASK_INTERVAL));
        return newParam;
    }

    @Override
    protected void validateParams(Map<String, String> newParams) {
        for (Map.Entry<String, String> keyAndVal : newParams.entrySet()) {
            final String key = keyAndVal.getKey();
            final String val = keyAndVal.getValue();
            ParamValidationUtils.validateTransactionIdleTimeoutParam(key, val);
        }
    }
}
