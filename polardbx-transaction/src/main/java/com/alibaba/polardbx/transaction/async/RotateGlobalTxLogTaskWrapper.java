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

import com.alibaba.polardbx.common.async.AsyncTaskUtils;
import com.alibaba.polardbx.common.async.TimeInterval;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.properties.ConnectionProperties;
import com.alibaba.polardbx.common.properties.ParamManager;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.gms.topology.SystemDbHelper;
import com.alibaba.polardbx.transaction.TransactionExecutor;
import com.alibaba.polardbx.transaction.TransactionLogger;
import com.alibaba.polardbx.transaction.utils.ParamValidationUtils;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TimerTask;

import static com.alibaba.polardbx.common.constants.ServerVariables.MODIFIABLE_PURGE_TRANS_PARAM;

/**
 * @author wuzhe
 */
public class RotateGlobalTxLogTaskWrapper extends BaseTimerTaskWrapper {
    private static final String DELAY = "DELAY";
    private static final String SCHEMA = "SCHEMA";

    private final String schemaName;
    private final TransactionExecutor executor;

    public RotateGlobalTxLogTaskWrapper(Map<String, Object> properties, String schemaName,
                                        AsyncTaskQueue asyncTaskQueue,
                                        TransactionExecutor executor) {
        super(properties, asyncTaskQueue);
        this.schemaName = schemaName;
        this.executor = executor;
        // Enable an active task when this timer task wrapper is created.
        resetTask();
    }

    @Override
    public void resetTask() {
        if (ConfigDataMode.isFastMock() || SystemDbHelper.isDBBuildInExceptCdc(schemaName)) {
            cancel();
            return;
        }

        // 1. Get new parameters.
        final Map<String, String> newParam = getNewParams();

        // 2. Validate parameters.
        validateParams(newParam);

        // 3. If new parameters are identical to the current running ones, ignore the reset.
        final Map<String, String> currentParam = getCurrentParam();
        if (null != currentParam &&
            ParamValidationUtils.isIdentical(newParam, currentParam, MODIFIABLE_PURGE_TRANS_PARAM)) {
            return;
        }

        // Use DELAY to determine, whether run a task immediately after the reset.
        if (isTimerTaskNull()) {
            // If the timer task is null, it may be the first time to set a clean task,
            // so run a clean task immediately after this reset.
            newParam.put(DELAY, "FALSE");
        } else {
            newParam.put(DELAY, "TRUE");
        }

        // For logger usage.
        newParam.put(SCHEMA, schemaName);

        // 4. Reset the timer task.
        innerReset(newParam);
    }

    @Override
    protected Set<String> getParamsDef() {
        return MODIFIABLE_PURGE_TRANS_PARAM;
    }

    @Override
    public TimerTask createTask(Map<String, String> newParam) {
        if (ConfigDataMode.isFastMock() || SystemDbHelper.isDBBuildInExceptCdc(schemaName)) {
            return null;
        }

        final String purgeStartTime = newParam.get(ConnectionProperties.PURGE_TRANS_START_TIME);
        final int purgeInterval = Integer.parseInt(newParam.get(ConnectionProperties.PURGE_TRANS_INTERVAL));
        final int purgeBefore = Integer.parseInt(newParam.get(ConnectionProperties.PURGE_TRANS_BEFORE));
        final boolean isDelay = Boolean.parseBoolean(newParam.get(DELAY));

        // 1. Calculate the available running time interval.
        TimeInterval parsed = null;
        try {
            parsed = AsyncTaskUtils.parseTimeInterval(purgeStartTime);
        } catch (Exception ex) {
            TransactionLogger.warn("Failed to parse purgeStartTime: " + purgeStartTime);
        }

        final Calendar startTime = Calendar.getInstance();
        final Calendar endTime = Calendar.getInstance();
        if (parsed != null) {
            startTime.set(Calendar.HOUR_OF_DAY, parsed.getStartTime().getHour());
            startTime.set(Calendar.MINUTE, parsed.getStartTime().getMinute());
            startTime.set(Calendar.SECOND, parsed.getStartTime().getSecond());

            endTime.set(Calendar.HOUR_OF_DAY, parsed.getEndTime().getHour());
            endTime.set(Calendar.MINUTE, parsed.getEndTime().getMinute());
            endTime.set(Calendar.SECOND, parsed.getEndTime().getSecond());
        } else {
            // (parsed == null) should never happen since we validate the parameters before.
            // But for safety, we still set a default value: 00:00-23:59.
            startTime.set(Calendar.HOUR_OF_DAY, 0);
            startTime.set(Calendar.MINUTE, 0);
            startTime.set(Calendar.SECOND, 0);

            endTime.set(Calendar.HOUR_OF_DAY, 23);
            endTime.set(Calendar.MINUTE, 59);
            endTime.set(Calendar.SECOND, 59);
        }

        // 2. Create a new rotate log task.
        final Runnable task = new RotateGlobalTxLogTask(executor, startTime, endTime, purgeBefore,
            purgeInterval * 2, asyncTaskQueue);

        // 3. Calculate the first time the timer task runs.
        final Calendar currentTime = Calendar.getInstance();
        Calendar tryToRunAtTime;
        long delay;
        if (currentTime.after(startTime) && currentTime.before(endTime)) {
            // Current time is within (startTime, endTime),
            // delay the task if isDelay is set or run it immediately.
            tryToRunAtTime = (Calendar) currentTime.clone();
            if (isDelay) {
                // Delay the task: run it in next interval.
                tryToRunAtTime.add(Calendar.SECOND, purgeInterval);
            }
        } else {
            // Current time is out of (startTime, endTime),
            // try to run the task at the startTime after some random seconds.
            final int afterSeconds = new Random().nextInt(
                (int) Math.min(purgeInterval / 2, (endTime.getTimeInMillis() - startTime.getTimeInMillis()) / 1000));
            tryToRunAtTime = (Calendar) startTime.clone();
            tryToRunAtTime.add(Calendar.SECOND, afterSeconds);
            if (currentTime.after(endTime)) {
                // Run it tomorrow.
                tryToRunAtTime.add(Calendar.DATE, 1);
            }
        }

        delay = tryToRunAtTime.getTimeInMillis() - currentTime.getTimeInMillis();

        TransactionLogger.info(schemaName + ": Scheduled rotate task. Try to run it at " +
            new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(tryToRunAtTime.getTime()));

        return asyncTaskQueue.scheduleAutoCleanTask(purgeInterval, Math.max(0, delay), getTask(task));
    }

    @Override
    protected Map<String, String> getNewParams() {
        final Map<String, String> newParam = new HashMap<>(8);
        ParamManager paramManager = new ParamManager(properties);
        newParam.put(ConnectionProperties.PURGE_TRANS_START_TIME,
            paramManager.get(ConnectionParams.PURGE_TRANS_START_TIME));
        newParam.put(ConnectionProperties.PURGE_TRANS_INTERVAL,
            paramManager.get(ConnectionParams.PURGE_TRANS_INTERVAL));
        newParam.put(ConnectionProperties.PURGE_TRANS_BEFORE,
            paramManager.get(ConnectionParams.PURGE_TRANS_BEFORE));
        return newParam;
    }

    @Override
    protected void validateParams(Map<String, String> newParam) {
        for (Map.Entry<String, String> keyAndVal : newParam.entrySet()) {
            final String key = keyAndVal.getKey();
            final String val = keyAndVal.getValue();
            ParamValidationUtils.validateTrxLogPurgeParam(key, val);
        }
    }
}
