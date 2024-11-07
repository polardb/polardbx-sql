package com.alibaba.polardbx.transaction.async;

import com.alibaba.polardbx.common.properties.ConnectionProperties;
import com.alibaba.polardbx.common.properties.DynamicConfig;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.transaction.utils.ParamValidationUtils;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TimerTask;

import static com.alibaba.polardbx.common.constants.ServerVariables.MODIFIABLE_SYNC_POINT_PARAM;

public class SyncPointTaskWrapper extends BaseTimerTaskWrapper {

    public SyncPointTaskWrapper(Map<String, Object> properties, AsyncTaskQueue asyncTaskQueue) {
        super(properties, asyncTaskQueue);
        // Enable an active task when this timer task wrapper is created.
        resetTask();
    }

    @Override
    Set<String> getParamsDef() {
        return MODIFIABLE_SYNC_POINT_PARAM;
    }

    @Override
    void validateParams(Map<String, String> newParams) {
        for (Map.Entry<String, String> keyAndVal : newParams.entrySet()) {
            final String key = keyAndVal.getKey();
            final String val = keyAndVal.getValue();
            ParamValidationUtils.validateSyncPointParam(key, val);
        }
    }

    @Override
    Map<String, String> getNewParams() {
        final Map<String, String> newParam = new HashMap<>(2);
        newParam.put(ConnectionProperties.ENABLE_SYNC_POINT,
            String.valueOf(DynamicConfig.getInstance().isEnableSyncPoint()));
        newParam.put(ConnectionProperties.SYNC_POINT_TASK_INTERVAL,
            String.valueOf(DynamicConfig.getInstance().getSyncPointTaskInterval()));
        return newParam;
    }

    @Override
    TimerTask createTask(Map<String, String> newParam) {
        if (ConfigDataMode.isFastMock()) {
            return null;
        }

        final boolean enable = DynamicConfig.getInstance().isEnableSyncPoint();
        if (!enable) {
            return null;
        }

        final long interval = Long.parseLong(newParam.get(ConnectionProperties.SYNC_POINT_TASK_INTERVAL));

        return asyncTaskQueue.scheduleSyncPointTask(interval, getTask(new SyncPointTask()));
    }
}
