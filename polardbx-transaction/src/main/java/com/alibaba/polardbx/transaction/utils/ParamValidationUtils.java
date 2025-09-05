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

package com.alibaba.polardbx.transaction.utils;

import com.alibaba.polardbx.common.async.AsyncTaskUtils;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.config.ConfigDataMode;

import java.util.Map;
import java.util.Properties;
import java.util.Set;

import static com.alibaba.polardbx.common.constants.ServerVariables.MODIFIABLE_DEADLOCK_DETECTION_PARAM;
import static com.alibaba.polardbx.common.constants.ServerVariables.MODIFIABLE_PURGE_TRANS_PARAM;
import static com.alibaba.polardbx.common.constants.ServerVariables.MODIFIABLE_SYNC_POINT_PARAM;
import static com.alibaba.polardbx.common.constants.ServerVariables.MODIFIABLE_TIMER_TASK_PARAM;
import static com.alibaba.polardbx.common.constants.ServerVariables.MODIFIABLE_TRANSACTION_STATISTICS_PARAM;
import static com.alibaba.polardbx.common.constants.ServerVariables.MODIFIABLE_TRX_IDLE_TIMEOUT_PARAM;
import static com.alibaba.polardbx.common.properties.ConnectionProperties.DEADLOCK_DETECTION_INTERVAL;
import static com.alibaba.polardbx.common.properties.ConnectionProperties.ENABLE_DEADLOCK_DETECTION;
import static com.alibaba.polardbx.common.properties.ConnectionProperties.ENABLE_SYNC_POINT;
import static com.alibaba.polardbx.common.properties.ConnectionProperties.ENABLE_TRANSACTION_RECOVER_TASK;
import static com.alibaba.polardbx.common.properties.ConnectionProperties.ENABLE_TRANSACTION_STATISTICS;
import static com.alibaba.polardbx.common.properties.ConnectionProperties.ENABLE_TRX_IDLE_TIMEOUT_TASK;
import static com.alibaba.polardbx.common.properties.ConnectionProperties.PURGE_TRANS_BEFORE;
import static com.alibaba.polardbx.common.properties.ConnectionProperties.PURGE_TRANS_INTERVAL;
import static com.alibaba.polardbx.common.properties.ConnectionProperties.PURGE_TRANS_START_TIME;
import static com.alibaba.polardbx.common.properties.ConnectionProperties.SYNC_POINT_TASK_INTERVAL;
import static com.alibaba.polardbx.common.properties.ConnectionProperties.TRANSACTION_STATISTICS_TASK_INTERVAL;
import static com.alibaba.polardbx.common.properties.ConnectionProperties.TRX_IDLE_TIMEOUT_TASK_INTERVAL;
import static com.alibaba.polardbx.common.properties.ConnectionProperties.XA_RECOVER_INTERVAL;

/**
 * @author wuzhe
 */
public class ParamValidationUtils {
    public static void validateTrxLogPurgeParam(String parameter, String value) {
        if (PURGE_TRANS_INTERVAL.equals(parameter)) {
            final int intVal = Integer.parseInt(value);
            if (intVal < 300) {
                throw new TddlRuntimeException(ErrorCode.ERR_VALIDATE,
                    "invalid parameter: " + parameter + ", it should >= 300 (5 minutes)");
            }
            return;
        }
        if (PURGE_TRANS_BEFORE.equals(parameter)) {
            final int intVal = Integer.parseInt(value);
            if (intVal < 1800) {
                throw new TddlRuntimeException(ErrorCode.ERR_VALIDATE,
                    "invalid parameter: " + parameter + ", it should >= 1800 (30 minutes)");
            }
            return;
        }
        if (PURGE_TRANS_START_TIME.equals(parameter)) {
            try {
                Assert.assertTrue(AsyncTaskUtils.parseTimeInterval(value).getDuration() > 0);
            } catch (Throwable t) {
                throw new TddlRuntimeException(ErrorCode.ERR_VALIDATE,
                    "invalid parameter: " + parameter + ", example format: 00:00-23:59");
            }
            return;
        }
        throw new TddlRuntimeException(ErrorCode.ERR_VALIDATE, "Unknown trx log cleaning task parameter " + parameter);
    }

    public static void validateDeadlockDetectionParam(String parameter, String value) {
        if (ENABLE_DEADLOCK_DETECTION.equals(parameter)) {
            final Boolean boolVal = GeneralUtil.convertStringToBoolean(value);
            if (boolVal == null) {
                throw new TddlRuntimeException(ErrorCode.ERR_VALIDATE,
                    "invalid parameter: " + parameter + ", it should be TRUE/FALSE");
            }
            return;
        }
        if (DEADLOCK_DETECTION_INTERVAL.equals(parameter)) {
            final int intVal = Integer.parseInt(value);
            if (intVal < 1000) {
                throw new TddlRuntimeException(ErrorCode.ERR_VALIDATE,
                    "invalid parameter: " + parameter + ", it should >= 1000(ms)");
            }
            return;
        }

        throw new TddlRuntimeException(ErrorCode.ERR_VALIDATE, "Unknown deadlock detection parameter " + parameter);
    }

    public static void validateTransactionStatisticsParam(String parameter, String value) {
        if (TRANSACTION_STATISTICS_TASK_INTERVAL.equals(parameter)) {
            final long longVal = Long.parseLong(value);
            if (longVal < 1000) {
                throw new TddlRuntimeException(ErrorCode.ERR_VALIDATE,
                    "invalid parameter: " + parameter + ", it should >= 1000(ms)");
            }
            return;
        }

        if (ENABLE_TRANSACTION_STATISTICS.equals(parameter)) {
            final Boolean boolVal = GeneralUtil.convertStringToBoolean(value);
            if (boolVal == null) {
                throw new TddlRuntimeException(ErrorCode.ERR_VALIDATE,
                    "invalid parameter: " + parameter + ", it should be TRUE/FALSE");
            }
            return;
        }

        throw new TddlRuntimeException(ErrorCode.ERR_VALIDATE, "Unknown transaction statistics parameter " + parameter);
    }

    public static void validateTransactionIdleTimeoutParam(String parameter, String value) {
        if (TRX_IDLE_TIMEOUT_TASK_INTERVAL.equals(parameter)) {
            final long longVal = Long.parseLong(value);
            if (longVal < 1) {
                throw new TddlRuntimeException(ErrorCode.ERR_VALIDATE,
                    "invalid parameter: " + parameter + ", it should >= 1s");
            }
            return;
        }

        if (ENABLE_TRX_IDLE_TIMEOUT_TASK.equals(parameter)) {
            final Boolean boolVal = GeneralUtil.convertStringToBoolean(value);
            if (boolVal == null) {
                throw new TddlRuntimeException(ErrorCode.ERR_VALIDATE,
                    "invalid parameter: " + parameter + ", it should be TRUE/FALSE");
            }
            return;
        }

        throw new TddlRuntimeException(ErrorCode.ERR_VALIDATE, "Unknown transaction statistics parameter " + parameter);
    }

    public static void validateSyncPointParam(String parameter, String value) {
        if (ENABLE_SYNC_POINT.equals(parameter)) {
            final Boolean boolVal = GeneralUtil.convertStringToBoolean(value);
            if (boolVal == null) {
                throw new TddlRuntimeException(ErrorCode.ERR_VALIDATE,
                    "invalid parameter: " + parameter + ", it should be TRUE/FALSE");
            }
            return;
        }
        if (SYNC_POINT_TASK_INTERVAL.equals(parameter)) {
            final int intVal = Integer.parseInt(value);
            if (intVal < 1000) {
                throw new TddlRuntimeException(ErrorCode.ERR_VALIDATE,
                    "invalid parameter: " + parameter + ", it should >= 1000(ms)");
            }
            return;
        }

        throw new TddlRuntimeException(ErrorCode.ERR_VALIDATE, "Unknown sync point task parameter " + parameter);
    }

    public static void validateXaRecoverParam(String parameter, String value) {
        if (XA_RECOVER_INTERVAL.equals(parameter)) {
            final long longVal = Long.parseLong(value);
            if (longVal < 1) {
                throw new TddlRuntimeException(ErrorCode.ERR_VALIDATE,
                    "invalid parameter: " + parameter + ", it should >= 1(s)");
            }
            return;
        }

        if (ENABLE_TRANSACTION_RECOVER_TASK.equals(parameter)) {
            final Boolean boolVal = GeneralUtil.convertStringToBoolean(value);
            if (boolVal == null) {
                throw new TddlRuntimeException(ErrorCode.ERR_VALIDATE,
                    "invalid parameter: " + parameter + ", it should be TRUE/FALSE");
            }
            return;
        }

        throw new TddlRuntimeException(ErrorCode.ERR_VALIDATE, "Unknown xa recover task parameter " + parameter);
    }

    public static boolean isIdentical(Map<String, String> newParam, Map<String, String> oldParam,
                                      Set<String> paramNames) {
        for (String paramName : paramNames) {
            if (!newParam.get(paramName).equalsIgnoreCase(oldParam.get(paramName))) {
                return false;
            }
        }
        return true;
    }

    public static boolean isTimerTaskParam(String parameter, String value) {
        if (MODIFIABLE_PURGE_TRANS_PARAM.contains(parameter)) {
            validateTrxLogPurgeParam(parameter, value);
            return true;
        }

        if (MODIFIABLE_DEADLOCK_DETECTION_PARAM.contains(parameter)) {
            validateDeadlockDetectionParam(parameter, value);
            return true;
        }

        if (MODIFIABLE_TRANSACTION_STATISTICS_PARAM.contains(parameter)) {
            validateTransactionStatisticsParam(parameter, value);
        }

        if (MODIFIABLE_TRX_IDLE_TIMEOUT_PARAM.contains(parameter)) {
            validateTransactionIdleTimeoutParam(parameter, value);
        }

        if (MODIFIABLE_SYNC_POINT_PARAM.contains(parameter)) {
            validateSyncPointParam(parameter, value);
        }

        return false;
    }

    public static boolean isAnyTimerTaskParam(Properties properties) {
        Set<String> allPropertyNames = properties.stringPropertyNames();
        for (String paramName : MODIFIABLE_TIMER_TASK_PARAM) {
            if (allPropertyNames.contains(paramName)) {
                return true;
            }
        }
        return false;
    }
}
