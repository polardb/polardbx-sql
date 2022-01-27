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

package com.alibaba.polardbx.common.properties;

import org.slf4j.Logger;

public class DynamicConfig {

    public static DynamicConfig getInstance() {
        return instance;
    }

    public void loadValue(Logger logger, String key, String value) {
        if (key != null && value != null) {
            switch (key.toUpperCase()) {
            case ConnectionProperties.GENERAL_DYNAMIC_SPEED_LIMITATION:
                generalDynamicSpeedLimitation = parseValue(value, Long.class, generalDynamicSpeedLimitationDefault);
                break;

            case ConnectionProperties.XPROTO_MAX_DN_CONCURRENT:
                xprotoMaxDnConcurrent = parseValue(value, Long.class, xprotoMaxDnConcurrentDefault);
                break;

            case ConnectionProperties.XPROTO_MAX_DN_WAIT_CONNECTION:
                xprotoMaxDnWaitConnection = parseValue(value, Long.class, xprotoMaxDnWaitConnectionDefault);
                break;

            case ConnectionProperties.AUTO_PARTITION_PARTITIONS:
                autoPartitionPartitions = parseValue(value, Long.class, autoPartitionPartitionsDefault);
                break;

            case ConnectionProperties.STORAGE_DELAY_THRESHOLD:
                delayThreshold = parseValue(value, Integer.class, 3);
                break;
            case ConnectionProperties.STORAGE_BUSY_THRESHOLD:
                busyThreshold = parseValue(value, Integer.class, 100);
                break;
            case ConnectionProperties.KEEP_TSO_HEARTBEAT_ON_CDC_CON:
                keepTsoBasedCDC = parseValue(value, Boolean.class, true);
                break;
            case ConnectionProperties.FORCE_RECREATE_GROUP_DATASOURCE:
                enableCreateGroupDataSource = parseValue(value, Boolean.class, false);
                break;
            case ConnectionProperties.PURGE_HISTORY_MS: {
                long tempPurgeHistoryMs = parseValue(value, Long.class, 600 * 1000L);
                if (tempPurgeHistoryMs > 0 && tempPurgeHistoryMs < purgeHistoryMs) {
                    purgeHistoryMs = tempPurgeHistoryMs;
                } else {
                    logger.warn("invalid values " + tempPurgeHistoryMs);
                }
                break;
            }

            default:
                break;
            }
        }
    }

    private static final long generalDynamicSpeedLimitationDefault =
        parseValue(ConnectionParams.GENERAL_DYNAMIC_SPEED_LIMITATION.getDefault(), Long.class, -1L);
    private volatile long generalDynamicSpeedLimitation = generalDynamicSpeedLimitationDefault;

    public long getGeneralDynamicSpeedLimitation() {
        return generalDynamicSpeedLimitation;
    }

    private static final long xprotoMaxDnConcurrentDefault =
        parseValue(ConnectionParams.XPROTO_MAX_DN_CONCURRENT.getDefault(), Long.class, 500L);
    private volatile long xprotoMaxDnConcurrent = xprotoMaxDnConcurrentDefault;

    public long getXprotoMaxDnConcurrent() {
        return xprotoMaxDnConcurrent;
    }

    private static final long xprotoMaxDnWaitConnectionDefault =
        parseValue(ConnectionParams.XPROTO_MAX_DN_WAIT_CONNECTION.getDefault(), Long.class, 32L);
    private volatile long xprotoMaxDnWaitConnection = xprotoMaxDnWaitConnectionDefault;

    public long getXprotoMaxDnWaitConnection() {
        return xprotoMaxDnWaitConnection;
    }

    private static final long autoPartitionPartitionsDefault =
        parseValue(ConnectionParams.AUTO_PARTITION_PARTITIONS.getDefault(), Long.class, 64L);
    private volatile long autoPartitionPartitions = autoPartitionPartitionsDefault;

    public long getAutoPartitionPartitions() {
        return autoPartitionPartitions;
    }

    private volatile int delayThreshold = 3;

    public int getDelayThreshold() {
        return delayThreshold;
    }

    private volatile int busyThreshold = 100;

    public int getBusyThreshold() {
        return busyThreshold;
    }

    private volatile boolean keepTsoBasedCDC = true;

    public boolean isKeepTsoBasedCDC() {
        return keepTsoBasedCDC;
    }

    private volatile boolean enableCreateGroupDataSource = false;

    public boolean forceCreateGroupDataSource() {
        return enableCreateGroupDataSource;
    }

    private static final long defaultPurgeHistoryMs = 600 * 1000L;

    private static final long maxPurgeHistoryMs = 600 * 1000L;

    private volatile long purgeHistoryMs = 36 * 24 * 60 * 60 * 1000L;

    public long getPurgeHistoryMs() {
        return purgeHistoryMs;
    }

    public static <T> T parseValue(String value, Class<T> type, T defaultValue) {
        if (value == null) {
            return defaultValue;
        } else if (type == String.class) {
            return (T) value;
        } else if (type == Integer.class) {
            return (T) (Integer.valueOf(value));
        } else if (type == Long.class) {
            return (T) (Long.valueOf(value));
        } else if (type == Float.class) {
            return (T) (Float.valueOf(value));
        } else if (type == Double.class) {
            return (T) (Double.valueOf(value));
        } else if (type == Boolean.class) {
            return (T) (Boolean.valueOf(value));
        } else {
            return defaultValue;
        }
    }

    private static final DynamicConfig instance = new DynamicConfig();
}
