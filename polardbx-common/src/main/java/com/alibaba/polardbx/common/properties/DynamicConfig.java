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

import com.alibaba.polardbx.common.statementsummary.StatementSummaryManager;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;

import java.util.regex.Pattern;

/**
 * @version 1.0
 */
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

            case ConnectionProperties.XPROTO_ALWAYS_KEEP_FILTER_ON_XPLAN_GET:
                xprotoAlwaysKeepFilterOnXplanGet =
                    parseValue(value, Boolean.class, xprotoAlwaysKeepFilterOnXplanGetDefault);
                break;

            case ConnectionProperties.XPROTO_PROBE_TIMEOUT:
                xprotoProbeTimeout = parseValue(value, Integer.class, xprotoProbeTimeoutDefault);
                break;

            case ConnectionProperties.XPROTO_GALAXY_PREPARE:
                xprotoGalaxyPrepare = parseValue(value, Boolean.class, xprotoGalaxyPrepareDefault);
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
            case ConnectionProperties.ENABLE_PLAN_TYPE_DIGEST:
                enablePlanTypeDigest = parseValue(value, Boolean.class, true);
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

            case ConnectionProperties.MAX_PARTITION_COLUMN_COUNT:
                maxPartitionColumnCount = parseValue(value, Integer.class, maxPartitionColumnCountDefault);
                break;

            case ConnectionProperties.MAX_SESSION_PREPARED_STMT_COUNT:
                maxSessionPreparedStmtCount = parseValue(value, Integer.class, maxSessionPreparedStmtCountDefault);
                break;

            case ConnectionProperties.ENABLE_TRANS_LOG:
                enableTransLog = parseValue(value, Boolean.class, true);
                break;

            case ConnectionProperties.PLAN_CACHE_EXPIRE_TIME:
                planCacheExpireTime = parseValue(value, Integer.class, 12 * 3600 * 1000);   // 12h
                break;
            case ConnectionProperties.ENABLE_EXTREME_PERFORMANCE:
                enableExtremePerformance = parseValue(value, Boolean.class, true);
                break;
            case ConnectionProperties.ENBALE_BIND_PARAM_TYPE:
                enableBindType = parseValue(value, Boolean.class, true);
                break;
            case ConnectionProperties.ENABLE_CLEAN_FAILED_PLAN:
                enableClearFailedPlan = parseValue(value, Boolean.class, true);
                break;
            case ConnectionProperties.USE_PARAMETER_DELEGATE:
                useParameterDelegate = parseValue(value, Boolean.class, true);
                break;
            case ConnectionProperties.USE_JDK_DEFAULT_SER:
                useJdkDefaultSer = parseValue(value, Boolean.class, false);
                break;
            case ConnectionProperties.ENABLE_STATEMENTS_SUMMARY:
                int enableStatementsSummary = parseValue(value, Boolean.class, true) ? 1 : 0;
                StatementSummaryManager.getInstance().getConfig().setEnableStmtSummary(enableStatementsSummary);
                break;
            case ConnectionProperties.STATEMENTS_SUMMARY_PERIOD_SEC:
                long stmtSummaryRefreshInterval = parseValue(value, Long.class,
                    (long) StatementSummaryManager.StatementSummaryConfig.USE_DEFAULT_VALUE);
                StatementSummaryManager.getInstance().getConfig()
                    .setStmtSummaryRefreshInterval(stmtSummaryRefreshInterval);
                break;
            case ConnectionProperties.STATEMENTS_SUMMARY_HISTORY_PERIOD_NUM:
                int stmtSummaryHistorySize =
                    parseValue(value, Integer.class, StatementSummaryManager.StatementSummaryConfig.USE_DEFAULT_VALUE);
                StatementSummaryManager.getInstance().getConfig().setStmtSummaryHistorySize(stmtSummaryHistorySize);
                break;
            case ConnectionProperties.STATEMENTS_SUMMARY_MAX_SQL_TEMPLATE_COUNT:
                int stmtSummaryMaxStmtCount =
                    parseValue(value, Integer.class, StatementSummaryManager.StatementSummaryConfig.USE_DEFAULT_VALUE);
                StatementSummaryManager.getInstance().getConfig().setStmtSummaryMaxStmtCount(stmtSummaryMaxStmtCount);
                break;
            case ConnectionProperties.STATEMENTS_SUMMARY_RECORD_INTERNAL:
                int recordIntervalStatement = parseValue(value, Boolean.class, true) ? 1 : 0;
                StatementSummaryManager.getInstance().getConfig().setRecordIntervalStatement(recordIntervalStatement);
                break;
            case ConnectionProperties.STATEMENTS_SUMMARY_MAX_SQL_LENGTH:
                int stmtSummaryMaxSqlLength =
                    parseValue(value, Integer.class, StatementSummaryManager.StatementSummaryConfig.USE_DEFAULT_VALUE);
                StatementSummaryManager.getInstance().getConfig().setStmtSummaryMaxSqlLength(stmtSummaryMaxSqlLength);
                break;
            case ConnectionProperties.STATEMENTS_SUMMARY_PERCENT:
                int stmtSummaryPercent = parseValue(value, Integer.class,
                    StatementSummaryManager.StatementSummaryConfig.DEFAULT_VALUE_PERCENT);
                StatementSummaryManager.getInstance().getConfig().setStmtSummaryPercent(stmtSummaryPercent);
                break;
            case ConnectionProperties.PASSWORD_CHECK_PATTERN:
                String patternStr = parseValue(value, String.class, DEFAULT_PASSWORD_CHECK_PATTERN_STR);
                if (StringUtils.isBlank(patternStr)) {
                    patternStr = DEFAULT_PASSWORD_CHECK_PATTERN_STR;
                }
                Pattern pattern;
                try {
                    pattern = Pattern.compile(patternStr);
                } catch (Throwable t) {
                    logger.error(t.getMessage());
                    pattern = DEFAULT_PASSWORD_CHECK_PATTERN;
                }
                this.passwordCheckPattern = pattern;
                break;
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

    // XPROTO_ALWAYS_KEEP_FILTER_ON_XPLAN_GET
    private static final boolean xprotoAlwaysKeepFilterOnXplanGetDefault =
        parseValue(ConnectionParams.XPROTO_ALWAYS_KEEP_FILTER_ON_XPLAN_GET.getDefault(), Boolean.class, true);
    private volatile boolean xprotoAlwaysKeepFilterOnXplanGet = xprotoAlwaysKeepFilterOnXplanGetDefault;

    public boolean getXprotoAlwaysKeepFilterOnXplanGet() {
        return xprotoAlwaysKeepFilterOnXplanGet;
    }

    // XPROTO_PROBE_TIMEOUT
    private static final int xprotoProbeTimeoutDefault =
        parseValue(ConnectionParams.XPROTO_PROBE_TIMEOUT.getDefault(), Integer.class, 5000);
    private volatile int xprotoProbeTimeout = xprotoProbeTimeoutDefault;

    public int getXprotoProbeTimeout() {
        return xprotoProbeTimeout;
    }

    private static final boolean xprotoGalaxyPrepareDefault =
        parseValue(ConnectionParams.XPROTO_GALAXY_PREPARE.getDefault(), Boolean.class, false);
    private volatile boolean xprotoGalaxyPrepare = xprotoGalaxyPrepareDefault;

    public boolean getXprotoGalaxyPrepare() {
        return xprotoGalaxyPrepare;
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

    private volatile boolean enableTransLog = true;

    public boolean isEnableTransLog() {
        return enableTransLog;
    }

    private volatile boolean enableCreateGroupDataSource = false;

    public boolean forceCreateGroupDataSource() {
        return enableCreateGroupDataSource;
    }

    private volatile boolean enablePlanTypeDigest = true;

    public boolean enablePlanTypeDigest() {
        return enablePlanTypeDigest;
    }

    private static final long defaultPurgeHistoryMs = 600 * 1000L;

    private static final long maxPurgeHistoryMs = 600 * 1000L;

    private volatile long purgeHistoryMs = 36 * 24 * 60 * 60 * 1000L;

    public long getPurgeHistoryMs() {
        return purgeHistoryMs;
    }

    private volatile int planCacheExpireTime = 12 * 3600 * 1000; // 12h

    public int planCacheExpireTime() {
        return planCacheExpireTime;
    }

    private static final int maxPartitionColumnCountDefault =
        parseValue(ConnectionParams.MAX_PARTITION_COLUMN_COUNT.getDefault(), Integer.class, 3);
    private volatile int maxPartitionColumnCount = maxPartitionColumnCountDefault;

    public int getMaxPartitionColumnCount() {
        return maxPartitionColumnCount;
    }

    private volatile boolean enableExtremePerformance = false;

    public boolean enableExtremePerformance() {
        return enableExtremePerformance;
    }

    private volatile boolean enableBindType = true;

    public boolean enableBindType() {
        return enableBindType;
    }

    private volatile boolean enableClearFailedPlan = true;

    public boolean enableClearFailedPlan() {
        return enableClearFailedPlan;
    }

    private volatile boolean useParameterDelegate = true;

    public boolean useParameterDelegate() {
        return useParameterDelegate;
    }

    private volatile boolean useJdkDefaultSer = true;

    public boolean useJdkDefaultSer() {
        return useJdkDefaultSer;
    }

    private static final int maxSessionPreparedStmtCountDefault =
        parseValue(ConnectionParams.MAX_SESSION_PREPARED_STMT_COUNT.getDefault(), Integer.class, 100);

    private volatile int maxSessionPreparedStmtCount = maxSessionPreparedStmtCountDefault;

    public int getMaxSessionPreparedStmtCount() {
        return maxSessionPreparedStmtCount;
    }

    private static final String DEFAULT_PASSWORD_CHECK_PATTERN_STR = "^[0-9A-Za-z@#$%^&+=]{6,20}$";
    private static final Pattern DEFAULT_PASSWORD_CHECK_PATTERN = Pattern.compile(DEFAULT_PASSWORD_CHECK_PATTERN_STR);

    private volatile Pattern passwordCheckPattern = DEFAULT_PASSWORD_CHECK_PATTERN;

    public Pattern getPasswordCheckPattern() {
        return passwordCheckPattern;
    }

    public boolean isDefaultPasswordCheckPattern() {
        return DEFAULT_PASSWORD_CHECK_PATTERN_STR.equals(passwordCheckPattern.pattern());
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
