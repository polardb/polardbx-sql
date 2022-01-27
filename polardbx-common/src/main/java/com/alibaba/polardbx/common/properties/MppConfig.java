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

import com.alibaba.polardbx.common.utils.thread.ThreadCpuStatUtil;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import org.slf4j.Logger;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static com.alibaba.polardbx.common.properties.ConnectionProperties.MPP_ALLOCATOR_SIZE;
import static com.alibaba.polardbx.common.properties.ConnectionProperties.MPP_AP_PRIORITY;
import static com.alibaba.polardbx.common.properties.ConnectionProperties.MPP_AVAILABLE_SPILL_SPACE_THRESHOLD;
import static com.alibaba.polardbx.common.properties.ConnectionProperties.MPP_CLUSTER_NAME;
import static com.alibaba.polardbx.common.properties.ConnectionProperties.MPP_CPU_CFS_MAX_QUOTA;
import static com.alibaba.polardbx.common.properties.ConnectionProperties.MPP_CPU_CFS_MIN_QUOTA;
import static com.alibaba.polardbx.common.properties.ConnectionProperties.MPP_CPU_CFS_PERIOD_US;
import static com.alibaba.polardbx.common.properties.ConnectionProperties.MPP_CPU_CFS_QUOTA;
import static com.alibaba.polardbx.common.properties.ConnectionProperties.MPP_EXCHANGE_CLIENT_THREAD_SIZE;
import static com.alibaba.polardbx.common.properties.ConnectionProperties.MPP_EXCHANGE_CONCURRENT_MULTIPLIER;
import static com.alibaba.polardbx.common.properties.ConnectionProperties.MPP_EXCHANGE_MAX_ERROR_DURATION;
import static com.alibaba.polardbx.common.properties.ConnectionProperties.MPP_EXCHANGE_MAX_RESPONSE_SIZE;
import static com.alibaba.polardbx.common.properties.ConnectionProperties.MPP_EXCHANGE_MIN_ERROR_DURATION;
import static com.alibaba.polardbx.common.properties.ConnectionProperties.MPP_GLOBAL_MEMORY_LIMIT_RATIO;
import static com.alibaba.polardbx.common.properties.ConnectionProperties.MPP_HTTP_CLIENT_MAX_CONNECTIONS;
import static com.alibaba.polardbx.common.properties.ConnectionProperties.MPP_HTTP_CLIENT_MAX_CONNECTIONS_PER_SERVER;
import static com.alibaba.polardbx.common.properties.ConnectionProperties.MPP_HTTP_CLIENT_MAX_THREADS;
import static com.alibaba.polardbx.common.properties.ConnectionProperties.MPP_HTTP_CLIENT_MIN_THREADS;
import static com.alibaba.polardbx.common.properties.ConnectionProperties.MPP_HTTP_MAX_REQUESTS_PER_DESTINATION;
import static com.alibaba.polardbx.common.properties.ConnectionProperties.MPP_HTTP_RESPONSE_THREAD_SIZE;
import static com.alibaba.polardbx.common.properties.ConnectionProperties.MPP_HTTP_SERVER_MAX_THREADS;
import static com.alibaba.polardbx.common.properties.ConnectionProperties.MPP_HTTP_SERVER_MIN_THREADS;
import static com.alibaba.polardbx.common.properties.ConnectionProperties.MPP_HTTP_TIMEOUT_THREAD_SIZE;
import static com.alibaba.polardbx.common.properties.ConnectionProperties.MPP_INFO_UPDATE_INTERVAL;
import static com.alibaba.polardbx.common.properties.ConnectionProperties.MPP_LESS_REVOKE_BYTES;
import static com.alibaba.polardbx.common.properties.ConnectionProperties.MPP_LOW_PRIORITY_ENABLED;
import static com.alibaba.polardbx.common.properties.ConnectionProperties.MPP_MAX_QUERY_EXPIRED_RESERVETION_TIME;
import static com.alibaba.polardbx.common.properties.ConnectionProperties.MPP_MAX_QUERY_HISTORY;
import static com.alibaba.polardbx.common.properties.ConnectionProperties.MPP_MAX_QUERY_SPILL_SPACE_THRESHOLD;
import static com.alibaba.polardbx.common.properties.ConnectionProperties.MPP_MAX_SPILL_FD_THRESHOLD;
import static com.alibaba.polardbx.common.properties.ConnectionProperties.MPP_MAX_SPILL_SPACE_THRESHOLD;
import static com.alibaba.polardbx.common.properties.ConnectionProperties.MPP_MAX_SPILL_THREADS;
import static com.alibaba.polardbx.common.properties.ConnectionProperties.MPP_MAX_WORKER_THREAD_SIZE;
import static com.alibaba.polardbx.common.properties.ConnectionProperties.MPP_MEMORY_REVOKING_TARGET;
import static com.alibaba.polardbx.common.properties.ConnectionProperties.MPP_MEMORY_REVOKING_THRESHOLD;
import static com.alibaba.polardbx.common.properties.ConnectionProperties.MPP_MIN_QUERY_EXPIRE_TIME;
import static com.alibaba.polardbx.common.properties.ConnectionProperties.MPP_NOTIFY_BLOCKED_QUERY_MEMORY;
import static com.alibaba.polardbx.common.properties.ConnectionProperties.MPP_QUERY_CLIENT_TIMEOUT;
import static com.alibaba.polardbx.common.properties.ConnectionProperties.MPP_QUERY_DELAY_COUNT;
import static com.alibaba.polardbx.common.properties.ConnectionProperties.MPP_QUERY_EXECUTION_THREAD_SIZE;
import static com.alibaba.polardbx.common.properties.ConnectionProperties.MPP_QUERY_MANAGER_THREAD_SIZE;
import static com.alibaba.polardbx.common.properties.ConnectionProperties.MPP_QUERY_MAX_DELAY_PENDING_RATIO;
import static com.alibaba.polardbx.common.properties.ConnectionProperties.MPP_QUERY_MAX_DELAY_TIME;
import static com.alibaba.polardbx.common.properties.ConnectionProperties.MPP_QUERY_MIN_DELAY_PENDING_RATIO;
import static com.alibaba.polardbx.common.properties.ConnectionProperties.MPP_QUERY_MIN_DELAY_TIME;
import static com.alibaba.polardbx.common.properties.ConnectionProperties.MPP_QUERY_REMOTE_TASK_MAX_ERROR;
import static com.alibaba.polardbx.common.properties.ConnectionProperties.MPP_QUERY_REMOTE_TASK_MIN_ERROR;
import static com.alibaba.polardbx.common.properties.ConnectionProperties.MPP_REMOTE_TASK_CALLBACK_THREAD_SIZE;
import static com.alibaba.polardbx.common.properties.ConnectionProperties.MPP_SCHEMA_MAX_MEM;
import static com.alibaba.polardbx.common.properties.ConnectionProperties.MPP_SPILL_PATHS;
import static com.alibaba.polardbx.common.properties.ConnectionProperties.MPP_SPLIT_RUN_QUANTA;
import static com.alibaba.polardbx.common.properties.ConnectionProperties.MPP_STATUS_REFRESH_MAX_WAIT;
import static com.alibaba.polardbx.common.properties.ConnectionProperties.MPP_TABLESCAN_CONNECTION_STRATEGY;
import static com.alibaba.polardbx.common.properties.ConnectionProperties.MPP_TABLESCAN_DS_MAX_SIZE;
import static com.alibaba.polardbx.common.properties.ConnectionProperties.MPP_TASKINFO_CACHE_MAX_ALIVE_MILLIS;
import static com.alibaba.polardbx.common.properties.ConnectionProperties.MPP_TASK_CLIENT_TIMEOUT;
import static com.alibaba.polardbx.common.properties.ConnectionProperties.MPP_TASK_FUTURE_CALLBACK_THREAD_SIZE;
import static com.alibaba.polardbx.common.properties.ConnectionProperties.MPP_TASK_MAX_RUN_TIME;
import static com.alibaba.polardbx.common.properties.ConnectionProperties.MPP_TASK_NOTIFICATION_THREAD_SIZE;
import static com.alibaba.polardbx.common.properties.ConnectionProperties.MPP_TASK_WORKER_THREADS_RATIO;
import static com.alibaba.polardbx.common.properties.ConnectionProperties.MPP_TASK_YIELD_THREAD_SIZE;
import static com.alibaba.polardbx.common.properties.ConnectionProperties.MPP_TP_TASK_WORKER_THREADS_RATIO;

public class MppConfig {

    public static MppConfig getInstance() {
        return instance;
    }

    private MppConfig() {
    }

    public void loadValue(Logger logger, String key, String value) {
        if (key != null && value != null) {
            switch (key.toUpperCase()) {
            case MPP_QUERY_MANAGER_THREAD_SIZE:
                queryManagerThreadPoolSize = parseValue(value, Integer.class,
                    DEFAULT_MPP_QUERY_MANAGER_THREAD_POOL_SIZE);
                break;
            case MPP_TASK_MAX_RUN_TIME:
                taskMaxRunTime = parseValue(value, Long.class, DEFAULT_MPP_TASK_MAX_RUN_TIME);
                break;
            case MPP_MIN_QUERY_EXPIRE_TIME:
                minQueryExpireTime = parseValue(value, Long.class, DEFAULT_MPP_MIN_QUERY_EXPIRE_TIME);
                break;
            case MPP_QUERY_MAX_DELAY_TIME:
                queryMaxDelayTime = parseValue(value, Long.class, DEFAULT_MPP_QUERY_MAX_DELAY_TIME);
                break;
            case MPP_QUERY_MIN_DELAY_TIME:
                queryMinDelayTime = parseValue(value, Long.class, DEFAULT_MPP_QUERY_MIN_DELAY_TIME);
                break;
            case MPP_QUERY_DELAY_COUNT:
                queryDelayCount = parseValue(value, Integer.class, DEFAULT_MPP_QUERY_DELAY_COUNT);
                break;
            case MPP_QUERY_MAX_DELAY_PENDING_RATIO:
                queryMaxDelayPendingRatio = parseValue(value, Integer.class, DEFAULT_MPP_QUERY_MAX_DELAY_PENDING_RATIO);
                break;
            case MPP_QUERY_MIN_DELAY_PENDING_RATIO:
                queryMinDelayPendingRatio = parseValue(value, Integer.class, DEFAULT_MPP_QUERY_MIN_DELAY_PENDING_RATIO);
                break;
            case MPP_MAX_QUERY_EXPIRED_RESERVETION_TIME:
                maxQueryExpiredReservationTime =
                    parseValue(value, Long.class, DEFAULT_MPP_MAX_QUERY_EXPIRED_RESERVETION_TIME);
                break;
            case MPP_MAX_QUERY_HISTORY:
                maxQueryHistory = parseValue(value, Integer.class, DEFAULT_MPP_MAX_QUERY_HISTORY);
                break;
            case MPP_QUERY_CLIENT_TIMEOUT:
                clientTimeout = parseValue(value, Long.class, DEFAULT_MPP_QUERY_CLIENT_TIMEOUT);
                break;
            case MPP_QUERY_REMOTE_TASK_MIN_ERROR:
                remoteTaskMinErrorDuration = parseValue(value, Long.class, DEFAULT_MPP_QUERY_REMOTE_TASK_MIN_ERROR);
                break;
            case MPP_QUERY_REMOTE_TASK_MAX_ERROR:
                remoteTaskMaxErrorDuration = parseValue(value, Long.class, DEFAULT_MPP_QUERY_REMOTE_TASK_MAX_ERROR);
                break;
            case MPP_REMOTE_TASK_CALLBACK_THREAD_SIZE:
                remoteTaskMaxCallbackThreads =
                    parseValue(value, Integer.class, DEFAULT_MPP_REMOTE_TASK_CALLBACK_THREADS);
                break;
            case MPP_QUERY_EXECUTION_THREAD_SIZE:
                queryExecutionThreadPoolSize =
                    parseValue(value, Integer.class, DEFAULT_MPP_QUERY_EXECUTION_THREADPOOL_SIZE);
                break;
            case MPP_CPU_CFS_PERIOD_US:
                cpuCfsPeriodUs = parseValue(value, Integer.class, DEFAULT_MPP_CPU_CFS_PERIOD_US);
                break;
            case MPP_CPU_CFS_QUOTA:
                cpuCfsQuota = parseValue(value, Integer.class, DEFAULT_MPP_CPU_CFS_QUOTA);
                break;
            case MPP_CPU_CFS_MIN_QUOTA:
                cpuCfsMinQuota = parseValue(value, Integer.class, DEFAULT_MPP_CPU_CFS_MIN_QUOTA);
                break;
            case MPP_CPU_CFS_MAX_QUOTA:
                cpuCfsMaxQuota = parseValue(value, Integer.class, DEFAULT_MPP_CPU_CFS_MAX_QUOTA);
                break;
            case MPP_AP_PRIORITY:
                cpuCfsMaxQuota = parseValue(value, Integer.class, DEFAULT_MPP_AP_PRIORITY);
                break;
            case MPP_TP_TASK_WORKER_THREADS_RATIO:
                tpTaskWorkerThreadsRatio = parseValue(value, Integer.class, DEFAULT_MPP_TP_TASK_WORKER_THREADS_RATIO);
                break;
            case MPP_TASK_WORKER_THREADS_RATIO:
                taskWorkerThreadsRatio = parseValue(value, Integer.class, DEFAULT_MPP_TASK_WORKER_THREADS_RATIO);
                break;
            case MPP_SPLIT_RUN_QUANTA:
                splitRunQuanta = parseValue(value, Long.class, DEFAULT_MPP_SPLIT_RUN_QUANTA);
                break;
            case MPP_STATUS_REFRESH_MAX_WAIT:
                statusRefreshMaxWait = parseValue(value, Long.class, DEFAULT_MPP_STATUS_REFRESH_MAX_WAIT);
                break;
            case MPP_INFO_UPDATE_INTERVAL:
                infoUpdateInterval = parseValue(value, Long.class, DEFAULT_MPP_INFO_UPDATE_INTERVAL);
                break;
            case MPP_MAX_WORKER_THREAD_SIZE:
                maxWorkerThreads = parseValue(value, Integer.class, DEFAULT_MPP_MAX_WORKER_THREADS);
                break;
            case MPP_TASK_CLIENT_TIMEOUT:
                taskClientTimeout = parseValue(value, Long.class, DEFAULT_MPP_TASK_CLIENT_TIMEOUT);
                break;
            case MPP_TASKINFO_CACHE_MAX_ALIVE_MILLIS:
                taskInfoCacheMaxAliveMillis =
                    parseValue(value, Long.class, DEFAULT_MPP_TASKINFO_CACHE_MAX_ALIVE_MILLIS);
                break;
            case MPP_HTTP_RESPONSE_THREAD_SIZE:
                httpResponseThreads = parseValue(value, Integer.class, DEFAULT_MPP_HTTP_RESPONSE_THREADS);
                break;
            case MPP_HTTP_TIMEOUT_THREAD_SIZE:
                httpTimeoutThreads = parseValue(value, Integer.class, DEFAULT_MPP_HTTP_TIMEOUT_THREADS);
                break;
            case MPP_TASK_NOTIFICATION_THREAD_SIZE:
                taskNotificationThreads = parseValue(value, Integer.class, DEFAULT_MPP_TASK_NOTIFICATION_THEADS);
                break;
            case MPP_TASK_YIELD_THREAD_SIZE:
                taskYieldThreads = parseValue(value, Integer.class, DEFAULT_MPP_TASK_YIELD_THEADS);
                break;
            case MPP_LOW_PRIORITY_ENABLED:
                taskExecutorLowPriorityEnabled = parseValue(value, Boolean.class, DEFAULT_MPP_LOW_PRIORITY_ENABLED);
                break;
            case MPP_TASK_FUTURE_CALLBACK_THREAD_SIZE:
                taskFutureCallbackThreads = parseValue(value, Integer.class, DEFAULT_MPP_TASK_FUTURE_CALLBACK_THREADS);
                break;
            case MPP_EXCHANGE_CONCURRENT_MULTIPLIER:
                exchangeConcurrentRequestMultiplier =
                    parseValue(value, Integer.class, DEFAULT_MPP_EXCHANGE_CONCURRENT_MULTIPLIER);
                break;
            case MPP_EXCHANGE_MIN_ERROR_DURATION:
                exchangeMinErrorDuration = parseValue(value, Long.class, DEFAULT_MPP_EXCHANGE_MIN_ERROR_DURATION);
                break;
            case MPP_EXCHANGE_MAX_ERROR_DURATION:
                exchangeMaxErrorDuration = parseValue(value, Long.class, DEFAULT_MPP_EXCHANGE_MAX_ERROR_DURATION);
                break;
            case MPP_EXCHANGE_MAX_RESPONSE_SIZE:
                exchangeMaxResponseSize = parseValue(value, Long.class, DEFAULT_MPP_EXCHANGE_MAX_RESPONSE_SIZE);
                break;
            case MPP_EXCHANGE_CLIENT_THREAD_SIZE:
                exchangeClientThreads = parseValue(value, Integer.class, DEFAULT_MPP_EXCHANGE_CLIENT_THREADS);
                break;
            case MPP_HTTP_MAX_REQUESTS_PER_DESTINATION:
                httpMaxRequestsPerDestination =
                    parseValue(value, Integer.class, DEFAULT_MPP_HTTP_MAX_REQUESTS_PER_DESTINATION);
                break;
            case MPP_TABLESCAN_CONNECTION_STRATEGY:
                tablescanConnectionStrategy =
                    parseValue(value, Integer.class, DEFAULT_MPP_TABLESCAN_CONNECTION_STRATEGY);
                break;
            case MPP_TABLESCAN_DS_MAX_SIZE:
                tablescanDsMaxSize =
                    parseValue(value, Integer.class, DEFAULT_MPP_TABLESCAN_DS_MAX_SIZE);
                break;
            case MPP_SCHEMA_MAX_MEM:
                schemaMaxMemory = parseValue(value, Long.class, DEFAULT_MPP_SCHEMA_MAX_MEM);
                break;
            case MPP_MEMORY_REVOKING_THRESHOLD:
                memoryRevokingThreshold = parseValue(value, Double.class, DEFAULT_MPP_MEMORY_REVOKING_THRESHOLD);
                break;
            case MPP_MEMORY_REVOKING_TARGET:
                memoryRevokingTarget = parseValue(value, Double.class, DEFAULT_MPP_MEMORY_REVOKING_TARGET);
                break;
            case MPP_NOTIFY_BLOCKED_QUERY_MEMORY:
                notifyBlockedQueryMemory = parseValue(value, long.class, DEFAULT_MPP_NOTIFY_BLOCKED_QUERY_MEMORY);
                break;
            case MPP_HTTP_SERVER_MAX_THREADS:
                httpServerMaxThreads = parseValue(value, Integer.class, DEFAULT_MPP_HTTP_SERVER_MAX_THREADS);
                break;
            case MPP_HTTP_SERVER_MIN_THREADS:
                httpServerMinThreads = parseValue(value, Integer.class, DEFAULT_MPP_HTTP_SERVER_MIN_THREADS);
                break;
            case MPP_HTTP_CLIENT_MAX_THREADS:
                httpClientMaxThreads = parseValue(value, Integer.class, DEFAULT_MPP_HTTP_CLIENT_MAX_THREADS);
                break;
            case MPP_HTTP_CLIENT_MIN_THREADS:
                httpClientMinThreads = parseValue(value, Integer.class, DEFAULT_MPP_HTTP_CLIENT_MIN_THREADS);
                break;
            case MPP_HTTP_CLIENT_MAX_CONNECTIONS:
                httpClientMaxConnections = parseValue(value, Integer.class, DEFAULT_MPP_HTTP_CLIENT_MAX_CONNECTIONS);
                break;
            case MPP_HTTP_CLIENT_MAX_CONNECTIONS_PER_SERVER:
                httpClientMaxConnectionsPerServer =
                    parseValue(value, Integer.class, DEFAULT_MPP_HTTP_CLIENT_MAX_CONNECTIONS_PER_SERVER);
                break;
            case MPP_MAX_SPILL_THREADS:
                maxSpillThreads = parseValue(value, Integer.class, DEFAULT_MAX_SPILL_THREADS);
                break;
            case MPP_GLOBAL_MEMORY_LIMIT_RATIO:
                globalMemoryLimitRatio = parseValue(value, Double.class, DEFAULT_MPP_GLOBAL_MEMORY_LIMIT_RATIO);
                break;
            case MPP_LESS_REVOKE_BYTES:
                lessRevokeBytes = parseValue(value, Long.class, DEFAULT_MPP_LESS_REVOKE_BYTES);
                break;
            case MPP_ALLOCATOR_SIZE:
                blockSize = parseValue(value, Long.class, DEFAULT_MPP_ALLOCATOR_SIZE);
                break;
            case MPP_CLUSTER_NAME:
                defaultCluster = parseValue(value, String.class, DEFAULT_MPP_CLUSTER_NAME);
                break;
            case MPP_MAX_SPILL_FD_THRESHOLD:
                maxSpillFdThreshold = parseValue(value, Integer.class, DEFAULT_MAX_SPILL_FD_THRESHOLD);
                break;
            case MPP_MAX_SPILL_SPACE_THRESHOLD:
                maxSpillSpaceThreshold = parseValue(value, Double.class, DEFAULT_MAX_SPILL_SPACE_THRESHOLD);
                break;
            case MPP_AVAILABLE_SPILL_SPACE_THRESHOLD:
                maxAvaliableSpaceThreshold = parseValue(value, Double.class, DEFAULT_AVALIABLE_SPACE_THRESHOLD);
                break;
            case MPP_MAX_QUERY_SPILL_SPACE_THRESHOLD:
                maxQuerySpillSpaceThreshold = parseValue(value, Double.class, DEFAULT_MAX_QUERY_SPILL_SPACE_THRESHOLD);
                break;
            case MPP_SPILL_PATHS:
                List<String> spillPathsSplit = ImmutableList.copyOf(
                    Splitter.on(",").trimResults().omitEmptyStrings().split(value));
                spillPaths = spillPathsSplit.stream().map(path -> Paths.get(path)).collect(Collectors.toList());
                break;
            default:
                logger.warn("unknown mpp config:" + key + ",value=" + value);
            }
        }
    }



    private static final int DEFAULT_MPP_QUERY_MANAGER_THREAD_POOL_SIZE = 1;
    private int queryManagerThreadPoolSize = DEFAULT_MPP_QUERY_MANAGER_THREAD_POOL_SIZE;

    public int getQueryManagerThreadPoolSize() {
        return queryManagerThreadPoolSize;
    }

    private static final int DEFAULT_MPP_QUERY_EXECUTION_THREADPOOL_SIZE = 128;
    private int queryExecutionThreadPoolSize = DEFAULT_MPP_QUERY_EXECUTION_THREADPOOL_SIZE;

    public int getQueryExecutionThreadPoolSize() {
        return queryExecutionThreadPoolSize;
    }

    private static final int DEFAULT_MPP_REMOTE_TASK_CALLBACK_THREADS = 100;
    private int remoteTaskMaxCallbackThreads = DEFAULT_MPP_REMOTE_TASK_CALLBACK_THREADS;

    public int getRemoteTaskMaxCallbackThreads() {
        return remoteTaskMaxCallbackThreads;
    }

    private static final long DEFAULT_MPP_TASK_MAX_RUN_TIME = 1800000L;
    private long taskMaxRunTime = DEFAULT_MPP_TASK_MAX_RUN_TIME;

    public long getTaskMaxRunTime() {
        return taskMaxRunTime;
    }

    private static final int DEFAULT_MPP_CPU_CFS_PERIOD_US = 100000;
    private int cpuCfsPeriodUs = DEFAULT_MPP_CPU_CFS_PERIOD_US;

    public int getCpuCfsPeriodUs() {
        return cpuCfsPeriodUs;
    }

    private static final int DEFAULT_MPP_CPU_CFS_QUOTA = 0;
    private int cpuCfsQuota = DEFAULT_MPP_CPU_CFS_QUOTA;

    private static final int DEFAULT_MPP_CPU_CFS_MIN_QUOTA = 5;
    private int cpuCfsMinQuota = DEFAULT_MPP_CPU_CFS_MIN_QUOTA;

    public int getCpuCfsMinQuota() {
        return cpuCfsMinQuota;
    }

    private static final int DEFAULT_MPP_CPU_CFS_MAX_QUOTA = 95;
    private int cpuCfsMaxQuota = DEFAULT_MPP_CPU_CFS_MAX_QUOTA;

    public int getCpuCfsMaxQuota() {
        return cpuCfsMaxQuota;
    }

    private static final int DEFAULT_MPP_AP_PRIORITY = 3;
    private int apPriority = DEFAULT_MPP_AP_PRIORITY;

    public int getApPriority() {
        return apPriority;
    }

    private static final long DEFAULT_MPP_QUERY_MAX_DELAY_TIME = 1000L;
    private long queryMaxDelayTime = DEFAULT_MPP_QUERY_MAX_DELAY_TIME;

    public long getQueryMaxDelayTime() {
        return queryMaxDelayTime;
    }

    private static final long DEFAULT_MPP_QUERY_MIN_DELAY_TIME = 100L;
    private long queryMinDelayTime = DEFAULT_MPP_QUERY_MIN_DELAY_TIME;

    public long getQueryMinDelayTime() {
        return queryMinDelayTime;
    }

    private static final int DEFAULT_MPP_QUERY_DELAY_COUNT = 3;
    private int queryDelayCount = DEFAULT_MPP_QUERY_DELAY_COUNT;

    public int getQueryDelayCount() {
        return queryDelayCount;
    }

    private static final int DEFAULT_MPP_QUERY_MAX_DELAY_PENDING_RATIO = 10;
    private int queryMaxDelayPendingRatio = DEFAULT_MPP_QUERY_MAX_DELAY_PENDING_RATIO;

    public int getQueryMaxDelayPendingRatio() {
        return queryMaxDelayPendingRatio;
    }

    private static final int DEFAULT_MPP_QUERY_MIN_DELAY_PENDING_RATIO = 2;
    private int queryMinDelayPendingRatio = DEFAULT_MPP_QUERY_MIN_DELAY_PENDING_RATIO;

    public int getQueryMinDelayPendingRatio() {
        return queryMinDelayPendingRatio;
    }

    private static final long DEFAULT_MPP_MIN_QUERY_EXPIRE_TIME = 0L;
    private long minQueryExpireTime = DEFAULT_MPP_MIN_QUERY_EXPIRE_TIME;

    public long getMinQueryExpireTime() {
        return minQueryExpireTime;
    }

    private static final long DEFAULT_MPP_MAX_QUERY_EXPIRED_RESERVETION_TIME = 10 * 3600 * 24 * 1000L;
    private long maxQueryExpiredReservationTime = DEFAULT_MPP_MAX_QUERY_EXPIRED_RESERVETION_TIME;

    public long getMaxQueryExpiredReservationTime() {
        return maxQueryExpiredReservationTime;
    }

    private static final int DEFAULT_MPP_MAX_QUERY_HISTORY = 10;
    private int maxQueryHistory = DEFAULT_MPP_MAX_QUERY_HISTORY;

    public int getMaxQueryHistory() {
        return maxQueryHistory;
    }

    private static final long DEFAULT_MPP_QUERY_CLIENT_TIMEOUT = 120 * 60 * 1000L;
    private long clientTimeout = DEFAULT_MPP_QUERY_CLIENT_TIMEOUT;

    public long getClientTimeout() {
        return clientTimeout;
    }

    private static final long DEFAULT_MPP_QUERY_REMOTE_TASK_MIN_ERROR = 30000L;
    private long remoteTaskMinErrorDuration = DEFAULT_MPP_QUERY_REMOTE_TASK_MIN_ERROR;

    public long getRemoteTaskMinErrorDuration() {
        return remoteTaskMinErrorDuration;
    }

    private static final long DEFAULT_MPP_QUERY_REMOTE_TASK_MAX_ERROR = 180000L;
    private long remoteTaskMaxErrorDuration = DEFAULT_MPP_QUERY_REMOTE_TASK_MAX_ERROR;

    public long getRemoteTaskMaxErrorDuration() {
        return remoteTaskMaxErrorDuration;
    }

    private static final Long DEFAULT_MPP_SCHEMA_MAX_MEM = Long.MAX_VALUE;
    private long schemaMaxMemory = DEFAULT_MPP_SCHEMA_MAX_MEM;

    @Deprecated
    public long getSchemaMaxQueryMemory() {
        return schemaMaxMemory;
    }

    private static final Double DEFAULT_MPP_MEMORY_REVOKING_THRESHOLD = 0.85;
    private double memoryRevokingThreshold = DEFAULT_MPP_MEMORY_REVOKING_THRESHOLD;

    public double getMemoryRevokingThreshold() {
        return memoryRevokingThreshold;
    }

    private static final Double DEFAULT_MPP_MEMORY_REVOKING_TARGET = 0.75;
    private double memoryRevokingTarget = DEFAULT_MPP_MEMORY_REVOKING_TARGET;

    public double getMemoryRevokingTarget() {
        return memoryRevokingTarget;
    }

    private static final long DEFAULT_MPP_NOTIFY_BLOCKED_QUERY_MEMORY = 1024 * 1024 * 16;
    private long notifyBlockedQueryMemory = DEFAULT_MPP_NOTIFY_BLOCKED_QUERY_MEMORY;

    public long getNotifyBlockedQueryMemory() {
        return notifyBlockedQueryMemory;
    }

    private static final int DEFAULT_MPP_TASK_NOTIFICATION_THEADS = 16;
    private int taskNotificationThreads = DEFAULT_MPP_TASK_NOTIFICATION_THEADS;

    public int getTaskNotificationThreads() {
        return taskNotificationThreads;
    }

    private static final int DEFAULT_MPP_TASK_YIELD_THEADS = 1;
    private int taskYieldThreads = DEFAULT_MPP_TASK_YIELD_THEADS;

    public int getTaskYieldThreads() {
        return taskYieldThreads;
    }

    private static final int DEFAULT_MPP_MAX_WORKER_THREADS = 1024;
    private int maxWorkerThreads = DEFAULT_MPP_MAX_WORKER_THREADS;

    public int getMaxWorkerThreads() {
        return maxWorkerThreads;
    }

    private static final int DEFAULT_MPP_TASK_FUTURE_CALLBACK_THREADS = 16;
    private int taskFutureCallbackThreads = DEFAULT_MPP_TASK_FUTURE_CALLBACK_THREADS;

    public int getTaskFutureCallbackThreads() {
        return taskFutureCallbackThreads;
    }

    private static final int DEFAULT_MPP_TP_TASK_WORKER_THREADS_RATIO = 4;
    private int tpTaskWorkerThreadsRatio = DEFAULT_MPP_TP_TASK_WORKER_THREADS_RATIO;

    public int getTpTaskWorkerThreadsRatio() {
        return tpTaskWorkerThreadsRatio;
    }

    private static final int DEFAULT_MPP_TASK_WORKER_THREADS_RATIO = 4;
    private int taskWorkerThreadsRatio = DEFAULT_MPP_TASK_WORKER_THREADS_RATIO;

    public int getTaskWorkerThreadsRatio() {
        return taskWorkerThreadsRatio;
    }

    private static final long DEFAULT_MPP_SPLIT_RUN_QUANTA = 1000L;
    private long splitRunQuanta = DEFAULT_MPP_SPLIT_RUN_QUANTA;

    public long getSplitRunQuanta() {
        return splitRunQuanta;
    }

    private static final long DEFAULT_MPP_STATUS_REFRESH_MAX_WAIT = 300000L;
    private long statusRefreshMaxWait = DEFAULT_MPP_STATUS_REFRESH_MAX_WAIT;

    public long getStatusRefreshMaxWait() {
        return statusRefreshMaxWait;
    }

    private static final long DEFAULT_MPP_INFO_UPDATE_INTERVAL = 3000L;
    private long infoUpdateInterval = DEFAULT_MPP_INFO_UPDATE_INTERVAL;

    public long getInfoUpdateInterval() {
        return infoUpdateInterval;
    }

    private static final long DEFAULT_MPP_TASK_CLIENT_TIMEOUT = 60000L;
    private long taskClientTimeout = DEFAULT_MPP_TASK_CLIENT_TIMEOUT;

    public long getTaskClientTimeout() {
        return taskClientTimeout;
    }

    private static final long DEFAULT_MPP_TASKINFO_CACHE_MAX_ALIVE_MILLIS = 60000L;
    private long taskInfoCacheMaxAliveMillis = DEFAULT_MPP_TASKINFO_CACHE_MAX_ALIVE_MILLIS;

    public long getTaskInfoCacheMaxAliveMillis() {
        return taskInfoCacheMaxAliveMillis;
    }

    private static final boolean DEFAULT_MPP_LOW_PRIORITY_ENABLED = false;
    private boolean taskExecutorLowPriorityEnabled = DEFAULT_MPP_LOW_PRIORITY_ENABLED;

    public boolean isTaskExecutorLowPriorityEnabled() {
        return taskExecutorLowPriorityEnabled;
    }

    private static final int DEFAULT_MPP_EXCHANGE_CONCURRENT_MULTIPLIER = 3;
    private int exchangeConcurrentRequestMultiplier = DEFAULT_MPP_EXCHANGE_CONCURRENT_MULTIPLIER;

    public int getExchangeConcurrentRequestMultiplier() {
        return exchangeConcurrentRequestMultiplier;
    }

    private static final long DEFAULT_MPP_EXCHANGE_MIN_ERROR_DURATION = 60000L;
    private long exchangeMinErrorDuration = DEFAULT_MPP_EXCHANGE_MIN_ERROR_DURATION;

    public long getExchangeMinErrorDuration() {
        return exchangeMinErrorDuration;
    }

    private static final long DEFAULT_MPP_EXCHANGE_MAX_ERROR_DURATION = 180000L;
    private long exchangeMaxErrorDuration = DEFAULT_MPP_EXCHANGE_MAX_ERROR_DURATION;

    public long getExchangeMaxErrorDuration() {
        return exchangeMaxErrorDuration;
    }

    private static final long DEFAULT_MPP_EXCHANGE_MAX_RESPONSE_SIZE = 1000000L;
    private long exchangeMaxResponseSize = DEFAULT_MPP_EXCHANGE_MAX_RESPONSE_SIZE;

    public long getExchangeMaxResponseSize() {
        return exchangeMaxResponseSize;
    }

    private static final int DEFAULT_MPP_EXCHANGE_CLIENT_THREADS = 50;
    private int exchangeClientThreads = DEFAULT_MPP_EXCHANGE_CLIENT_THREADS;

    public int getExchangeClientThreads() {
        return exchangeClientThreads;
    }

    private static final int DEFAULT_MPP_HTTP_RESPONSE_THREADS = 100;
    private int httpResponseThreads = DEFAULT_MPP_HTTP_RESPONSE_THREADS;

    public int getHttpResponseThreads() {
        return httpResponseThreads;
    }

    private static final int DEFAULT_MPP_HTTP_TIMEOUT_THREADS = 3;
    private int httpTimeoutThreads = DEFAULT_MPP_HTTP_TIMEOUT_THREADS;

    public int getHttpTimeoutThreads() {
        return httpTimeoutThreads;
    }

    private static final int DEFAULT_MPP_HTTP_SERVER_MAX_THREADS = 200;
    private int httpServerMaxThreads = DEFAULT_MPP_HTTP_SERVER_MAX_THREADS;

    public int getHttpServerMaxThreads() {
        return httpServerMaxThreads;
    }

    private static final int DEFAULT_MPP_HTTP_SERVER_MIN_THREADS = 2;
    private int httpServerMinThreads = DEFAULT_MPP_HTTP_SERVER_MIN_THREADS;

    public int getHttpServerMinThreads() {
        return httpServerMinThreads;
    }

    private static final int DEFAULT_MPP_HTTP_CLIENT_MAX_THREADS = 200;
    private int httpClientMaxThreads = DEFAULT_MPP_HTTP_CLIENT_MAX_THREADS;

    public int getHttpClientMaxThreads() {
        return httpClientMaxThreads;
    }

    private static final int DEFAULT_MPP_HTTP_CLIENT_MIN_THREADS = 8;
    private int httpClientMinThreads = DEFAULT_MPP_HTTP_CLIENT_MIN_THREADS;

    public int getHttpClientMinThreads() {
        return httpClientMinThreads;
    }

    private static final int DEFAULT_MPP_HTTP_CLIENT_MAX_CONNECTIONS = 1024;
    private int httpClientMaxConnections = DEFAULT_MPP_HTTP_CLIENT_MAX_CONNECTIONS;

    public int getHttpClientMaxConnections() {
        return httpClientMaxConnections;
    }

    private static final int DEFAULT_MPP_HTTP_CLIENT_MAX_CONNECTIONS_PER_SERVER = 250;
    private int httpClientMaxConnectionsPerServer = DEFAULT_MPP_HTTP_CLIENT_MAX_CONNECTIONS_PER_SERVER;

    public int getDefaultMppHttpClientMaxConnectionsPerServer() {
        return httpClientMaxConnectionsPerServer;
    }

    private static final int DEFAULT_MPP_HTTP_MAX_REQUESTS_PER_DESTINATION = 50000;
    private int httpMaxRequestsPerDestination = DEFAULT_MPP_HTTP_MAX_REQUESTS_PER_DESTINATION;

    public int getHttpMaxRequestsPerDestination() {
        return httpMaxRequestsPerDestination;
    }

    private static final int DEFAULT_MPP_TABLESCAN_CONNECTION_STRATEGY = 0;
    private int tablescanConnectionStrategy = DEFAULT_MPP_TABLESCAN_CONNECTION_STRATEGY;

    public int getTableScanConnectionStrategy() {
        return tablescanConnectionStrategy;
    }

    private static final int DEFAULT_MPP_TABLESCAN_DS_MAX_SIZE = -1;
    private int tablescanDsMaxSize = DEFAULT_MPP_TABLESCAN_DS_MAX_SIZE;

    public int getTableScanDsMaxSize() {
        return tablescanDsMaxSize;
    }

    private static final int DEFAULT_MAX_SPILL_THREADS = ThreadCpuStatUtil.NUM_CORES;
    private int maxSpillThreads = DEFAULT_MAX_SPILL_THREADS;

    public int getMaxSpillThreads() {
        return maxSpillThreads;
    }

    private static final double DEFAULT_MPP_GLOBAL_MEMORY_LIMIT_RATIO = 1.0;
    private double globalMemoryLimitRatio = DEFAULT_MPP_GLOBAL_MEMORY_LIMIT_RATIO;

    public double getGlobalMemoryLimitRatio() {
        return globalMemoryLimitRatio;
    }

    private static final long DEFAULT_MPP_LESS_REVOKE_BYTES = 32 * (1L << 20);
    private long lessRevokeBytes = DEFAULT_MPP_LESS_REVOKE_BYTES;

    public long getLessRevokeBytes() {
        return lessRevokeBytes;
    }

    private static final long DEFAULT_MPP_ALLOCATOR_SIZE = 1L << 17;
    private long blockSize = DEFAULT_MPP_ALLOCATOR_SIZE;

    public long getBlockSize() {
        return blockSize;
    }

    private static String DEFAULT_MPP_CLUSTER_NAME = "DEFAULT";
    private String defaultCluster = DEFAULT_MPP_CLUSTER_NAME;

    public String getDefaultCluster() {
        return defaultCluster;
    }

    private static final int DEFAULT_MAX_SPILL_FD_THRESHOLD = 10000;
    private int maxSpillFdThreshold = DEFAULT_MAX_SPILL_FD_THRESHOLD;

    public int getMaxSpillFdThreshold() {
        return maxSpillFdThreshold;
    }

    private static final double DEFAULT_MAX_SPILL_SPACE_THRESHOLD = 0.1;
    private double maxSpillSpaceThreshold = DEFAULT_MAX_SPILL_SPACE_THRESHOLD;

    public double getMaxSpillSpaceThreshold() {
        return maxSpillSpaceThreshold;
    }

    private static final double DEFAULT_AVALIABLE_SPACE_THRESHOLD = 0.9;
    private double maxAvaliableSpaceThreshold = DEFAULT_AVALIABLE_SPACE_THRESHOLD;

    public double getAvaliableSpillSpaceThreshold() {
        return maxAvaliableSpaceThreshold;
    }

    private static final double DEFAULT_MAX_QUERY_SPILL_SPACE_THRESHOLD = 0.3;
    private double maxQuerySpillSpaceThreshold = DEFAULT_MAX_QUERY_SPILL_SPACE_THRESHOLD;

    public double getMaxQuerySpillSpaceThreshold() {
        return maxQuerySpillSpaceThreshold;
    }

    private static final List<Path> DEFAULT_SPILL_PATHS = initDefaultPathList();

    private static List<Path> initDefaultPathList() {
        List<Path> paths = new ArrayList<>();
        paths.add(Paths.get("../spill"));
        return paths;
    }

    private List<Path> spillPaths = DEFAULT_SPILL_PATHS;

    public List<Path> getSpillPaths() {
        return spillPaths;
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

    private static MppConfig instance = new MppConfig();
}
