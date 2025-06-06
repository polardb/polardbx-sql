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

package com.alibaba.polardbx.gms.scheduler;

import com.alibaba.polardbx.gms.module.Module;

import static com.alibaba.polardbx.gms.module.Module.METRIC;
import static com.alibaba.polardbx.gms.module.Module.MODULE_LOG;
import static com.alibaba.polardbx.gms.module.Module.OPTIMIZER;
import static com.alibaba.polardbx.gms.module.Module.OSS;
import static com.alibaba.polardbx.gms.module.Module.SPM;
import static com.alibaba.polardbx.gms.module.Module.STATISTICS;
import static com.alibaba.polardbx.gms.module.Module.TRX;
import static com.alibaba.polardbx.gms.module.Module.UNKNOWN;

public enum ScheduledJobExecutorType {

    LOCAL_PARTITION(UNKNOWN),
    TTL_JOB(UNKNOWN),
    REBALANCE(UNKNOWN),
    PARTITION_VISUALIZER(UNKNOWN),
    REFRESH_MATERIALIZED_VIEW(UNKNOWN),
    BASELINE_SYNC(SPM),
    STATISTIC_SAMPLE_SKETCH(STATISTICS),
    STATISTIC_CHECK(STATISTICS),
    STATISTIC_HLL_SKETCH(STATISTICS),
    STATISTIC_ROWCOUNT_COLLECTION(STATISTICS),
    STATISTIC_RELOAD(STATISTICS),
    STATISTIC_INFO_SCHEMA_TABLES(STATISTICS),
    PURGE_OSS_FILE(OSS),

    OPTIMIZER_ALERT(OPTIMIZER),
    AUTO_SPLIT_TABLE_GROUP(UNKNOWN),
    PERSIST_GSI_STATISTICS(UNKNOWN),
    @Deprecated
    CLEAN_LOG_TABLE(TRX),
    CLEAN_LOG_TABLE_V2(TRX),
    CHECK_CCI(TRX),
    GENERATE_COLUMNAR_SNAPSHOT(TRX),

    LOG_SYSTEM_METRICS(METRIC);

    ScheduledJobExecutorType(Module module) {
        m = module;
    }

    private final Module m;

    public Module module() {
        return m;
    }
}