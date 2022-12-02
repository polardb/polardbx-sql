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

import static com.alibaba.polardbx.gms.module.Module.OSS;
import static com.alibaba.polardbx.gms.module.Module.SPM;
import static com.alibaba.polardbx.gms.module.Module.STATISTIC;
import static com.alibaba.polardbx.gms.module.Module.UNKNOWN;

public enum ScheduledJobExecutorType {

    LOCAL_PARTITION(UNKNOWN),
    REBALANCE(UNKNOWN),
    PARTITION_VISUALIZER(UNKNOWN),
    BASELINE_SYNC(SPM),
    STATISTIC_SAMPLE_SKETCH(STATISTIC),
    STATISTIC_ROWCOUNT_COLLECTION(STATISTIC),
    PURGE_OSS_FILE(OSS),
    AUTO_SPLIT_TABLE_GROUP(UNKNOWN);

    ScheduledJobExecutorType(Module module) {
        m = module;
    }

    private final Module m;

    public Module module() {
        return m;
    }
}