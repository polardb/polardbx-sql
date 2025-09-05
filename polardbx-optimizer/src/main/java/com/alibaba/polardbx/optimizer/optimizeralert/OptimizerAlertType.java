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

package com.alibaba.polardbx.optimizer.optimizeralert;

public enum OptimizerAlertType {
    BKA_TOO_MUCH,

    TP_SLOW,

    SELECTIVITY_ERR,

    SPM_ERR,

    PRUNING_SLOW,


    /*-----------STATISTIC-------------*/

    STATISTIC_MISS,

    STATISTIC_JOB_INTERRUPT,

    STATISTIC_INCONSISTENT,

    STATISTIC_SAMPLE_FAIL,

    STATISTIC_HLL_FAIL,

    STATISTIC_COLLECT_ROWCOUNT_FAIL,

    STATISTIC_PERSIST_FAIL,

    STATISTIC_SYNC_FAIL,

    STATISTIC_SCHEDULE_JOB_INFORMATION_TABLES_FAIL,
    STATISTIC_SCHEDULE_JOB_SAMPLE_FAIL,
    STATISTIC_SCHEDULE_JOB_HLL_FAIL,

    ;

}
