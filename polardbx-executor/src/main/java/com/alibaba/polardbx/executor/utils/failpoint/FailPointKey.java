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

package com.alibaba.polardbx.executor.utils.failpoint;

/**
 * fail point injection key
 * use set command to enable specific FailPoint
 */
public interface FailPointKey {

    /**
     * set @FP_ASSERT='true';
     * 注入assert
     * 默认开启，default=true
     */
    String FP_ASSERT = "FP_ASSERT";

    /**
     * set @FP_RANDOM_FAIL='30';
     * 注入随机概率异常
     */
    String FP_RANDOM_FAIL = "FP_RANDOM_FAIL";

    /**
     * set @FP_RANDOM_SUSPEND='30,3000';
     * 注入随机概率停顿，单位是ms
     */
    String FP_RANDOM_SUSPEND = "FP_RANDOM_SUSPEND";

    /**
     * set @FP_RANDOM_HANG='30';
     * 注入随机概率死循环
     */
    String FP_RANDOM_HANG = "FP_RANDOM_HANG";

    /**
     * set @FP_RANDOM_CRASH='30';
     * 注入随机概率crash
     */
    String FP_RANDOM_CRASH = "FP_RANDOM_CRASH";

    String FP_OVERRIDE_NOW = "FP_OVERRIDE_NOW";

    /**
     * set @FP_RANDOM_PHYSICAL_DDL_EXCEPTION='30';
     * inject exception during executing physical DDL
     * 执行物理DDL时随机失败，可指定失败概率
     */
    String FP_RANDOM_PHYSICAL_DDL_EXCEPTION = "FP_RANDOM_PHYSICAL_DDL_EXCEPTION";

    /**
     * set @FP_BEFORE_PHYSICAL_DDL_EXCEPTION='true';
     * 执行物理DDL之前全部分片失败
     */
    String FP_BEFORE_PHYSICAL_DDL_EXCEPTION = "FP_BEFORE_PHYSICAL_DDL_EXCEPTION";

    /**
     * set @FP_AFTER_PHYSICAL_DDL_EXCEPTION='true';
     * 执行物理DDL之后全部分片失败
     */
    String FP_AFTER_PHYSICAL_DDL_EXCEPTION = "FP_AFTER_PHYSICAL_DDL_EXCEPTION";

    /**
     * set @FP_BEFORE_PHYSICAL_DDL_PARTIAL_EXCEPTION='true';
     * 执行物理DDL之前部分分片失败
     */
    String FP_BEFORE_PHYSICAL_DDL_PARTIAL_EXCEPTION = "FP_BEFORE_PHYSICAL_DDL_PARTIAL_EXCEPTION";

    /**
     * set @FP_AFTER_PHYSICAL_DDL_PARTIAL_EXCEPTION='true';
     * 执行物理DDL之后部分分片失败
     */
    String FP_AFTER_PHYSICAL_DDL_PARTIAL_EXCEPTION = "FP_AFTER_PHYSICAL_DDL_PARTIAL_EXCEPTION";

    /**
     * set @FP_PHYSICAL_DDL_INTERRUPTED='true';
     * 执行物理DDL期间中断
     */
    String FP_PHYSICAL_DDL_INTERRUPTED = "FP_PHYSICAL_DDL_INTERRUPTED";

    /**
     * set @FP_NEW_SEQ_EXCEPTION_RIGHT_AFTER_PHY_CREATION='true';
     * 创建New Seq的底层Seq后立即失败
     */
    String FP_NEW_SEQ_EXCEPTION_RIGHT_AFTER_PHY_CREATION = "FP_NEW_SEQ_EXCEPTION_RIGHT_AFTER_PHY_CREATION";

    /**
     * set @FP_SPECIFIED_TABLE_DROP_PHY_EXCEPTION='tableName';
     * 指定表执行 DROP TABLE 物理 DDL 异常
     */
    String FP_SPECIFIED_TABLE_DROP_PHY_EXCEPTION = "FP_SPECIFIED_TABLE_DROP_PHY_EXCEPTION";

    /**
     * set @FP_PHYSICAL_DDL_TIMEOUT='1000';
     * 执行物理DDL时超时（毫秒）
     */
    String FP_PHYSICAL_DDL_TIMEOUT = "FP_PHYSICAL_DDL_TIMEOUT";

    /**
     * changeset catchup task sleep time (ms)
     */
    String FP_CATCHUP_TASK_SUSPEND = "FP_CATCHUP_TASK_SUSPEND";

    /**
     * set @FP_RANDOM_BACKFILL_EXCEPTION='30';
     * Backfill时随机失败，可指定失败概率
     */
    String FP_RANDOM_BACKFILL_EXCEPTION = "FP_RANDOM_BACKFILL_EXCEPTION";

    /**
     * set @FP_FAIL_ON_DDL_TASK_NAME='GsiInsertMetaTask';
     * 根据taskName注入异常
     */
    String FP_FAIL_ON_DDL_TASK_NAME = "FP_FAIL_ON_DDL_TASK_NAME";

    /**
     * set @FP_PAUSE_AFTER_DDL_TASK_EXECUTION='GsiInsertMetaTask';
     * 根据taskName注入异常
     */
    String FP_PAUSE_AFTER_DDL_TASK_EXECUTION = "FP_PAUSE_AFTER_DDL_TASK_EXECUTION";

    /**
     * set @FP_ROLLBACK_AFTER_DDL_TASK_EXECUTION='FP_FAIL_AFTER_DDL_TASK_EXECUTION';
     * 根据taskName注入异常
     */
    String FP_ROLLBACK_AFTER_DDL_TASK_EXECUTION = "FP_ROLLBACK_AFTER_DDL_TASK_EXECUTION";

    /**
     * set @FP_HIJACK_DDL_JOB='15,6,30';
     * 劫持所有的JOB, 替换为MockDdlJob
     */
    String FP_HIJACK_DDL_JOB = "FP_HIJACK_DDL_JOB";

    /**
     * set @FP_HIJACK_DDL_JOB_FORMAT='RANDOM';
     * set @FP_HIJACK_DDL_JOB_FORMAT='SEQUELTIAL';
     */
    String FP_HIJACK_DDL_JOB_FORMAT = "FP_HIJACK_DDL_JOB_FORMAT";

    /**
     * set @FP_INJECT_SUBJOB='true';
     * Inject subjob into MockDdl
     */
    String FP_INJECT_SUBJOB = "FP_INJECT_SUBJOB";

    /**
     * set @FP_PAUSE_DDL_JOB_ONCE_CREATED='true';
     * 创建完DDL之后立刻暂停
     */
    String FP_PAUSE_DDL_JOB_ONCE_CREATED = "FP_PAUSE_DDL_JOB_ONCE_CREATED";

    /**
     * set @FP_TRUNCATE_CUTOVER_FAIL='true';
     * 在 Truncate Table with GSI 的 CutOver 时失败
     */
    String FP_TRUNCATE_CUTOVER_FAILED = "FP_TRUNCATE_CUTOVER_FAIL";

    /**
     * set @FP_TRUNCATE_SYNC_FAIL='true';
     * 在 Truncate Table with GSI 的 Sync 时失败
     */
    String FP_TRUNCATE_SYNC_FAILED = "FP_TRUNCATE_SYNC_FAIL";

    /**
     * set @FP_MOCK_TASK_RANDOM_FAIL='30';
     * 设置mock task有30%概率失败
     */
    String FP_MOCK_TASK_RANDOM_FAIL = "FP_MOCK_TASK_RANDOM_FAIL";

    /**
     * set @FP_MOCK_TASK_RANDOM_ROLLBACK_POLOCY='50';
     * 设置mock task有30%概率失败
     */
    String FP_MOCK_TASK_RANDOM_ROLLBACK_POLOCY = "FP_MOCK_TASK_RANDOM_ROLLBACK_POLOCY";

    /**
     * set @fp_ddl_internal_max_parallelism='5';
     * 设置DDL引擎最大并行度
     */
    String FP_DDL_INTERNAL_MAX_PARALLELISM = "FP_DDL_INTERNAL_MAX_PARALLELISM";

    /**
     * set @fp_ddl_restore_job_suspend='20000';
     * 设置DDL引擎反序列化Job时的延迟，单位是ms
     */
    String FP_DDL_RESTORE_JOB_SUSPEND = "FP_DDL_RESTORE_JOB_SUSPEND";

    /**
     * set @FP_EACH_DDL_TASK_FAIL_ONCE='true';
     * 让每个task失败1次。不影响BaseValidateTask。
     */
    String FP_EACH_DDL_TASK_FAIL_ONCE = "FP_EACH_DDL_TASK_FAIL_ONCE";

    /**
     * set @FP_EACH_DDL_TASK_EXECUTE_TWICE='true';
     * 让每个task执行2次
     */
    String FP_EACH_DDL_TASK_EXECUTE_TWICE = "FP_EACH_DDL_TASK_EXECUTE_TWICE";

    /**
     * set @FP_EACH_DDL_TASK_BACK_AND_FORTH='true';
     * 让每个Task先执行execute、再执行rollback、再执行execute
     */
    String FP_EACH_DDL_TASK_BACK_AND_FORTH = "FP_EACH_DDL_TASK_BACK_AND_FORTH";

    /**
     * set @FP_SKIP_TASK_EXECUTION_BY_NAMES='TaskName1,TaskName2,TaskName3';
     * 跳过task的执行
     */
    String FP_SKIP_TASK_EXECUTION_BY_NAMES = "FP_SKIP_TASK_EXECUTION_BY_NAMES";

    /**
     * set @FP_DDL_TASK_SUSPEND_WHEN_FAILED='5000';
     * 当task失败时，sleep一段时间。
     * 当JOB按照DAG图并发执行时，这个注入点可以制造task并发失败的场景。
     * 可以用于测试并发TASK失败时，引擎调度的正确性。
     * 期望：只有一个task能修改Job的失败策略
     */
    String FP_DDL_TASK_SUSPEND_WHEN_FAILED = "FP_DDL_TASK_SUSPEND_WHEN_FAILED";

    /**
     * set @FP_DDL_GSI_CHECK_FAILED='true';
     * 注入gsi check失败
     */
    String FP_DDL_GSI_CHECK_FAILED = "FP_DDL_GSI_CHECK_FAILED";

    /**
     * set @FP_INJECT_FAILURE_TO_LEGACY_DDL_ENGINE_BEFORE_DO='true';
     * 在老引擎的beforeDo()之前注入失败
     */
    String FP_INJECT_FAILURE_TO_LEGACY_DDL_ENGINE_BEFORE_DO = "FP_INJECT_FAILURE_TO_LEGACY_DDL_ENGINE_BEFORE_DO";

    /**
     * set @FP_INJECT_FAILURE_TO_LEGACY_DDL_ENGINE_DO_HANDLE='true';
     * 在老引擎的doHandle()之前注入失败
     */
    String FP_INJECT_FAILURE_TO_LEGACY_DDL_ENGINE_DO_HANDLE = "FP_INJECT_FAILURE_TO_LEGACY_DDL_ENGINE_DO_HANDLE";

    /**
     * set @FP_INJECT_FAILURE_TO_LEGACY_DDL_ENGINE_AFTER_DO='true';
     * 在老引擎的afterDo()之前注入失败
     */
    String FP_INJECT_FAILURE_TO_LEGACY_DDL_ENGINE_AFTER_DO = "FP_INJECT_FAILURE_TO_LEGACY_DDL_ENGINE_AFTER_DO";

    /**
     * set @FP_INJECT_FAILURE_TO_CDC_AFTER_ADD_NEW_GROUP='true';
     * 在cdc系统库新增Group之后注入失败
     */
    String FP_INJECT_FAILURE_TO_CDC_AFTER_ADD_NEW_GROUP = "FP_INJECT_FAILURE_TO_CDC_AFTER_ADD_NEW_GROUP";

    /**
     * set @FP_INJECT_FAILURE_TO_CDC_AFTER_REMOVE_GROUP='true';
     * 在cdc系统库新增Group之后注入失败
     */
    String FP_INJECT_FAILURE_TO_CDC_AFTER_REMOVE_GROUP = "FP_INJECT_FAILURE_TO_CDC_AFTER_REMOVE_GROUP";

    /**
     * set @FP_INJECT_INTERRUPTED_TO_SCHEDULE_JOB='true';
     */
    String FP_INJECT_IGNORE_INTERRUPTED_TO_STATISTIC_SCHEDULE_JOB =
        "FP_INJECT_IGNORE_INTERRUPTED_TO_STATISTIC_SCHEDULE_JOB";

    String FP_INJECT_IGNORE_STATISTIC_QUICK_FAIL = "FP_INJECT_IGNORE_STATISTIC_QUICK_FAIL";
    String FP_INJECT_IGNORE_SAMPLE_TASK_EXCEPTION = "FP_INJECT_IGNORE_SAMPLE_TASK_EXCEPTION";

    String FP_INJECT_IGNORE_HLL_TASK_EXCEPTION = "FP_INJECT_IGNORE_HLL_TASK_EXCEPTION";

    String FP_INJECT_IGNORE_ROWCOUNT_TASK_EXCEPTION = "FP_INJECT_IGNORE_ROWCOUNT_TASK_EXCEPTION";

    String FP_INJECT_IGNORE_PERSIST_TASK_EXCEPTION = "FP_INJECT_IGNORE_PERSIST_TASK_EXCEPTION";

    String FP_INJECT_IGNORE_SYNC_TASK_EXCEPTION = "FP_INJECT_IGNORE_SYNC_TASK_EXCEPTION";

    String FP_INJECT_IGNORE_PERSIST_TABLE_STATISTIC = "FP_INJECT_IGNORE_PERSIST_TABLE_STATISTIC";

    String FP_INJECT_IGNORE_PERSIST_COLUMN_STATISTIC = "FP_INJECT_IGNORE_PERSIST_COLUMN_STATISTIC";

    String FP_INJECT_IGNORE_PERSIST_NDV_STATISTIC = "FP_INJECT_IGNORE_PERSIST_NDV_STATISTIC";

    /**
     * set @FP_INJECT_STATISTIC_SCHEDULE_JOB_HLL_EXCEPTION='true';
     * 注入统计信息定时采集任务的超时失败
     */
    String FP_INJECT_STATISTIC_SCHEDULE_JOB_HLL_EXCEPTION =
        "FP_INJECT_STATISTIC_SCHEDULE_JOB_HLL_EXCEPTION";

    /**
     * set @FP_INJECT_IGNORE_INTERRUPTED_TO_LOCAL_PARTITION_SCHEDULE_JOB='true';
     * 忽略 expire local partition 定时任务的中断
     */
    String FP_INJECT_IGNORE_INTERRUPTED_TO_LOCAL_PARTITION_SCHEDULE_JOB =
        "FP_INJECT_IGNORE_INTERRUPTED_TO_LOCAL_PARTITION_SCHEDULE_JOB";

    /**
     * set @FP_INJECT_IGNORE_INNER_INTERRUPTION_TO_LOCAL_PARTITION='true';
     * 忽略 local partition 子任务之间的中断
     */
    String FP_INJECT_IGNORE_INNER_INTERRUPTION_TO_LOCAL_PARTITION =
        "FP_INJECT_IGNORE_INNER_INTERRUPTION_TO_LOCAL_PARTITION";

    /**
     * set @FP_LOCAL_PARTITION_SCHEDULED_JOB_ERROR='true';
     * 向 expire local partition 定时任务注入中断
     */
    String FP_LOCAL_PARTITION_SCHEDULED_JOB_ERROR = "FP_LOCAL_PARTITION_SCHEDULED_JOB_ERROR";

    /**
     * set @FP_TTL_PAUSE='20'
     * 调整expire local partition的超时时间，单位是秒
     */
    String FP_TTL_PAUSE = "FP_TTL_PAUSE";

    /**
     * Fail before creating tmp tables at status 0.
     * No tmp table is created.
     */
    String FP_TRX_LOG_TB_FAILED_BEFORE_CREATE_TMP = "FP_TRX_LOG_TB_FAILED_BEFORE_CREATE_TMP";

    /**
     * Fail during creating tmp tables at status 0.
     * At least one DN finishes creating tmp table.
     * Status is still 0.
     */
    String FP_TRX_LOG_TB_FAILED_DURING_CREATE_TMP = "FP_TRX_LOG_TB_FAILED_DURING_CREATE_TMP";

    /**
     * Fail before switching tables at status 1.
     */
    String FP_TRX_LOG_TB_FAILED_BEFORE_SWITCH_TABLE = "FP_TRX_LOG_TB_FAILED_BEFORE_SWITCH_TABLE";

    /**
     * Fail during switching tables at status 1.
     * At least one DN finishes switching tables.
     * Status is still 1.
     */
    String FP_TRX_LOG_TB_FAILED_DURING_SWITCH_TABLE = "FP_TRX_LOG_TB_FAILED_DURING_SWITCH_TABLE";

    /**
     * Fail before dropping archive tables at status 2.
     */
    String FP_TRX_LOG_TB_FAILED_BEFORE_DROP_TABLE = "FP_TRX_LOG_TB_FAILED_BEFORE_DROP_TABLE";

    /**
     * Fail during switching tables at status 2.
     * At least one DN finishes dropping archive tables.
     * Status is still 2.
     */
    String FP_TRX_LOG_TB_FAILED_DURING_DROP_TABLE = "FP_TRX_LOG_TB_FAILED_DURING_DROP_TABLE";

    /**
     * Fail before table sync task.
     */
    String FP_FAILED_TABLE_SYNC = "FP_FAILED_TABLE_SYNC";

    String FP_UPDATE_TABLES_VERSION_ERROR = "FP_UPDATE_TABLES_VERSION_ERROR";

    String FP_FASTCHECKER_IDLE_QUERY_SLEEP = "FP_FASTCHECKER_IDLE_QUERY_SLEEP";

    String FP_FASTCHECKER_RESEND_SNAPSHOT_EXCEPTION = "FP_FASTCHECKER_RESEND_SNAPSHOT_EXCEPTION";
}
