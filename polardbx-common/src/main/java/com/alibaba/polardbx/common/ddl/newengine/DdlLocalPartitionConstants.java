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

package com.alibaba.polardbx.common.ddl.newengine;

public class DdlLocalPartitionConstants {

    public static final String DDL_LOCAL_PARTITION_DISPATCHER_NAME = "DDL-LocalPartition-Dispatcher";
    public static final String DDL_LOCAL_PARTITION_SCHEDULER_NAME = "DDL-LocalPartition-Scheduler";
    public static final String DDL_LOCAL_PARTITION_EXECUTORS_NAME = "DDL-LocalPartition-Executor-";

    public static final String PMAX = "pmax";
    public static final String MAXVALUE = "MAXVALUE";

    public static final String NOW_FUNCTION = "NOW()";

    public static final String VALID_PIVOT_DATE_METHOD_NOW = "NOW";
    public static final String VALID_PIVOT_DATE_METHOD_DATE_ADD = "DATE_ADD";
    public static final String VALID_PIVOT_DATE_METHOD_DATE_SUB = "DATE_SUB";

    public static final String DEFAULT_EXPIRE_AFTER = "-1";
    public static final String DEFAULT_PRE_ALLOCATE = "3";

    /**
     * default cron expression for local partition scheduler
     *
     * every day at 1 am
     */
    public static final String DEFAULT_SCHEDULE_CRON_EXPR = "0 0 1 * * ?";

    /**
     * 每个物理表最大物理分区限制
     */
    public static final Integer MAX_LOCAL_PARTITION_COUNT = 32;
}