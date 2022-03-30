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