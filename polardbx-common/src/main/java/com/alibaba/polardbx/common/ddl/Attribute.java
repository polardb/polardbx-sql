package com.alibaba.polardbx.common.ddl;

public class Attribute {

    public static final String INSTANCE_PROP_DATA_ID = "com.taobao.tddl.instance_properties.%s";
    public static final String DATABASE_PROP_DATA_ID = "com.taobao.tddl.%s.properties";
    public static final String PROPERTIES_CHARSET = "utf8";

    public static final boolean DEFAULT_ENABLE_ASYNC_DDL = true;
    public static final boolean DEFAULT_PURE_ASYNC_DDL_MODE = false;

    public static final boolean DEFAULT_ENABLE_RANDOM_PHY_TABLE_NAME = true;
    public static final int MAX_TABLE_NAME_LENGTH_MYSQL_ALLOWS = 64;
    public static final int RANDOM_SUFFIX_LENGTH_OF_PHYSICAL_TABLE_NAME = 4;

    // Logical DDL parallelism by node, i.e. how many logical DDLs can be
    // executed concurrently on a DRDS node.
    public static final int MIN_LOGICAL_DDL_PARALLELISM = 1;
    public static final int MAX_LOGICAL_DDL_PARALLELISM = 16;
    public static final int DEFAULT_LOGICAL_DDL_PARALLELISM = MIN_LOGICAL_DDL_PARALLELISM;

    // The number of job schedulers. This is a instance-level parameter.
    public static final int MIN_NUM_OF_JOB_SCHEDULERS = 1;
    public static final int MAX_NUM_OF_JOB_SCHEDULERS = Integer.MAX_VALUE;
    public static final int DEFAULT_NUM_OF_JOB_SCHEDULERS = 64;

    public static final int JOB_QUEUE_SIZE = 1024;

    // Waiting time when no job is being handled, i.e. idle. This is to avoid
    // frequent access to database.
    public static final int MIN_JOB_IDLE_WAITING_TIME = 10;
    public static final int MIN_PHYSICAL_DDL_MDL_WAITING_TIMEOUT = 5;
    public static final int MEDIAN_JOB_IDLE_WAITING_TIME = 1000;
    public static final int MORE_JOB_IDLE_WAITING_TIME = 10000;
    public static final int MAX_JOB_IDLE_WAITING_TIME = 60000;
    public static final int MAX_PHYSICAL_DDL_MDL_WAITING_TIMEOUT = Integer.MAX_VALUE;
    public static final int DEFAULT_JOB_IDLE_WAITING_TIME = MORE_JOB_IDLE_WAITING_TIME;
    public static final int PHYSICAL_DDL_MDL_WAITING_TIMEOUT = 15;

    // Async DDL job timeout
    public static final int JOB_REQUEST_TIMEOUT = 900000;
    public static final int JOB_WAITING_TIME = MIN_JOB_IDLE_WAITING_TIME;

    // Check if server should automatically recover left jobs during
    // initialization.
    public static final boolean DEFAULT_AUTOMATIC_DDL_JOB_RECOVERY = false;

    // Limit of the number of table partitions per database.
    public static final int MIN_MAX_TABLE_PARTITIONS_PER_DB = 1;
    public static final int MAX_MAX_TABLE_PARTITIONS_PER_DB = 65535;
    public static final int DEFAULT_MAX_TABLE_PARTITIONS_PER_DB = 128;

    // Limit of the number of left DDL jobs in system table.
    public static final int MAX_LEFT_DDL_JOBS_IN_SYS_TABLE = 65535;

    // Maximum number of characters SHOW DDL displays for remark.
    public static final int MAX_CHARS_FOR_SHOW = 256;

    /**
     * For INSTANT ADD COLUMN
     */
    public static final String XDB_VARIABLE_INSTANT_ADD_COLUMN = "innodb_support_instant_add_column";
    public static final String MYSQL_VARIABLE_VERSION = "version";
    public static final String ALTER_TABLE_ALGORITHM_CLAUSE = "ALGORITHM";
    public static final String ALTER_TABLE_ALGORITHM_DEFAULT = "DEFAULT";
    public static final String ALTER_TABLE_ALGORITHM_INSTANT = "INSTANT";
    public static final String ALTER_TABLE_COMPRESSION_CLAUSE = "COMPRESSION";

    public static final String COLUMN_SERVER = "SERVER";
    public static final String COLUMN_JOB_ID = "JOB_ID";
    public static final String COLUMN_LEFT_OBJ_SCHEMA = "LEFT_OBJ_SCHEMA";
    public static final String COLUMN_LEFT_OBJ_NAME = "LEFT_OBJ_NAME";
    public static final String COLUMN_LEFT_JOB_TYPE = "LEFT_JOB_TYPE";
    public static final String COLUMN_ONGOING_OBJ_SCHEMA = "ONGOING_OBJ_SCHEMA";
    public static final String COLUMN_ONGOING_OBJ_NAME = "ONGOING_OBJ_NAME";
    public static final String COLUMN_FENCED_OBJ_SCHEMA = "FENCED_OBJ_SCHEMA";
    public static final String COLUMN_FENCED_OBJ_NAME = "FENCED_OBJ_NAME";
    public static final String COLUMN_FENCED_JOB_TYPE = "FENCED_JOB_TYPE";

    public static final String COLUMN_RESPONSE_TYPE = "RESPONSE";
    public static final String COLUMN_RESPONSE_CONTENT = "CONTENT";

    public static final String TYPE_ON_PRIMARY = "PRIMARY";
    public static final String TYPE_ON_GSI = "GSI";

}
