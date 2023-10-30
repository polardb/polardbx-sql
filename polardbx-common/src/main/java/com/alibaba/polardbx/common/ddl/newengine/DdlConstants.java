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

public class DdlConstants {

    public static final String DOT = ".";
    public static final String COMMA = ",";
    public static final String SEMICOLON = ";";
    public static final String COLON = ":";
    public static final String HYPHEN = "-";
    public static final String BACKTICK = "`";
    public static final String UNDERSCORE = "_";
    public static final String QUESTION_MARK = "?";
    public static final String PERCENTAGE = "%";
    public static final String NONE = "--";

    public static final String TEST_MODE = "TEST_MODE";

    public static final String ENGINE_TYPE_DAG = "DAG";
    public static final String ENGINE_TYPE_LEGACY = "LEGACY";

    public static final String DDL_LEADER_KEY = "DDL_LEADER";
    public static final long DDL_LEADER_TTL_IN_MILLIS = 30_000L;

    public static final String EMPTY_CONTENT = "";
    public static final int MIN_NUM_OF_THREAD_NAME_PARTS = 5;

    public static final int RECOVER_MAX_RETRY_TIMES = 3;
    public static final int ROLLBACK_MAX_RETRY_TIMES = 3;

    public static final String SQLSTATE_TABLE_EXISTS = "42S01";
    public static final int ERROR_TABLE_EXISTS = 1050;
    public static final String SQLSTATE_UNKNOWN_TABLE = "42S02";
    public static final int ERROR_UNKNOWN_TABLE = 1051;

    public static final String SQLSTATE_VIOLATION = "42000";
    public static final int ERROR_DUPLICATE_KEY = 1061;
    public static final int ERROR_CANT_DROP_KEY = 1091;

    public static final int MIN_NUM_OF_DDL_SCHEDULERS = 1;
    public static final int MAX_NUM_OF_DDL_SCHEDULERS = 512;
    public static final int DEFAULT_NUM_OF_DDL_SCHEDULERS = 64;

    public static final String DDL_DISPATCHER_NAME = "DDL-Engine-Dispatcher";
    public static final String DDL_SCHEDULER_NAME = "DDL-Engine-Scheduler";
    public static final String DDL_EXECUTORS_NAME = "DDL-Engine-Executor-";
    public static final String DDL_PHY_OBJ_RECORDER_NAME = "DDL-Engine-Phy-Obj-Recorder-%s-";
    public static final String DDL_ARCHIVE_CLEANER_NAME = "DDL-Engine-Archive-Cleaner-";
    public static final String DDL_LEADER_ELECTION_NAME = "DDL-Engine-Leader-Election-Thread-";
    public static final int DEFAULT_RUNNING_DDL_RESCHEDULE_INTERVAL_IN_MINUTES = 1;
    // 默认每隔120分钟，重新调度失败的任务
    public static final int DEFAULT_PAUSED_DDL_RESCHEDULE_INTERVAL_IN_MINUTES = 120;

    public static final int MAX_TABLE_NAME_LENGTH_MYSQL_ALLOWS = 64;
    public static final int RANDOM_SUFFIX_LENGTH_OF_PHYSICAL_TABLE_NAME = 4;

    public static final int MIN_LOGICAL_DDL_PARALLELISM = 1;
    public static final int MAX_LOGICAL_DDL_PARALLELISM = 16;
    public static final int DEFAULT_LOGICAL_DDL_PARALLELISM = 4;

    public static final int MIN_ALLOWED_TABLE_SHARDS_PER_DB = 1;
    public static final int MAX_ALLOWED_TABLE_SHARDS_PER_DB = 65535;
    public static final int DEFAULT_ALLOWED_TABLE_SHARDS_PER_DB = 128;

    public static final int MIN_WAITING_TIME = 10;
    public static final int LESS_WAITING_TIME = 100;
    public static final int MEDIAN_WAITING_TIME = 1000;
    public static final int MORE_WAITING_TIME = 2000;
    public static final int MAX_WAITING_TIME = 60000;
    public static final int DEFAULT_WAITING_TIME = MORE_WAITING_TIME;

    public static final int MIN_PHYSICAL_DDL_MDL_WAITING_TIMEOUT = 5;
    public static final int MAX_PHYSICAL_DDL_MDL_WAITING_TIMEOUT = Integer.MAX_VALUE;
    public static final int PHYSICAL_DDL_MDL_WAITING_TIMEOUT = 15;

    public static final int MAX_LEFT_DDL_JOBS_IN_SYS_TABLE = 65535;

    public static final String TYPE_ON_GSI = "GSI";

    public static final String PARENT_TASK_ID = "ParentTaskId";
    public static final String SUB_JOB_ID = "SubJobId";
    public static final String JOB_ID = "JOB_ID";

    public static final long TRANSIENT_SUB_JOB_ID = -1L;

    public static final String SUB_JOB_RETRY_ERRER_MESSAGE = "please retry this command";

    public static final long ROLLBACK_DDL_WAIT_TIMES = 10L;
}
