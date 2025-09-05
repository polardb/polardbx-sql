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

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.ParameterMethod;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.gms.metadb.accessor.AbstractAccessor;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import org.apache.commons.collections.CollectionUtils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.alibaba.polardbx.gms.metadb.GmsSystemTables.SCHEDULED_JOBS;

/**
 * @author guxu
 */
public class ScheduledJobsAccessor extends AbstractAccessor {
    private static final Logger logger = LoggerFactory.getLogger(ScheduledJobsAccessor.class);

    private static final String ALL_COLUMNS = "`schedule_id`," +
        "`create_time`," +
        "`update_time`," +
        "`table_schema`," +
        "`table_name`," +
        "`schedule_name`," +
        "`schedule_comment`," +
        "`executor_type`," +
        "`schedule_context`," +
        "`executor_contents`," +
        "`status`," +
        "`schedule_type`," +
        "`schedule_expr`," +
        "`time_zone`," +
        "`last_fire_time`," +
        "`next_fire_time`," +
        "`starts`," +
        "`ends`," +
        "`schedule_policy`," +
        "`table_group_name`";

    private static final String COLUMNS_TO_SET =
        "`table_schema` = ?," +
            "`table_name` = ?," +
            "`schedule_name` = ?," +
            "`schedule_comment` = ?," +
            "`executor_type` = ?," +
            "`schedule_context` = ?," +
            "`executor_contents` = ?," +
            "`status` = ?," +
            "`schedule_type` = ?," +
            "`schedule_expr` = ?," +
            "`time_zone` = ?," +
            "`last_fire_time` = ?," +
            "`next_fire_time` = ?," +
            "`starts`  = ?," +
            "`ends` = ?," +
            "`schedule_policy`  = ?," +
            "`table_group_name` = ?";

    private static final String ALL_VALUES = "(null,null,null,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";

    private static final String INSERT_TABLE_SCHEDULED_JOBS =
        "insert into " + SCHEDULED_JOBS + " (" + ALL_COLUMNS + ") VALUES " + ALL_VALUES;

    private static final String REPLACE_TABLE_SCHEDULED_JOBS =
        "replace into " + SCHEDULED_JOBS + " (" + ALL_COLUMNS + ") VALUES " + ALL_VALUES;

    private static final String UPDATE_TABLE_SCHEDULED_JOBS_BY_SCHEDULED_ID =
        "update " + SCHEDULED_JOBS + " SET  " + COLUMNS_TO_SET + " WHERE schedule_id=?";

    private static final String GET_TABLE_SCHEDULED_JOBS = "select " + ALL_COLUMNS + " from " + SCHEDULED_JOBS;

    private static final String GET_TABLE_SCHEDULED_JOBS_BY_ID = GET_TABLE_SCHEDULED_JOBS + " where schedule_id=?";

    private static final String GET_TABLE_SCHEDULED_JOBS_BY_TABLE_NAME =
        "select " + ALL_COLUMNS + " from " + SCHEDULED_JOBS + " where table_schema=? and table_name=?";

    private static final String GET_TABLE_SCHEDULED_JOBS_BY_TABLE_NAME_AND_EXECUTOR_TYPE =
        "select " + ALL_COLUMNS + " from " + SCHEDULED_JOBS
            + " where table_schema=? and table_name=? and executor_type = ?";

    private static final String GET_TABLE_SCHEDULED_JOBS_BY_SCHEDULE_TYPE =
        "select " + ALL_COLUMNS + " from " + SCHEDULED_JOBS + " where schedule_type=?";

    private static final String GET_TABLE_SCHEDULED_JOBS_BY_EXECUTOR_TYPE =
        "select " + ALL_COLUMNS + " from " + SCHEDULED_JOBS + " where executor_type=?";

    private static final String RENAME_TABLE_BY_TABLE_NAME =
        "update " + SCHEDULED_JOBS + " set table_name = ?, schedule_name = ? where table_schema=? and schedule_name=?";

    private static final String GET_TABLE_SCHEDULED_JOBS_BY_TABLE_GROUP_NAME =
        "select " + ALL_COLUMNS + " from " + SCHEDULED_JOBS + " where table_schema=? and table_group_name=?";

    private static final String POLL_SQL =
        "select " + ALL_COLUMNS + " from " + SCHEDULED_JOBS + " "
            + "WHERE `status` = 'ENABLED' AND schedule_expr IS NOT NULL "
            + "AND ("
            + "        next_fire_time IS NULL "
            + "     OR next_fire_time = 0 "
            + "     OR next_fire_time <= UNIX_TIMESTAMP() "
            + ") "
            + "FOR UPDATE";

    private static final String FIRE_SQL =
        "UPDATE " + SCHEDULED_JOBS + " "
            + "SET last_fire_time=next_fire_time, "
            + "next_fire_time=? "
            + "WHERE schedule_id=?";

    private static final String UPDATE_NEXT_FIRE_TIME_SQL =
        "UPDATE " + SCHEDULED_JOBS + " "
            + "SET "
            + "next_fire_time=? "
            + "WHERE schedule_id=?";

    public int insert(ScheduledJobsRecord record) {
        try {
            return MetaDbUtil.insert(INSERT_TABLE_SCHEDULED_JOBS, record.buildParams(), connection);
        } catch (Exception e) {
            throw logAndThrow("Failed to insert into " + SCHEDULED_JOBS, "insert into", e);
        }
    }

    public int insertIgnoreFail(ScheduledJobsRecord record) {
        try {
            return MetaDbUtil.insert(INSERT_TABLE_SCHEDULED_JOBS, record.buildParams(), connection);
        } catch (Exception e) {
            logger.error("Failed to insert into " + SCHEDULED_JOBS, e);
            return 0;
        }
    }

    public int updateScheduledJobsRecordByScheduleId(ScheduledJobsRecord record) {
        try {

//                "`table_schema` = ?," +  // varchar,string
//                "`table_name` = ?," +  // varchar,string
//                "`schedule_name` = ?," +  // varchar,string
//                "`schedule_comment` = ?," +  // varchar,string
//                "`executor_type` = ?," + // varchar,string
//                "`schedule_context` = ?," + // longtext,string
//                "`executor_contents` = ?," + // longtext,string
//                "`status` = ?," + // longtext,string
//                "`schedule_type` = ?," + // varchar,string
//                "`schedule_expr` = ?," + // varchar,string
//                "`time_zone` = ?," + // varchar,string
//                "`last_fire_time` = ?," + // bigint,long
//                "`next_fire_time` = ?," + // bigint,long
//                "`starts`  = ?," + // bigint,long
//                "`ends` = ?," + // bigint,long
//                "`schedule_policy`  = ?," + // varchar,string
//                "`table_group_name` = ?"; // varchar,string
//                where schedule_id = ？ // bigint,long

            final Map<Integer, ParameterContext> params = new HashMap<>();
            int paramIndex = 0; // start with 1

            MetaDbUtil.setParameter(++paramIndex, params, ParameterMethod.setString, record.getTableSchema());
            MetaDbUtil.setParameter(++paramIndex, params, ParameterMethod.setString, record.getTableName());
            MetaDbUtil.setParameter(++paramIndex, params, ParameterMethod.setString, record.getScheduleName());
            MetaDbUtil.setParameter(++paramIndex, params, ParameterMethod.setString, record.getScheduleComment());
            MetaDbUtil.setParameter(++paramIndex, params, ParameterMethod.setString, record.getExecutorType());
            MetaDbUtil.setParameter(++paramIndex, params, ParameterMethod.setString, record.getScheduleContext());
            MetaDbUtil.setParameter(++paramIndex, params, ParameterMethod.setString, record.getExecutorContents());
            MetaDbUtil.setParameter(++paramIndex, params, ParameterMethod.setString, record.getStatus());
            MetaDbUtil.setParameter(++paramIndex, params, ParameterMethod.setString, record.getScheduleType());
            MetaDbUtil.setParameter(++paramIndex, params, ParameterMethod.setString, record.getScheduleExpr());
            MetaDbUtil.setParameter(++paramIndex, params, ParameterMethod.setString, record.getTimeZone());
            MetaDbUtil.setParameter(++paramIndex, params, ParameterMethod.setLong, record.getLastFireTime());
            MetaDbUtil.setParameter(++paramIndex, params, ParameterMethod.setLong, record.getNextFireTime());
            MetaDbUtil.setParameter(++paramIndex, params, ParameterMethod.setLong, record.getStarts());
            MetaDbUtil.setParameter(++paramIndex, params, ParameterMethod.setLong, record.getEnds());
            MetaDbUtil.setParameter(++paramIndex, params, ParameterMethod.setString, record.getSchedulePolicy());
            MetaDbUtil.setParameter(++paramIndex, params, ParameterMethod.setString, record.getTableGroupName());
            MetaDbUtil.setParameter(++paramIndex, params, ParameterMethod.setLong, record.getScheduleId());
            return MetaDbUtil.update(UPDATE_TABLE_SCHEDULED_JOBS_BY_SCHEDULED_ID, params, connection);
        } catch (Exception e) {
            throw logAndThrow("Failed to insert into " + SCHEDULED_JOBS, "insert into", e);
        }
    }

    public int rename(String newTableName, String newScheduleName, String schemaName, String oldScheduleName) {
        try {
            final Map<Integer, ParameterContext> params = new HashMap<>(4);
            MetaDbUtil.setParameter(1, params, ParameterMethod.setString, newTableName);
            MetaDbUtil.setParameter(2, params, ParameterMethod.setString, newScheduleName);
            MetaDbUtil.setParameter(3, params, ParameterMethod.setString, schemaName);
            MetaDbUtil.setParameter(4, params, ParameterMethod.setString, oldScheduleName);
            return MetaDbUtil.update(RENAME_TABLE_BY_TABLE_NAME, params, connection);
        } catch (Exception e) {
            throw logAndThrow("Failed to update " + SCHEDULED_JOBS, "update scheduled job info by scheduled_id", e);
        }
    }

    public List<ScheduledJobsRecord> query() {
        try {
            return MetaDbUtil.query(GET_TABLE_SCHEDULED_JOBS, ScheduledJobsRecord.class, connection);
        } catch (Exception e) {
            throw logAndThrow("Failed to query " + SCHEDULED_JOBS, "query", e);
        }
    }

    public ScheduledJobsRecord queryById(long scheduleId) {
        try {
            final Map<Integer, ParameterContext> params = new HashMap<>(1);
            MetaDbUtil.setParameter(1, params, ParameterMethod.setLong, scheduleId);
            List<ScheduledJobsRecord> list =
                MetaDbUtil.query(GET_TABLE_SCHEDULED_JOBS_BY_ID, params, ScheduledJobsRecord.class, connection);
            if (CollectionUtils.isEmpty(list)) {
                return null;
            }
            return list.get(0);
        } catch (Exception e) {
            throw logAndThrow("Failed to query " + SCHEDULED_JOBS, "query", e);
        }
    }

    public List<ScheduledJobsRecord> query(String schemaName, String tableName) {
        try {
            final Map<Integer, ParameterContext> params = new HashMap<>(3);
            MetaDbUtil.setParameter(1, params, ParameterMethod.setString, schemaName);
            MetaDbUtil.setParameter(2, params, ParameterMethod.setString, tableName);
            return MetaDbUtil.query(GET_TABLE_SCHEDULED_JOBS_BY_TABLE_NAME, params, ScheduledJobsRecord.class,
                connection);
        } catch (Exception e) {
            throw logAndThrow("Failed to query " + SCHEDULED_JOBS, "query", e);
        }
    }

    public List<ScheduledJobsRecord> query(String schemaName, String tableName, String executorType) {
        try {
            final Map<Integer, ParameterContext> params = new HashMap<>(3);
            MetaDbUtil.setParameter(1, params, ParameterMethod.setString, schemaName);
            MetaDbUtil.setParameter(2, params, ParameterMethod.setString, tableName);
            MetaDbUtil.setParameter(3, params, ParameterMethod.setString, executorType);
            return MetaDbUtil.query(GET_TABLE_SCHEDULED_JOBS_BY_TABLE_NAME_AND_EXECUTOR_TYPE, params,
                ScheduledJobsRecord.class, connection);
        } catch (Exception e) {
            throw logAndThrow("Failed to query " + SCHEDULED_JOBS, "query", e);
        }
    }

    public List<ScheduledJobsRecord> queryByScheduleType(String scheduleType) {
        try {
            final Map<Integer, ParameterContext> params = new HashMap<>(3);
            MetaDbUtil.setParameter(1, params, ParameterMethod.setString, scheduleType);
            return MetaDbUtil.query(GET_TABLE_SCHEDULED_JOBS_BY_SCHEDULE_TYPE, params, ScheduledJobsRecord.class,
                connection);
        } catch (Exception e) {
            throw logAndThrow("Failed to query " + SCHEDULED_JOBS, "query", e);
        }
    }

    public List<ScheduledJobsRecord> queryByExecutorType(String executorType) {
        try {
            final Map<Integer, ParameterContext> params = new HashMap<>(3);
            MetaDbUtil.setParameter(1, params, ParameterMethod.setString, executorType);
            return MetaDbUtil.query(GET_TABLE_SCHEDULED_JOBS_BY_EXECUTOR_TYPE, params, ScheduledJobsRecord.class,
                connection);
        } catch (Exception e) {
            throw logAndThrow("Failed to query " + SCHEDULED_JOBS, "query", e);
        }
    }

    public List<ScheduledJobsRecord> queryByTableGroupName(String schemaName, String tableGroupName) {
        try {
            final Map<Integer, ParameterContext> params = new HashMap<>(3);
            MetaDbUtil.setParameter(1, params, ParameterMethod.setString, schemaName);
            MetaDbUtil.setParameter(2, params, ParameterMethod.setString, tableGroupName);
            return MetaDbUtil.query(GET_TABLE_SCHEDULED_JOBS_BY_TABLE_GROUP_NAME, params, ScheduledJobsRecord.class,
                connection);
        } catch (Exception e) {
            throw logAndThrow("Failed to query " + SCHEDULED_JOBS, "query", e);
        }
    }

    public List<ScheduledJobsRecord> scan() {
        try {
            return MetaDbUtil.query(POLL_SQL, ScheduledJobsRecord.class, connection);
        } catch (Exception e) {
            throw logAndThrow("Failed to query " + SCHEDULED_JOBS, "query", e);
        }
    }

    public boolean fire(long epochSeconds, long scheduleId) {
        try {
            final Map<Integer, ParameterContext> params = new HashMap<>(3);

            MetaDbUtil.setParameter(1, params, ParameterMethod.setLong, epochSeconds);
            MetaDbUtil.setParameter(2, params, ParameterMethod.setLong, scheduleId);
            return MetaDbUtil.update(FIRE_SQL, params, connection) > 0;
        } catch (Exception e) {
            throw logAndThrow("Failed to update " + SCHEDULED_JOBS, "update", e);
        }
    }

    public boolean updateNextFireTime(long epochSeconds, long scheduleId) {
        try {
            final Map<Integer, ParameterContext> params = new HashMap<>(3);

            MetaDbUtil.setParameter(1, params, ParameterMethod.setLong, epochSeconds);
            MetaDbUtil.setParameter(2, params, ParameterMethod.setLong, scheduleId);
            return MetaDbUtil.update(UPDATE_NEXT_FIRE_TIME_SQL, params, connection) > 0;
        } catch (Exception e) {
            throw logAndThrow("Failed to update " + SCHEDULED_JOBS, "update", e);
        }
    }

    private static final String WHERE_SCHEMA_NAME = " where `table_schema` = ?";
    private static final String WHERE_SCHEDULE_ID = " where `schedule_id` = ?";

    private static final String DELETE_BY_SCHEMA_NAME = "delete from " + SCHEDULED_JOBS + WHERE_SCHEMA_NAME;

    private static final String DELETE_BY_SCHEDULE_ID = "delete from " + SCHEDULED_JOBS + WHERE_SCHEDULE_ID;

    private static final String DISABLE_BY_SCHEDULE_ID =
        "update " + SCHEDULED_JOBS + " set status='DISABLED' " + WHERE_SCHEDULE_ID;
    private static final String ENABLE_BY_SCHEDULE_ID =
        "update " + SCHEDULED_JOBS + " set status='ENABLED' " + WHERE_SCHEDULE_ID;

    public int deleteById(long scheduleId) {
        try {
            final Map<Integer, ParameterContext> params =
                MetaDbUtil.buildParameters(ParameterMethod.setLong, new Object[] {scheduleId});
            return MetaDbUtil.delete(DELETE_BY_SCHEDULE_ID, params, connection);
        } catch (Exception e) {
            throw logAndThrow("Failed to delete from " + SCHEDULED_JOBS + " for schedule_id: " + scheduleId,
                "delete from", e);
        }
    }

    public int deleteAll(String schemaName) {
        try {
            final Map<Integer, ParameterContext> params =
                MetaDbUtil.buildParameters(ParameterMethod.setString, new String[] {schemaName});
            return MetaDbUtil.delete(DELETE_BY_SCHEMA_NAME, params, connection);
        } catch (Exception e) {
            throw logAndThrow("Failed to delete from " + SCHEDULED_JOBS + " for schemaName: " + schemaName,
                "delete from",
                e);
        }
    }

    public int disableById(long scheduleId) {
        try {
            final Map<Integer, ParameterContext> params =
                MetaDbUtil.buildParameters(ParameterMethod.setLong, new Object[] {scheduleId});
            return MetaDbUtil.delete(DISABLE_BY_SCHEDULE_ID, params, connection);
        } catch (Exception e) {
            throw logAndThrow("Failed to update " + SCHEDULED_JOBS + " for schedule_id: " + scheduleId,
                "update", e);
        }
    }

    public int enableById(long scheduleId) {
        try {
            final Map<Integer, ParameterContext> params =
                MetaDbUtil.buildParameters(ParameterMethod.setLong, new Object[] {scheduleId});
            return MetaDbUtil.delete(ENABLE_BY_SCHEDULE_ID, params, connection);
        } catch (Exception e) {
            throw logAndThrow("Failed to update " + SCHEDULED_JOBS + " for schedule_id: " + scheduleId,
                "update", e);
        }
    }

    /**
     * Failed to {0} the system table {1}. Caused by: {2}.
     */
    private TddlRuntimeException logAndThrow(String errMsg, String action, Exception e) {
        logger.error(errMsg, e);
        return new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e,
            action,
            SCHEDULED_JOBS,
            e.getMessage()
        );
    }

}