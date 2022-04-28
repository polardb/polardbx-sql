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
import com.alibaba.polardbx.common.scheduler.FiredScheduledJobState;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.gms.metadb.accessor.AbstractAccessor;
import com.alibaba.polardbx.gms.metadb.misc.DdlEngineRecord;
import com.alibaba.polardbx.gms.util.DdlMetaLogUtil;
import com.alibaba.polardbx.gms.util.MetaDbUtil;

import java.time.ZonedDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.alibaba.polardbx.gms.metadb.GmsSystemTables.FIRED_SCHEDULED_JOBS;
import static com.alibaba.polardbx.gms.metadb.GmsSystemTables.SCHEDULED_JOBS;

/**
 * @author guxu
 */
public class FiredScheduledJobsAccessor extends AbstractAccessor {
    private static final Logger logger = LoggerFactory.getLogger(FiredScheduledJobsAccessor.class);

    private static final String ALL_COLUMNS =
        "`schedule_id`," +
        "`table_schema`," +
        "`table_name`," +
        "`fire_time`," +
        "`start_time`," +
        "`finish_time`," +
        "`state`," +
        "`remark`," +
        "`result`";

    private static final String ALL_VALUES = "(?,?,?,?,?,?,?,?,?)";

    private static final String INSERT_TABLE_SCHEDULED_JOBS =
        "insert into " + FIRED_SCHEDULED_JOBS + " (" + ALL_COLUMNS + ") VALUES " + ALL_VALUES;

    private static final String SELECT_SPECIFIC_FOR_UPDATE =
        "SELECT * FROM " + FIRED_SCHEDULED_JOBS + " WHERE `schedule_id`=? AND `fire_time`=? for update";

    private static final String GET_TABLE_FIRED_SCHEDULED_JOBS =
        "SELECT " +
            "S.`schedule_id`, " +
            "S.`table_schema`, " +
            "S.`table_name`, " +
            "S.`schedule_name`, " +
            "S.`schedule_comment`, " +
            "S.`executor_type`, " +
            "S.`schedule_context`, " +
            "S.`executor_contents`, " +
            "S.`status`, " +
            "S.`schedule_type`, " +
            "S.`schedule_expr`, " +
            "S.`time_zone`, " +
            "S.`schedule_policy`, " +
            "F.`fire_time`, " +
            "F.`start_time`, " +
            "F.`finish_time`, " +
            "F.`state`, " +
            "F.`remark`, " +
            "F.`result` " +
        "FROM " + FIRED_SCHEDULED_JOBS + " F " +
        "LEFT OUTER JOIN " + SCHEDULED_JOBS + " S " +
        "ON F.`schedule_id`=S.`schedule_id` " +
        "WHERE F.`fire_time` <= UNIX_TIMESTAMP() " +
        "AND F.`state` IN ('QUEUED') "
        ;

    private static final String QUERY_BY_SCHEDULED_ID =
        "SELECT " +
            "S.`schedule_id`, " +
            "S.`table_schema`, " +
            "S.`table_name`, " +
            "S.`schedule_name`, " +
            "S.`schedule_comment`, " +
            "S.`executor_type`, " +
            "S.`schedule_context`, " +
            "S.`executor_contents`, " +
            "S.`status`, " +
            "S.`schedule_type`, " +
            "S.`schedule_expr`, " +
            "S.`time_zone`, " +
            "S.`schedule_policy`, " +
            "F.`fire_time`, " +
            "F.`start_time`, " +
            "F.`finish_time`, " +
            "F.`state`, " +
            "F.`remark`, " +
            "F.`result` " +
            "FROM " + FIRED_SCHEDULED_JOBS + " F " +
            "LEFT OUTER JOIN " + SCHEDULED_JOBS + " S " +
            "ON F.`schedule_id`=S.`schedule_id` " +
            "WHERE F.`fire_time` <= UNIX_TIMESTAMP() " +
            "AND F.`schedule_id` = ? " +
            "ORDER BY F.`fire_time` DESC " +
            "LIMIT 100"
        ;

    public List<ExecutableScheduledJob> getQueuedJobs() {
        try {
            return MetaDbUtil.query(GET_TABLE_FIRED_SCHEDULED_JOBS, ExecutableScheduledJob.class, connection);
        } catch (Exception e) {
            throw logAndThrow("Failed to query " + FIRED_SCHEDULED_JOBS, "query", e);
        }
    }

    public List<ExecutableScheduledJob> queryByScheduleId(long scheduleId){
        try {
            final Map<Integer, ParameterContext> params = new HashMap<>(1);
            MetaDbUtil.setParameter(1, params, ParameterMethod.setLong, scheduleId);
            return MetaDbUtil.query(QUERY_BY_SCHEDULED_ID, params, ExecutableScheduledJob.class, connection);
        } catch (Exception e) {
            throw logAndThrow("Failed to query " + FIRED_SCHEDULED_JOBS, "query", e);
        }
    }

    public FiredScheduledJobsRecord queryForUpdate(long schedulerId,
                                                   long fireTime){
        try {
            final Map<Integer, ParameterContext> params = new HashMap<>(2);
            MetaDbUtil.setParameter(1, params, ParameterMethod.setLong, schedulerId);
            MetaDbUtil.setParameter(2, params, ParameterMethod.setLong, fireTime);

            List<FiredScheduledJobsRecord> records =
                MetaDbUtil.query(SELECT_SPECIFIC_FOR_UPDATE, params, FiredScheduledJobsRecord.class, connection);

            if (records != null && records.size() > 0) {
                return records.get(0);
            }
            return null;
        } catch (Exception e) {
            throw logAndThrow("Failed to query from " + FIRED_SCHEDULED_JOBS, "query from", e);
        }
    }

    private static final String UPDATE_STATE =
        "UPDATE " + FIRED_SCHEDULED_JOBS + " " +
            "SET `state` = ? " +
            " , `remark` = ? " +
            " , `result` = ? " +
            "WHERE " +
            " `schedule_id` = ? " +
            "AND `fire_time` = ? ";

    private static final String CAS_STATE_WITH_START_TIME =
        "UPDATE " + FIRED_SCHEDULED_JOBS + " " +
        "SET `state` = ? " +
        " , `start_time` = ? " +
        "WHERE `state` = ? " +
        "AND `schedule_id` = ? " +
        "AND `fire_time` = ? ";

    private static final String CAS_STATE_WITH_FINISH_TIME =
        "UPDATE " + FIRED_SCHEDULED_JOBS + " " +
        "SET `state` = ? " +
        " , `finish_time` = ? " +
        " , `remark` = ? " +
        "WHERE `state` = ? " +
        "AND `schedule_id` = ? " +
        "AND `fire_time` = ? ";

    public boolean updateState(long schedulerId,
                               long fireTime,
                               FiredScheduledJobState newState,
                               String remark,
                               String result) {
        try {
            final Map<Integer, ParameterContext> params = new HashMap<>(4);
            int index = 1;
            MetaDbUtil.setParameter(index++, params, ParameterMethod.setString, newState.name());
            MetaDbUtil.setParameter(index++, params, ParameterMethod.setString, remark);
            MetaDbUtil.setParameter(index++, params, ParameterMethod.setString, result);
            MetaDbUtil.setParameter(index++, params, ParameterMethod.setLong, schedulerId);
            MetaDbUtil.setParameter(index++, params, ParameterMethod.setLong, fireTime);
            return MetaDbUtil.update(UPDATE_STATE, params, connection) > 0;
        } catch (Exception e) {
            throw logAndThrow(
                "Failed to Update " + FIRED_SCHEDULED_JOBS +
                    " for scheduler_id " + schedulerId + " to state " + newState.name(),
                "update", e);
        }
    }

    public boolean compareAndSetStateWithStartTime(long schedulerId,
                                               long fireTime,
                                               FiredScheduledJobState currentState,
                                               FiredScheduledJobState newState,
                                               Long startTime) {
        try {
            final Map<Integer, ParameterContext> params = new HashMap<>(4);
            int index = 1;
            MetaDbUtil.setParameter(index++, params, ParameterMethod.setString, newState.name());
            MetaDbUtil.setParameter(index++, params, ParameterMethod.setLong, startTime);
            MetaDbUtil.setParameter(index++, params, ParameterMethod.setString, currentState.name());
            MetaDbUtil.setParameter(index++, params, ParameterMethod.setLong, schedulerId);
            MetaDbUtil.setParameter(index++, params, ParameterMethod.setLong, fireTime);
            return MetaDbUtil.update(CAS_STATE_WITH_START_TIME, params, connection) > 0;
        } catch (Exception e) {
            throw logAndThrow(
                "Failed to Compare-And-Set " + FIRED_SCHEDULED_JOBS +
                    " for scheduler_id " + schedulerId + " to state " + newState.name(),
                "update", e);
        }
    }

    public boolean compareAndSetStateWithFinishTime(long schedulerId,
                                                    long fireTime,
                                                    FiredScheduledJobState currentState,
                                                    FiredScheduledJobState newState,
                                                    Long finishTime,
                                                    String remark) {
        try {
            final Map<Integer, ParameterContext> params = new HashMap<>(4);
            int index = 1;
            MetaDbUtil.setParameter(index++, params, ParameterMethod.setString, newState.name());
            MetaDbUtil.setParameter(index++, params, ParameterMethod.setLong, finishTime);
            MetaDbUtil.setParameter(index++, params, ParameterMethod.setString, remark);
            MetaDbUtil.setParameter(index++, params, ParameterMethod.setString, currentState.name());
            MetaDbUtil.setParameter(index++, params, ParameterMethod.setLong, schedulerId);
            MetaDbUtil.setParameter(index++, params, ParameterMethod.setLong, fireTime);
            return MetaDbUtil.update(CAS_STATE_WITH_FINISH_TIME, params, connection) > 0;
        } catch (Exception e) {
            throw logAndThrow(
                "Failed to Compare-And-Set " + FIRED_SCHEDULED_JOBS +
                    " for scheduler_id " + schedulerId + " to state " + newState.name(),
                "update", e);
        }
    }

    public int fire(FiredScheduledJobsRecord record) {
        try {
            return MetaDbUtil.insert(INSERT_TABLE_SCHEDULED_JOBS, record.buildParams(), connection);
        } catch (Exception e) {
            throw logAndThrow("Failed to insert into " + FIRED_SCHEDULED_JOBS, "insert", e);
        }
    }

    private static final String WHERE_SCHEMA_NAME = " where `table_schema` = ?";
    private static final String WHERE_SCHEDULE_ID = " where `schedule_id` = ?";

    private static final String DELETE_BY_SCHEMA_NAME = "delete from " + FIRED_SCHEDULED_JOBS + WHERE_SCHEMA_NAME;

    private static final String DELETE_BY_SCHEDULE_ID = "delete from " + FIRED_SCHEDULED_JOBS + WHERE_SCHEDULE_ID;

    public int deleteById(long scheduleId) {
        try {
            final Map<Integer, ParameterContext> params =
                MetaDbUtil.buildParameters(ParameterMethod.setLong, new Object[]{scheduleId});
            return MetaDbUtil.delete(DELETE_BY_SCHEDULE_ID, params, connection);
        } catch (Exception e) {
            throw logAndThrow("Failed to delete from " + FIRED_SCHEDULED_JOBS + " for schedule_id: " + scheduleId,
                "delete from", e);
        }
    }

    public int deleteAll(String schemaName) {
        try {
            final Map<Integer, ParameterContext> params =
                MetaDbUtil.buildParameters(ParameterMethod.setString, new String[] {schemaName});
            return MetaDbUtil.delete(DELETE_BY_SCHEMA_NAME, params, connection);
        } catch (Exception e) {
            throw logAndThrow("Failed to delete from " + FIRED_SCHEDULED_JOBS + " for schemaName: " + schemaName,
                "delete from",
                e);
        }
    }

    private static final String CLEANUP_SQL =
        "DELETE FROM " + FIRED_SCHEDULED_JOBS + " " +
        "WHERE `fire_time` <= ? " +
        "AND `state` IN ('SUCCESS', 'FAILED', 'SKIPPED') "
        ;

    private static final String SET_FIRED_JOBS_TIMEOUT_SQL =
        "UPDATE " + FIRED_SCHEDULED_JOBS + " " +
            "SET `state` = 'FAILED' " +
            "WHERE `fire_time` <= ? and `gmt_modified` <= TIMESTAMP(DATE_SUB(CURDATE(), INTERVAL ? HOUR)) " +
            "AND `state` = 'RUNNING' "
        ;

    public int setFiredJobsTimeout(long hours){
        try {
            int finishedCount = 0;

            long epochSecond = ZonedDateTime.now().minusHours(hours).toEpochSecond();

            {
                final Map<Integer, ParameterContext> params = new HashMap<>(1);

                MetaDbUtil.setParameter(1, params, ParameterMethod.setLong, epochSecond);
                MetaDbUtil.setParameter(2, params, ParameterMethod.setLong, hours);
                finishedCount += MetaDbUtil.update(SET_FIRED_JOBS_TIMEOUT_SQL, params, connection);
            }

            return finishedCount;
        } catch (Exception e) {
            throw logAndThrow("Failed to delete from " + FIRED_SCHEDULED_JOBS, "delete from", e);
        }
    }

    public int cleanup(long hours){
        try {
            int finishedCount = 0;

            long epochSecond = ZonedDateTime.now().minusHours(hours).toEpochSecond();

            {
                final Map<Integer, ParameterContext> params = new HashMap<>(1);

                MetaDbUtil.setParameter(1, params, ParameterMethod.setLong, epochSecond);
                finishedCount += MetaDbUtil.delete(CLEANUP_SQL, params, connection);
            }

            return finishedCount;
        } catch (Exception e) {
            throw logAndThrow("Failed to delete from " + FIRED_SCHEDULED_JOBS, "delete from", e);
        }
    }


    /**
     * Failed to {0} the system table {1}. Caused by: {2}.
     */
    private TddlRuntimeException logAndThrow(String errMsg, String action, Exception e) {
        logger.error(errMsg, e);
        return new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e,
            action,
            FIRED_SCHEDULED_JOBS,
            e.getMessage()
        );
    }
}