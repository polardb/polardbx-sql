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

    private static final String ALL_COLUMNS =
        "`schedule_id`," +
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
        "`schedule_policy`";

    private static final String ALL_VALUES = "(null,null,null,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";

    private static final String INSERT_TABLE_SCHEDULED_JOBS =
        "insert into " + SCHEDULED_JOBS + " (" + ALL_COLUMNS + ") VALUES " + ALL_VALUES;

    private static final String REPLACE_TABLE_SCHEDULED_JOBS =
        "replace into " + SCHEDULED_JOBS + " (" + ALL_COLUMNS + ") VALUES " + ALL_VALUES;

    private static final String GET_TABLE_SCHEDULED_JOBS = "select " + ALL_COLUMNS + " from " + SCHEDULED_JOBS;

    private static final String GET_TABLE_SCHEDULED_JOBS_BY_ID = GET_TABLE_SCHEDULED_JOBS + " where schedule_id=?";

    private static final String GET_TABLE_SCHEDULED_JOBS_BY_TABLE_NAME =
        "select " + ALL_COLUMNS + " from " + SCHEDULED_JOBS + " where table_schema=? and table_name=?";

    private static final String POLL_SQL =
          "select " + ALL_COLUMNS + " from " + SCHEDULED_JOBS + " "
        + "WHERE `status` = 'ENABLED' AND schedule_expr IS NOT NULL "
        + "AND ("
        + "        next_fire_time IS NULL "
        + "     OR next_fire_time = 0 "
        + "     OR next_fire_time <= UNIX_TIMESTAMP() "
        + ") "
        + "FOR UPDATE"
        ;

    private static final String FIRE_SQL =
          "UPDATE " + SCHEDULED_JOBS + " "
        + "SET last_fire_time=next_fire_time, "
        + "next_fire_time=? "
        + "WHERE schedule_id=?"
        ;

    public int insert(ScheduledJobsRecord record) {
        try {
            return MetaDbUtil.insert(INSERT_TABLE_SCHEDULED_JOBS, record.buildParams(), connection);
        } catch (Exception e) {
            throw logAndThrow("Failed to insert into " + SCHEDULED_JOBS, "insert into", e);
        }
    }

    public int replace(ScheduledJobsRecord record) {
        try {
            return MetaDbUtil.insert(REPLACE_TABLE_SCHEDULED_JOBS, record.buildParams(), connection);
        } catch (Exception e) {
            throw logAndThrow("Failed to insert into " + SCHEDULED_JOBS, "insert into", e);
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
            if(CollectionUtils.isEmpty(list)){
                return null;
            }
            return list.get(0);
        } catch (Exception e) {
            throw logAndThrow("Failed to query " + SCHEDULED_JOBS, "query", e);
        }
    }

    public List<ScheduledJobsRecord> query(String schemaName, String tableName){
        try {
            final Map<Integer, ParameterContext> params = new HashMap<>(3);
            MetaDbUtil.setParameter(1, params, ParameterMethod.setString, schemaName);
            MetaDbUtil.setParameter(2, params, ParameterMethod.setString, tableName);
            return MetaDbUtil.query(GET_TABLE_SCHEDULED_JOBS_BY_TABLE_NAME, params, ScheduledJobsRecord.class, connection);
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

    public boolean fire(long epochSeconds, long scheduleId){
        try {
            final Map<Integer, ParameterContext> params = new HashMap<>(3);

            MetaDbUtil.setParameter(1, params, ParameterMethod.setLong, epochSeconds);
            MetaDbUtil.setParameter(2, params, ParameterMethod.setLong, scheduleId);
            return MetaDbUtil.update(FIRE_SQL, params, connection)>0;
        } catch (Exception e) {
            throw logAndThrow("Failed to update " + SCHEDULED_JOBS, "update", e);
        }
    }

    private static final String WHERE_SCHEMA_NAME = " where `table_schema` = ?";
    private static final String WHERE_SCHEDULE_ID = " where `schedule_id` = ?";

    private static final String DELETE_BY_SCHEMA_NAME = "delete from " + SCHEDULED_JOBS + WHERE_SCHEMA_NAME;

    private static final String DELETE_BY_SCHEDULE_ID = "delete from " + SCHEDULED_JOBS + WHERE_SCHEDULE_ID;

    public int deleteById(long scheduleId) {
        try {
            final Map<Integer, ParameterContext> params =
                MetaDbUtil.buildParameters(ParameterMethod.setLong, new Object[]{scheduleId});
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