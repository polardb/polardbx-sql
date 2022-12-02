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

import com.alibaba.polardbx.common.ddl.newengine.DdlPlanState;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.ParameterMethod;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.gms.metadb.accessor.AbstractAccessor;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import org.apache.commons.collections.CollectionUtils;

import java.sql.Timestamp;
import java.time.ZonedDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.alibaba.polardbx.gms.metadb.GmsSystemTables.DDL_PLAN;

/**
 * @author luoyanxin
 */
public class DdlPlanAccessor extends AbstractAccessor {
    private static final Logger logger = LoggerFactory.getLogger(DdlPlanAccessor.class);

    private static final String ALL_COLUMNS = "`id`," +
            "`plan_id`," +
            "`job_id`," +
            "`table_schema`," +
            "`ddl_stmt`," +
            "`state`," +
            "`ddl_type`," +
            "`progress`," +
            "`retry_count`," +
            "`result`," +
            "`extras`," +
            "`gmt_created`," +
            "`gmt_modified`," +
            "`resource`";

    private static final String ALL_VALUES = "(null,?,null,?,?,?,?,?,?,?,?, null, null, ?)";

    private static final String INSERT_TABLE_DDL_PLAN =
        "insert into " + DDL_PLAN + " (" + ALL_COLUMNS + ") VALUES " + ALL_VALUES;

    private static final String SELECT_DDL_FOR_UPDATE =
        "SELECT * FROM " + DDL_PLAN + " WHERE `plan_id`=? for update";

    public DdlPlanRecord queryForUpdate(long planId) {
        try {
            final Map<Integer, ParameterContext> params = new HashMap<>(1);
            MetaDbUtil.setParameter(1, params, ParameterMethod.setLong, planId);

            List<DdlPlanRecord> records =
                MetaDbUtil.query(SELECT_DDL_FOR_UPDATE, params, DdlPlanRecord.class, connection);

            if (records.size() > 0) {
                return records.get(0);
            }
            return null;
        } catch (Exception e) {
            throw logAndThrow("Failed to query from " + DDL_PLAN, "query from", e);
        }
    }

    private static final String UPDATE_STATE =
        "UPDATE " + DDL_PLAN + " " +
            "SET `state` = ? " +
            " , `result` = ? " +
            " , `job_id` = ? " +
            "WHERE " +
            " `plan_id` = ? ";

    public boolean updateState(long planId,
                               DdlPlanState newState,
                               String result,
                               long jobId) {
        try {
            final Map<Integer, ParameterContext> params = new HashMap<>(4);
            int index = 1;
            MetaDbUtil.setParameter(index++, params, ParameterMethod.setString, newState.name());
            MetaDbUtil.setParameter(index++, params, ParameterMethod.setString, result);
            MetaDbUtil.setParameter(index++, params, ParameterMethod.setLong, jobId);
            MetaDbUtil.setParameter(index, params, ParameterMethod.setLong, planId);
            return MetaDbUtil.update(UPDATE_STATE, params, connection) > 0;
        } catch (Exception e) {
            throw logAndThrow(
                "Failed to Update " + DDL_PLAN +
                    " for plan_id " + planId + " to state " + newState.name(),
                "update", e);
        }
    }

    private static final String UPDATE_EXTRA =
        "UPDATE " + DDL_PLAN + " " +
            "SET `extras` = ? " +
            "WHERE " +
            " `plan_id` = ? ";

    public boolean updateExtra(long planId, String extra) {
        try {
            final Map<Integer, ParameterContext> params = new HashMap<>(4);
            int index = 1;
            MetaDbUtil.setParameter(index++, params, ParameterMethod.setString, extra);
            MetaDbUtil.setParameter(index++, params, ParameterMethod.setLong, planId);
            return MetaDbUtil.update(UPDATE_EXTRA, params, connection) > 0;
        } catch (Exception e) {
            throw logAndThrow(
                "Failed to Update " + DDL_PLAN +
                    " for plan_id " + planId + " with extra info ",
                "update", e);
        }
    }

    private static final String INCREMENT_RETRY_COUNT =
        "UPDATE " + DDL_PLAN + " " +
            "SET `retry_count` = retry_count+1 " +
            "WHERE " +
            " `plan_id` = ? ";

    public boolean incrementRetryCount(long planId) {
        try {
            final Map<Integer, ParameterContext> params = new HashMap<>(4);
            int index = 1;
            MetaDbUtil.setParameter(index, params, ParameterMethod.setLong, planId);
            return MetaDbUtil.update(INCREMENT_RETRY_COUNT, params, connection) > 0;
        } catch (Exception e) {
            throw logAndThrow(
                "Failed to Update " + DDL_PLAN +
                    " for plan_id " + planId + " to increment retry count",
                "update", e);
        }
    }

    private static final String SELECT_DDL_BY_TYPE =
        "SELECT * FROM " + DDL_PLAN + " WHERE `ddl_type`=? AND `state` != 'SUCCESS' AND `state` != 'TERMINATED'";

    public List<DdlPlanRecord> queryByType(String ddlType) {
        try {
            final Map<Integer, ParameterContext> params = new HashMap<>(1);
            MetaDbUtil.setParameter(1, params, ParameterMethod.setString, ddlType);

            return MetaDbUtil.query(SELECT_DDL_BY_TYPE, params, DdlPlanRecord.class, connection);
        } catch (Exception e) {
            throw logAndThrow("Failed to query from " + DDL_PLAN, "query from", e);
        }
    }

    private static final String SELECT_DDL_BY_JOB_ID =
        "SELECT * FROM " + DDL_PLAN + " WHERE job_id = ?";

    public Optional<DdlPlanRecord> queryByJobId(long jobId) {
        try {
            final Map<Integer, ParameterContext> params = new HashMap<>(1);
            MetaDbUtil.setParameter(1, params, ParameterMethod.setLong, jobId);

            List<DdlPlanRecord> result =
                MetaDbUtil.query(SELECT_DDL_BY_JOB_ID, params, DdlPlanRecord.class, connection);
            return CollectionUtils.isEmpty(result) ?
                Optional.empty() :
                Optional.of(result.get(0));
        } catch (Exception e) {
            throw logAndThrow("Failed to query from " + DDL_PLAN, "query from", e);
        }
    }

    public int addDdlPlan(DdlPlanRecord record) {
        try {
            return MetaDbUtil.insert(INSERT_TABLE_DDL_PLAN, record.buildParams(), connection);
        } catch (Exception e) {
            throw logAndThrow("Failed to insert into " + DDL_PLAN, "insert", e);
        }
    }

    private static final String DELETE_BASE = "delete from " + DDL_PLAN;

    private static final String WHERE_SCHEMA_NAME = " where table_schema = ?";

    private static final String DELETE_BY_SCHEMA_NAME = DELETE_BASE + WHERE_SCHEMA_NAME;

    public int deleteAll(String schemaName) {
        try {
            final Map<Integer, ParameterContext> params =
                MetaDbUtil.buildParameters(ParameterMethod.setString, new String[] {schemaName});
            return MetaDbUtil.delete(DELETE_BY_SCHEMA_NAME, params, connection);
        } catch (Exception e) {
            throw logAndThrow("Failed to delete from " + DDL_PLAN + " for schemaName: " + schemaName,
                "delete from",
                e);
        }
    }

    private static final String CLEANUP_SQL =
        "DELETE FROM " + DDL_PLAN + " " +
            "WHERE `gmt_created` <= ? " +
            "AND `state` IN ('SUCCESS', 'TERMINATED') ";

    public int cleanup(long days) {
        try {
            int finishedCount = 0;

            ZonedDateTime expireTime = ZonedDateTime.now().minusDays(days);
            Timestamp timestamp = Timestamp.valueOf(expireTime.toLocalDateTime());

            {
                final Map<Integer, ParameterContext> params = new HashMap<>(1);
                MetaDbUtil.setParameter(1, params, ParameterMethod.setTimestamp1, timestamp);
                finishedCount += MetaDbUtil.delete(CLEANUP_SQL, params, connection);
            }

            return finishedCount;
        } catch (Exception e) {
            throw logAndThrow("Failed to delete from " + DDL_PLAN, "delete from", e);
        }
    }

    private static final String SELECT_ALL_DDL_PLAN =
        "SELECT * FROM " + DDL_PLAN;

    public List<DdlPlanRecord> queryAll() {
        try {
            final Map<Integer, ParameterContext> params = new HashMap<>(1);

            return MetaDbUtil.query(SELECT_ALL_DDL_PLAN, params, DdlPlanRecord.class, connection);
        } catch (Exception e) {
            throw logAndThrow("Failed to query from " + DDL_PLAN, "query from", e);
        }
    }

    /**
     * Failed to {0} the system table {1}. Caused by: {2}.
     */
    private TddlRuntimeException logAndThrow(String errMsg, String action, Exception e) {
        logger.error(errMsg, e);
        return new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e,
            action,
            DDL_PLAN,
            e.getMessage()
        );
    }
}