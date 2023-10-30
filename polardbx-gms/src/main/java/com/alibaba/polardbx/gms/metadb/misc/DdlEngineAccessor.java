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

package com.alibaba.polardbx.gms.metadb.misc;

import com.alibaba.polardbx.common.ddl.newengine.DdlConstants;
import com.alibaba.polardbx.common.ddl.newengine.DdlState;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.ParameterMethod;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.gms.metadb.GmsSystemTables;
import com.alibaba.polardbx.gms.metadb.accessor.AbstractAccessor;
import com.alibaba.polardbx.gms.metadb.record.CountRecord;
import com.alibaba.polardbx.gms.util.DdlMetaLogUtil;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.google.common.base.Preconditions;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static com.alibaba.polardbx.common.ddl.newengine.DdlState.PAUSED_POLICY_VALUES;
import static com.alibaba.polardbx.common.ddl.newengine.DdlState.ROLLBACK_PAUSED_POLICY_VALUES;

public class DdlEngineAccessor extends AbstractAccessor {

    private static final Logger LOGGER = LoggerFactory.getLogger(DdlEngineAccessor.class);

    public static final String DDL_ENGINE_TABLE = wrap(GmsSystemTables.DDL_ENGINE);

    public static final String DDL_ENGINE_TABLE_ARCHIVE = wrap(GmsSystemTables.DDL_ENGINE_ARCHIVE);

    private static final String INSERT_DATA =
        "insert into " + DDL_ENGINE_TABLE
            + "(`job_id`, `ddl_type`, `schema_name`, `object_name`, `response_node`, `execution_node`, "
            + "`state`, `resources`, `progress`, `trace_id`, `context`, `task_graph`, `result`, `ddl_stmt`, "
            + "`gmt_created`, `gmt_modified`, `max_parallelism`, `supported_commands`, `paused_policy`, `rollback_paused_policy`) "
            + "values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

    private static final String SELECT_FULL =
        "select `job_id`, `ddl_type`, `schema_name`, `object_name`, `response_node`, `execution_node`, "
            + "`state`, `resources`, `progress`, `trace_id`, `context`, `task_graph`, `result`, `ddl_stmt`, "
            + "`gmt_created`, `gmt_modified`, `max_parallelism`, `supported_commands`, `paused_policy`, `rollback_paused_policy`";

    private static final String FROM_TABLE = " from " + DDL_ENGINE_TABLE;

    private static final String WHERE_JOB_ID = " where `job_id` = ?";

    private static final String WHERE_JOB_ID_LIST = " where `job_id` in (%s)";

    private static final String WHERE_SCHEMA = " where `schema_name` = ?";

    private static final String WHERE_SCHEMA_OBJECT = WHERE_SCHEMA + " and `object_name` = ?";

    private static final String WHERE_STATE = " where `state` in (%s)";

    private static final String ORDER_BY_JOB_ID = " order by `job_id` asc";

    private static final String SELECT_BASE = SELECT_FULL + FROM_TABLE;

    public static final String SELECT_JOB = SELECT_BASE + WHERE_SCHEMA_OBJECT;

    private static final String SELECT_SCHEMA = SELECT_BASE + WHERE_SCHEMA + ORDER_BY_JOB_ID;

    private static final String SELECT_STATES = SELECT_BASE + WHERE_STATE + ORDER_BY_JOB_ID;

    // where gmt_modified <= DATE_ADD(now(),INTERVAL -1 * ? MINUTE)
    private static final String SELECT_STATES_IN_MINUTES = SELECT_BASE
        + WHERE_STATE + " and gmt_modified <= ? " + ORDER_BY_JOB_ID;

    private static final String SELECT_SPECIFIC = SELECT_BASE + WHERE_JOB_ID;

    private static final String SELECT_SPECIFIC_FOR_UPDATE = SELECT_BASE + WHERE_JOB_ID + " for update";

    private static final String SELECT_SPECIFIC_LIST = SELECT_BASE + WHERE_JOB_ID_LIST;

    private static final String SELECT_COUNT_ALL = "select count(1)" + FROM_TABLE + WHERE_SCHEMA;

    private static final String UPDATE_BASE = "update " + DDL_ENGINE_TABLE + " set `gmt_modified` = ?, ";

    private static final String FORCE_UPDATE_STATE = UPDATE_BASE + "`state` = ?" + WHERE_JOB_ID;

    private static final String CAS_STATE = UPDATE_BASE + "`state` = ?" + WHERE_JOB_ID + " and `state` = ?";

    private static final String UPDATE_PROGRESS = UPDATE_BASE + "`progress` = ?" + WHERE_JOB_ID;

    private static final String UPDATE_PAUSED_POLICY = UPDATE_BASE
        + "`paused_policy` = ?, `rollback_paused_policy` = ? " + WHERE_JOB_ID;

    private static final String UPDATE_SUPPORTED_COMMANDS = UPDATE_BASE + "`supported_commands` = ?" + WHERE_JOB_ID;

    private static final String UPDATE_CONTEXT = UPDATE_BASE + "`context` = ?" + WHERE_JOB_ID;

    private static final String UPDATE_RESULT = UPDATE_BASE + "`result` = ?" + WHERE_JOB_ID;

    private static final String DELETE_BASE = "delete" + FROM_TABLE;

    private static final String DELETE_SPECIFIC = DELETE_BASE + WHERE_JOB_ID;

    private static final String DELETE_BY_SCHEMA_NAME = DELETE_BASE + WHERE_SCHEMA;

    private static final String SELECT_ARCHIVE_SPECIFIC =
        SELECT_FULL + " from " + DDL_ENGINE_TABLE_ARCHIVE + WHERE_JOB_ID;

    private static final String DELETE_ARCHIVE_BASE = "delete from " + DDL_ENGINE_TABLE_ARCHIVE;

    private static final String DELETE_ARCHIVE_BY_JOB_ID = DELETE_ARCHIVE_BASE + WHERE_JOB_ID;

    private static final String DELETE_ARCHIVE_BY_SCHEMA_NAME = DELETE_ARCHIVE_BASE + WHERE_SCHEMA;

    private static final String ARCHIVE_BASE =
        "insert into " + DDL_ENGINE_TABLE_ARCHIVE + " select * from " + DDL_ENGINE_TABLE;

    private static final String ARCHIVE_SPECIFIC = ARCHIVE_BASE + WHERE_JOB_ID;

    private static final String CLEAN_ARCHIVE_IN_MINUTS =
        "DELETE j,t FROM `ddl_engine_archive` j JOIN `ddl_engine_task_archive` t ON j.job_id=t.job_id where j.gmt_modified <= ?";

    public int insert(DdlEngineRecord record) {
        try {
            DdlMetaLogUtil.logSql(INSERT_DATA, record.buildParams());
            return MetaDbUtil.insert(INSERT_DATA, record.buildParams(), connection);
        } catch (Exception e) {
            throw logAndThrow("Failed to insert a new record into " + DDL_ENGINE_TABLE, "insert into", e);
        }
    }

    public List<DdlEngineRecord> query(String schemaName) {
        return query(SELECT_SCHEMA, DDL_ENGINE_TABLE, DdlEngineRecord.class, schemaName);
    }

    public List<DdlEngineRecord> query(String schemaName, String object) {
        return query(SELECT_JOB, DDL_ENGINE_TABLE, DdlEngineRecord.class, schemaName, object);
    }

    public List<DdlEngineRecord> query(Set<DdlState> states) {
        try {
            String sql = fillInQuestionMarks(SELECT_STATES, states.size());
            final Map<Integer, ParameterContext> params = MetaDbUtil.buildStringParameters(buildStateArray(states));
            return MetaDbUtil.query(sql, params, DdlEngineRecord.class, connection);
        } catch (Exception e) {
            throw logAndThrow("Failed to query by state from " + DDL_ENGINE_TABLE, "query by states from", e);
        }
    }

    public List<DdlEngineRecord> query(Set<DdlState> states, int minutes) {
        try {
            String sql = fillInQuestionMarks(SELECT_STATES_IN_MINUTES, states.size());
            LocalDateTime.now().minusMinutes(minutes);
            final Map<Integer, ParameterContext> params = MetaDbUtil.buildStringParameters(buildStateArray(states));
            MetaDbUtil.setParameter(params.size() + 1, params, ParameterMethod.setLong,
                System.currentTimeMillis() - TimeUnit.MINUTES.toMillis(minutes));
            return MetaDbUtil.query(sql, params, DdlEngineRecord.class, connection);
        } catch (Exception e) {
            throw logAndThrow("Failed to query by state from " + DDL_ENGINE_TABLE, "query by states from", e);
        }
    }

    public List<DdlEngineRecord> query(List<Long> jobIds) {
        try {
            if (CollectionUtils.isEmpty(jobIds)) {
                return new ArrayList<>();
            }
            String sql = String.format(SELECT_SPECIFIC_LIST, concatIds(jobIds));
            return MetaDbUtil.query(sql, DdlEngineRecord.class, connection);
        } catch (Exception e) {
            throw logAndThrow("Failed to query by job ids from " + DDL_ENGINE_TABLE, "query by job ids from", e);
        }
    }

    public int count(String schemaName) {
        List<CountRecord> records = query(SELECT_COUNT_ALL, DDL_ENGINE_TABLE, CountRecord.class, schemaName);
        if (records != null && records.size() > 0) {
            return records.get(0).count;
        }
        return 0;
    }

    public DdlEngineRecord query(long jobId) {
        try {
            final Map<Integer, ParameterContext> params =
                MetaDbUtil.buildParameters(ParameterMethod.setLong, new Long[] {jobId});

            List<DdlEngineRecord> records =
                MetaDbUtil.query(SELECT_SPECIFIC, params, DdlEngineRecord.class, connection);

            if (records != null && records.size() > 0) {
                return records.get(0);
            }
            return null;
        } catch (Exception e) {
            throw logAndThrow("Failed to query from " + DDL_ENGINE_TABLE, "query from", e);
        }
    }

    public DdlEngineRecord queryArchive(long jobId) {
        try {
            final Map<Integer, ParameterContext> params =
                MetaDbUtil.buildParameters(ParameterMethod.setLong, new Long[] {jobId});

            List<DdlEngineRecord> records =
                MetaDbUtil.query(SELECT_ARCHIVE_SPECIFIC, params, DdlEngineRecord.class, connection);

            if (records != null && records.size() > 0) {
                return records.get(0);
            }
            return null;
        } catch (Exception e) {
            throw logAndThrow("Failed to query from " + DDL_ENGINE_TABLE_ARCHIVE, "query from", e);
        }
    }

    public DdlEngineRecord queryForUpdate(long jobId) {
        try {
            final Map<Integer, ParameterContext> params =
                MetaDbUtil.buildParameters(ParameterMethod.setLong, new Long[] {jobId});

            List<DdlEngineRecord> records =
                MetaDbUtil.query(SELECT_SPECIFIC_FOR_UPDATE, params, DdlEngineRecord.class, connection);

            if (records != null && records.size() > 0) {
                return records.get(0);
            }
            return null;
        } catch (Exception e) {
            throw logAndThrow("Failed to query from " + DDL_ENGINE_TABLE, "query from", e);
        }
    }

    public int forceUpdateDdlState(long jobId, DdlState newState) {
        try {
            final Map<Integer, ParameterContext> params = new HashMap<>(3);
            MetaDbUtil.setParameter(1, params, ParameterMethod.setLong, System.currentTimeMillis());
            MetaDbUtil.setParameter(2, params, ParameterMethod.setString, newState.name());
            MetaDbUtil.setParameter(3, params, ParameterMethod.setLong, jobId);
            DdlMetaLogUtil.logSql(FORCE_UPDATE_STATE, params);
            return MetaDbUtil.update(FORCE_UPDATE_STATE, params, connection);
        } catch (Exception e) {
            throw logAndThrow(
                "Failed to force update " + DDL_ENGINE_TABLE + " for job " + jobId + " to state " + newState
                    .name(),
                "update", e);
        }
    }

    public int compareAndSetDdlState(long jobId, DdlState newState, DdlState currentState) {
        try {
            final Map<Integer, ParameterContext> params = new HashMap<>(3);
            MetaDbUtil.setParameter(1, params, ParameterMethod.setLong, System.currentTimeMillis());
            MetaDbUtil.setParameter(2, params, ParameterMethod.setString, newState.name());
            MetaDbUtil.setParameter(3, params, ParameterMethod.setLong, jobId);
            MetaDbUtil.setParameter(4, params, ParameterMethod.setString, currentState.name());
            DdlMetaLogUtil.logSql(CAS_STATE, params);
            return MetaDbUtil.update(CAS_STATE, params, connection);
        } catch (Exception e) {
            throw logAndThrow(
                "Failed to Compare-And-Set " + DDL_ENGINE_TABLE + " for job " + jobId + " to state " + newState
                    .name(),
                "update", e);
        }
    }

    /**
     * update ddl_engine set progress = GREATEST(progress, ?) where job_id = ?;
     */
    public int updateProgress(long jobId, int progress) {
        try {
            final Map<Integer, ParameterContext> params = new HashMap<>(3);
            MetaDbUtil.setParameter(1, params, ParameterMethod.setLong, System.currentTimeMillis());
            MetaDbUtil.setParameter(2, params, ParameterMethod.setInt, progress);
            MetaDbUtil.setParameter(3, params, ParameterMethod.setLong, jobId);
            return MetaDbUtil.update(UPDATE_PROGRESS, params, connection);
        } catch (Exception e) {
            throw logAndThrow("Failed to update " + DDL_ENGINE_TABLE + " for job " + jobId + " to change progress",
                "update", e);
        }
    }

    public int updatePausedPolicy(long jobId, DdlState pausedPolicy, DdlState rollbackPausedPolicy) {
        try {
            Preconditions.checkArgument(PAUSED_POLICY_VALUES.contains(pausedPolicy), "pausedPolicy is invalid");
            Preconditions.checkArgument(ROLLBACK_PAUSED_POLICY_VALUES.contains(rollbackPausedPolicy),
                "rollbackPausedPolicy is invalid");
            final Map<Integer, ParameterContext> params = new HashMap<>(4);
            MetaDbUtil.setParameter(1, params, ParameterMethod.setLong, System.currentTimeMillis());
            MetaDbUtil.setParameter(2, params, ParameterMethod.setString, pausedPolicy.name());
            MetaDbUtil.setParameter(3, params, ParameterMethod.setString, rollbackPausedPolicy.name());
            MetaDbUtil.setParameter(4, params, ParameterMethod.setLong, jobId);
            return MetaDbUtil.update(UPDATE_PAUSED_POLICY, params, connection);
        } catch (Exception e) {
            throw logAndThrow("Failed to update " + DDL_ENGINE_TABLE + " for job " + jobId + " to change paused policy",
                "update", e);
        }
    }

    public int updateSupportedCommands(long jobId, int supportedCommands) {
        try {
            final Map<Integer, ParameterContext> params = new HashMap<>(3);
            MetaDbUtil.setParameter(1, params, ParameterMethod.setLong, System.currentTimeMillis());
            MetaDbUtil.setParameter(2, params, ParameterMethod.setInt, supportedCommands);
            MetaDbUtil.setParameter(3, params, ParameterMethod.setLong, jobId);
            return MetaDbUtil.update(UPDATE_SUPPORTED_COMMANDS, params, connection);
        } catch (Exception e) {
            throw logAndThrow(
                "Failed to update " + DDL_ENGINE_TABLE + " for job " + jobId + " to change supported_commands",
                "update", e);
        }
    }

    public int update(long jobId, String content, boolean isContext) {
        try {
            final Map<Integer, ParameterContext> params = new HashMap<>(3);
            MetaDbUtil.setParameter(1, params, ParameterMethod.setLong, System.currentTimeMillis());
            MetaDbUtil.setParameter(2, params, ParameterMethod.setString, content);
            MetaDbUtil.setParameter(3, params, ParameterMethod.setLong, jobId);
            return MetaDbUtil.update(isContext ? UPDATE_CONTEXT : UPDATE_RESULT, params, connection);
        } catch (Exception e) {
            throw logAndThrow(
                "Failed to update " + DDL_ENGINE_TABLE + " for job " + jobId + " with "
                    + (isContext ? "context" : "result"), "update", e);
        }
    }

    public int delete(long jobId) {
        try {
            final Map<Integer, ParameterContext> params =
                MetaDbUtil.buildParameters(ParameterMethod.setLong, new Long[] {jobId});
            DdlMetaLogUtil.logSql(DELETE_SPECIFIC, params);
            deleteArchive(jobId);
            archive(jobId);
            return MetaDbUtil.delete(DELETE_SPECIFIC, params, connection);
        } catch (Exception e) {
            throw logAndThrow("Failed to delete from " + DDL_ENGINE_TABLE + " for job " + jobId, "delete from", e);
        }
    }

    public int deleteAll(String schemaName) {
        try {
            final Map<Integer, ParameterContext> params =
                MetaDbUtil.buildParameters(ParameterMethod.setString, new String[] {schemaName});
            DdlMetaLogUtil.logSql(DELETE_BY_SCHEMA_NAME, params);
            return MetaDbUtil.delete(DELETE_BY_SCHEMA_NAME, params, connection);
        } catch (Exception e) {
            throw logAndThrow("Failed to delete from " + DDL_ENGINE_TABLE + " for schemaName " + schemaName,
                "delete from", e);
        }
    }

    public int deleteArchive(long jobId) {
        try {
            final Map<Integer, ParameterContext> params =
                MetaDbUtil.buildParameters(ParameterMethod.setLong, new Long[] {jobId});
            DdlMetaLogUtil.logSql(DELETE_ARCHIVE_BY_JOB_ID, params);
            return MetaDbUtil.delete(DELETE_ARCHIVE_BY_JOB_ID, params, connection);
        } catch (Exception e) {
            throw logAndThrow("Failed to delete from " + DDL_ENGINE_TABLE + " for job " + jobId, "delete from", e);
        }
    }

    public int deleteAllArchive(String schemaName) {
        try {
            final Map<Integer, ParameterContext> params =
                MetaDbUtil.buildParameters(ParameterMethod.setString, new String[] {schemaName});
            DdlMetaLogUtil.logSql(DELETE_ARCHIVE_BY_SCHEMA_NAME, params);
            return MetaDbUtil.delete(DELETE_ARCHIVE_BY_SCHEMA_NAME, params, connection);
        } catch (Exception e) {
            throw logAndThrow("Failed to delete from " + DDL_ENGINE_TABLE_ARCHIVE + " for schemaName " + schemaName,
                "delete from", e);
        }
    }

    public int archive(long jobId) {
        try {
            final Map<Integer, ParameterContext> params =
                MetaDbUtil.buildParameters(ParameterMethod.setLong, new Long[] {jobId});
            DdlMetaLogUtil.logSql(ARCHIVE_SPECIFIC, params);
            return MetaDbUtil.delete(ARCHIVE_SPECIFIC, params, connection);
        } catch (Exception e) {
            throw logAndThrow("Failed to copy record from " + DDL_ENGINE_TABLE + " for job " + jobId, "archive", e);
        }
    }

    public int cleanUpArchive(long minutes) {
        try {
            LocalDateTime.now().minusMinutes(minutes);
            final Map<Integer, ParameterContext> params = new HashMap<>();
            MetaDbUtil.setParameter(params.size() + 1, params, ParameterMethod.setLong,
                System.currentTimeMillis() - TimeUnit.MINUTES.toMillis(minutes));

            return MetaDbUtil.delete(CLEAN_ARCHIVE_IN_MINUTS, params, connection);
        } catch (Exception e) {
            throw logAndThrow("Failed to clean archive from " + DDL_ENGINE_TABLE_ARCHIVE, "delete from", e);
        }
    }

    private TddlRuntimeException logAndThrow(String errMsg, String action, Exception e) {
        LOGGER.error(errMsg, e);
        return new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, action,
            DDL_ENGINE_TABLE, e.getMessage());
    }

    private String fillInQuestionMarks(String sqlTemplate, int count) {
        StringBuilder buf = new StringBuilder();
        for (int i = 0; i < count; i++) {
            buf.append(DdlConstants.COMMA).append(DdlConstants.QUESTION_MARK);
        }
        return String.format(sqlTemplate, buf.deleteCharAt(0).toString());
    }

    public String[] buildStateArray(Set<DdlState> states) {
        List<String> stateList = new ArrayList<>(states.size());
        for (DdlState state : states) {
            stateList.add(state.name());
        }
        return stateList.toArray(new String[states.size()]);
    }

    private String concatIds(List<Long> ids) {
        return StringUtils.join(ids, DdlConstants.COMMA);
    }

}
