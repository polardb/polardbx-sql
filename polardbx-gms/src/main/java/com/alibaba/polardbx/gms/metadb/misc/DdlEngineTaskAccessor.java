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

import com.alibaba.fastjson.JSON;
import com.alibaba.polardbx.common.ddl.newengine.DdlConstants;
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
import org.apache.commons.collections.CollectionUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class DdlEngineTaskAccessor extends AbstractAccessor {

    private static final Logger LOGGER = LoggerFactory.getLogger(DdlEngineTaskAccessor.class);

    public static final String DDL_ENGINE_TASK_TABLE = wrap(GmsSystemTables.DDL_ENGINE_TASK);

    public static final String DDL_ENGINE_TASK_TABLE_ARCHIVE = wrap(GmsSystemTables.DDL_ENGINE_TASK_ARCHIVE);

    private static final String INSERT_DATA =
        "insert into " + DDL_ENGINE_TASK_TABLE
            + "(`job_id`, `task_id`, `schema_name`, `name`, `state`, `exception_action`, `value`, `extra`, `cost`, `root_job_id`) "
            + "values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

    private static final String SELECT_FULL =
        "select `job_id`, `task_id`, `schema_name`, `name`, `state`, `exception_action`, `value`, `extra`, `cost`, `root_job_id`";

    private static final String SELECT_SIMPLE =
        "select `job_id`, `task_id`, `schema_name`, `name`, `state`, `exception_action`, null as `value`, null as `extra`, `cost`, `root_job_id` ";

    private static final String SELECT_PARTIAL =
        "select `job_id`, `task_id`, `schema_name`, `name`, `state`, `exception_action`,  `value`, null as `extra`, `cost`, `root_job_id` ";

    private static final String FROM_TABLE = " from " + DDL_ENGINE_TASK_TABLE;

    private static final String WHERE_JOB_ID = " where `job_id` = ?";

    private static final String WHERE_ROOT_JOB_ID = " where `root_job_id` = ?";

    private static final String WHERE_SCHEMA_NAME = " where `schema_name` = ?";

    private static final String WITH_TASK_ID = " and `task_id` = ?";

    private static final String WITH_ONGOING_TASK = " and `state` <> 'SUCCESS' and `state` <> 'ROLLBACK_SUCCESS' ";

    private static final String WITH_TASK_NAME = " and `name` = ?";

    private static final String SELECT_BASE = SELECT_FULL + FROM_TABLE;

    private static final String SELECT_BY_JOB_ID = SELECT_BASE + WHERE_JOB_ID;

    private static final String SELECT_BY_JOB_ID_TASK_ID = SELECT_BASE + WHERE_JOB_ID + WITH_TASK_ID;

    private static final String SELECT_ONGOING_TASK =
        SELECT_BASE + WHERE_SCHEMA_NAME + WITH_ONGOING_TASK + WITH_TASK_NAME;

    private static final String SELECT_BY_JOB_ID_TASK_NAME = SELECT_BASE + WHERE_JOB_ID + WITH_TASK_NAME;

    private static final String SELECT_TASK_SIMPLE_INFO_BY_ROOT_JOB_ID =
        SELECT_SIMPLE + FROM_TABLE + " " + WHERE_ROOT_JOB_ID;

    private static final String SELECT_TASK_SIMPLE_INFO_BY_ROOT_JOB_ID_IN_ARCHIVE =
        SELECT_SIMPLE + " FROM " + DDL_ENGINE_TASK_TABLE_ARCHIVE + " " + WHERE_ROOT_JOB_ID;

    private static final String SELECT_TASK_PARTIAL_INFO_BY_ROOT_JOB_ID =
        SELECT_PARTIAL + FROM_TABLE + " " + WHERE_ROOT_JOB_ID;

    private static final String SELECT_TASK_PARTIAL_INFO_BY_ROOT_JOB_ID_IN_ARCHIVE =
        SELECT_PARTIAL + " FROM " + DDL_ENGINE_TASK_TABLE_ARCHIVE + " " + WHERE_ROOT_JOB_ID;

    private static final String SELECT_TASK_BY_NAME =
        SELECT_FULL + " FROM " + DDL_ENGINE_TASK_TABLE + " " + "WHERE ";

    private static final String SELECT_TASK_BY_NAME_ARCHIVE =
        SELECT_FULL + " FROM " + DDL_ENGINE_TASK_TABLE_ARCHIVE + " " + "WHERE ";
    private static final String SELECT_TASK_INFO_BY_JOB_ID_WITH_EXTRA_NOT_NULL =
        SELECT_FULL + " FROM " + DDL_ENGINE_TASK_TABLE + WHERE_ROOT_JOB_ID + " and EXTRA is NOT NULL";

    private static final String SELECT_TASK_INFO_BY_JOB_ID_WITH_EXTRA_NOT_NULL_IN_ARCHIVE =
        SELECT_FULL + " FROM " + DDL_ENGINE_TASK_TABLE_ARCHIVE + WHERE_ROOT_JOB_ID + " and EXTRA is NOT NULL";

    private static final String UPDATE_BASE = "update " + DDL_ENGINE_TASK_TABLE + " set ";

    private static final String UPDATE_TASK_DONE = UPDATE_BASE + "`state` = 'SUCCESS'" + WHERE_JOB_ID + WITH_TASK_ID;

    private static final String UPDATE_TASK_UN_DONE = UPDATE_BASE + "`state` = 'INIT'" + WHERE_JOB_ID + WITH_TASK_ID;

    private static final String UPDATE_TASK = UPDATE_BASE + "`state`=?, `value`=?" + WHERE_JOB_ID + WITH_TASK_ID;

    private static final String APPEND_PHY_OBJECT_DONE =
        UPDATE_BASE + "`extra` = concat_ws('" + DdlConstants.SEMICOLON + "', `extra`, ?)" + WHERE_JOB_ID + WITH_TASK_ID;

    private static final String RESET_PHY_OBJECT_DONE = UPDATE_BASE + "`extra` = ?" + WHERE_JOB_ID + WITH_TASK_ID;

    private static final String UPDATE_EXTRA_FOR_CREATE_DATABASE_AS_LIKE =
        UPDATE_BASE + "`extra` = ?" + WHERE_JOB_ID + WITH_TASK_ID;

    private static final String DELETE_BASE = "delete" + FROM_TABLE;

    private static final String DELETE_BY_JOB_ID = DELETE_BASE + WHERE_JOB_ID;

    private static final String DELETE_BY_SCHEMA_NAME = DELETE_BASE + WHERE_SCHEMA_NAME;

    private static final String DELETE_ARCHIVE_BASE = "delete from " + DDL_ENGINE_TASK_TABLE_ARCHIVE;

    private static final String DELETE_ARCHIVE_BY_JOB_ID = DELETE_ARCHIVE_BASE + WHERE_JOB_ID;

    private static final String DELETE_ARCHIVE_BY_SCHEMA_NAME = DELETE_ARCHIVE_BASE + WHERE_SCHEMA_NAME;

    private static final String ARCHIVE_BASE =
        "insert into " + DDL_ENGINE_TASK_TABLE_ARCHIVE + " select * from " + DDL_ENGINE_TASK_TABLE;

    private static final String ARCHIVE_SPECIFIC = ARCHIVE_BASE + WHERE_JOB_ID;

    private static final String ARCHIVE_SELECT =
        SELECT_FULL + " from " + DDL_ENGINE_TASK_TABLE_ARCHIVE + WHERE_JOB_ID + WITH_TASK_ID;

    private static final String WHERE_TASK_NAME = " where `name` = ?";
    private static final String EXISTS_PHYSICAL_BACKFILL_TASK =
        "select 1 from " + DDL_ENGINE_TASK_TABLE + WHERE_TASK_NAME + " limit 1";

    public int insert(List<DdlEngineTaskRecord> recordList) {
        try {
            if (CollectionUtils.isEmpty(recordList)) {
                return 0;
            }
            List<Map<Integer, ParameterContext>> paramsBatch =
                recordList.stream().map(e -> e.buildParams()).collect(Collectors.toList());
            DdlMetaLogUtil.logSql(INSERT_DATA + " record count: " + recordList.size());
            int[] r = MetaDbUtil.insert(INSERT_DATA, paramsBatch, connection);
            return Arrays.stream(r).sum();
        } catch (Exception e) {
            throw logAndThrow("Failed to insert a new record into " + DDL_ENGINE_TASK_TABLE, "insert into", e);
        }
    }

    public List<DdlEngineTaskRecord> query(long jobId) {
        try {
            final Map<Integer, ParameterContext> params =
                MetaDbUtil.buildParameters(ParameterMethod.setLong, new Long[] {jobId});

            List<DdlEngineTaskRecord> records =
                MetaDbUtil.query(SELECT_BY_JOB_ID, params, DdlEngineTaskRecord.class, connection);

            if (records != null) {
                return records;
            }
            return null;
        } catch (Exception e) {
            throw logAndThrow("Failed to query from " + DDL_ENGINE_TASK_TABLE, "query from", e);
        }
    }

    public DdlEngineTaskRecord query(long jobId, long taskId) {
        try {
            final Map<Integer, ParameterContext> params = new HashMap<>();
            MetaDbUtil.setParameter(1, params, ParameterMethod.setLong, jobId);
            MetaDbUtil.setParameter(2, params, ParameterMethod.setLong, taskId);

            List<DdlEngineTaskRecord> records =
                MetaDbUtil.query(SELECT_BY_JOB_ID_TASK_ID, params, DdlEngineTaskRecord.class, connection);

            if (records != null && records.size() > 0) {
                return records.get(0);
            }
            return null;
        } catch (Exception e) {
            throw logAndThrow("Failed to query from " + DDL_ENGINE_TASK_TABLE, "query from", e);
        }
    }

    public List<DdlEngineTaskRecord> query(long jobId, String name) {
        try {
            final Map<Integer, ParameterContext> params = new HashMap<>();
            MetaDbUtil.setParameter(1, params, ParameterMethod.setLong, jobId);
            MetaDbUtil.setParameter(2, params, ParameterMethod.setString, name);

            List<DdlEngineTaskRecord> records =
                MetaDbUtil.query(SELECT_BY_JOB_ID_TASK_NAME, params, DdlEngineTaskRecord.class, connection);

            return records;
        } catch (Exception e) {
            throw logAndThrow("Failed to query from " + DDL_ENGINE_TASK_TABLE, "query from", e);
        }
    }

    public boolean hasOngoingTask(String schemaName, String taskName) {
        try {
            final Map<Integer, ParameterContext> params = new HashMap<>();
            MetaDbUtil.setParameter(1, params, ParameterMethod.setString, schemaName);
            MetaDbUtil.setParameter(2, params, ParameterMethod.setString, taskName);

            List<DdlEngineTaskRecord> records =
                MetaDbUtil.query(SELECT_ONGOING_TASK, params, DdlEngineTaskRecord.class, connection);
            return CollectionUtils.isNotEmpty(records);
        } catch (Exception e) {
            throw logAndThrow("Failed to query from " + DDL_ENGINE_TASK_TABLE, "query from", e);
        }
    }

    public List<DdlEngineTaskRecord> queryTaskSimpleInfoByJobId(long jobId, boolean archive) {
        try {
            final Map<Integer, ParameterContext> params = new HashMap<>();
            MetaDbUtil.setParameter(1, params, ParameterMethod.setLong, jobId);

            final String sql =
                archive ? SELECT_TASK_SIMPLE_INFO_BY_ROOT_JOB_ID_IN_ARCHIVE : SELECT_TASK_SIMPLE_INFO_BY_ROOT_JOB_ID;
            List<DdlEngineTaskRecord> records =
                MetaDbUtil.query(sql, params, DdlEngineTaskRecord.class, connection);

            return records;
        } catch (Exception e) {
            throw logAndThrow("Failed to query from " + DDL_ENGINE_TASK_TABLE, "query from", e);
        }
    }

    public List<DdlEngineTaskRecord> queryTaskPartialInfoByJobId(long jobId, boolean archive) {
        try {
            final Map<Integer, ParameterContext> params = new HashMap<>();
            MetaDbUtil.setParameter(1, params, ParameterMethod.setLong, jobId);

            final String sql =
                archive ? SELECT_TASK_PARTIAL_INFO_BY_ROOT_JOB_ID_IN_ARCHIVE : SELECT_TASK_PARTIAL_INFO_BY_ROOT_JOB_ID;
            List<DdlEngineTaskRecord> records =
                MetaDbUtil.query(sql, params, DdlEngineTaskRecord.class, connection);

            return records;
        } catch (Exception e) {
            throw logAndThrow("Failed to query from " + DDL_ENGINE_TASK_TABLE, "query from", e);
        }
    }

    public List<DdlEngineTaskRecord> queryAllTaskByNames(boolean archive, List<String> taskNames) {
        int paramNum = taskNames.size();
        if (paramNum == 0) {
            return new ArrayList<>();
        }
        try {
            String whereClause = "";
            for (int i = 0; i < paramNum - 1; i++) {
                whereClause = whereClause + " `name` = ? or ";
            }
            whereClause = whereClause + " `name` = ? ";
            final String sql = (archive ? SELECT_TASK_BY_NAME_ARCHIVE : SELECT_TASK_BY_NAME) + whereClause;

            final Map<Integer, ParameterContext> params = new HashMap<>();
            for (int i = 0; i < paramNum; i++) {
                MetaDbUtil.setParameter(i + 1, params, ParameterMethod.setString, taskNames.get(i));
            }

            return MetaDbUtil.query(sql, params, DdlEngineTaskRecord.class, connection);
        } catch (Exception e) {
            throw logAndThrow("Failed to query from " + DDL_ENGINE_TASK_TABLE, "query from", e);
        }
    }

    public int updateTaskDone(long jobId, long taskId) {
        try {
            final Map<Integer, ParameterContext> params = new HashMap<>();
            MetaDbUtil.setParameter(1, params, ParameterMethod.setLong, jobId);
            MetaDbUtil.setParameter(2, params, ParameterMethod.setLong, taskId);

            return MetaDbUtil.update(UPDATE_TASK_DONE, params, connection);
        } catch (Exception e) {
            String errMsg = String.format("Failed to update task done for taskId:%s, jobId:%s", taskId, jobId);
            throw logAndThrow(errMsg, "update done", e);
        }
    }

    public int updateTaskUnDone(long jobId, long taskId) {
        try {
            final Map<Integer, ParameterContext> params = new HashMap<>();
            MetaDbUtil.setParameter(1, params, ParameterMethod.setLong, jobId);
            MetaDbUtil.setParameter(2, params, ParameterMethod.setLong, taskId);

            return MetaDbUtil.update(UPDATE_TASK_UN_DONE, params, connection);
        } catch (Exception e) {
            String errMsg = String.format("Failed to update task undone for taskId:%s, jobId:%s", taskId, jobId);
            throw logAndThrow(errMsg, "update done", e);
        }
    }

    public int updateTask(DdlEngineTaskRecord extRecord) {
        try {
            Map<Integer, ParameterContext> params = new HashMap<>(16);
            int index = 0;
            MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, extRecord.state);
            MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, extRecord.value);
            MetaDbUtil.setParameter(++index, params, ParameterMethod.setLong, extRecord.jobId);
            MetaDbUtil.setParameter(++index, params, ParameterMethod.setLong, extRecord.taskId);

            return MetaDbUtil.update(UPDATE_TASK, params, connection);
        } catch (Exception e) {
            String errMsg = String.format("Failed to update task. value:%s", JSON.toJSON(extRecord));
            throw logAndThrow(errMsg, "update done", e);
        }
    }

    public int updatePhyDone(long jobId, long taskId, String phyObjectInfo, boolean isReset) {
        try {
            final Map<Integer, ParameterContext> params = new HashMap<>(3);
            MetaDbUtil.setParameter(1, params, ParameterMethod.setString, phyObjectInfo);
            MetaDbUtil.setParameter(2, params, ParameterMethod.setLong, jobId);
            MetaDbUtil.setParameter(3, params, ParameterMethod.setLong, taskId);
            return MetaDbUtil.update(isReset ? RESET_PHY_OBJECT_DONE : APPEND_PHY_OBJECT_DONE, params, connection);
        } catch (Exception e) {
            throw logAndThrow(
                "Failed to update " + DDL_ENGINE_TASK_TABLE + " for job " + jobId + " and task " + taskId + " with "
                    + (isReset ? "resetting" : "appending"), "update done", e);
        }
    }

    public int updateExtraInfoForCreateDbAsLike(long jobId, long taskId, String extraInfo) {
        try {
            final Map<Integer, ParameterContext> params = new HashMap<>(3);
            int i = 0;
            MetaDbUtil.setParameter(++i, params, ParameterMethod.setString, extraInfo);
            MetaDbUtil.setParameter(++i, params, ParameterMethod.setLong, jobId);
            MetaDbUtil.setParameter(++i, params, ParameterMethod.setLong, taskId);
            return MetaDbUtil.update(UPDATE_EXTRA_FOR_CREATE_DATABASE_AS_LIKE, params, connection);
        } catch (Exception e) {
            throw logAndThrow(
                "Failed to update " + DDL_ENGINE_TASK_TABLE + " for job " + jobId + " and task " + taskId
                    + " with updating",
                "update done",
                e
            );
        }
    }

    public int deleteByJobId(long jobId) {
        try {
            final Map<Integer, ParameterContext> params =
                MetaDbUtil.buildParameters(ParameterMethod.setLong, new Long[] {jobId});
            deleteArchiveByJobId(jobId);
            archive(jobId);
            return MetaDbUtil.delete(DELETE_BY_JOB_ID, params, connection);
        } catch (Exception e) {
            throw logAndThrow("Failed to delete from " + DDL_ENGINE_TASK_TABLE + " for job " + jobId, "delete from", e);
        }
    }

    public int deleteAll(String schemaName) {
        try {
            final Map<Integer, ParameterContext> params =
                MetaDbUtil.buildParameters(ParameterMethod.setString, new String[] {schemaName});
            return MetaDbUtil.delete(DELETE_BY_SCHEMA_NAME, params, connection);
        } catch (Exception e) {
            throw logAndThrow("Failed to delete from " + DDL_ENGINE_TASK_TABLE + " for schemaName " + schemaName,
                "delete from", e);
        }
    }

    public int deleteArchiveByJobId(long jobId) {
        try {
            final Map<Integer, ParameterContext> params =
                MetaDbUtil.buildParameters(ParameterMethod.setLong, new Long[] {jobId});
            return MetaDbUtil.delete(DELETE_ARCHIVE_BY_JOB_ID, params, connection);
        } catch (Exception e) {
            throw logAndThrow("Failed to delete from " + DDL_ENGINE_TASK_TABLE + " for job " + jobId, "delete from", e);
        }
    }

    public int deleteAllArchive(String schemaName) {
        try {
            final Map<Integer, ParameterContext> params =
                MetaDbUtil.buildParameters(ParameterMethod.setString, new String[] {schemaName});
            return MetaDbUtil.delete(DELETE_ARCHIVE_BY_SCHEMA_NAME, params, connection);
        } catch (Exception e) {
            throw logAndThrow(
                "Failed to delete from " + DDL_ENGINE_TASK_TABLE_ARCHIVE + " for schemaName " + schemaName,
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
            throw logAndThrow("Failed to copy record from " + DDL_ENGINE_TASK_TABLE + " for job " + jobId, "archive",
                e);
        }
    }

    public DdlEngineTaskRecord archiveQuery(long jobId, long taskId) {
        try {
            final Map<Integer, ParameterContext> params = new HashMap<>();
            MetaDbUtil.setParameter(1, params, ParameterMethod.setLong, jobId);
            MetaDbUtil.setParameter(2, params, ParameterMethod.setLong, taskId);

            List<DdlEngineTaskRecord> records =
                MetaDbUtil.query(ARCHIVE_SELECT, params, DdlEngineTaskRecord.class, connection);

            if (records != null && records.size() > 0) {
                return records.get(0);
            }
            return null;
        } catch (Exception e) {
            throw logAndThrow("Failed to query from " + DDL_ENGINE_TASK_TABLE_ARCHIVE, "query from", e);
        }
    }

    public List<DdlEngineTaskRecord> queryTaskWithExtraNotNull(long jobId, boolean archive) {
        try {
            final Map<Integer, ParameterContext> params = new HashMap<>();
            int i = 0;
            MetaDbUtil.setParameter(++i, params, ParameterMethod.setLong, jobId);

            String sql = archive ? SELECT_TASK_INFO_BY_JOB_ID_WITH_EXTRA_NOT_NULL_IN_ARCHIVE
                : SELECT_TASK_INFO_BY_JOB_ID_WITH_EXTRA_NOT_NULL;

            return MetaDbUtil.query(sql, params, DdlEngineTaskRecord.class, connection);

        } catch (Exception e) {
            throw logAndThrow(
                "Failed to query from " + (archive ? DDL_ENGINE_TASK_TABLE_ARCHIVE : DDL_ENGINE_TASK_TABLE),
                "query from", e);
        }
    }

    public boolean existPhysicalBackfillTask() {
        final Map<Integer, ParameterContext> params = new HashMap<>();
        int i = 0;
        MetaDbUtil.setParameter(++i, params, ParameterMethod.setString, "PhysicalBackfillTask");
        List<CountRecord> records =
            query(EXISTS_PHYSICAL_BACKFILL_TASK, DDL_ENGINE_TASK_TABLE, CountRecord.class, params);
        if (records != null && records.size() > 0) {
            return records.get(0).count > 0;
        }
        return false;
    }

    private TddlRuntimeException logAndThrow(String errMsg, String action, Exception e) {
        LOGGER.error(errMsg, e);
        return new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, action,
            DDL_ENGINE_TASK_TABLE, e.getMessage());
    }

}
