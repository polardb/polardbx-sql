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
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import org.apache.commons.collections.CollectionUtils;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class DdlEngineTaskAccessor extends AbstractAccessor {

    private static final Logger LOGGER = LoggerFactory.getLogger(DdlEngineTaskAccessor.class);

    public static final String DDL_ENGINE_TASK_TABLE = wrap(GmsSystemTables.DDL_ENGINE_TASK);

    private static final String INSERT_DATA =
        "insert into " + DDL_ENGINE_TASK_TABLE
            + "(`job_id`, `task_id`, `schema_name`, `name`, `state`, `exception_action`, `value`, `extra`) "
            + "values (?, ?, ?, ?, ?, ?, ?, ?)";

    private static final String SELECT_FULL =
        "select `job_id`, `task_id`, `schema_name`, `name`, `state`, `exception_action`, `value`, `extra`";

    private static final String FROM_TABLE = " from " + DDL_ENGINE_TASK_TABLE;

    private static final String WHERE_JOB_ID = " where `job_id` = ?";

    private static final String WHERE_SCHEMA_NAME = " where `schema_name` = ?";

    private static final String WITH_TASK_ID = " and `task_id` = ?";

    private static final String SELECT_BASE = SELECT_FULL + FROM_TABLE;

    private static final String SELECT_BY_JOB_ID = SELECT_BASE + WHERE_JOB_ID;

    private static final String SELECT_BY_JOB_ID_TASK_ID = SELECT_BASE + WHERE_JOB_ID + WITH_TASK_ID;

    private static final String UPDATE_BASE = "update " + DDL_ENGINE_TASK_TABLE + " set ";

    private static final String UPDATE_TASK_DONE = UPDATE_BASE + "`state` = 'SUCCESS'" + WHERE_JOB_ID + WITH_TASK_ID;

    private static final String UPDATE_TASK_UN_DONE = UPDATE_BASE + "`state` = 'INIT'" + WHERE_JOB_ID + WITH_TASK_ID;

    private static final String UPDATE_TASK = UPDATE_BASE + "`state`=?, `value`=?" + WHERE_JOB_ID + WITH_TASK_ID;

    private static final String APPEND_PHY_OBJECT_DONE =
        UPDATE_BASE + "`extra` = concat_ws('" + DdlConstants.SEMICOLON + "', `extra`, ?)" + WHERE_JOB_ID + WITH_TASK_ID;

    private static final String RESET_PHY_OBJECT_DONE = UPDATE_BASE + "`extra` = ?" + WHERE_JOB_ID + WITH_TASK_ID;

    private static final String DELETE_BASE = "delete" + FROM_TABLE;

    private static final String DELETE_BY_JOB_ID = DELETE_BASE + WHERE_JOB_ID;

    private static final String DELETE_BY_SCHEMA_NAME = DELETE_BASE + WHERE_SCHEMA_NAME;

    public int insert(List<DdlEngineTaskRecord> recordList) {
        try {
            if (CollectionUtils.isEmpty(recordList)) {
                return 0;
            }
            List<Map<Integer, ParameterContext>> paramsBatch =
                recordList.stream().map(e -> e.buildParams()).collect(Collectors.toList());
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

    public int deleteByJobId(long jobId) {
        try {
            final Map<Integer, ParameterContext> params =
                MetaDbUtil.buildParameters(ParameterMethod.setLong, new Long[] {jobId});
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

    private TddlRuntimeException logAndThrow(String errMsg, String action, Exception e) {
        LOGGER.error(errMsg, e);
        return new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, action,
            DDL_ENGINE_TASK_TABLE, e.getMessage());
    }

}
