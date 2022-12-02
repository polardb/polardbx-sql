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

package com.alibaba.polardbx.gms.tablegroup;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.ParameterMethod;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.gms.metadb.GmsSystemTables;
import com.alibaba.polardbx.gms.metadb.accessor.AbstractAccessor;
import com.alibaba.polardbx.gms.util.DdlMetaLogUtil;
import com.alibaba.polardbx.gms.util.MetaDbUtil;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by luoyanxin.
 *
 * @author luoyanxin
 */
public class ComplexTaskOutlineAccessor extends AbstractAccessor {
    private static final Logger LOGGER = LoggerFactory.getLogger(ComplexTaskOutlineAccessor.class);
    private static final String ALL_COLUMNS =
        "`id`,`job_id`, `gmt_create`,`gmt_modified`,`table_schema`,`tg_name`,`object_name`,`type`, `status`, `extra`, `source_sql`, `sub_task`";

    private static final String ALL_VALUES = "(null,?, now(),now(),?,?,?,?,?,?,?,?)";

    private static final String INSERT_COMPLEXTASK_OUTLINE =
        "insert into " + GmsSystemTables.COMPLEX_TASK_OUTLINE + " (" + ALL_COLUMNS + ") VALUES " + ALL_VALUES;

    private static final String GET_ALL_UNFINISH_COMPLEX_TASK =
        "select " + ALL_COLUMNS + " from " + GmsSystemTables.COMPLEX_TASK_OUTLINE + " where status<>-1";

    private static final String UPDATE_PARENT_COMPLEXTASK_STATUS_BY_JOBID_SCH =
        "update " + GmsSystemTables.COMPLEX_TASK_OUTLINE + " set status = ? where status = ? and job_id = ? "
            + "and table_schema=? and status<>-1 and sub_task=0";

    private static final String UPDATE_SUBTASK_STATUS_BY_JOBID_SCH_OBJ =
        "update " + GmsSystemTables.COMPLEX_TASK_OUTLINE + " set status = ? where status = ? and job_id = ? "
            + "and table_schema=? and object_name=? and status<>-1 and sub_task=1";

    private static final String UPDATE_SUBTASK_STATUS_BY_JOBID_SCH =
        "update " + GmsSystemTables.COMPLEX_TASK_OUTLINE
            + " set status = ? where status = ? and job_id = ? and table_schema=? and sub_task=1";

    private static final String DELETE_COMPLEXTASK_STATUS_BY_JOB_ID_AND_SCH_NAME_AND_OBJ_NAME =
        "delete from " + GmsSystemTables.COMPLEX_TASK_OUTLINE + " where job_id=? and table_schema=? and object_name=?";

    private static final String DELETE_COMPLEXTASK_STATUS_BY_JOB_ID =
        "delete from " + GmsSystemTables.COMPLEX_TASK_OUTLINE + " where table_schema=? and job_id=?";

    private static final String DELETE_COMPLEXTASK_BY_SCHEMA =
        "delete from " + GmsSystemTables.COMPLEX_TASK_OUTLINE + " where table_schema=?";

    private static final String GET_ALL_UNFINISH_PARENT_COMPLEX_TASK_BY_SCH =
        "select " + ALL_COLUMNS + " from " + GmsSystemTables.COMPLEX_TASK_OUTLINE
            + " where status<>-1 and sub_task=0 and table_schema=?";

    private static final String GET_ALL_UNFINISH_COMPLEX_TASK_BY_SCH =
        "select " + ALL_COLUMNS + " from " + GmsSystemTables.COMPLEX_TASK_OUTLINE
            + " where status<>-1 and table_schema=?";

    private static final String GET_ALL_SCALEOUT_TASK_BY_SCH =
        "select " + ALL_COLUMNS + " from " + GmsSystemTables.COMPLEX_TASK_OUTLINE
            + " where type=? and table_schema=? and sub_task=0";

    private static final String GET_UNFINISH_COMPLEX_TASK_BY_SCH_TB =
        "select " + ALL_COLUMNS + " from " + GmsSystemTables.COMPLEX_TASK_OUTLINE
            + " where status<>-1 and sub_task=1 and table_schema=? and object_name=?"
            + " union all (select " + ALL_COLUMNS + " from " + GmsSystemTables.COMPLEX_TASK_OUTLINE
            + " where status<>-1 and sub_task=0 and job_id in "
            + "(select job_id from " + GmsSystemTables.COMPLEX_TASK_OUTLINE
            + " where status<>-1 and sub_task=1 and table_schema=? and object_name=?))";

    private static final String DELETE_SCALEOUT_SUBTASK_BY_JOB_ID =
        "delete from " + GmsSystemTables.COMPLEX_TASK_OUTLINE + " where job_id=? and sub_task=1 and type=?";

    private static final String INVALID_SCALEOUT_TASK_BY_JOB_ID =
        "update " + GmsSystemTables.COMPLEX_TASK_OUTLINE + " set status=-1 where job_id=? and sub_task=0 and type=?";

    public Long addNewComplexTaskOutline(ComplexTaskOutlineRecord complexTaskOutlineRecord) {
        try {
            List<Map<Integer, ParameterContext>> paramsBatch = new ArrayList<>();
            Map<Integer, ParameterContext> params = new HashMap<>();

            int index = 1;
            MetaDbUtil.setParameter(index++, params, ParameterMethod.setLong, complexTaskOutlineRecord.getJob_id());
            MetaDbUtil
                .setParameter(index++, params, ParameterMethod.setString, complexTaskOutlineRecord.getTableSchema());
            MetaDbUtil
                .setParameter(index++, params, ParameterMethod.setString, complexTaskOutlineRecord.getTableGroupName());
            MetaDbUtil
                .setParameter(index++, params, ParameterMethod.setString, complexTaskOutlineRecord.getObjectName());
            MetaDbUtil.setParameter(index++, params, ParameterMethod.setInt, complexTaskOutlineRecord.getType());
            MetaDbUtil.setParameter(index++, params, ParameterMethod.setInt, complexTaskOutlineRecord.getStatus());
            MetaDbUtil.setParameter(index++, params, ParameterMethod.setString, complexTaskOutlineRecord.getExtra());
            MetaDbUtil
                .setParameter(index++, params, ParameterMethod.setString, complexTaskOutlineRecord.getSourceSql());
            MetaDbUtil.setParameter(index++, params, ParameterMethod.setInt, complexTaskOutlineRecord.getSubTask());

            paramsBatch.add(params);

            DdlMetaLogUtil.logSql(INSERT_COMPLEXTASK_OUTLINE, paramsBatch);

            return MetaDbUtil
                .insertAndRetureLastInsertId(INSERT_COMPLEXTASK_OUTLINE, paramsBatch, this.connection);
        } catch (Exception e) {
            LOGGER.error("Failed to insert into the system table " + GmsSystemTables.COMPLEX_TASK_OUTLINE, e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e,
                e.getMessage());
        }
    }

    public void updateParentComplexTaskStatusByJobId(Long jobId, String schemaName, int oldStatus, int newStatus) {
        try {

            Map<Integer, ParameterContext> params = new HashMap<>();
            MetaDbUtil.setParameter(1, params, ParameterMethod.setInt, newStatus);
            MetaDbUtil.setParameter(2, params, ParameterMethod.setInt, oldStatus);
            MetaDbUtil.setParameter(3, params, ParameterMethod.setLong, jobId);
            MetaDbUtil.setParameter(4, params, ParameterMethod.setString, schemaName);

            DdlMetaLogUtil.logSql(UPDATE_PARENT_COMPLEXTASK_STATUS_BY_JOBID_SCH, params);

            MetaDbUtil.update(UPDATE_PARENT_COMPLEXTASK_STATUS_BY_JOBID_SCH, params, connection);
            return;
        } catch (Exception e) {
            LOGGER.error("Failed to query the system table " + GmsSystemTables.COMPLEX_TASK_OUTLINE, e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e,
                e.getMessage());
        }
    }

    public void updateAllSubTasksStatusByJobId(Long jobId, String schemaName, int oldStatus, int newStatus) {
        try {

            Map<Integer, ParameterContext> params = new HashMap<>();
            MetaDbUtil.setParameter(1, params, ParameterMethod.setInt, newStatus);
            MetaDbUtil.setParameter(2, params, ParameterMethod.setInt, oldStatus);
            MetaDbUtil.setParameter(3, params, ParameterMethod.setLong, jobId);
            MetaDbUtil.setParameter(4, params, ParameterMethod.setString, schemaName);

            DdlMetaLogUtil.logSql(UPDATE_SUBTASK_STATUS_BY_JOBID_SCH, params);

            MetaDbUtil.update(UPDATE_SUBTASK_STATUS_BY_JOBID_SCH, params, connection);
            return;
        } catch (Exception e) {
            LOGGER.error("Failed to query the system table " + GmsSystemTables.COMPLEX_TASK_OUTLINE, e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e,
                e.getMessage());
        }
    }

    public void updateSubTasksStatusByJobIdAndObjName(Long jobId, String schemaName, String objectName, int oldStatus,
                                                      int newStatus) {
        try {

            Map<Integer, ParameterContext> params = new HashMap<>();
            MetaDbUtil.setParameter(1, params, ParameterMethod.setInt, newStatus);
            MetaDbUtil.setParameter(2, params, ParameterMethod.setInt, oldStatus);
            MetaDbUtil.setParameter(3, params, ParameterMethod.setLong, jobId);
            MetaDbUtil.setParameter(4, params, ParameterMethod.setString, schemaName);
            MetaDbUtil.setParameter(5, params, ParameterMethod.setString, objectName);
            DdlMetaLogUtil.logSql(UPDATE_SUBTASK_STATUS_BY_JOBID_SCH_OBJ, params);

            MetaDbUtil.update(UPDATE_SUBTASK_STATUS_BY_JOBID_SCH_OBJ, params, connection);
            return;
        } catch (Exception e) {
            LOGGER.error("Failed to query the system table " + GmsSystemTables.COMPLEX_TASK_OUTLINE, e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e,
                e.getMessage());
        }
    }

    public List<ComplexTaskOutlineRecord> getAllUnFinishComplexTask() {
        try {

            List<ComplexTaskOutlineRecord> records;
            Map<Integer, ParameterContext> params = new HashMap<>();

            records =
                MetaDbUtil.query(GET_ALL_UNFINISH_COMPLEX_TASK, params, ComplexTaskOutlineRecord.class, connection);

            return records;
        } catch (Exception e) {
            LOGGER.error("Failed to query the system table " + GmsSystemTables.COMPLEX_TASK_OUTLINE, e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e,
                e.getMessage());
        }
    }

    public void deleteComplexTaskByJobIdAndObjName(Long jobId, String schemaName, String objectName) {
        try {

            Map<Integer, ParameterContext> params = new HashMap<>();
            MetaDbUtil.setParameter(1, params, ParameterMethod.setLong, jobId);
            MetaDbUtil.setParameter(2, params, ParameterMethod.setString, schemaName);
            MetaDbUtil.setParameter(3, params, ParameterMethod.setString, objectName);
            DdlMetaLogUtil.logSql(DELETE_COMPLEXTASK_STATUS_BY_JOB_ID_AND_SCH_NAME_AND_OBJ_NAME, params);

            MetaDbUtil.update(DELETE_COMPLEXTASK_STATUS_BY_JOB_ID_AND_SCH_NAME_AND_OBJ_NAME, params, connection);
            return;
        } catch (Exception e) {
            LOGGER.error("Failed to query the system table " + GmsSystemTables.COMPLEX_TASK_OUTLINE, e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e,
                e.getMessage());
        }
    }

    public void deleteComplexTaskByJobId(String schemaName, Long jobId) {
        try {

            Map<Integer, ParameterContext> params = new HashMap<>();
            MetaDbUtil.setParameter(1, params, ParameterMethod.setString, schemaName);
            MetaDbUtil.setParameter(2, params, ParameterMethod.setLong, jobId);

            DdlMetaLogUtil.logSql(DELETE_COMPLEXTASK_STATUS_BY_JOB_ID, params);

            MetaDbUtil.update(DELETE_COMPLEXTASK_STATUS_BY_JOB_ID, params, connection);
            return;
        } catch (Exception e) {
            LOGGER.error("Failed to query the system table " + GmsSystemTables.COMPLEX_TASK_OUTLINE, e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e,
                e.getMessage());
        }
    }

    public void deleteScaleOutSubTaskByJobId(Long jobId, int type) {
        try {

            Map<Integer, ParameterContext> params = new HashMap<>();
            MetaDbUtil.setParameter(1, params, ParameterMethod.setLong, jobId);
            MetaDbUtil.setParameter(2, params, ParameterMethod.setInt, type);
            DdlMetaLogUtil.logSql(DELETE_SCALEOUT_SUBTASK_BY_JOB_ID, params);

            MetaDbUtil.update(DELETE_SCALEOUT_SUBTASK_BY_JOB_ID, params, connection);
            return;
        } catch (Exception e) {
            LOGGER.error("Failed to query the system table " + GmsSystemTables.COMPLEX_TASK_OUTLINE, e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e,
                e.getMessage());
        }
    }

    public void invalidScaleOutTaskByJobId(Long jobId, int type) {
        try {

            Map<Integer, ParameterContext> params = new HashMap<>();
            MetaDbUtil.setParameter(1, params, ParameterMethod.setLong, jobId);
            MetaDbUtil.setParameter(2, params, ParameterMethod.setInt, type);
            MetaDbUtil.update(INVALID_SCALEOUT_TASK_BY_JOB_ID, params, connection);

            MetaDbUtil.update(INVALID_SCALEOUT_TASK_BY_JOB_ID, params, connection);
            return;
        } catch (Exception e) {
            LOGGER.error("Failed to query the system table " + GmsSystemTables.COMPLEX_TASK_OUTLINE, e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e,
                e.getMessage());
        }
    }

    public void deleteComplexTaskBySchema(String schemaName) {
        try {

            Map<Integer, ParameterContext> params = new HashMap<>();
            MetaDbUtil.setParameter(1, params, ParameterMethod.setString, schemaName);
            MetaDbUtil.update(DELETE_COMPLEXTASK_BY_SCHEMA, params, connection);

            MetaDbUtil.update(DELETE_COMPLEXTASK_BY_SCHEMA, params, connection);
            return;
        } catch (Exception e) {
            LOGGER.error("Failed to query the system table " + GmsSystemTables.COMPLEX_TASK_OUTLINE, e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e,
                e.getMessage());
        }
    }

    public List<ComplexTaskOutlineRecord> getAllUnFinishParentComlexTask(String schemaName) {
        try {
            List<ComplexTaskOutlineRecord> records;
            Map<Integer, ParameterContext> params = new HashMap<>();
            MetaDbUtil.setParameter(1, params, ParameterMethod.setString, schemaName);
            records =
                MetaDbUtil
                    .query(GET_ALL_UNFINISH_PARENT_COMPLEX_TASK_BY_SCH, params, ComplexTaskOutlineRecord.class,
                        connection);

            return records;
        } catch (Exception e) {
            LOGGER.error("Failed to query the system table " + GmsSystemTables.COMPLEX_TASK_OUTLINE, e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e,
                e.getMessage());
        }
    }

    public List<ComplexTaskOutlineRecord> getAllUnFinishComplexTaskBySch(String schemaName) {
        try {
            List<ComplexTaskOutlineRecord> records;
            Map<Integer, ParameterContext> params = new HashMap<>();
            MetaDbUtil.setParameter(1, params, ParameterMethod.setString, schemaName);
            records =
                MetaDbUtil
                    .query(GET_ALL_UNFINISH_COMPLEX_TASK_BY_SCH, params, ComplexTaskOutlineRecord.class,
                        connection);

            return records;
        } catch (Exception e) {
            LOGGER.error("Failed to query the system table " + GmsSystemTables.COMPLEX_TASK_OUTLINE, e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e,
                e.getMessage());
        }
    }

    public List<ComplexTaskOutlineRecord> getAllScaleOutComplexTaskBySch(int type, String schemaName) {
        try {
            List<ComplexTaskOutlineRecord> records;
            Map<Integer, ParameterContext> params = new HashMap<>();
            MetaDbUtil.setParameter(1, params, ParameterMethod.setInt, type);
            MetaDbUtil.setParameter(2, params, ParameterMethod.setString, schemaName);
            records =
                MetaDbUtil
                    .query(GET_ALL_SCALEOUT_TASK_BY_SCH, params, ComplexTaskOutlineRecord.class,
                        connection);

            return records;
        } catch (Exception e) {
            LOGGER.error("Failed to query the system table " + GmsSystemTables.COMPLEX_TASK_OUTLINE, e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e,
                e.getMessage());
        }
    }

    public List<ComplexTaskOutlineRecord> getAllUnFinishComlexTaskBySchemaAndTable(String schemaName,
                                                                                   String tableName) {
        try {
            List<ComplexTaskOutlineRecord> records;
            Map<Integer, ParameterContext> params = new HashMap<>();
            int i = 1;
            MetaDbUtil.setParameter(i++, params, ParameterMethod.setString, schemaName);
            MetaDbUtil.setParameter(i++, params, ParameterMethod.setString, tableName);
            MetaDbUtil.setParameter(i++, params, ParameterMethod.setString, schemaName);
            MetaDbUtil.setParameter(i++, params, ParameterMethod.setString, tableName);
            records =
                MetaDbUtil
                    .query(GET_UNFINISH_COMPLEX_TASK_BY_SCH_TB, params, ComplexTaskOutlineRecord.class,
                        connection);

            return records;
        } catch (Exception e) {
            LOGGER.error("Failed to query the system table " + GmsSystemTables.COMPLEX_TASK_OUTLINE, e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e,
                e.getMessage());
        }
    }
}
