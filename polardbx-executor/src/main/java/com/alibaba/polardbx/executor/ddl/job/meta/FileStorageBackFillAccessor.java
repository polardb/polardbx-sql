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

package com.alibaba.polardbx.executor.ddl.job.meta;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.ParameterMethod;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.gsi.GsiBackfillManager;
import com.alibaba.polardbx.gms.metadb.GmsSystemTables;
import com.alibaba.polardbx.gms.metadb.accessor.AbstractAccessor;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import org.jetbrains.annotations.NotNull;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class FileStorageBackFillAccessor extends AbstractAccessor {

    private static final Logger LOGGER = LoggerFactory.getLogger("oss");

    private static final String SYSTABLE_BACKFILL_OBJECTS = GmsSystemTables.BACKFILL_OBJECTS;
    private static final String FILE_STORAGE_BACKFILL_OBJECTS = GmsSystemTables.FILE_STORAGE_BACKFILL_OBJECTS;

    private static final String SQL_SELECT_COUNT_FILE_BACKFILL_OBJECT = "SELECT COUNT(1) FROM "
        + FILE_STORAGE_BACKFILL_OBJECTS + " WHERE JOB_ID = ? AND PHYSICAL_DB = ? AND PHYSICAL_TABLE = ?";

    private static final String SQL_SELECT_BACKFILL_PROGRESS =
        "SELECT ID,JOB_ID,TASK_ID,TABLE_SCHEMA,TABLE_NAME,INDEX_SCHEMA,INDEX_NAME,PHYSICAL_DB,PHYSICAL_TABLE,COLUMN_INDEX,PARAMETER_METHOD,`LAST_VALUE`,MAX_VALUE,STATUS,MESSAGE,SUCCESS_ROW_COUNT,START_TIME,END_TIME,EXTRA FROM "
            + SYSTABLE_BACKFILL_OBJECTS + " WHERE JOB_ID = ? AND PHYSICAL_DB IS NULL AND PHYSICAL_TABLE IS NULL";

    private static final String SQL_SELECT_FILE_BACKFILL_OBJECT =
        "SELECT ID,JOB_ID,TASK_ID,TABLE_SCHEMA,TABLE_NAME,INDEX_SCHEMA,INDEX_NAME,PHYSICAL_DB,PHYSICAL_TABLE,COLUMN_INDEX,PARAMETER_METHOD,`LAST_VALUE`,MAX_VALUE,STATUS,MESSAGE,SUCCESS_ROW_COUNT,START_TIME,END_TIME,EXTRA FROM "
            + FILE_STORAGE_BACKFILL_OBJECTS + " WHERE JOB_ID = ? AND PHYSICAL_DB = ? AND PHYSICAL_TABLE = ?";

    private static final String SQL_INSERT_FILE_BACKFILL_OBJECT = "INSERT INTO "
        + FILE_STORAGE_BACKFILL_OBJECTS
        + "(JOB_ID,TASK_ID,TABLE_SCHEMA,TABLE_NAME,INDEX_SCHEMA,"
        + "INDEX_NAME,PHYSICAL_DB,PHYSICAL_TABLE,COLUMN_INDEX,PARAMETER_METHOD,`LAST_VALUE`,MAX_VALUE,STATUS,MESSAGE,SUCCESS_ROW_COUNT,START_TIME,END_TIME,EXTRA) "
        + "VALUES(? , ? , ?, ? , ? , ? , ? , ? , ? , ? , ? , ? , ? , ? , ? , ?, ?, ?)";

    private static final String SQL_INSERT_IGNORE_FILE_BACKFILL_OBJECT = "INSERT IGNORE INTO "
        + FILE_STORAGE_BACKFILL_OBJECTS
        + "(JOB_ID,TASK_ID,TABLE_SCHEMA,TABLE_NAME,INDEX_SCHEMA,"
        + "INDEX_NAME,PHYSICAL_DB,PHYSICAL_TABLE,COLUMN_INDEX,PARAMETER_METHOD,`LAST_VALUE`,MAX_VALUE,STATUS,MESSAGE,SUCCESS_ROW_COUNT,START_TIME,END_TIME,EXTRA) "
        + "VALUES(? , ? , ?, ? , ? , ? , ? , ? , ? , ? , ? , ? , ? , ? , ? , ?, ?, ?)";

    private static final String SQL_REPLACE_BACKFILL_OBJECT = "REPLACE INTO "
        + SYSTABLE_BACKFILL_OBJECTS
        + "(JOB_ID,TASK_ID,TABLE_SCHEMA,TABLE_NAME,INDEX_SCHEMA,"
        + "INDEX_NAME,PHYSICAL_DB,PHYSICAL_TABLE,COLUMN_INDEX,PARAMETER_METHOD,`LAST_VALUE`,MAX_VALUE,STATUS,MESSAGE,SUCCESS_ROW_COUNT,START_TIME,END_TIME,EXTRA) "
        + "VALUES(? , ? , ? , ?, ? , ? , ? , ? , ? , ? , ? , ? , ? , ? , ? , ?, ?, ?)";

    private static final String SQL_UPDATE_FILE_BACKFILL_PROGRESS = "UPDATE "
        + FILE_STORAGE_BACKFILL_OBJECTS
        + " SET `LAST_VALUE` = ?, `STATUS` = ?, `SUCCESS_ROW_COUNT` = ?, `END_TIME`=?"
        + " WHERE `JOB_ID` = ? AND `PHYSICAL_DB` = ? AND `PHYSICAL_TABLE` = ? AND `COLUMN_INDEX` = ? ";

    private static final String SQL_SELECT_BACKFILL_OBJECT =
        "SELECT ID,JOB_ID,TASK_ID,TABLE_SCHEMA,TABLE_NAME,INDEX_SCHEMA,INDEX_NAME,PHYSICAL_DB,PHYSICAL_TABLE,COLUMN_INDEX,PARAMETER_METHOD,`LAST_VALUE`,MAX_VALUE,STATUS,MESSAGE,SUCCESS_ROW_COUNT,START_TIME,END_TIME,EXTRA FROM "
            + SYSTABLE_BACKFILL_OBJECTS + " WHERE JOB_ID = ? AND PHYSICAL_DB = ? AND PHYSICAL_TABLE = ?";

    private static final String SQL_SELECT_FILE_BACKFILL_OBJECT_BY_ID =
        "SELECT ID,JOB_ID,TASK_ID,TABLE_SCHEMA,TABLE_NAME,INDEX_SCHEMA,INDEX_NAME,PHYSICAL_DB,PHYSICAL_TABLE,COLUMN_INDEX,PARAMETER_METHOD,`LAST_VALUE`,MAX_VALUE,STATUS,MESSAGE,SUCCESS_ROW_COUNT,START_TIME,END_TIME,EXTRA FROM "
            + FILE_STORAGE_BACKFILL_OBJECTS + " WHERE JOB_ID = ? ";

    private static final String SQL_DELETE_FILE_BACKFILL_OBJECT_BY_ID = "DELETE FROM "
        + FILE_STORAGE_BACKFILL_OBJECTS + " WHERE JOB_ID = ? ";

    Long job_id;
    String physicalDB;
    String physicalTable;

    public FileStorageBackFillAccessor() {
    }

    public FileStorageBackFillAccessor(Long job_id, String physicalDB, String physicalTable) {
        this.job_id = job_id;
        this.physicalDB = physicalDB;
        this.physicalTable = physicalTable;
    }

    public Long selectCountBackfillObjectFromFileStorage() {
        try {
            Map<Integer, ParameterContext> paramMap = new HashMap<>(3);
            int index = 0;
            MetaDbUtil.setParameter(++index, paramMap, ParameterMethod.setLong, job_id);
            MetaDbUtil.setParameter(++index, paramMap, ParameterMethod.setString, physicalDB);
            MetaDbUtil.setParameter(++index, paramMap, ParameterMethod.setString, physicalTable);
            List<Map<String, Object>> result =
                MetaDbUtil.executeCount(SQL_SELECT_COUNT_FILE_BACKFILL_OBJECT, paramMap, connection);
            if (result.size() == 1 && result.get(0).size() == 1) {
                for (Object obj : result.get(0).values()) {
                    if (obj instanceof Long) {
                        return (Long) obj;
                    }
                }
            }
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_UNEXPECTED,
                "unexpected result of " + SQL_SELECT_COUNT_FILE_BACKFILL_OBJECT);
        } catch (Exception e) {
            LOGGER.error(e.getMessage());
            throw GeneralUtil.nestedException(e);
        }
    }

    public void insertIgnoreFileBackfillMeta(List<GsiBackfillManager.BackfillObjectRecord> backfillObjectRecords) {
        final List<Map<Integer, ParameterContext>> params =
            backfillObjectRecords.stream().map(GsiBackfillManager.Orm::params)
                .collect(ArrayList::new, ArrayList::add, ArrayList::addAll);
        try {
            MetaDbUtil.update(SQL_INSERT_IGNORE_FILE_BACKFILL_OBJECT, params, connection);
        } catch (Exception e) {
            LOGGER.error(e.getMessage());
            throw GeneralUtil.nestedException(e);
        }
    }

    public void replaceBackfillMeta(List<GsiBackfillManager.BackfillObjectRecord> backfillObjectRecords) {
        final List<Map<Integer, ParameterContext>> params =
            backfillObjectRecords.stream().map(GsiBackfillManager.Orm::params)
                .collect(ArrayList::new, ArrayList::add, ArrayList::addAll);
        try {
            MetaDbUtil.update(SQL_REPLACE_BACKFILL_OBJECT, params, connection);
        } catch (Exception e) {
            LOGGER.error(e.getMessage());
            throw GeneralUtil.nestedException(e);
        }
    }

    public List<GsiBackfillManager.BackfillObjectRecord> selectBackfillObjectFromFileStorage() {
        return selectBackfillObject(SQL_SELECT_FILE_BACKFILL_OBJECT);
    }

    public List<GsiBackfillManager.BackfillObjectRecord> selectBackfillObjectFromBasic() {
        return selectBackfillObject(SQL_SELECT_BACKFILL_OBJECT);
    }

    public List<GsiBackfillManager.BackfillObjectRecord> selectBackfillProgress(Long jobId) {
        Map<Integer, ParameterContext> paramMap = new HashMap<>(1);
        int index = 0;
        MetaDbUtil.setParameter(++index, paramMap, ParameterMethod.setLong, jobId);
        return getBackfillObjectRecords(SQL_SELECT_BACKFILL_PROGRESS, paramMap);
    }

    public List<GsiBackfillManager.BackfillObjectRecord> selectBackfillObjectFromFileStorageById(Long jobId) {
        Map<Integer, ParameterContext> paramMap = new HashMap<>(1);
        int index = 0;
        MetaDbUtil.setParameter(++index, paramMap, ParameterMethod.setLong, jobId);
        return getBackfillObjectRecords(SQL_SELECT_FILE_BACKFILL_OBJECT_BY_ID, paramMap);
    }

    private List<GsiBackfillManager.BackfillObjectRecord> selectBackfillObject(String sql) {
        Map<Integer, ParameterContext> paramMap = new HashMap<>(3);
        int index = 0;
        MetaDbUtil.setParameter(++index, paramMap, ParameterMethod.setLong, job_id);
        MetaDbUtil.setParameter(++index, paramMap, ParameterMethod.setString, physicalDB);
        MetaDbUtil.setParameter(++index, paramMap, ParameterMethod.setString, physicalTable);
        return getBackfillObjectRecords(sql, paramMap);
    }

    @NotNull
    private List<GsiBackfillManager.BackfillObjectRecord> getBackfillObjectRecords(String sql,
                                                                                   Map<Integer, ParameterContext> paramMap) {
        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            ParameterMethod.setParameters(ps, paramMap);
            final ResultSet rs = ps.executeQuery();
            final List<GsiBackfillManager.BackfillObjectRecord> result = new ArrayList<>();
            while (rs.next()) {
                result.add(GsiBackfillManager.BackfillObjectRecord.ORM.convert(rs));
            }
            return result;
        } catch (Exception e) {
            LOGGER.error(e.getMessage());
            throw GeneralUtil.nestedException(e);
        }
    }

    public void deleteFileBackfillMeta(Long jobId) {
        Map<Integer, ParameterContext> params = new HashMap<>(1);
        int index = 0;
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setLong, jobId);
        try {
            MetaDbUtil.update(SQL_DELETE_FILE_BACKFILL_OBJECT_BY_ID, params, connection);
        } catch (Exception e) {
            LOGGER.error(e.getMessage());
            throw GeneralUtil.nestedException(e);
        }
    }

    public void updateBackfillObjectToFileStorage(List<GsiBackfillManager.BackfillObjectRecord> backfillObjectRecords) {
        updateBackfillObject(backfillObjectRecords, SQL_UPDATE_FILE_BACKFILL_PROGRESS);
    }

    private void updateBackfillObject(List<GsiBackfillManager.BackfillObjectRecord> backfillObjectRecords,
                                      String sqlUpdateBackfillProgress) {
        final List<Map<Integer, ParameterContext>> params = backfillObjectRecords.stream()
            .map(bfo -> {
                Map<Integer, ParameterContext> paramMap = new HashMap<>(10);
                int index = 0;
                MetaDbUtil.setParameter(++index, paramMap, ParameterMethod.setString, bfo.getLastValue());
                MetaDbUtil.setParameter(++index, paramMap, ParameterMethod.setLong, bfo.getStatus());
                MetaDbUtil.setParameter(++index, paramMap, ParameterMethod.setLong, bfo.getSuccessRowCount());
                MetaDbUtil.setParameter(++index, paramMap, ParameterMethod.setString, bfo.getEndTime());
                MetaDbUtil.setParameter(++index, paramMap, ParameterMethod.setLong, bfo.getJobId());
                MetaDbUtil.setParameter(++index, paramMap, ParameterMethod.setString, bfo.getPhysicalDb());
                MetaDbUtil.setParameter(++index, paramMap, ParameterMethod.setString, bfo.getPhysicalTable());
                MetaDbUtil.setParameter(++index, paramMap, ParameterMethod.setLong, bfo.getColumnIndex());
                return paramMap;
            })
            .collect(ArrayList::new, ArrayList::add, ArrayList::addAll);

        try {
            MetaDbUtil.update(sqlUpdateBackfillProgress, params, connection);
        } catch (Exception e) {
            LOGGER.error(e.getMessage());
            throw GeneralUtil.nestedException(e);
        }
    }

}
