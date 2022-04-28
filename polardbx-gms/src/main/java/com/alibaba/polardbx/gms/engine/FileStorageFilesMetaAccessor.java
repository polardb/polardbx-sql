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

package com.alibaba.polardbx.gms.engine;

import com.alibaba.polardbx.common.Engine;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.ParameterMethod;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.gms.metadb.GmsSystemTables;
import com.alibaba.polardbx.gms.metadb.accessor.AbstractAccessor;
import com.alibaba.polardbx.gms.util.DdlMetaLogUtil;
import com.alibaba.polardbx.gms.util.MetaDbUtil;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class FileStorageFilesMetaAccessor extends AbstractAccessor {
    private static final Logger LOGGER = LoggerFactory.getLogger("oss");

    private static final String TABLE_NAME = wrap(GmsSystemTables.FILE_STORAGE_FILES_META);

    private static final String REPLACE_SQL = "replace into " + TABLE_NAME
        + " (`file_name`,`engine`,`commit_ts`,`remove_ts`) values (?,?,?,?)";

    private static final String QUERY_BY_ENGINE = "select * from " + TABLE_NAME + " where engine = ? ";

    private static final String QUERY_TABLE_NEED_TO_PURGE = "select * from " + TABLE_NAME + " where engine = ? and remove_ts < ?";

    private static final String QUERY_BY_FILE_NAME = "select * from " + TABLE_NAME + " where file_name = ? ";

    private static final String DELETE_BY_ENGINE = "delete from " + TABLE_NAME + " where engine = ?";

    private static final String DELETE_BY_FILE_NAME = "delete from " + TABLE_NAME + " where file_name = ?";

    private static final String UPDATE_FILE_REMOVE_TS = "update " + TABLE_NAME + " set remove_ts = ? where id = ?";

    private static final String UPDATE_FILE_COMMIT_TS = "update " + TABLE_NAME + " set commit_ts = ? where id = ?";

    public List<FileStorageFilesMetaRecord> query(Engine engine) {
        Map<Integer, ParameterContext> params = new HashMap<>();
        MetaDbUtil.setParameter(1, params, ParameterMethod.setString, engine.name());

        try {
            DdlMetaLogUtil.logSql(QUERY_BY_ENGINE, params);
            List<FileStorageFilesMetaRecord> recordList =
                MetaDbUtil.query(QUERY_BY_ENGINE, params, FileStorageFilesMetaRecord.class, connection);
            return recordList;
        } catch (Exception e) {
            throw GeneralUtil.nestedException(e);
        }
    }

    public List<FileStorageFilesMetaRecord> queryTableNeedToPurge(Engine engine, long removeTs) {
        Map<Integer, ParameterContext> params = new HashMap<>();
        MetaDbUtil.setParameter(1, params, ParameterMethod.setString, engine.name());
        MetaDbUtil.setParameter(2, params, ParameterMethod.setLong, removeTs);

        try {
            DdlMetaLogUtil.logSql(QUERY_TABLE_NEED_TO_PURGE, params);
            List<FileStorageFilesMetaRecord> recordList =
                MetaDbUtil.query(QUERY_TABLE_NEED_TO_PURGE, params, FileStorageFilesMetaRecord.class, connection);
            return recordList;
        } catch (Exception e) {
            throw GeneralUtil.nestedException(e);
        }
    }

    public List<FileStorageFilesMetaRecord> query(String fileName) {
        Map<Integer, ParameterContext> params = new HashMap<>();
        MetaDbUtil.setParameter(1, params, ParameterMethod.setString, fileName);

        try {
            DdlMetaLogUtil.logSql(QUERY_BY_FILE_NAME, params);
            List<FileStorageFilesMetaRecord> recordList =
                MetaDbUtil.query(QUERY_BY_FILE_NAME, params, FileStorageFilesMetaRecord.class, connection);
            return recordList;
        } catch (Exception e) {
            throw GeneralUtil.nestedException(e);
        }
    }

    public int delete(Engine engine) {
        Map<Integer, ParameterContext> params = new HashMap<>();
        MetaDbUtil.setParameter(1, params, ParameterMethod.setString, engine.name());

        try {
            DdlMetaLogUtil.logSql(DELETE_BY_ENGINE, params);
            return MetaDbUtil.delete(DELETE_BY_ENGINE, params, connection);
        } catch (Exception e) {
            throw GeneralUtil.nestedException(e);
        }
    }

    public int deleteByFileName(String fileName) {
        Map<Integer, ParameterContext> params = new HashMap<>();
        MetaDbUtil.setParameter(1, params, ParameterMethod.setString, fileName);

        try {
            DdlMetaLogUtil.logSql(DELETE_BY_FILE_NAME, params);
            return MetaDbUtil.delete(DELETE_BY_FILE_NAME, params, connection);
        } catch (Exception e) {
            throw GeneralUtil.nestedException(e);
        }
    }

    public int[] replace(List<FileStorageFilesMetaRecord> records) {
        List<Map<Integer, ParameterContext>> paramsBatch = new ArrayList<>(records.size());
        for (FileStorageFilesMetaRecord record : records) {
            paramsBatch.add(record.buildInsertParams());
        }
        try {
            DdlMetaLogUtil.logSql(REPLACE_SQL, paramsBatch);
            return MetaDbUtil.insert(REPLACE_SQL, paramsBatch, connection);
        } catch (SQLException e) {
            LOGGER.error("Failed to insert a batch of new records into " + TABLE_NAME, e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "batch insert into",
                TABLE_NAME,
                e.getMessage());
        }
    }

    public void updateRemoveTs(long primaryKey, Long removeTs) {
        Map<Integer, ParameterContext> params = new HashMap<>(4);
        if (removeTs == null) {
            MetaDbUtil.setParameter(1, params, ParameterMethod.setNull1, null);
        } else {
            MetaDbUtil.setParameter(1, params, ParameterMethod.setLong, removeTs);
        }
        MetaDbUtil.setParameter(2, params, ParameterMethod.setLong, primaryKey);
        try {
            DdlMetaLogUtil.logSql(UPDATE_FILE_REMOVE_TS, params);
            MetaDbUtil.update(UPDATE_FILE_REMOVE_TS, params, connection);
        } catch (Exception e) {
            throw GeneralUtil.nestedException(e);
        }
    }

    public void updateCommitTs(long primaryKey, Long commitTs) {
        Map<Integer, ParameterContext> params = new HashMap<>(4);
        if (commitTs == null) {
            MetaDbUtil.setParameter(1, params, ParameterMethod.setNull1, null);
        } else {
            MetaDbUtil.setParameter(1, params, ParameterMethod.setLong, commitTs);
        }
        MetaDbUtil.setParameter(2, params, ParameterMethod.setLong, primaryKey);
        try {
            DdlMetaLogUtil.logSql(UPDATE_FILE_COMMIT_TS, params);
            MetaDbUtil.update(UPDATE_FILE_COMMIT_TS, params, connection);
        } catch (Exception e) {
            throw GeneralUtil.nestedException(e);
        }
    }
}

