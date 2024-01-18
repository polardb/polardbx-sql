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

public class FileStorageInfoAccessor extends AbstractAccessor {
    private static final Logger LOGGER = LoggerFactory.getLogger("oss");

    private static final String TABLE_NAME = wrap(GmsSystemTables.FILE_STORAGE_INFO);

    private static final String QUERY_MAX_PRIORITY = "select * from "
        + TABLE_NAME + " where engine = ? and status != 0 order by priority desc limit 1";

    private static final String QUERY_PRIORITY_DESC = "select * from "
            + TABLE_NAME + " where engine = ? and status != 0 order by priority desc";

    private static final String INSERT_SQL = "insert ignore into " + TABLE_NAME
        + " (`inst_id`,`engine`,`external_endpoint`,`internal_classic_endpoint`,`internal_vpc_endpoint`,`file_uri`,`file_system_conf`,`access_key_id`,`access_key_secret`,`priority`,`region_id`,`azone_id`, `cache_policy`, `delete_policy`, `status`) values "
        + " (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";

    private static final String DELETE_ENGINE = "delete from "
            + TABLE_NAME + " where engine = ?";

    public FileStorageInfoRecord queryLatest(Engine engine) {
        Map<Integer, ParameterContext> params = new HashMap<>();
        MetaDbUtil.setParameter(1, params, ParameterMethod.setString, engine.name());

        try {
            DdlMetaLogUtil.logSql(QUERY_MAX_PRIORITY, params);
            List<FileStorageInfoRecord> recordList =
                MetaDbUtil.query(QUERY_MAX_PRIORITY, params, FileStorageInfoRecord.class, connection);
            return recordList.get(0);
        } catch (Exception e) {
            throw GeneralUtil.nestedException(e);
        }
    }

    public List<FileStorageInfoRecord> query(Engine engine) {
        Map<Integer, ParameterContext> params = new HashMap<>();
        MetaDbUtil.setParameter(1, params, ParameterMethod.setString, engine.name());

        try {
            DdlMetaLogUtil.logSql(QUERY_PRIORITY_DESC, params);
            List<FileStorageInfoRecord> recordList =
                    MetaDbUtil.query(QUERY_PRIORITY_DESC, params, FileStorageInfoRecord.class, connection);
            return recordList;
        } catch (Exception e) {
            throw GeneralUtil.nestedException(e);
        }
    }

    public int delete(Engine engine) {
        Map<Integer, ParameterContext> params = new HashMap<>();
        MetaDbUtil.setParameter(1, params, ParameterMethod.setString, engine.name());

        try {
            DdlMetaLogUtil.logSql(DELETE_ENGINE, params);
            return MetaDbUtil.delete(DELETE_ENGINE, params, connection);
        } catch (Exception e) {
            throw GeneralUtil.nestedException(e);
        }
    }

    public int[] insertIgnore(List<FileStorageInfoRecord> records) {
        List<Map<Integer, ParameterContext>> paramsBatch = new ArrayList<>(records.size());
        for (FileStorageInfoRecord record : records) {
            paramsBatch.add(record.buildInsertParams());
        }
        try {
            DdlMetaLogUtil.logSql(INSERT_SQL, paramsBatch);
            return MetaDbUtil.insert(INSERT_SQL, paramsBatch, connection);
        } catch (SQLException e) {
            LOGGER.error("Failed to insert a batch of new records into " + TABLE_NAME, e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "batch insert into",
                TABLE_NAME,
                e.getMessage());
        }
    }
}
