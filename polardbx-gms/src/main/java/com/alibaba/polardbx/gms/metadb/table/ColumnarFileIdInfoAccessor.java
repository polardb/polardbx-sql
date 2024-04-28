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

package com.alibaba.polardbx.gms.metadb.table;

import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.ParameterMethod;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.gms.metadb.accessor.AbstractAccessor;
import com.alibaba.polardbx.gms.util.DdlMetaLogUtil;
import com.alibaba.polardbx.gms.util.MetaDbUtil;

import java.util.List;
import java.util.Map;

import static com.alibaba.polardbx.gms.metadb.GmsSystemTables.COLUMNAR_FILE_ID_INFO;

/**
 * @author wenki
 */
public class ColumnarFileIdInfoAccessor extends AbstractAccessor {
    private static final Logger LOGGER = LoggerFactory.getLogger("oss");
    private static final String COLUMNAR_FILE_ID_INFO_TABLE = wrap(COLUMNAR_FILE_ID_INFO);

    private static final String UPDATE_MAX_ID = "update " + COLUMNAR_FILE_ID_INFO_TABLE
        + " set `max_id` = ?, `version` = `version` + 1"
        + " where `id` = ? and `max_id` = ? and `version` = ? and `type` = ?";

    private static final String QUERY_BY_TYPE = "select * from " + COLUMNAR_FILE_ID_INFO_TABLE
        + " where `type` = ?";

    private static final String DELETE_BY_TYPE = "delete from " + COLUMNAR_FILE_ID_INFO_TABLE
        + " where `type` = ?";

    private static final String INSERT_ILE_ID_INFO_RECORD = "insert into " + COLUMNAR_FILE_ID_INFO_TABLE
        + " (`type`, `max_id`, `step`, `version`) "
        + "values (?, ?, ?, ?) ";

    public List<ColumnarFileIdInfoRecord> query(String type) {
        try {
            Map<Integer, ParameterContext> params = MetaDbUtil.buildParameters(
                ParameterMethod.setObject1,
                new Object[] {type});

            return MetaDbUtil.query(QUERY_BY_TYPE, params, ColumnarFileIdInfoRecord.class, connection);
        } catch (Exception e) {
            throw GeneralUtil.nestedException(e);
        }
    }

    public int update(Long id, Long newMaxId, Long oldMaxId, Long version, String type) {
        Map<Integer, ParameterContext> params = MetaDbUtil.buildParameters(
            ParameterMethod.setObject1,
            new Object[] {newMaxId, id, oldMaxId, version, type});
        try {
            DdlMetaLogUtil.logSql(UPDATE_MAX_ID, params);
            return MetaDbUtil.update(UPDATE_MAX_ID, params, connection);
        } catch (Exception e) {
            throw GeneralUtil.nestedException(e);
        }
    }

    public int delete(String schemaName, String tableName) {
        String type = schemaName + "/" + tableName;
        Map<Integer, ParameterContext> params = MetaDbUtil.buildParameters(
            ParameterMethod.setObject1,
            new Object[] {type});
        try {
            DdlMetaLogUtil.logSql(DELETE_BY_TYPE, params);
            return MetaDbUtil.delete(DELETE_BY_TYPE, params, connection);
        } catch (Exception e) {
            throw GeneralUtil.nestedException(e);
        }
    }

    public int insert(String schemaName, String tableName, Long maxId, int step, Long version) {
        String type = schemaName + "/" + tableName;
        Map<Integer, ParameterContext> params = MetaDbUtil.buildParameters(
            ParameterMethod.setObject1,
            new Object[] {type, maxId, step, version});
        try {
            DdlMetaLogUtil.logSql(INSERT_ILE_ID_INFO_RECORD, params);
            return MetaDbUtil.insert(INSERT_ILE_ID_INFO_RECORD, params, connection);
        } catch (Exception e) {
            throw GeneralUtil.nestedException(e);
        }
    }
}
