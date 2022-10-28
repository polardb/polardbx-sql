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
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.gms.metadb.GmsSystemTables;
import com.alibaba.polardbx.gms.metadb.accessor.AbstractAccessor;
import com.alibaba.polardbx.gms.util.DdlMetaLogUtil;
import com.alibaba.polardbx.gms.util.MetaDbUtil;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by luoyanxin.
 *
 * @author luoyanxin
 */
public class JoinGroupTableDetailAccessor extends AbstractAccessor {
    private static final Logger LOGGER = LoggerFactory.getLogger(JoinGroupTableDetailAccessor.class);

    private static final String ALL_COLUMNS =
        "`id`,`gmt_create`,`gmt_modified`,`table_schema`,`join_group_id`,`table_Name`";

    private static final String ALL_VALUES = " (null,null,now(),?,?,?)";

    private static final String INSERT_JOINGROUP_TABLE_DETAIL = "insert into " + GmsSystemTables.JOIN_GROUP_TABLE_DETAIL
        + "(" + ALL_COLUMNS + " ) VALUES " + ALL_VALUES;
    private static final String INSERT_IGNORE_JOINGROUP_TABLE_DETAIL =
        "insert ignore into " + GmsSystemTables.JOIN_GROUP_TABLE_DETAIL
            + "(" + ALL_COLUMNS + " ) VALUES " + ALL_VALUES;
    private static final String FOR_UPDATE = " for update";
    private static final String GET_JOIN_GROUP_DETAIL_BY_SCHEMA_JOINGROUP_ID =
        "select " + ALL_COLUMNS + " from " + GmsSystemTables.JOIN_GROUP_TABLE_DETAIL
            + " where table_schema=? and join_group_id=?";

    private static final String GET_JOIN_GROUP_DETAIL_BY_SCHEMA_JOINGROUP_ID_FOR_UPDATE =
        GET_JOIN_GROUP_DETAIL_BY_SCHEMA_JOINGROUP_ID + FOR_UPDATE;

    private static final String GET_JOIN_GROUP_DETAIL_BY_SCHEMA_JOINGROUP_ID_TABLE =
        "select " + ALL_COLUMNS + " from " + GmsSystemTables.JOIN_GROUP_TABLE_DETAIL
            + " where table_schema=? and join_group_id=? and table_name=?";

    private static final String GET_JOIN_GROUP_DETAIL_BY_SCHEMA_TABLE =
        "select " + ALL_COLUMNS + " from " + GmsSystemTables.JOIN_GROUP_TABLE_DETAIL
            + " where table_schema=? and table_name=?";

    private static final String DELETE_JOIN_GROUP_TABLE_DETAIL_BY_SCHEMA =
        "delete from " + GmsSystemTables.JOIN_GROUP_TABLE_DETAIL + " where table_schema=?";

    private static final String DELETE_JOIN_GROUP_TABLE_DETAIL_BY_SCHEMA_JOINID_TABLE =
        "delete from " + GmsSystemTables.JOIN_GROUP_TABLE_DETAIL
            + " where table_schema=? and join_group_id=? and table_Name=? ";

    private static final String DELETE_JOIN_GROUP_TABLE_DETAIL_BY_SCHEMA_TABLE =
        "delete from " + GmsSystemTables.JOIN_GROUP_TABLE_DETAIL
            + " where table_schema=? and table_Name=? ";

    private static final String UPDATE_JOIN_GROUP_ID =
        "update " + GmsSystemTables.JOIN_GROUP_TABLE_DETAIL + " set join_group_id=? where "
            + " id = ?";

    public void insertJoingroupTableDetail(String tableSchema, Long joinGroupId, String tableName) {
        try {

            Map<Integer, ParameterContext> params = new HashMap<>();
            MetaDbUtil.setParameter(1, params, ParameterMethod.setString, tableSchema);
            MetaDbUtil.setParameter(2, params, ParameterMethod.setLong, joinGroupId);
            MetaDbUtil.setParameter(3, params, ParameterMethod.setString, tableName);

            DdlMetaLogUtil.logSql(INSERT_JOINGROUP_TABLE_DETAIL, params);

            MetaDbUtil.update(INSERT_JOINGROUP_TABLE_DETAIL, params, connection);
            return;
        } catch (Exception e) {
            LOGGER.error("Failed to query the system table " + GmsSystemTables.JOIN_GROUP_TABLE_DETAIL, e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e,
                e.getMessage());
        }
    }

    public void insertIgnoreJoingroupTableDetail(String tableSchema, Long joinGroupId, String tableName) {
        try {

            Map<Integer, ParameterContext> params = new HashMap<>();
            MetaDbUtil.setParameter(1, params, ParameterMethod.setString, tableSchema);
            MetaDbUtil.setParameter(2, params, ParameterMethod.setLong, joinGroupId);
            MetaDbUtil.setParameter(3, params, ParameterMethod.setString, tableName);

            DdlMetaLogUtil.logSql(INSERT_IGNORE_JOINGROUP_TABLE_DETAIL, params);

            MetaDbUtil.update(INSERT_IGNORE_JOINGROUP_TABLE_DETAIL, params, connection);
            return;
        } catch (Exception e) {
            LOGGER.error("Failed to query the system table " + GmsSystemTables.JOIN_GROUP_TABLE_DETAIL, e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e,
                e.getMessage());
        }
    }

    public List<JoinGroupTableDetailRecord> getJoinGroupDetailBySchemaJoinGroupId(String tableSchema, long joinGroupId,
                                                                                  boolean forUpdate) {
        try {

            List<JoinGroupTableDetailRecord> records;
            Map<Integer, ParameterContext> params = new HashMap<>();

            MetaDbUtil.setParameter(1, params, ParameterMethod.setString, tableSchema);
            MetaDbUtil.setParameter(2, params, ParameterMethod.setLong, joinGroupId);

            String sql =
                forUpdate ? GET_JOIN_GROUP_DETAIL_BY_SCHEMA_JOINGROUP_ID_FOR_UPDATE :
                    GET_JOIN_GROUP_DETAIL_BY_SCHEMA_JOINGROUP_ID;

            records =
                MetaDbUtil.query(sql, params, JoinGroupTableDetailRecord.class, connection);

            return records;
        } catch (Exception e) {
            LOGGER.error("Failed to query the system table " + GmsSystemTables.JOIN_GROUP_TABLE_DETAIL, e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e,
                e.getMessage());
        }
    }

    public JoinGroupTableDetailRecord getJoinGroupDetailBySchemaTableName(String tableSchema,
                                                                          String tableName) {
        try {

            List<JoinGroupTableDetailRecord> records;
            Map<Integer, ParameterContext> params = new HashMap<>();

            MetaDbUtil.setParameter(1, params, ParameterMethod.setString, tableSchema);
            MetaDbUtil.setParameter(2, params, ParameterMethod.setString, tableName);

            records =
                MetaDbUtil.query(GET_JOIN_GROUP_DETAIL_BY_SCHEMA_TABLE, params,
                    JoinGroupTableDetailRecord.class, connection);
            if (GeneralUtil.isNotEmpty(records)) {
                return records.get(0);
            } else {
                return null;
            }
        } catch (Exception e) {
            LOGGER.error("Failed to query the system table " + GmsSystemTables.JOIN_GROUP_TABLE_DETAIL, e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e,
                e.getMessage());
        }
    }

    public void deleteJoinGroupTableDetailBySchema(String tableSchema) {
        try {

            Map<Integer, ParameterContext> params = new HashMap<>();
            MetaDbUtil.setParameter(1, params, ParameterMethod.setString, tableSchema);

            DdlMetaLogUtil.logSql(DELETE_JOIN_GROUP_TABLE_DETAIL_BY_SCHEMA, params);

            MetaDbUtil.update(DELETE_JOIN_GROUP_TABLE_DETAIL_BY_SCHEMA, params, connection);
            return;
        } catch (Exception e) {
            LOGGER.error("Failed to query the system table " + GmsSystemTables.JOIN_GROUP_TABLE_DETAIL, e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e,
                e.getMessage());
        }
    }

    public void deleteJoinGroupTableDetailBySchemaJoinIdTable(String tableSchema, Long joinGroupId, String tableName) {
        try {
            Map<Integer, ParameterContext> params = new HashMap<>();
            MetaDbUtil.setParameter(1, params, ParameterMethod.setString, tableSchema);
            MetaDbUtil.setParameter(2, params, ParameterMethod.setLong, joinGroupId);
            MetaDbUtil.setParameter(3, params, ParameterMethod.setString, tableName);

            DdlMetaLogUtil.logSql(DELETE_JOIN_GROUP_TABLE_DETAIL_BY_SCHEMA_JOINID_TABLE, params);

            MetaDbUtil.update(DELETE_JOIN_GROUP_TABLE_DETAIL_BY_SCHEMA_JOINID_TABLE, params, connection);
            return;
        } catch (Exception e) {
            LOGGER.error("Failed to query the system table " + GmsSystemTables.JOIN_GROUP_TABLE_DETAIL, e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e,
                e.getMessage());
        }
    }

    public void deleteJoinGroupTableDetailBySchemaTable(String tableSchema, String tableName) {
        try {
            Map<Integer, ParameterContext> params = new HashMap<>();
            MetaDbUtil.setParameter(1, params, ParameterMethod.setString, tableSchema);
            MetaDbUtil.setParameter(2, params, ParameterMethod.setString, tableName);

            DdlMetaLogUtil.logSql(DELETE_JOIN_GROUP_TABLE_DETAIL_BY_SCHEMA_TABLE, params);

            MetaDbUtil.update(DELETE_JOIN_GROUP_TABLE_DETAIL_BY_SCHEMA_TABLE, params, connection);
            return;
        } catch (Exception e) {
            LOGGER.error("Failed to query the system table " + GmsSystemTables.JOIN_GROUP_TABLE_DETAIL, e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e,
                e.getMessage());
        }
    }

    public void updateJoinGroupId(Long newJoinGroupId, Long id) {
        try {

            Map<Integer, ParameterContext> params = new HashMap<>();
            MetaDbUtil.setParameter(1, params, ParameterMethod.setLong, newJoinGroupId);
            MetaDbUtil.setParameter(2, params, ParameterMethod.setLong, id);

            DdlMetaLogUtil.logSql(UPDATE_JOIN_GROUP_ID, params);

            MetaDbUtil.update(UPDATE_JOIN_GROUP_ID, params, connection);
            return;
        } catch (Exception e) {
            LOGGER.error("Failed to query the system table " + GmsSystemTables.JOIN_GROUP_TABLE_DETAIL, e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e,
                e.getMessage());
        }
    }
}
