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

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by luoyanxin.
 *
 * @author luoyanxin
 */
public class JoinGroupInfoAccessor extends AbstractAccessor {
    private static final Logger LOGGER = LoggerFactory.getLogger(JoinGroupInfoAccessor.class);
    private static final String ALL_COLUMNS =
        "`id`,`gmt_create`,`gmt_modified`,`table_schema`,`join_group_name`,`locality`";

    private static final String ALL_VALUES = "(null,null,now(),?,?,?)";

    private static final String INSERT_IGNORE_JOIN_GROUP =
        "insert ignore into " + GmsSystemTables.JOIN_GROUP_INFO + " (" + ALL_COLUMNS + ") VALUES " + ALL_VALUES;

    private static final String INSERT_JOIN_GROUP =
        "insert into " + GmsSystemTables.JOIN_GROUP_INFO + " (" + ALL_COLUMNS + ") VALUES " + ALL_VALUES;

    private static final String FOR_UPDATE = " for update";
    private static final String GET_JOIN_GROUP_BY_SCHEMA_JOINGROUP_NAME =
        "select " + ALL_COLUMNS + " from " + GmsSystemTables.JOIN_GROUP_INFO
            + " where table_schema=? and join_group_name=?";

    private static final String GET_JOIN_GROUP_BY_SCHEMA_JOINGROUP_NAME_FOR_UPDATE =
        GET_JOIN_GROUP_BY_SCHEMA_JOINGROUP_NAME + FOR_UPDATE;

    private static final String DELETE_JOIN_GROUP_BY_SCHEMA_JOINGROUP_NAME =
        "delete from " + GmsSystemTables.JOIN_GROUP_INFO + " where table_schema=? and join_group_name=?";

    private static final String DELETE_JOIN_GROUP_BY_SCHEMA =
        "delete from " + GmsSystemTables.JOIN_GROUP_INFO + " where table_schema=?";

    private static final String GET_JOIN_GROUP_BY_SCHEMA_ID =
        "select " + ALL_COLUMNS + " from " + GmsSystemTables.JOIN_GROUP_INFO
            + " where table_schema=? and id=?";

    private static final String GET_ALL_JOIN_GROUP_INFO =
        "select " + ALL_COLUMNS + " from " + GmsSystemTables.JOIN_GROUP_INFO + " order by table_schema";

    private static final String GET_ALL_DISTINCT_SCHEMA_NAME =
        "select distinct table_schema from " + GmsSystemTables.JOIN_GROUP_INFO;

    public void addJoinGroup(JoinGroupInfoRecord joinGroupInfoRecord, boolean ifNotExists) {
        try {
            List<Map<Integer, ParameterContext>> paramsBatch = new ArrayList<>();
            Map<Integer, ParameterContext> params = new HashMap<>();

            int i = 1;
            MetaDbUtil.setParameter(i++, params, ParameterMethod.setString, joinGroupInfoRecord.tableSchema);
            MetaDbUtil.setParameter(i++, params, ParameterMethod.setString, joinGroupInfoRecord.joinGroupName);
            MetaDbUtil.setParameter(i++, params, ParameterMethod.setString, joinGroupInfoRecord.locality);

            paramsBatch.add(params);

            String sql = ifNotExists ? INSERT_IGNORE_JOIN_GROUP : INSERT_JOIN_GROUP;
            DdlMetaLogUtil.logSql(sql, params);

            MetaDbUtil.insert(sql, paramsBatch, this.connection);

        } catch (Exception e) {
            LOGGER.error("Failed to insert into the system table " + GmsSystemTables.JOIN_GROUP_INFO, e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e,
                e.getMessage());
        }
    }

    public JoinGroupInfoRecord getJoinGroupInfoByName(String tableSchema, String joinGroupName,
                                                      boolean forUpdate) {
        try {

            List<JoinGroupInfoRecord> records;
            Map<Integer, ParameterContext> params = new HashMap<>();

            MetaDbUtil.setParameter(1, params, ParameterMethod.setString, tableSchema);
            MetaDbUtil.setParameter(2, params, ParameterMethod.setString, joinGroupName);

            String sql =
                forUpdate ? GET_JOIN_GROUP_BY_SCHEMA_JOINGROUP_NAME_FOR_UPDATE :
                    GET_JOIN_GROUP_BY_SCHEMA_JOINGROUP_NAME;

            records =
                MetaDbUtil.query(sql, params, JoinGroupInfoRecord.class, connection);
            if (GeneralUtil.isNotEmpty(records)) {
                assert records.size() == 1;
                return records.get(0);
            } else {
                return null;
            }
        } catch (Exception e) {
            LOGGER.error("Failed to query the system table " + GmsSystemTables.JOIN_GROUP_INFO, e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e,
                e.getMessage());
        }
    }

    public JoinGroupInfoRecord getJoinGroupInfoById(String tableSchema, Long id) {
        try {

            List<JoinGroupInfoRecord> records;
            Map<Integer, ParameterContext> params = new HashMap<>();

            MetaDbUtil.setParameter(1, params, ParameterMethod.setString, tableSchema);
            MetaDbUtil.setParameter(2, params, ParameterMethod.setLong, id);

            records =
                MetaDbUtil.query(GET_JOIN_GROUP_BY_SCHEMA_ID, params, JoinGroupInfoRecord.class, connection);
            if (GeneralUtil.isNotEmpty(records)) {
                return records.get(0);
            } else {
                return null;
            }
        } catch (Exception e) {
            LOGGER.error("Failed to query the system table " + GmsSystemTables.JOIN_GROUP_INFO, e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e,
                e.getMessage());
        }
    }

    public void deleteJoinGroupInfoByName(String tableSchema, String joinGroupName) {
        try {

            Map<Integer, ParameterContext> params = new HashMap<>();
            MetaDbUtil.setParameter(1, params, ParameterMethod.setString, tableSchema);
            MetaDbUtil.setParameter(2, params, ParameterMethod.setString, joinGroupName);

            DdlMetaLogUtil.logSql(DELETE_JOIN_GROUP_BY_SCHEMA_JOINGROUP_NAME, params);

            MetaDbUtil.update(DELETE_JOIN_GROUP_BY_SCHEMA_JOINGROUP_NAME, params, connection);
            return;
        } catch (Exception e) {
            LOGGER.error("Failed to query the system table " + GmsSystemTables.JOIN_GROUP_INFO, e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e,
                e.getMessage());
        }
    }

    public void deleteJoinGroupInfoBySchema(String tableSchema) {
        try {

            Map<Integer, ParameterContext> params = new HashMap<>();
            MetaDbUtil.setParameter(1, params, ParameterMethod.setString, tableSchema);

            DdlMetaLogUtil.logSql(DELETE_JOIN_GROUP_BY_SCHEMA, params);

            MetaDbUtil.update(DELETE_JOIN_GROUP_BY_SCHEMA, params, connection);
            return;
        } catch (Exception e) {
            LOGGER.error("Failed to query the system table " + GmsSystemTables.JOIN_GROUP_INFO, e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e,
                e.getMessage());
        }
    }

    public List<JoinGroupInfoRecord> getJoinGroupInfos() {
        try {
            Map<Integer, ParameterContext> params = new HashMap<>();

            return MetaDbUtil.query(GET_ALL_JOIN_GROUP_INFO, params, JoinGroupInfoRecord.class, connection);

        } catch (Exception e) {
            LOGGER.error("Failed to query the system table " + GmsSystemTables.JOIN_GROUP_INFO, e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e,
                e.getMessage());
        }
    }

    public List<String> getDistinctSchemaNames() {
        try {
            List<String> schemaNames = new ArrayList<>();
            try (PreparedStatement ps = connection.prepareStatement(GET_ALL_DISTINCT_SCHEMA_NAME)) {
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        schemaNames.add(rs.getString("table_schema"));
                    }
                }
            }
            return schemaNames;
        } catch (Exception e) {
            LOGGER.error("Failed to query the system table " + GmsSystemTables.JOIN_GROUP_INFO, e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e,
                e.getMessage());
        }
    }
}
