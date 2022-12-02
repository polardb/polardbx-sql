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

import com.alibaba.polardbx.common.ddl.tablegroup.AutoSplitPolicy;
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

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Created by luoyanxin.
 *
 * @author luoyanxin
 */
public class TableGroupAccessor extends AbstractAccessor {
    private static final Logger LOGGER = LoggerFactory.getLogger(TableGroupAccessor.class);
    private static final String ALL_COLUMNS =
        "`id`,`gmt_create`,`gmt_modified`,`schema_name`,`tg_name`,`locality`, `primary_zone`,`inited`,`meta_version`, `manual_create`, `tg_type`, `auto_split_policy`";

    private static final String ALL_VALUES = "(null,null,now(),?,?,?,?,?,?,?,?,?)";

    private static final String INSERT_IGNORE_TABLE_GROUP =
        "insert ignore into " + GmsSystemTables.TABLE_GROUP + " (" + ALL_COLUMNS + ") VALUES " + ALL_VALUES;

    private static final String UPDATE_GROUP_NAME_BY_ID =
        "update " + GmsSystemTables.TABLE_GROUP + " set tg_name = ? where id = ?";

    private static final String UPDATE_GROUP_LOCALITY_BY_SCHEMA_GROUP_NAME =
        "update " + GmsSystemTables.TABLE_GROUP + " set locality = ? where schema_name = ? and tg_name = ?";

    private static final String GET_TABLE_GROUP_BY_ID =
        "select " + ALL_COLUMNS + " from " + GmsSystemTables.TABLE_GROUP + " where id=?";

    private static final String GET_TABLE_GROUP_BY_SCHEMA_GROUP_NAME =
        "select " + ALL_COLUMNS + " from " + GmsSystemTables.TABLE_GROUP + " where schema_name=? and tg_name=?";

    private static final String GET_ALL_TABLE_GROUP =
        "select " + ALL_COLUMNS + " from " + GmsSystemTables.TABLE_GROUP + " where schema_name=?";

    private static final String GET_ALL_TABLES_CNT_PER_GROUP =
        "select a.tg_id as tg_id,count(1) as cnt from partition_group a inner join table_partitions b on a.id=b.group_id where a.tg_id in ({0}) and b.part_level <> 0 group by tg_id";

    private static final String DELETE_ALL_BY_SCHEMA_NAME =
        "delete from " + GmsSystemTables.TABLE_GROUP + " where schema_name=?";

    private static final String DELETE_ALL_BY_SCHEMA_NAME_ID =
        "delete from " + GmsSystemTables.TABLE_GROUP + " where schema_name=? and id=?";

    private static final String GET_ALL_DISTINCT_SCHEMA_NAME =
        "select distinct schema_name from " + GmsSystemTables.TABLE_GROUP;

    private static final String UPDATE_GROUP_TYPE_BY_ID =
        "update " + GmsSystemTables.TABLE_GROUP + " set tg_type = ? where id = ?";

    private static final String GET_TABLE_GROUP_BY_SCHEMA_GROUP_NAME_FOR_UPDATE =
        "select " + ALL_COLUMNS + " from " + GmsSystemTables.TABLE_GROUP
            + " where schema_name=? and tg_name=? for update";

    private static final String UPDATE_INITED_BY_ID =
        "update " + GmsSystemTables.TABLE_GROUP + " set inited = ? where id = ?";

    private static final String UPDATE_AUTO_SPLIT_POLICY_BY_ID =
        "update " + GmsSystemTables.TABLE_GROUP + " set auto_split_policy = ? where id = ?";

    public List<TableGroupRecord> getTableGroupsByID(Long id) {
        try {

            List<TableGroupRecord> records;
            Map<Integer, ParameterContext> params = new HashMap<>();

            MetaDbUtil.setParameter(1, params, ParameterMethod.setLong, id);
            records =
                MetaDbUtil.query(GET_TABLE_GROUP_BY_ID, params, TableGroupRecord.class, connection);

            return records;
        } catch (Exception e) {
            LOGGER.error("Failed to query the system table " + GmsSystemTables.TABLE_GROUP, e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e,
                e.getMessage());
        }
    }

    public List<TableGroupRecord> getTableGroupsBySchemaAndName(String schemaName, String tableGroupName,
                                                                boolean forUpdate) {
        try {

            List<TableGroupRecord> records;
            Map<Integer, ParameterContext> params = new HashMap<>();

            MetaDbUtil.setParameter(1, params, ParameterMethod.setString, schemaName);
            MetaDbUtil.setParameter(2, params, ParameterMethod.setString, tableGroupName);

            String sql =
                forUpdate ? GET_TABLE_GROUP_BY_SCHEMA_GROUP_NAME_FOR_UPDATE : GET_TABLE_GROUP_BY_SCHEMA_GROUP_NAME;

            records =
                MetaDbUtil.query(sql, params, TableGroupRecord.class, connection);

            return records;
        } catch (Exception e) {
            LOGGER.error("Failed to query the system table " + GmsSystemTables.TABLE_GROUP, e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e,
                e.getMessage());
        }
    }

    public List<TableGroupRecord> getAllTableGroups(String schemaName) {
        try {

            List<TableGroupRecord> records;
            Map<Integer, ParameterContext> params = new HashMap<>();

            MetaDbUtil.setParameter(1, params, ParameterMethod.setString, schemaName);
            records =
                MetaDbUtil.query(GET_ALL_TABLE_GROUP, params, TableGroupRecord.class, connection);

            return records;
        } catch (Exception e) {
            LOGGER.error("Failed to query the system table " + GmsSystemTables.TABLE_GROUP, e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e,
                e.getMessage());
        }
    }

    public Long addNewTableGroup(TableGroupRecord tableGroupRecord) {
        try {
            List<Map<Integer, ParameterContext>> paramsBatch = new ArrayList<>();
            Map<Integer, ParameterContext> params = new HashMap<>();

            int i = 1;
            MetaDbUtil.setParameter(i++, params, ParameterMethod.setString, tableGroupRecord.schema);
            MetaDbUtil.setParameter(i++, params, ParameterMethod.setString, tableGroupRecord.tg_name);
            MetaDbUtil.setParameter(i++, params, ParameterMethod.setString, tableGroupRecord.locality);
            MetaDbUtil.setParameter(i++, params, ParameterMethod.setString, tableGroupRecord.primary_zone);
            MetaDbUtil.setParameter(i++, params, ParameterMethod.setInt, tableGroupRecord.getInited());
            MetaDbUtil.setParameter(i++, params, ParameterMethod.setLong, tableGroupRecord.meta_version);
            MetaDbUtil.setParameter(i++, params, ParameterMethod.setInt, tableGroupRecord.manual_create);
            MetaDbUtil.setParameter(i++, params, ParameterMethod.setInt, tableGroupRecord.tg_type);
            MetaDbUtil.setParameter(i++, params, ParameterMethod.setInt, tableGroupRecord.auto_split_policy);

            paramsBatch.add(params);
            DdlMetaLogUtil.logSql(INSERT_IGNORE_TABLE_GROUP, params);

            return MetaDbUtil.insertAndRetureLastInsertId(INSERT_IGNORE_TABLE_GROUP, paramsBatch, this.connection);
        } catch (Exception e) {
            LOGGER.error("Failed to insert into the system table " + GmsSystemTables.TABLE_GROUP, e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e,
                e.getMessage());
        }
    }

    public void updateTableGroupLocality(String schema, String tableGroupName, String locality) {
        try {

            Map<Integer, ParameterContext> params = new HashMap<>();
            MetaDbUtil.setParameter(1, params, ParameterMethod.setString, locality);
            MetaDbUtil.setParameter(2, params, ParameterMethod.setString, schema);
            MetaDbUtil.setParameter(3, params, ParameterMethod.setString, tableGroupName);

            DdlMetaLogUtil.logSql(UPDATE_GROUP_LOCALITY_BY_SCHEMA_GROUP_NAME, params);

            MetaDbUtil.update(UPDATE_GROUP_LOCALITY_BY_SCHEMA_GROUP_NAME, params, connection);
            return;
        } catch (Exception e) {
            LOGGER.error("Failed to update the system table 'table_group'", e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e,
                e.getMessage());
        }
    }

    public void updateTableGroupName(Long groupId, String groupName) {
        try {

            Map<Integer, ParameterContext> params = new HashMap<>();
            MetaDbUtil.setParameter(1, params, ParameterMethod.setString, groupName);
            MetaDbUtil.setParameter(2, params, ParameterMethod.setLong, groupId);

            DdlMetaLogUtil.logSql(UPDATE_GROUP_NAME_BY_ID, params);

            MetaDbUtil.update(UPDATE_GROUP_NAME_BY_ID, params, connection);
            return;
        } catch (Exception e) {
            LOGGER.error("Failed to query the system table 'table_group'", e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e,
                e.getMessage());
        }
    }

    public Map<Long, Long> getTableCountPerGroup(List<Long> tableGroupIds) {
        try {

            //key:table group id
            //value: physical table count
            Map<Long, Long> tableRefPerGroup = new HashMap<>();
            StringBuilder tableGroupIdsSB = new StringBuilder();
            assert tableGroupIds != null && tableGroupIds.size() > 0;

            Map<Integer, ParameterContext> params = new HashMap<>();
            String sql = expandInCondition(GET_ALL_TABLES_CNT_PER_GROUP, tableGroupIds.size());
            int i = 1;
            for (Long id : tableGroupIds) {
                MetaDbUtil.setParameter(i, params, ParameterMethod.setLong, id);
                i++;
            }
            try (PreparedStatement ps = connection.prepareStatement(sql)) {
                if (params != null && params.size() > 0) {
                    for (ParameterContext param : params.values()) {
                        param.getParameterMethod().setParameter(ps, param.getArgs());
                    }
                }
                ResultSet rs = ps.executeQuery();
                while (rs.next()) {
                    tableRefPerGroup.put(rs.getLong("tg_id"), rs.getLong("cnt"));
                }
            }
            return tableRefPerGroup;
        } catch (Exception e) {
            LOGGER.error("Failed to query the system table " + GmsSystemTables.TABLE_GROUP, e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e,
                e.getMessage());
        }
    }

    public void setAutoCommit(boolean val) {
        try {
            this.connection.setAutoCommit(val);
        } catch (SQLException e) {
            LOGGER.error("Failed to set the connection auto commit property", e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_GENERIC, e,
                e.getMessage());
        }
    }

    public void deleteTableGroupsBySchema(String schemaName) {
        try {

            Map<Integer, ParameterContext> params = new HashMap<>();
            MetaDbUtil.setParameter(1, params, ParameterMethod.setString, schemaName);

            DdlMetaLogUtil.logSql(DELETE_ALL_BY_SCHEMA_NAME, params);

            MetaDbUtil.update(DELETE_ALL_BY_SCHEMA_NAME, params, connection);
            return;
        } catch (Exception e) {
            LOGGER.error("Failed to query the system table 'table_group'", e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e,
                e.getMessage());
        }
    }

    public void deleteTableGroupsById(String schemaName, Long tableGroupId) {
        try {

            Map<Integer, ParameterContext> params = new HashMap<>();
            MetaDbUtil.setParameter(1, params, ParameterMethod.setString, schemaName);
            MetaDbUtil.setParameter(2, params, ParameterMethod.setLong, tableGroupId);

            DdlMetaLogUtil.logSql(DELETE_ALL_BY_SCHEMA_NAME_ID, params);

            MetaDbUtil.update(DELETE_ALL_BY_SCHEMA_NAME_ID, params, connection);
            return;
        } catch (Exception e) {
            LOGGER.error("Failed to query the system table 'table_group'", e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e,
                e.getMessage());
        }
    }

    private String expandInCondition(String tpl, int statusCount) {
        return MessageFormat.format(tpl, IntStream.range(0, statusCount).mapToObj(i -> "?")
            .collect(Collectors.joining(", ")));
    }

    public List<String> getDistinctSchemaNames() {
        try {
            List<String> schemaNames = new ArrayList<>();
            try (PreparedStatement ps = connection.prepareStatement(GET_ALL_DISTINCT_SCHEMA_NAME)) {
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        schemaNames.add(rs.getString("schema_name"));
                    }
                }
            }
            return schemaNames;
        } catch (Exception e) {
            LOGGER.error("Failed to query the system table " + GmsSystemTables.TABLE_GROUP, e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e,
                e.getMessage());
        }
    }

    public void updateTableGroupType(Long groupId, int tableGroupType) {
        try {

            Map<Integer, ParameterContext> params = new HashMap<>();
            MetaDbUtil.setParameter(1, params, ParameterMethod.setInt, tableGroupType);
            MetaDbUtil.setParameter(2, params, ParameterMethod.setLong, groupId);

            DdlMetaLogUtil.logSql(UPDATE_GROUP_TYPE_BY_ID, params);

            MetaDbUtil.update(UPDATE_GROUP_TYPE_BY_ID, params, connection);
            return;
        } catch (Exception e) {
            LOGGER.error("Failed to query the system table 'table_group'", e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e,
                e.getMessage());
        }
    }

    public void updateInitedById(Long groupId, int inited) {
        try {

            Map<Integer, ParameterContext> params = new HashMap<>();
            MetaDbUtil.setParameter(1, params, ParameterMethod.setInt, inited);
            MetaDbUtil.setParameter(2, params, ParameterMethod.setLong, groupId);

            DdlMetaLogUtil.logSql(UPDATE_INITED_BY_ID, params);

            MetaDbUtil.update(UPDATE_INITED_BY_ID, params, connection);
            return;
        } catch (Exception e) {
            LOGGER.error("Failed to query the system table 'table_group'", e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e,
                e.getMessage());
        }
    }

    public void updateAutoSplitPolicyById(Long groupId, AutoSplitPolicy policy) {
        try {

            Map<Integer, ParameterContext> params = new HashMap<>();
            MetaDbUtil.setParameter(1, params, ParameterMethod.setInt, policy.getValue());
            MetaDbUtil.setParameter(2, params, ParameterMethod.setLong, groupId);

            DdlMetaLogUtil.logSql(UPDATE_AUTO_SPLIT_POLICY_BY_ID, params);

            MetaDbUtil.update(UPDATE_AUTO_SPLIT_POLICY_BY_ID, params, connection);
            return;
        } catch (Exception e) {
            LOGGER.error("Failed to query the system table 'table_group'", e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e,
                e.getMessage());
        }
    }
}
