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

package com.alibaba.polardbx.gms.metadb.schema;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.ParameterMethod;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.gms.metadb.record.CountRecord;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.gms.metadb.GmsSystemTables;
import com.alibaba.polardbx.gms.metadb.accessor.AbstractAccessor;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SchemaChangeAccessor extends AbstractAccessor {

    private static final Logger LOGGER = LoggerFactory.getLogger(SchemaChangeAccessor.class);

    private static final String SCHEMA_CHANGE_TABLE = wrap(GmsSystemTables.SCHEMA_CHANGE);

    private static final String SCHEMA_CHANGE_LOCK = "__polardbx_schema_change_lock__";
    private static final int SCHEMA_CHANGE_LOCK_TIMEOUT = 10;

    private static final String CREATE_SCHEMA_CHANGE_TABLE =
        "create table if not exists " + SCHEMA_CHANGE_TABLE + "("
            + "  `id` bigint not null auto_increment,"
            + "  `table_name` varchar(64) not null,"
            + "  `version` int unsigned not null,"
            + "  `gmt_created` timestamp not null default current_timestamp,"
            + "  `gmt_modified` timestamp not null default current_timestamp on update current_timestamp,"
            + "  primary key (`id`),"
            + "  unique key (`table_name`)"
            + ") charset=utf8";

    private static final String INSERT_SCHEMA_CHANGE_TABLE =
        "insert ignore into " + SCHEMA_CHANGE_TABLE + "(`table_name`, `version`) values(?, ?)";

    private static final String SELECT_SCHEMA_CHANGE_TABLE =
        "select `table_name`, `version` from " + SCHEMA_CHANGE_TABLE;

    private static final String UPDATE_SCHEMA_CHANGE_TABLE =
        "update " + SCHEMA_CHANGE_TABLE + " set `version` = ? where `table_name` = ?";

    private static final String CHECK_SYSTEM_TABLE =
        "select count(*) from information_schema.tables where table_schema = database() and table_name = ?";

    private static final String GET_SCHEMA_CHANGE_LOCK =
        "select get_lock('" + SCHEMA_CHANGE_LOCK + "', " + SCHEMA_CHANGE_LOCK_TIMEOUT + ")";

    private static final String RELEASE_SCHEMA_CHANGE_LOCK = "select release_lock('" + SCHEMA_CHANGE_LOCK + "')";

    @Override
    protected void doInit() {
        create();
    }

    public void create() {
        try (Connection metaDbConn = MetaDbUtil.getConnection()) {
            MetaDbUtil.executeDDL(CREATE_SCHEMA_CHANGE_TABLE, metaDbConn);
        } catch (SQLException e) {
            LOGGER.error("Failed to create the system table " + SCHEMA_CHANGE_TABLE, e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "create", SCHEMA_CHANGE_TABLE,
                e.getMessage());
        }
    }

    public void insert(String tableName, int version) {
        try {
            Map<Integer, ParameterContext> params = new HashMap<>(2);
            MetaDbUtil.setParameter(1, params, ParameterMethod.setString, tableName);
            MetaDbUtil.setParameter(2, params, ParameterMethod.setInt, version);
            MetaDbUtil.insert(INSERT_SCHEMA_CHANGE_TABLE, params, connection);
        } catch (SQLException e) {
            LOGGER.error("Failed to insert a record into system table " + SCHEMA_CHANGE_TABLE, e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "insert", SCHEMA_CHANGE_TABLE,
                e.getMessage());
        }
    }

    public List<SchemaChangeRecord> query() {
        try {
            return MetaDbUtil.query(SELECT_SCHEMA_CHANGE_TABLE, SchemaChangeRecord.class, connection);
        } catch (Exception e) {
            LOGGER.error("Failed to query system table " + SCHEMA_CHANGE_TABLE, e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "query", SCHEMA_CHANGE_TABLE,
                e.getMessage());
        }
    }

    public int update(String tableName, int version) {
        try {
            Map<Integer, ParameterContext> params = new HashMap<>(2);
            MetaDbUtil.setParameter(1, params, ParameterMethod.setInt, version);
            MetaDbUtil.setParameter(2, params, ParameterMethod.setString, tableName);
            return MetaDbUtil.update(UPDATE_SCHEMA_CHANGE_TABLE, params, connection);
        } catch (SQLException e) {
            LOGGER.error("Failed to update system table " + SCHEMA_CHANGE_TABLE, e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "update", SCHEMA_CHANGE_TABLE,
                e.getMessage());
        }
    }

    public void execute(String ddl) {
        try {
            MetaDbUtil.executeDDL(ddl, connection);
        } catch (SQLException e) {
            String errMsg = "Failed to execute the DDL statement: " + ddl + ". Caused by: " + e.getMessage();
            LOGGER.error(errMsg, e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_GENERIC, e, errMsg);
        }
    }

    public boolean check(String systemTableName) {
        try {
            Map<Integer, ParameterContext> params = new HashMap<>(1);
            MetaDbUtil.setParameter(1, params, ParameterMethod.setString, systemTableName);
            List<CountRecord> records = MetaDbUtil.query(CHECK_SYSTEM_TABLE, params, CountRecord.class, connection);
            return records != null && records.size() > 0 && records.get(0).count > 0;
        } catch (Exception e) {
            LOGGER.error("Failed to check if system table '" + systemTableName + "' exists", e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "check", systemTableName,
                e.getMessage());
        }
    }

    public boolean getLock() {
        return getOrReleaseLock(true);
    }

    public boolean releaseLock() {
        return getOrReleaseLock(false);
    }

    private boolean getOrReleaseLock(boolean isGetLock) {
        String action = isGetLock ? "get" : "release";
        try {
            List<SchemaChangeLockRecord> records =
                MetaDbUtil.query(isGetLock ? GET_SCHEMA_CHANGE_LOCK : RELEASE_SCHEMA_CHANGE_LOCK,
                    SchemaChangeLockRecord.class, connection);
            if (records == null || records.isEmpty()) {
                throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE,
                    action + " lock for schema change", SCHEMA_CHANGE_TABLE, "no record returned");
            }
            return records.get(0).successful == 1;
        } catch (Exception e) {
            LOGGER.error("Failed to " + action + " lock for schema change", e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e,
                action + " lock for schema change", SCHEMA_CHANGE_TABLE, e.getMessage());
        }
    }

}
