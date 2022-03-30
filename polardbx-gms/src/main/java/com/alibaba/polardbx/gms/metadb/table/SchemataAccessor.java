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

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.gms.metadb.GmsSystemTables;
import com.alibaba.polardbx.gms.metadb.accessor.AbstractAccessor;
import com.alibaba.polardbx.gms.util.MetaDbUtil;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

public class SchemataAccessor extends AbstractAccessor {

    private static final String SCHEMATA_TABLE = wrap(GmsSystemTables.SCHEMATA);

    private static final String INSERT_TABLES =
        "insert into " + SCHEMATA_TABLE
            + "(`schema_name`, `default_character_set_name`, `default_collation_name`, `default_db_index`) "
            + "values(?, ?, ?, ?)";

    private static final String WHERE_CLAUSE = " where `schema_name` = ?";

    private static final String SELECT_CLAUSE =
        "select `schema_name`, `default_character_set_name`, `default_collation_name`, `default_db_index` from ";

    private static final String SELECT_TABLES_WITH_WHERE = SELECT_CLAUSE + SCHEMATA_TABLE + WHERE_CLAUSE;

    private static final String SELECT_TABLES = SELECT_CLAUSE + SCHEMATA_TABLE;

    private static final String SELECT_ALL = SELECT_CLAUSE + SCHEMATA_TABLE;

    private static final String DELETE_TABLES = "delete from " + SCHEMATA_TABLE + WHERE_CLAUSE;

    public int insert(SchemataRecord record) {
        return insert(INSERT_TABLES, SCHEMATA_TABLE, record.buildParams());
    }

    public static List<SchemataRecord> getAllSchemata() {
        try (Connection metaDbConn = MetaDbUtil.getConnection()) {
            return AbstractAccessor.query(SELECT_TABLES, SCHEMATA_TABLE, SchemataRecord.class, null,
                null, metaDbConn);
        } catch (SQLException e) {
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "query",
                SCHEMATA_TABLE, e.getMessage());
        }
    }

    public SchemataRecord query(String schemaName) {
        List<SchemataRecord> records =
            query(SELECT_TABLES_WITH_WHERE, SCHEMATA_TABLE, SchemataRecord.class, schemaName);
        if (records != null && records.size() > 0) {
            return records.get(0);
        }
        return null;
    }

    public List<SchemataRecord> queryAll() {
        Map<Integer, ParameterContext> params = new HashMap<>();
        List<SchemataRecord> records = null;
        try {
            records = MetaDbUtil.query(SELECT_ALL, params, SchemataRecord.class, connection);
        } catch (Exception e) {
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "query",
                SCHEMATA_TABLE, e.getMessage());
        }
        return records;
    }

    public int delete(String schemaName) {
        return delete(DELETE_TABLES, SCHEMATA_TABLE, schemaName);
    }

}
