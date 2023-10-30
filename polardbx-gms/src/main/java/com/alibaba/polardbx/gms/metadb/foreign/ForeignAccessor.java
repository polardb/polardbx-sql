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

package com.alibaba.polardbx.gms.metadb.foreign;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.ParameterMethod;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.gms.metadb.GmsSystemTables;
import com.alibaba.polardbx.gms.metadb.accessor.AbstractAccessor;
import com.alibaba.polardbx.gms.util.MetaDbUtil;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ForeignAccessor extends AbstractAccessor {
    private static final Logger logger = LoggerFactory.getLogger(ForeignAccessor.class);
    private static final String FOREIGN_TABLE = wrap(GmsSystemTables.FOREIGN_KEY);

    private static final String INSERT_TABLES =
        "insert into " + FOREIGN_TABLE
            + "(`id`, `schema_name`, `table_name`, `index_name`, `constraint_name`, "
            + "`ref_schema_name`, `ref_table_name`, `ref_index_name`, `n_cols`, `update_rule`, `delete_rule`, `push_down`) "
            + "values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

    private static final String WHERE_CLAUSE = " where `id` = ?";

    private static final String WHERE_SCHEMA = " where `schema_name` = ?";

    private static final String AND_TABLE = " and `table_name` = ?";

    private static final String AND_INDEX = " and `index_name` = ?";

    private static final String SELECT_CLAUSE =
        "select `id`, `schema_name`, `table_name`, `index_name`, `constraint_name`, `ref_schema_name`, `ref_table_name`, "
            + "`ref_index_name`, `n_cols`, `update_rule`, `delete_rule`,  `push_down` from ";

    private static final String WHERE_REF_SCHEMA = " where `ref_schema_name` = ?";

    private static final String WHERE_REF_NAME = " where `ref_schema_name` = ? and `ref_table_name` = ?";

    private static final String SELECT_FK_WITH_ID = SELECT_CLAUSE + FOREIGN_TABLE + WHERE_CLAUSE;

    private static final String SELECT_FOR_SCHEMA_FK = SELECT_CLAUSE + FOREIGN_TABLE + WHERE_SCHEMA;

    private static final String SELECT_FOR_TABLE_FK = SELECT_CLAUSE + FOREIGN_TABLE + WHERE_SCHEMA + AND_TABLE;

    private static final String SELECT_REF_SCHEMA_FK = SELECT_CLAUSE + FOREIGN_TABLE + WHERE_REF_SCHEMA;

    private static final String SELECT_REF_TABLE_FK = SELECT_CLAUSE + FOREIGN_TABLE + WHERE_REF_NAME;

    private static final String SELECT_FK_WITH_INDEX =
        SELECT_CLAUSE + FOREIGN_TABLE + WHERE_SCHEMA + AND_TABLE + AND_INDEX;

    private static final String DELETE_FK_BY_SCHEMA = "delete from " + FOREIGN_TABLE + WHERE_SCHEMA;

    private static final String DELETE_FK_BY_TABLE = "delete from " + FOREIGN_TABLE + WHERE_SCHEMA + AND_TABLE;

    private static final String DELETE_FK_BY_INDEX = DELETE_FK_BY_TABLE + AND_INDEX;

    private static final String UPDATE_FOR_TABLE =
        "update " + FOREIGN_TABLE + "set `table_name` = ?, `constraint_name` = ?" + WHERE_SCHEMA + AND_TABLE
            + AND_INDEX;

    private static final String UPDATE_REF_TABLE =
        "update" + FOREIGN_TABLE + " set `ref_table_name` = ?" + WHERE_REF_NAME;

    private static final String UPDATE_PUSH_DOWN =
        "update " + FOREIGN_TABLE + " set `push_down` = ? " + WHERE_SCHEMA + AND_TABLE + AND_INDEX;

    public int insert(ForeignRecord record) {
        return insert(INSERT_TABLES, FOREIGN_TABLE, record.buildInsertParams());
    }

    public ForeignRecord query(long id) {
        try {
            Map<Integer, ParameterContext> params = new HashMap<>();
            MetaDbUtil.setParameter(1, params, ParameterMethod.setLong, id);
            List<ForeignRecord> records =
                MetaDbUtil.query(SELECT_FK_WITH_ID, params, ForeignRecord.class, connection);
            if (records.size() > 0) {
                return records.get(0);
            }
            return null;
        } catch (Exception e) {
            logger.error("Failed to query the system table '" + FOREIGN_TABLE + "'", e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "query",
                FOREIGN_TABLE,
                e.getMessage());
        }
    }

    public List<ForeignRecord> queryReferencedForeignKeys(String tableSchema) {
        try {
            Map<Integer, ParameterContext> params = new HashMap<>();
            MetaDbUtil.setParameter(1, params, ParameterMethod.setString, tableSchema);
            return MetaDbUtil.query(SELECT_REF_SCHEMA_FK, params, ForeignRecord.class, connection);
        } catch (Exception e) {
            logger.error("Failed to query the system table '" + FOREIGN_TABLE + "'", e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "query",
                FOREIGN_TABLE,
                e.getMessage());
        }
    }

    public List<ForeignRecord> queryReferencedForeignKeys(String tableSchema, String tableName) {
        try {
            Map<Integer, ParameterContext> params = new HashMap<>();
            MetaDbUtil.setParameter(1, params, ParameterMethod.setString, tableSchema);
            MetaDbUtil.setParameter(2, params, ParameterMethod.setString, tableName);
            return MetaDbUtil.query(SELECT_REF_TABLE_FK, params, ForeignRecord.class, connection);
        } catch (Exception e) {
            logger.error("Failed to query the system table '" + FOREIGN_TABLE + "'", e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "query",
                FOREIGN_TABLE,
                e.getMessage());
        }
    }

    public List<ForeignRecord> queryForeignKeys(String tableSchema) {
        try {
            Map<Integer, ParameterContext> params = new HashMap<>();
            MetaDbUtil.setParameter(1, params, ParameterMethod.setString, tableSchema);
            return MetaDbUtil.query(SELECT_FOR_SCHEMA_FK, params, ForeignRecord.class, connection);
        } catch (Exception e) {
            logger.error("Failed to query the system table '" + FOREIGN_TABLE + "'", e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "query",
                FOREIGN_TABLE,
                e.getMessage());
        }
    }

    public List<ForeignRecord> queryForeignKeys(String tableSchema, String tableName) {
        try {
            Map<Integer, ParameterContext> params = new HashMap<>();
            MetaDbUtil.setParameter(1, params, ParameterMethod.setString, tableSchema);
            MetaDbUtil.setParameter(2, params, ParameterMethod.setString, tableName);
            return MetaDbUtil.query(SELECT_FOR_TABLE_FK, params, ForeignRecord.class, connection);
        } catch (Exception e) {
            logger.error("Failed to query the system table '" + FOREIGN_TABLE + "'", e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "query",
                FOREIGN_TABLE,
                e.getMessage());
        }
    }

    public ForeignRecord queryForeignKey(String tableSchema, String tableName, String indexName) {
        try {
            Map<Integer, ParameterContext> params = new HashMap<>();
            MetaDbUtil.setParameter(1, params, ParameterMethod.setString, tableSchema);
            MetaDbUtil.setParameter(2, params, ParameterMethod.setString, tableName);
            MetaDbUtil.setParameter(3, params, ParameterMethod.setString, indexName);
            return MetaDbUtil.query(SELECT_FK_WITH_INDEX, params, ForeignRecord.class, connection).get(0);
        } catch (Exception e) {
            logger.error("Failed to query the system table '" + FOREIGN_TABLE + "'", e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "query",
                FOREIGN_TABLE,
                e.getMessage());
        }
    }

    public int updateReferencedForeignKeyTable(String tableSchema, String tableName, String newTableName) {
        Map<Integer, ParameterContext> params = MetaDbUtil
            .buildStringParameters(new String[] {
                newTableName,
                tableSchema,
                tableName,
            });
        return update(UPDATE_REF_TABLE, FOREIGN_TABLE, params);
    }

    public int updateForeignKeyTable(String tableSchema, String tableName, String indexName, String constraintName,
                                     String originalTableName) {
        Map<Integer, ParameterContext> params = MetaDbUtil
            .buildStringParameters(new String[] {
                tableName,
                constraintName,
                tableSchema,
                originalTableName,
                indexName,
            });
        return update(UPDATE_FOR_TABLE, FOREIGN_TABLE, params);
    }

    public int updateForeignKeyPushDown(String tableSchema, String tableName, String indexName, long pushDown) {
        Map<Integer, ParameterContext> params = new HashMap<>();
        MetaDbUtil.setParameter(1, params, ParameterMethod.setLong, pushDown);
        MetaDbUtil.setParameter(2, params, ParameterMethod.setString, tableSchema);
        MetaDbUtil.setParameter(3, params, ParameterMethod.setString, tableName);
        MetaDbUtil.setParameter(4, params, ParameterMethod.setString, indexName);
        return update(UPDATE_PUSH_DOWN, FOREIGN_TABLE, params);
    }

    public int delete(String schemaName) {
        return delete(DELETE_FK_BY_SCHEMA, FOREIGN_TABLE, schemaName);
    }

    public int delete(String schemaName, String tableName) {
        return delete(DELETE_FK_BY_TABLE, FOREIGN_TABLE, schemaName, tableName);
    }

    public int deleteForeignKey(String schemaName, String tableName, String indexName) {
        Map<Integer, ParameterContext> params = MetaDbUtil
            .buildStringParameters(new String[] {schemaName, tableName, indexName});
        return delete(DELETE_FK_BY_INDEX, FOREIGN_TABLE, params);
    }
}
