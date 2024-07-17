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
import com.alibaba.polardbx.gms.util.DdlMetaLogUtil;
import com.alibaba.polardbx.gms.util.MetaDbUtil;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author wenki
 */
public class ForeignColsAccessor extends AbstractAccessor {
    private static final Logger logger = LoggerFactory.getLogger(ForeignColsAccessor.class);
    private static final String FOREIGN_COLS_TABLE = wrap(GmsSystemTables.FOREIGN_KEY_COLS);

    private static final String INSERT_TABLES =
        "insert into " + FOREIGN_COLS_TABLE
            + "(`schema_name`, `table_name`, `index_name`, `for_col_name`, `ref_col_name`, `pos`) "
            + "values(?, ?, ?, ?, ?, ?)";

    private static final String WHERE_CLAUSE = " where `id` = ?";

    private static final String WHERE_SCHEMA = " where `schema_name` = ?";

    private static final String AND_TABLE = " and `table_name` = ?";

    private static final String AND_INDEX = " and `index_name` = ?";

    private static final String AND_FOR_COL_NAME = " and `for_col_name` = ?";

    private static final String AND_REF_COL_NAME = " and `ref_col_name` = ?";

    private static final String ORDER_BY_POS = " order by `pos`";

    private static final String SELECT_CLAUSE =
        "select `id`, `schema_name`, `table_name`, `index_name`, `for_col_name`, `ref_col_name`, `pos` from ";

    private static final String SELECT_FK =
        SELECT_CLAUSE + FOREIGN_COLS_TABLE + WHERE_SCHEMA + AND_TABLE + AND_INDEX + ORDER_BY_POS;

    private static final String SELECT_ALL = SELECT_CLAUSE + FOREIGN_COLS_TABLE;

    private static final String DELETE_FK_BY_SCHEMA = "delete from " + FOREIGN_COLS_TABLE + WHERE_SCHEMA;

    private static final String DELETE_FK_BY_TABLE = "delete from " + FOREIGN_COLS_TABLE + WHERE_SCHEMA + AND_TABLE;

    private static final String DELETE_FK_BY_INDEX = DELETE_FK_BY_TABLE + AND_INDEX;

    private static final String DELETE_FK_BY_COLS = DELETE_FK_BY_INDEX + " and `for_col_name` in (%s)";

    private static final String
        UPDATE_FOR_COL =
        "update " + FOREIGN_COLS_TABLE + " set `for_col_name` = ?" + WHERE_SCHEMA + AND_TABLE
            + AND_INDEX + AND_FOR_COL_NAME;

    private static final String
        UPDATE_REF_COL =
        "update" + FOREIGN_COLS_TABLE + " set `ref_col_name` = ?" + WHERE_SCHEMA + AND_TABLE
            + AND_INDEX + AND_REF_COL_NAME;

    private static final String UPDATE_FOR_TABLE =
        "update " + FOREIGN_COLS_TABLE + " set `table_name` = ?" + WHERE_SCHEMA + AND_TABLE;

    public int insert(ForeignColsRecord record) {
        return insert(INSERT_TABLES, FOREIGN_COLS_TABLE, record.buildInsertParams());
    }

    public int[] insert(List<ForeignColsRecord> records) {
        List<Map<Integer, ParameterContext>> paramsBatch = new ArrayList<>(records.size());
        for (ForeignColsRecord record : records) {
            paramsBatch.add(record.buildInsertParams());
        }
        try {
            DdlMetaLogUtil.logSql(INSERT_TABLES, paramsBatch);
            return MetaDbUtil.insert(INSERT_TABLES, paramsBatch, connection);
        } catch (SQLException e) {
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "batch insert into",
                FOREIGN_COLS_TABLE, e.getMessage());
        }
    }

    public List<ForeignColsRecord> queryForeignKeyCols(String tableSchema, String tableName, String indexName) {
        try {
            Map<Integer, ParameterContext> params = new HashMap<>();
            MetaDbUtil.setParameter(1, params, ParameterMethod.setString, tableSchema);
            MetaDbUtil.setParameter(2, params, ParameterMethod.setString, tableName);
            MetaDbUtil.setParameter(3, params, ParameterMethod.setString, indexName);
            return MetaDbUtil.query(SELECT_FK, params, ForeignColsRecord.class, connection);
        } catch (Exception e) {
            logger.error("Failed to query the system table '" + FOREIGN_COLS_TABLE + "'", e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "query",
                FOREIGN_COLS_TABLE,
                e.getMessage());
        }
    }

    public int updateReferencedForeignKeyColumn(String tableSchema, String tableName, String indexName,
                                                String columnName,
                                                String originColumnName) {
        Map<Integer, ParameterContext> params = MetaDbUtil
            .buildStringParameters(new String[] {
                columnName,
                tableSchema,
                tableName,
                indexName,
                originColumnName,
            });
        return update(UPDATE_REF_COL, FOREIGN_COLS_TABLE, params);
    }

    public int updateForeignKeyColumn(String tableSchema, String tableName, String indexName, String columnName,
                                      String originColumnName) {
        Map<Integer, ParameterContext> params = MetaDbUtil
            .buildStringParameters(new String[] {
                columnName,
                tableSchema,
                tableName,
                indexName,
                originColumnName,
            });
        return update(UPDATE_FOR_COL, FOREIGN_COLS_TABLE, params);
    }

    public int updateForeignKeyColsTable(String tableSchema, String tableName, String originalTableName) {
        Map<Integer, ParameterContext> params = MetaDbUtil
            .buildStringParameters(new String[] {
                tableName,
                tableSchema,
                originalTableName,
            });
        return update(UPDATE_FOR_TABLE, FOREIGN_COLS_TABLE, params);
    }

    public int delete(String schemaName) {
        return delete(DELETE_FK_BY_SCHEMA, FOREIGN_COLS_TABLE, schemaName);
    }

    public int delete(String schemaName, String tableName) {
        return delete(DELETE_FK_BY_TABLE, FOREIGN_COLS_TABLE, schemaName, tableName);
    }

    public int deleteForeignKey(String schemaName, String tableName, String indexName) {
        Map<Integer, ParameterContext> params = MetaDbUtil
            .buildStringParameters(new String[] {schemaName, tableName, indexName});
        return delete(DELETE_FK_BY_INDEX, FOREIGN_COLS_TABLE, params);
    }

    public int deleteForeignKeyCols(String schemaName, String tableName, String indexName, List<String> columnNames) {
        Map<Integer, ParameterContext> params = buildParams(schemaName, tableName, indexName, columnNames);
        return delete(String.format(DELETE_FK_BY_COLS, concatParams(columnNames)), FOREIGN_COLS_TABLE, params);
    }
}
