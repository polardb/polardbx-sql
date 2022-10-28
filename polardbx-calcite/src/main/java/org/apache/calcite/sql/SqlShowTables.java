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

package org.apache.calcite.sql;

import com.alibaba.polardbx.common.utils.TStringUtil;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;

import java.util.LinkedList;
import java.util.List;

/**
 * @author chenmo.cm
 * @date 2018/6/7 上午11:05
 */
public class SqlShowTables extends SqlShow {

    private boolean isFull = false;
    private SqlNode dbName;
    /**
     * For column name generating
     */
    private String schema;

    public SqlShowTables(SqlParserPos pos, List<SqlSpecialIdentifier> specialIdentifiers, List<SqlNode> operands,
                         SqlNode like, SqlNode where, SqlNode orderBy, SqlNode limit, boolean isFull, SqlNode dbName,
                         String schema){
        super(pos, specialIdentifiers, operands, like, where, orderBy, limit);
        this.isFull = isFull;
        this.dbName = dbName;
        this.schema = schema;
    }

    public static SqlShowTables create(SqlParserPos pos, boolean isFull, SqlNode dbName, String schema, SqlNode like, SqlNode where,
                                       SqlNode orderBy, SqlNode limit) {
        final List<SqlSpecialIdentifier> sqlSpecialIdentifiers = new LinkedList<>();
        final List<SqlNode> operands = new LinkedList<>();
        String realSchemaName = schema;

        if (isFull) {
            sqlSpecialIdentifiers.add(SqlSpecialIdentifier.FULL);
        }
        sqlSpecialIdentifiers.add(SqlSpecialIdentifier.TABLES);

        if (null != dbName) {
            sqlSpecialIdentifiers.add(SqlSpecialIdentifier.FROM);
            operands.add(dbName);
            realSchemaName = dbName.toString();
        }

        return new SqlShowTables(pos,
            sqlSpecialIdentifiers,
            operands,
            like,
            where,
            orderBy,
            limit,
            isFull,
            dbName,
            realSchemaName);
    }

    public boolean isFull() {
        return isFull;
    }

    public void setFull(boolean full) {
        isFull = full;
    }

    public SqlNode getDbName() {
        return dbName;
    }

    public void setDbName(SqlNode dbName) {
        this.dbName = dbName;
    }

    public String getSchema() {
        return schema;
    }

    public void setSchema(String schema) {
        this.schema = schema;
    }

    @Override
    protected boolean showWhere() {
        return false;
    }

    @Override
    public SqlOperator getOperator() {
        String dbNameStr = schema;
        if (null != dbName) {
            dbNameStr = dbName.toString();
        }
        if (TStringUtil.isNotBlank(dbNameStr)) {
            if ((dbNameStr.charAt(0) == '\'' && dbNameStr.charAt(dbNameStr.length() - 1) == '\'') ||
                (dbNameStr.charAt(0) == '`' && dbNameStr.charAt(dbNameStr.length() - 1) == '`')) {
                dbNameStr = dbNameStr.substring(1, dbNameStr.length() - 1);
            }
        }
        return new SqlShowTablesOperator(isFull, dbNameStr);
    }

    @Override
    public SqlKind getShowKind() {
        return SqlKind.SHOW_TABLES;
    }

    public static class SqlShowTablesOperator extends SqlSpecialOperator {

        boolean isFull = false;
        String  dbName;

        public SqlShowTablesOperator(boolean isFull, String dbName){
            super("SHOW_TABLES", SqlKind.SHOW_TABLES);
            this.isFull = isFull;
            this.dbName = dbName;
        }

        @Override
        public RelDataType deriveType(SqlValidator validator, SqlValidatorScope scope, SqlCall call) {
            final RelDataTypeFactory typeFactory = validator.getTypeFactory();
            return getRelDataType(typeFactory, dbName, isFull);
        }

        public static RelDataType getRelDataType(RelDataTypeFactory typeFactory, String dbName, boolean isFull) {
            List<RelDataTypeFieldImpl> columns = new LinkedList<>();
            StringBuilder columnName = new StringBuilder("Tables");
            if (TStringUtil.isNotBlank(dbName)) {
                columnName.append("_in_");
                columnName.append(dbName);
            }
            columns.add(new RelDataTypeFieldImpl(columnName.toString(),
                0,
                typeFactory.createSqlType(SqlTypeName.VARCHAR)));
            if (isFull) {
                columns.add(new RelDataTypeFieldImpl("Table_type", 0, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
                columns.add(new RelDataTypeFieldImpl("Auto_partition", 0, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
            }

            return typeFactory.createStructType(columns);
        }
    }
}
