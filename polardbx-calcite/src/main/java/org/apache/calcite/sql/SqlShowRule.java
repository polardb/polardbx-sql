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

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;

/**
 * @author chenmo.cm
 * @date 2018/6/12 下午12:44
 */
public class SqlShowRule extends SqlShow {

    private boolean full;
    private SqlNode tableName;

    public SqlShowRule(SqlParserPos pos, List<SqlSpecialIdentifier> specialIdentifiers, List<SqlNode> operands,
                       SqlNode like, SqlNode where, SqlNode orderBy, SqlNode limit, boolean full, SqlNode tableName){
        super(pos, specialIdentifiers, operands, like, where, orderBy, limit);
        this.full = full;
        this.tableName = tableName;
    }

    public SqlShowRule(SqlParserPos pos, List<SqlSpecialIdentifier> specialIdentifiers, List<SqlNode> operands,
                       SqlNode like, SqlNode where, SqlNode orderBy, SqlNode limit, boolean full, SqlNode tableName, Integer tableNameIndex){
        super(pos, specialIdentifiers, operands, like, where, orderBy, limit, tableNameIndex);
        this.full = full;
        this.tableName = tableName;
    }

    public static SqlShowRule create(SqlParserPos pos, boolean full, SqlNode tableName, SqlNode where, SqlNode orderBy,
                                     SqlNode limit) {
        List<SqlSpecialIdentifier> specialIdentifiers = new LinkedList<>();
        if (full) {
            specialIdentifiers.add(SqlSpecialIdentifier.FULL);
        }
        specialIdentifiers.add(SqlSpecialIdentifier.RULE);

        List<SqlNode> operands = new ArrayList<>();
        if (null != tableName) {
            specialIdentifiers.add(SqlSpecialIdentifier.FROM);
            operands.add(tableName);
            return new SqlShowRule(pos,
                specialIdentifiers,
                operands,
                null,
                where,
                orderBy,
                limit,
                full,
                tableName,
                specialIdentifiers.size() + operands.size() - 1);
        }

        return new SqlShowRule(pos, specialIdentifiers, operands, null, where, orderBy, limit, full, tableName);
    }

    public boolean isFull() {
        return full;
    }

    public void setFull(boolean full) {
        this.full = full;
    }

    public SqlNode getTableName() {
        return tableName;
    }

    public void setTableName(SqlNode tableName) {
        this.tableName = tableName;
    }

    @Override
    protected boolean showWhere() {
        return false;
    }

    @Override
    public SqlOperator getOperator() {
        return new SqlShowRuleOperator(full);
    }

    @Override
    public SqlKind getShowKind() {
        return SqlKind.SHOW_RULE;
    }

    public static class SqlShowRuleOperator extends SqlSpecialOperator {

        private final boolean full;

        public SqlShowRuleOperator(boolean full){
            super("SHOW_RULE", SqlKind.SHOW_RULE);
            this.full = full;
        }

        @Override
        public RelDataType deriveType(SqlValidator validator, SqlValidatorScope scope, SqlCall call) {
            final RelDataTypeFactory typeFactory = validator.getTypeFactory();
            List<RelDataTypeFieldImpl> columns = new LinkedList<>();
            if (!this.full) {
                columns.add(new RelDataTypeFieldImpl("Id", 0, typeFactory.createSqlType(SqlTypeName.INTEGER)));
                columns.add(new RelDataTypeFieldImpl("TABLE_NAME", 1, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
                columns.add(new RelDataTypeFieldImpl("BROADCAST", 2, typeFactory.createSqlType(SqlTypeName.BOOLEAN)));
                columns.add(new RelDataTypeFieldImpl("DB_PARTITION_KEY",
                    3,
                    typeFactory.createSqlType(SqlTypeName.VARCHAR)));
                columns.add(new RelDataTypeFieldImpl("DB_PARTITION_POLICY",
                    4,
                    typeFactory.createSqlType(SqlTypeName.VARCHAR)));
                columns.add(new RelDataTypeFieldImpl("DB_PARTITION_COUNT",
                    5,
                    typeFactory.createSqlType(SqlTypeName.VARCHAR)));
                columns.add(new RelDataTypeFieldImpl("TB_PARTITION_KEY",
                    6,
                    typeFactory.createSqlType(SqlTypeName.VARCHAR)));
                columns.add(new RelDataTypeFieldImpl("TB_PARTITION_POLICY",
                    7,
                    typeFactory.createSqlType(SqlTypeName.VARCHAR)));
                columns.add(new RelDataTypeFieldImpl("TB_PARTITION_COUNT",
                    8,
                    typeFactory.createSqlType(SqlTypeName.VARCHAR)));
            } else {
                columns.add(new RelDataTypeFieldImpl("Id", 0, typeFactory.createSqlType(SqlTypeName.INTEGER)));
                columns.add(new RelDataTypeFieldImpl("TABLE_NAME", 1, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
                columns.add(new RelDataTypeFieldImpl("BROADCAST", 2, typeFactory.createSqlType(SqlTypeName.BOOLEAN)));
                columns.add(new RelDataTypeFieldImpl("JOIN_GROUP", 3, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
                columns.add(new RelDataTypeFieldImpl("ALLOW_FULL_TABLE_SCAN",
                    4,
                    typeFactory.createSqlType(SqlTypeName.BOOLEAN)));
                columns.add(new RelDataTypeFieldImpl("DB_NAME_PATTERN",
                    5,
                    typeFactory.createSqlType(SqlTypeName.VARCHAR)));
                columns.add(new RelDataTypeFieldImpl("DB_RULES_STR", 6, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
                columns.add(new RelDataTypeFieldImpl("TB_NAME_PATTERN",
                    7,
                    typeFactory.createSqlType(SqlTypeName.VARCHAR)));
                columns.add(new RelDataTypeFieldImpl("TB_RULES_STR", 8, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
                columns.add(new RelDataTypeFieldImpl("PARTITION_KEYS",
                    9,
                    typeFactory.createSqlType(SqlTypeName.VARCHAR)));
                columns.add(new RelDataTypeFieldImpl("DEFAULT_DB_INDEX",
                    10,
                    typeFactory.createSqlType(SqlTypeName.VARCHAR)));

            }

            return typeFactory.createStructType(columns);
        }
    }
}
