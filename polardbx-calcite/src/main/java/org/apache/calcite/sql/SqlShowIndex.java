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
 * @date 2019/1/16 2:03 AM
 */
public class SqlShowIndex extends SqlShow {

    private static final SqlSpecialOperator OPERATOR = new SqlShowIndexOperator();

    public SqlShowIndex(SqlParserPos pos, List<SqlSpecialIdentifier> specialIdentifiers, List<SqlNode> operands,
                     SqlNode like, SqlNode where, SqlNode orderBy, SqlNode limit, int tableIndex, int dbIndex,
                     boolean dbWithFrom){
        super(pos, specialIdentifiers, operands, like, where, orderBy, limit, tableIndex, dbIndex, dbWithFrom);
    }

    @Override
    protected boolean showWhere() {
        return false;
    }

    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    @Override
    public SqlKind getShowKind() {
        return SqlKind.SHOW_INDEX;
    }

    public static class SqlShowIndexOperator extends SqlSpecialOperator {

        public SqlShowIndexOperator(){
            super("SHOW_INDEX", SqlKind.SHOW_INDEX);
        }

        @Override
        public RelDataType deriveType(SqlValidator validator, SqlValidatorScope scope, SqlCall call) {
            final RelDataTypeFactory typeFactory = validator.getTypeFactory();
            List<RelDataTypeFieldImpl> columns = new LinkedList<>();
            columns.add(new RelDataTypeFieldImpl("Table", 0, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
            columns.add(new RelDataTypeFieldImpl("Non_unique", 1, typeFactory.createSqlType(SqlTypeName.INTEGER)));
            columns.add(new RelDataTypeFieldImpl("Key_name", 2, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
            columns.add(new RelDataTypeFieldImpl("Seq_in_index", 3, typeFactory.createSqlType(SqlTypeName.INTEGER)));
            columns.add(new RelDataTypeFieldImpl("Column_name", 4, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
            columns.add(new RelDataTypeFieldImpl("Collation", 5, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
            columns.add(new RelDataTypeFieldImpl("Cardinality", 6, typeFactory.createSqlType(SqlTypeName.INTEGER)));
            columns.add(new RelDataTypeFieldImpl("Sub_part", 7, typeFactory.createSqlType(SqlTypeName.INTEGER)));
            columns.add(new RelDataTypeFieldImpl("Packed", 8, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
            columns.add(new RelDataTypeFieldImpl("Null", 9, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
            columns.add(new RelDataTypeFieldImpl("Index_type", 10, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
            columns.add(new RelDataTypeFieldImpl("Comment", 11, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
            columns.add(new RelDataTypeFieldImpl("Index_comment", 12, typeFactory.createSqlType(SqlTypeName.VARCHAR)));

            return typeFactory.createStructType(columns);
        }
    }
}
