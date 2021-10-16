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
 * @date 2018/6/8 下午3:00
 */
public class SqlShowTableStatus extends SqlShow {

    public enum DataType {
        STRING, LONG, BIGDECIMAL
    }

    public static final int                 NUM_OF_COLUMNS = 18;
    public static final List<String>        COLUMN_NAMES   = new ArrayList<>(NUM_OF_COLUMNS);
    public static final List<DataType>      COLUMN_TYPES   = new ArrayList<>(NUM_OF_COLUMNS);

    static {
        COLUMN_NAMES.add("Name");
        COLUMN_NAMES.add("Engine");
        COLUMN_NAMES.add("Version");
        COLUMN_NAMES.add("Row_format");
        COLUMN_NAMES.add("Rows");
        COLUMN_NAMES.add("Avg_row_length");
        COLUMN_NAMES.add("Data_length");
        COLUMN_NAMES.add("Max_data_length");
        COLUMN_NAMES.add("Index_length");
        COLUMN_NAMES.add("Data_free");
        COLUMN_NAMES.add("Auto_increment");
        COLUMN_NAMES.add("Create_time");
        COLUMN_NAMES.add("Update_time");
        COLUMN_NAMES.add("Check_time");
        COLUMN_NAMES.add("Collation");
        COLUMN_NAMES.add("Checksum");
        COLUMN_NAMES.add("Create_options");
        COLUMN_NAMES.add("Comment");

        COLUMN_TYPES.add(DataType.STRING);
        COLUMN_TYPES.add(DataType.STRING);
        COLUMN_TYPES.add(DataType.LONG);
        COLUMN_TYPES.add(DataType.STRING);
        COLUMN_TYPES.add(DataType.BIGDECIMAL);
        COLUMN_TYPES.add(DataType.BIGDECIMAL);
        COLUMN_TYPES.add(DataType.BIGDECIMAL);
        COLUMN_TYPES.add(DataType.BIGDECIMAL);
        COLUMN_TYPES.add(DataType.BIGDECIMAL);
        COLUMN_TYPES.add(DataType.BIGDECIMAL);
        COLUMN_TYPES.add(DataType.LONG);
        COLUMN_TYPES.add(DataType.STRING);
        COLUMN_TYPES.add(DataType.STRING);
        COLUMN_TYPES.add(DataType.STRING);
        COLUMN_TYPES.add(DataType.STRING);
        COLUMN_TYPES.add(DataType.STRING);
        COLUMN_TYPES.add(DataType.STRING);
        COLUMN_TYPES.add(DataType.STRING);
    }

    private static final SqlSpecialOperator OPERATOR       = new SqlShowTableStatusOperator();

    public SqlShowTableStatus(SqlParserPos pos, List<SqlSpecialIdentifier> specialIdentifiers, List<SqlNode> operands,
                              SqlNode like, SqlNode where, SqlNode orderBy, SqlNode limit, Integer dbIndex,
                              Boolean dbWithFrom){
        super(pos, specialIdentifiers, operands, like, where, orderBy, limit, -1, dbIndex, dbWithFrom);
    }

    public static SqlShowTableStatus create(SqlParserPos pos, SqlNode dbName, SqlNode like, SqlNode where) {
        final List<SqlSpecialIdentifier> specialIdentifiers = new LinkedList<>();
        specialIdentifiers.add(SqlSpecialIdentifier.TABLE);
        specialIdentifiers.add(SqlSpecialIdentifier.STATUS);

        int dbIndex = -1;
        List<SqlNode> operands = new ArrayList<>();
        if (null != dbName) {
            specialIdentifiers.add(SqlSpecialIdentifier.FROM);
            operands.add(dbName);
            dbIndex = specialIdentifiers.size() + operands.size() - 1;
        }

        return new SqlShowTableStatus(SqlParserPos.ZERO,
            specialIdentifiers,
            operands,
            like,
            where,
            null,
            null,
            dbIndex,
            true);
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
        return SqlKind.SHOW_TABLE_STATUS;
    }

    public static class SqlShowTableStatusOperator extends SqlSpecialOperator {

        public SqlShowTableStatusOperator(){
            super("SHOW_TABLE_STATUS", SqlKind.SHOW_TABLE_STATUS);
        }

        @Override
        public RelDataType deriveType(SqlValidator validator, SqlValidatorScope scope, SqlCall call) {
            final RelDataTypeFactory typeFactory = validator.getTypeFactory();
            return getRelDataType(typeFactory);
        }

        public static RelDataType getRelDataType(RelDataTypeFactory typeFactory) {
            List<RelDataTypeFieldImpl> columns = new LinkedList<>();
            for (int i = 0; i < NUM_OF_COLUMNS; i++) {
                SqlTypeName typeName;
                switch (COLUMN_TYPES.get(i)) {
                    case LONG:
                        typeName = SqlTypeName.BIGINT;
                        break;
                    case BIGDECIMAL:
                        typeName = SqlTypeName.BIGINT_UNSIGNED;
                        break;
                    case STRING:
                    default:
                        typeName = SqlTypeName.VARCHAR;
                        break;
                }
                columns.add(new RelDataTypeFieldImpl(COLUMN_NAMES.get(i), i, typeFactory.createSqlType(typeName)));
            }
            return typeFactory.createStructType(columns);
        }
    }
}
