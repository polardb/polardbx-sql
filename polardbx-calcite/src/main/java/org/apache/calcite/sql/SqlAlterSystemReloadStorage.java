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

import com.google.common.collect.ImmutableList;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

/**
 * @author chenghui.lch
 */
public class SqlAlterSystemReloadStorage extends SqlDal {

    public static class SqlAlterSystemReloadStorageOperator extends SqlSpecialOperator {

        public SqlAlterSystemReloadStorageOperator() {
            super("ALTER_SYSTEM_RELOAD_STORAGE", SqlKind.ALTER_SYSTEM_RELOAD_STORAGE);
        }

        @Override
        public RelDataType deriveType(SqlValidator validator, SqlValidatorScope scope, SqlCall call) {

            final RelDataTypeFactory typeFactory = validator.getTypeFactory();
            List<RelDataTypeFieldImpl> columns = new LinkedList<>();

            columns.add(new RelDataTypeFieldImpl("DN", 0, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
            columns.add(new RelDataTypeFieldImpl("RW_DN", 1, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
            columns.add(new RelDataTypeFieldImpl("KIND", 2, typeFactory.createSqlType(SqlTypeName.VARCHAR)));

            columns.add(new RelDataTypeFieldImpl("NODE", 3, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
            columns.add(new RelDataTypeFieldImpl("USER", 4, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
            columns.add(new RelDataTypeFieldImpl("PASSWD_ENC", 5, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
            columns.add(new RelDataTypeFieldImpl("ROLE", 6, typeFactory.createSqlType(SqlTypeName.VARCHAR)));

            columns.add(new RelDataTypeFieldImpl("IS_VIP", 7, typeFactory.createSqlType(SqlTypeName.BOOLEAN)));
            columns.add(new RelDataTypeFieldImpl("FROM", 8, typeFactory.createSqlType(SqlTypeName.BOOLEAN)));




            return typeFactory.createStructType(columns);

//            final RelDataType columnType = typeFactory.createSqlType(SqlTypeName.CHAR);
//            return typeFactory.createStructType(
//                ImmutableList.of((RelDataTypeField) new RelDataTypeFieldImpl("ALTER_SYSTEM_RELOAD_STORAGE_RESULT",
//                    0,
//                    columnType)));
        }
    }

    private static final SqlSpecialOperator OPERATOR = new SqlAlterSystemReloadStorage.SqlAlterSystemReloadStorageOperator();


    protected List<SqlNode> storageList = new ArrayList<>();
    public SqlAlterSystemReloadStorage(SqlParserPos pos) {
        super(pos);
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        final SqlWriter.Frame selectFrame = writer.startList(SqlWriter.FrameTypeEnum.SELECT);
        writer.print("ALTER SYSTEM RELOAD STORAGE");

        if (storageList.size() > 0) {
            writer.print(" ");
            for (int i = 0; i < storageList.size(); i++) {
                if (i > 0) {
                    writer.print(",");
                }
                storageList.get(i).unparse(writer, leftPrec, rightPrec);
            }
        }
        writer.endList(selectFrame);
    }

    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    public List<SqlNode> getStorageList() {
        return storageList;
    }

    public void setStorageList(List<SqlNode> storageList) {
        this.storageList = storageList;
    }
}
