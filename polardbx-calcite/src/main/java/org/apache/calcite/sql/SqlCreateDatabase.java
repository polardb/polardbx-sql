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
import com.google.common.collect.ImmutableList;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;

import java.util.List;

/**
 * @author chenmo.cm
 * @date 2018/6/19 下午4:38
 */
public class SqlCreateDatabase extends SqlDdl {

    private static final SqlSpecialOperator  OPERATOR = new SqlCreateDatabaseOperator();

    final boolean                            ifNotExists;
    final SqlIdentifier                      dbName;
    final String                             charSet;
    final String                             collate;
    final String                             locality;
    final String                             partitionMode;

    public SqlCreateDatabase(SqlParserPos pos, boolean ifNotExists, SqlIdentifier dbName, String charSet, String collate,
                             String locality, String partitionMode) {
        super(OPERATOR, pos);
        this.ifNotExists = ifNotExists;
        this.dbName = dbName;
        this.charSet = trim(charSet);
        this.collate = trim((collate));
        this.locality = locality;
        this.partitionMode = partitionMode;
    }

    private String trim(String str) {
        if (str == null) {
            return str;
        }
        if (str.charAt(0) == '`' || str.charAt(str.length() - 1) == '`') {
            // trim
            StringBuilder builder = new StringBuilder();
            for (int i = 0; i < str.length(); i++) {
                if (str.charAt(i) == '`') {
                    continue;
                }
                builder.append(str.charAt(i));
            }
            str = builder.toString();
        }
        return str;
    }

        @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword("CREATE DATABASE");

        if (ifNotExists) {
            writer.keyword("IF NOT EXISTS");
        }

        dbName.unparse(writer, leftPrec, rightPrec);

        if (TStringUtil.isNotBlank(charSet)) {
            writer.keyword("DEFAULT CHARACTER SET");
            writer.literal(charSet);
        } else if (TStringUtil.isNotBlank(collate)) {
            writer.keyword("DEFAULT COLLATE");
            writer.literal(collate);
        }

        if (TStringUtil.isNotBlank(locality)) {
            writer.keyword("LOCALITY = ");
            writer.identifier(locality);
        }
    }

    public boolean isIfNotExists() {
        return ifNotExists;
    }

    public SqlIdentifier getDbName() {
        return dbName;
    }

    public String getCharSet() {
        return charSet;
    }

    public String getCollate() {
        return collate;
    }

    public String getLocality() {
        return locality;
    }

    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    @Override
    public List<SqlNode> getOperandList() {
        return ImmutableList.of();
    }

    public String getPartitionMode() {
        return partitionMode;
    }

    public static class SqlCreateDatabaseOperator extends SqlSpecialOperator {

        public SqlCreateDatabaseOperator(){
            super("CREATE_DATABASE", SqlKind.CREATE_DATABASE);
        }

        @Override
        public RelDataType deriveType(SqlValidator validator, SqlValidatorScope scope, SqlCall call) {
            final RelDataTypeFactory typeFactory = validator.getTypeFactory();
            final RelDataType columnType = typeFactory.createSqlType(SqlTypeName.CHAR);

            return typeFactory.createStructType(ImmutableList.of((RelDataTypeField) new RelDataTypeFieldImpl("CREATE_DATABASE_RESULT",
                0,
                columnType)));
        }
    }
}
