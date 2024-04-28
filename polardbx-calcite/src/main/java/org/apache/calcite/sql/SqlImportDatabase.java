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

import java.util.List;

/**
 * Created by zhuqiwei.
 *
 * @author zhuqiwei
 */
public class SqlImportDatabase extends SqlDdl {

    private static final SqlOperator OPERATOR = new SqlImportDatabaseOperator();

    final String dstLogicalDb;
    final String srcPhyDb;

    final String locality;

    final boolean existStillImport;

    public SqlImportDatabase(SqlParserPos pos, String dstLogicalDb, String srcPhyDb, String locality,
                             boolean existStillImport) {
        super(OPERATOR, pos);
        this.dstLogicalDb = dstLogicalDb;
        this.srcPhyDb = srcPhyDb;
        this.locality = locality;
        this.existStillImport = existStillImport;
    }

    public SqlImportDatabase(SqlParserPos pos, String dstLogicalDb, String srcPhyDb, String locality) {
        super(OPERATOR, pos);
        this.dstLogicalDb = dstLogicalDb;
        this.srcPhyDb = srcPhyDb;
        this.locality = locality;
        this.existStillImport = false;
    }

    @Override
    public List<SqlNode> getOperandList() {
        return ImmutableList.of();
    }

    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    public String getDstLogicalDb() {
        return dstLogicalDb;
    }

    public String getSrcPhyDb() {
        return srcPhyDb;
    }

    public String getLocality() {
        return locality;
    }

    public static class SqlImportDatabaseOperator extends SqlSpecialOperator {
        public SqlImportDatabaseOperator() {
            super("IMPORT_DATABASE", SqlKind.IMPORT_DATABASE);
        }

        @Override
        public RelDataType deriveType(SqlValidator validator, SqlValidatorScope scope, SqlCall call) {
            final RelDataTypeFactory typeFactory = validator.getTypeFactory();
            final RelDataType columnType = typeFactory.createSqlType(SqlTypeName.CHAR);

            return typeFactory.createStructType(
                ImmutableList.of((RelDataTypeField) new RelDataTypeFieldImpl("IMPORT_DATABASE_RESULT",
                    0,
                    columnType)));
        }
    }

    public boolean isExistStillImport() {
        return existStillImport;
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword("IMPORT DATABASE ");
        writer.print(srcPhyDb);
        writer.keyword(" AS ");
        writer.print(dstLogicalDb);
        writer.keyword(" LOCALITY=");
        writer.print(locality);
    }
}
