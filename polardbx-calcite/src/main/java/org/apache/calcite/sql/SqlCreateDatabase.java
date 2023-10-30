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
 */
public class SqlCreateDatabase extends SqlDdl {

    private static final SqlSpecialOperator OPERATOR = new SqlCreateDatabaseOperator();

    final boolean ifNotExists;
    final SqlIdentifier dbName;
    final String charSet;
    final String collate;
    final Boolean encryption;
    final String locality;
    final String partitionMode;
    final Boolean defaultSingle; // ON/OFF 0/1 true/false

    final SqlIdentifier sourceDatabaseName;
    final boolean like;
    final boolean as;

    final List<SqlIdentifier> includeTables;
    final List<SqlIdentifier> excludeTables;

    //for create database like/as lock=true/false
    final boolean withLock;
    final boolean dryRun;
    final boolean createTables;

    public SqlCreateDatabase(SqlParserPos pos, boolean ifNotExists, SqlIdentifier dbName, String charSet,
                             String collate, Boolean encryption,
                             String locality, String partitionMode, Boolean defaultSingle, SqlIdentifier sourceDatabaseName,
                             boolean like, boolean as, List<SqlIdentifier> includeTables,
                             List<SqlIdentifier> excludeTables,
                             boolean withLock, boolean dryRun, boolean createTables) {
        super(OPERATOR, pos);
        this.ifNotExists = ifNotExists;
        this.dbName = dbName;
        this.charSet = trim(charSet);
        this.collate = trim((collate));
        this.encryption = encryption;
        this.locality = locality;
        this.partitionMode = partitionMode;
        this.defaultSingle = defaultSingle;
        this.sourceDatabaseName = sourceDatabaseName;
        this.like = like;
        this.as = as;
        this.includeTables = includeTables;
        this.excludeTables = excludeTables;
        this.withLock = withLock;
        this.dryRun = dryRun;
        this.createTables = createTables;
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

        if (defaultSingle != null) {
            if (defaultSingle) {
                writer.keyword("DEFAULT_SINGLE = 'on'");
            } else {
                writer.keyword("DEFAULT_SINGLE = 'off'");
            }
        }

        if (TStringUtil.isNotBlank(locality)) {
            writer.keyword("LOCALITY = ");
            writer.identifier(locality);
        }

        if (as == true || like == true) {
            if (as) {
                writer.keyword("AS");
            } else {
                writer.keyword("LIKE");
            }
            dbName.unparse(writer, leftPrec, rightPrec);

            if (this.withLock) {
                writer.keyword("LOCK = TRUE");
            } else {
                writer.keyword("LOCK = FALSE");
            }

            if (this.dryRun) {
                writer.keyword("DRY_RUN = TRUE");
            } else {
                writer.keyword("DRY_RUN = FALSE");
            }

            if (this.createTables) {
                writer.keyword("CREATE_TABLES = TRUE");
            } else {
                writer.keyword("CREATE_TABLES = FALSE");
            }

            if (!includeTables.isEmpty() || !excludeTables.isEmpty()) {
                if (!includeTables.isEmpty()) {
                    writer.keyword("INCLUDE");
                    for (int i = 0; i < includeTables.size() - 1; i++) {
                        includeTables.get(i).unparse(writer, leftPrec, rightPrec);
                        writer.print(",");
                    }
                    includeTables.get(includeTables.size() - 1).unparse(writer, leftPrec, rightPrec);
                } else {
                    writer.keyword("EXCLUDE");
                    for (int i = 0; i < excludeTables.size() - 1; i++) {
                        excludeTables.get(i).unparse(writer, leftPrec, rightPrec);
                        writer.print(",");
                    }
                    excludeTables.get(excludeTables.size() - 1).unparse(writer, leftPrec, rightPrec);
                }
            }
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

    public Boolean isEncryption() {
        return encryption;
    }

    public Boolean isDefaultSingle() {
        return defaultSingle;
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

    public SqlIdentifier getSourceDatabaseName() {
        return sourceDatabaseName;
    }

    public boolean getLike() {
        return like;
    }

    public boolean getAs() {
        return as;
    }

    public List<SqlIdentifier> getIncludeTables() {
        return includeTables;
    }

    public List<SqlIdentifier> getExcludeTables() {
        return excludeTables;
    }

    public boolean getWithLock() {
        return this.withLock;
    }

    public boolean isDryRun() {
        return dryRun;
    }

    public boolean isCreateTables() {
        return createTables;
    }

    public static class SqlCreateDatabaseOperator extends SqlSpecialOperator {

        public SqlCreateDatabaseOperator() {
            super("CREATE_DATABASE", SqlKind.CREATE_DATABASE);
        }

        @Override
        public RelDataType deriveType(SqlValidator validator, SqlValidatorScope scope, SqlCall call) {
            final RelDataTypeFactory typeFactory = validator.getTypeFactory();
            final RelDataType columnType = typeFactory.createSqlType(SqlTypeName.CHAR);

            return typeFactory.createStructType(
                ImmutableList.of((RelDataTypeField) new RelDataTypeFieldImpl("CREATE_DATABASE_RESULT",
                    0,
                    columnType)));
        }
    }
}
