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
import groovy.sql.Sql;
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
 * @author chenzilin
 * @date 2022/2/14 16:58
 */
public class SqlAlterFileStorage extends SqlDdl {
    /**
     * Creates a SqlDdl.
     */
    private static final SqlOperator OPERATOR = new SqlAlterFileStorageOperator();
    private final SqlNode fileStorageName;
    private SqlCharStringLiteral timestamp;
    private final String sourceSql;
    private boolean asOf = false;
    private boolean purgeBefore = false;
    private boolean backup = false;

    public SqlAlterFileStorage(SqlParserPos pos, SqlNode fileStorageName, SqlNode timestamp,
                               String sourceSql) {
        super(OPERATOR, pos);
        this.name = fileStorageName;
        this.fileStorageName = fileStorageName;
        if (!(timestamp instanceof SqlCharStringLiteral)) {
            throw new IllegalArgumentException("Timestamp format must be yyyy-mm-dd hh:mm:ss");
        }
        this.timestamp = (SqlCharStringLiteral) timestamp;
        this.sourceSql = sourceSql;
    }

    public SqlAlterFileStorage(SqlParserPos pos, SqlNode fileStorageName,
                               String sourceSql) {
        super(OPERATOR, pos);
        this.name = fileStorageName;
        this.fileStorageName = fileStorageName;
        this.sourceSql = sourceSql;
    }

    @Override
    public List<SqlNode> getOperandList() {
        return ImmutableList.of();
    }

    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    @Override
    public String toString() {
        return sourceSql;
    }

    public String getSourceSql() {
        return sourceSql;
    }


    public SqlNode getFileStorageName() {
        return fileStorageName;
    }

    public SqlCharStringLiteral getTimestamp() {
        return timestamp;
    }

    public static class SqlAlterFileStorageOperator extends SqlSpecialOperator {

        public SqlAlterFileStorageOperator() {
            super("ALTER_FILESTORAGE", SqlKind.ALTER_FILESTORAGE);
        }

        @Override
        public RelDataType deriveType(SqlValidator validator, SqlValidatorScope scope, SqlCall call) {
            final RelDataTypeFactory typeFactory = validator.getTypeFactory();
            final RelDataType columnType = typeFactory.createSqlType(SqlTypeName.CHAR);

            return typeFactory.createStructType(
                    ImmutableList.of((RelDataTypeField) new RelDataTypeFieldImpl("ALTER_FILESTORAGE_RESULT",
                            0,
                            columnType)));
        }
    }

    @Override
    public void validate(SqlValidator validator, SqlValidatorScope scope) {
        validator.setColumnReferenceExpansion(false);
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.print(toString());
    }

    public boolean isAsOf() {
        return asOf;
    }

    public void setAsOf(boolean asOf) {
        this.asOf = asOf;
    }

    public boolean isPurgeBefore() {
        return purgeBefore;
    }

    public void setPurgeBefore(boolean purgeBefore) {
        this.purgeBefore = purgeBefore;
    }

    public boolean isBackup() {
        return backup;
    }

    public void setBackup(boolean backup) {
        this.backup = backup;
    }
}

