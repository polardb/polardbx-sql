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
import com.google.common.collect.ImmutableList;

/**
 * @author chenmo.cm
 * @date 2018/6/18 下午12:56
 */
public class SqlSetNames extends SqlDal {

    private static final SqlSpecialOperator OPERATOR = new SqlSetNamesOperator();

    private final SqlNode                   charset;
    private final SqlNode                   collate;

    public SqlSetNames(SqlParserPos pos, SqlNode charset, SqlNode collate){
        super(pos);
        this.charset = charset;
        this.collate = collate;
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        final SqlWriter.Frame selectFrame = writer.startList(SqlWriter.FrameTypeEnum.SELECT);
        writer.sep("SET NAMES");
        if (charset instanceof SqlIdentifier) {
            writer.print(charset.toString());
        } else {
            charset.unparse(writer, leftPrec, rightPrec);
        }
        if (null != this.collate) {
            writer.sep("COLLATE");
            if (collate instanceof SqlIdentifier) {
                writer.print(collate.toString());
            } else {
                collate.unparse(writer, leftPrec, rightPrec);
            }
        }
        writer.endList(selectFrame);
    }

    public SqlNode getCharset() {
        return charset;
    }

    public SqlNode getCollate() {
        return collate;
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
    public SqlKind getKind() {
        return SqlKind.SQL_SET_NAMES;
    }

    @Override
    public void validate(SqlValidator validator, SqlValidatorScope scope) {
    }

    public static class SqlSetNamesOperator extends SqlSpecialOperator {

        public SqlSetNamesOperator(){
            super("SQL_SET_NAMES", SqlKind.SQL_SET_NAMES);
        }

        @Override
        public RelDataType deriveType(SqlValidator validator, SqlValidatorScope scope, SqlCall call) {
            final RelDataTypeFactory typeFactory = validator.getTypeFactory();
            List<RelDataTypeFieldImpl> columns = new LinkedList<>();
            columns.add(new RelDataTypeFieldImpl("RESULT", 0, typeFactory.createSqlType(SqlTypeName.INTEGER)));

            return typeFactory.createStructType(columns);
        }
    }
}
