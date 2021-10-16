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

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

/**
 * @version 1.0
 * @ClassName SqlCheckGlobalIndex
 * @description
 * @Author zzy
 * @Date 2019/11/11 16:04
 */
public class SqlCheckGlobalIndex extends SqlDdl { // Use DDL here to utilize async DDL framework.

    private static final SqlSpecialOperator OPERATOR = new SqlCheckGlobalIndex.SqlCheckGlobalIndexOperator();

    private SqlNode                         indexName;
    private SqlNode                         tableName;
    private String                          extraCmd;

    public SqlCheckGlobalIndex(SqlParserPos pos, SqlNode indexName, SqlNode tableName, String extraCmd){
        super(OPERATOR, pos);
        this.name = indexName;
        this.indexName = indexName;
        this.tableName = tableName;
        this.extraCmd = extraCmd;
    }

    public SqlNode getIndexName() {
        return indexName;
    }

    public void setIndexName(SqlNode indexName) {
        this.indexName = indexName;
    }

    public SqlNode getTableName() {
        return tableName;
    }

    public void setTableName(SqlNode tableName) {
        this.tableName = tableName;
    }

    public String getExtraCmd() {
        return extraCmd;
    }

    public void setExtraCmd(String extraCmd) {
        this.extraCmd = extraCmd;
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        final SqlWriter.Frame selectFrame = writer.startList(SqlWriter.FrameTypeEnum.SELECT);
        writer.sep("CHECK GLOBAL INDEX");

        if (indexName != null) {
            indexName.unparse(writer, leftPrec, rightPrec);
        }

        if (tableName != null) {
            writer.sep("ON");
            tableName.unparse(writer, leftPrec, rightPrec);
        }

        if (extraCmd != null) {
            writer.sep(extraCmd);
        }

        writer.endList(selectFrame);
    }

    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    @Override
    public List<SqlNode> getOperandList() {
        return Arrays.asList(name);
    }

    @Override
    public SqlKind getKind() {
        return SqlKind.CHECK_GLOBAL_INDEX;
    }

    @Override
    public void validate(SqlValidator validator, SqlValidatorScope scope) {
    }

    public static class SqlCheckGlobalIndexOperator extends SqlSpecialOperator {

        public SqlCheckGlobalIndexOperator(){
            super("CHECK_GLOBAL_INDEX", SqlKind.CHECK_GLOBAL_INDEX);
        }

        @Override
        public RelDataType deriveType(SqlValidator validator, SqlValidatorScope scope, SqlCall call) {
            final RelDataTypeFactory typeFactory = validator.getTypeFactory();
            List<RelDataTypeFieldImpl> columns = new LinkedList<>();
            columns.add(new RelDataTypeFieldImpl("Table", 0, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
            columns.add(new RelDataTypeFieldImpl("Op", 1, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
            columns.add(new RelDataTypeFieldImpl("Msg_type", 2, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
            columns.add(new RelDataTypeFieldImpl("Msg_text", 3, typeFactory.createSqlType(SqlTypeName.VARCHAR)));

            return typeFactory.createStructType(columns);
        }
    }

    @Override
    public SqlNode clone(SqlParserPos pos) {
        return new SqlCheckGlobalIndex(this.pos, indexName, tableName, extraCmd);
    }

}
