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
 * @date 2018/6/14 上午10:14
 */
public class SqlCheckTable extends SqlDal {

    private static final SqlSpecialOperator OPERATOR = new SqlCheckTableOperator();

    private List<SqlNode> tableNames;

    private boolean withLocalPartitions;

    private String displayMode;

    public SqlCheckTable(SqlParserPos pos, List<SqlNode> tableNames){
        super(pos);
        this.tableNames = tableNames;
        this.tableIndexes = new LinkedList<>();
        for (int i = 0; i < tableNames.size(); i++) {
            tableIndexes.add(i);
        }
        this.operands = tableNames;
    }

    public List<SqlNode> getTableNames() {
        return tableNames;
    }

    public void setTableNames(List<SqlNode> tableNames) {
        this.tableNames = tableNames;
    }

    public boolean isWithLocalPartitions() {
        return this.withLocalPartitions;
    }

    public void setWithLocalPartitions(final boolean withLocalPartitions) {
        this.withLocalPartitions = withLocalPartitions;
    }

    public void setDisplayMode(final String mode){
        this.displayMode = mode;
    }

    public String getDisplayMode(){
        return this.displayMode;
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        final SqlWriter.Frame selectFrame = writer.startList(SqlWriter.FrameTypeEnum.SELECT);
        writer.sep("CHECK TABLE");

        if(tableNames.size() > 0) {
            tableNames.get(0).unparse(writer, leftPrec, rightPrec);
            for (int index = 1; index < tableNames.size(); index++) {
                writer.print(", ");
                tableNames.get(index).unparse(writer, leftPrec, rightPrec);
            }
        }
        writer.endList(selectFrame);
    }

    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    @Override
    public SqlKind getKind() {
        return SqlKind.CHECK_TABLE;
    }

    @Override
    public void validate(SqlValidator validator, SqlValidatorScope scope) {
    }

    public static class SqlCheckTableOperator extends SqlSpecialOperator {

        public SqlCheckTableOperator(){
            super("CHECK_TABLE", SqlKind.CHECK_TABLE);
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
        return new SqlCheckTable(this.pos, tableNames);
    }
}
