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
 * @date 2018/6/14 下午12:49
 */
public class SqlOptimizeTable extends SqlDal {

    private static final SqlSpecialOperator OPERATOR = new SqlOptimizeTableOperator();

    private final List<SqlNode>             tableNames;
    private final boolean                   noWriteToBinlog;
    private final boolean                   local;

    public SqlOptimizeTable(SqlParserPos pos, List<SqlNode> tableNames, boolean noWriteToBinlog, boolean local){
        super(pos);
        this.tableNames = tableNames;
        this.noWriteToBinlog = noWriteToBinlog;
        this.local = local;
        this.tableIndexes = new LinkedList<>();
        for (int i = 0; i < tableNames.size(); i++) {
            tableIndexes.add(i);
        }

        this.operands = new LinkedList<>();
        this.operands.addAll(tableNames);
        if (noWriteToBinlog) {
            this.operands.add(SqlLiteral.createSymbol(Mode.NO_WRITE_TO_BINLOG, SqlParserPos.ZERO));
        } else if (local) {
            this.operands.add(SqlLiteral.createSymbol(Mode.LOCAL, SqlParserPos.ZERO));
        }
    }

    public static SqlOptimizeTable create(SqlParserPos pos, List<String> tableNames, boolean noWriteToBinlog, boolean local) {
        List<SqlNode> tableNameNodes = new LinkedList<>();
        for (String tableName : tableNames) {
            tableNameNodes.add(new SqlIdentifier(tableName, SqlParserPos.ZERO));
        }

        return new SqlOptimizeTable(pos, tableNameNodes, noWriteToBinlog, local);
    }

    @Override
    public List<SqlNode> getTableNames() {
        return tableNames;
    }

    public boolean isNoWriteToBinlog() {
        return noWriteToBinlog;
    }

    public boolean isLocal() {
        return local;
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        final SqlWriter.Frame selectFrame = writer.startList(SqlWriter.FrameTypeEnum.SELECT);
        writer.sep("OPTIMIZE");

        if (isNoWriteToBinlog()) {
            writer.sep("NO_WRITE_TO_BINLOG");
        } else if (isLocal()) {
            writer.sep("LOCAL");
        }

        writer.sep("TABLE");

        operands.get(tableIndexes.get(0)).unparse(writer, leftPrec, rightPrec);
        for (int index = 1; index < tableIndexes.size(); index++) {
            writer.print(", ");
            tableNames.get(tableIndexes.get(index)).unparse(writer, leftPrec, rightPrec);
        }

        writer.endList(selectFrame);
    }

    @Override
    public SqlKind getKind() {
        return SqlKind.OPTIMIZE_TABLE;
    }

    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    @Override
    public void validate(SqlValidator validator, SqlValidatorScope scope) {
    }

    public static enum Mode {
        NO_WRITE_TO_BINLOG, LOCAL
    }

    public static class SqlOptimizeTableOperator extends SqlSpecialOperator {

        public SqlOptimizeTableOperator(){
            super("OPTIMIZE_TABLE", SqlKind.OPTIMIZE_TABLE);
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
        return new SqlOptimizeTable(pos, super.getTableNames(), noWriteToBinlog, local);
    }
}
