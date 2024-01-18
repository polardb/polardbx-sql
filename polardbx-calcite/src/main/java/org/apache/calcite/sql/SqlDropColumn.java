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

import java.util.Arrays;
import java.util.List;

import org.apache.calcite.sql.parser.SqlParserPos;

/**
 * @author chenmo.cm
 * @date 2019/1/23 12:07 AM
 */
// | DROP [COLUMN] col_name
public class SqlDropColumn extends SqlAlterSpecification {

    private static final SqlOperator   OPERATOR = new SqlSpecialOperator("DROP COLUMN", SqlKind.DROP_COLUMN);
    private final SqlIdentifier        originTableName;
    private final SqlIdentifier        colName;
    private final String               sourceSql;
    private SqlNode                    tableName;

    /**
     * Creates a SqlDropColumn.
     *
     * @param tableName
     * @param pos
     */
    public SqlDropColumn(SqlIdentifier tableName, SqlIdentifier colName, String sql, SqlParserPos pos){
        super(pos);
        this.tableName = tableName;
        this.originTableName = tableName;
        this.colName = colName;
        this.sourceSql = sql;
    }

    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    @Override
    public List<SqlNode> getOperandList() {
        return Arrays.asList(tableName, colName);
    }

    public void setTargetTable(SqlNode tableName) {
        this.tableName = tableName;
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        final SqlWriter.Frame frame = writer.startList(SqlWriter.FrameTypeEnum.SELECT, "DROP COLUMN", "");

        colName.unparse(writer, leftPrec, rightPrec);

        writer.endList(frame);
    }

    public SqlNode getTableName() {
        return tableName;
    }

    public SqlIdentifier getColName() {
        return colName;
    }

    public String getSourceSql() {
        return sourceSql;
    }

    public SqlIdentifier getOriginTableName() {
        return originTableName;
    }

    @Override
    public SqlAlterSpecification replaceTableName(SqlIdentifier newTableName) {
        return new SqlDropColumn(newTableName, colName, sourceSql, getParserPosition());
    }

    @Override
    public boolean supportFileStorage() { return true;}
}
