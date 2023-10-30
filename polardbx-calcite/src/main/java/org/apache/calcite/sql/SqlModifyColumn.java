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

import static org.apache.calcite.sql.SqlAlterTable.genSourceSqlWithOutAfter;

/**
 * @author chenmo.cm
 * @date 2019/1/23 12:07 AM
 */
// |ALTER TABLE MODIFY [COLUMN] col_name column_definition
// [FIRST | AFTER col_name]
public class SqlModifyColumn extends SqlAlterSpecification {

    private static final SqlOperator   OPERATOR = new SqlSpecialOperator("MODIFY COLUMN", SqlKind.MODIFY_COLUMN);
    private final SqlIdentifier        originTableName;
    private final SqlIdentifier        colName;
    private final SqlColumnDeclaration colDef;
    private final boolean              first;
    private final SqlIdentifier        afterColumn;
    private final String               sourceSql;
    private SqlNode                    tableName;

    /**
     * Creates a SqlModifyColumn.
     *
     * @param tableName
     * @param pos
     */
    public SqlModifyColumn(SqlIdentifier tableName, SqlIdentifier colName, SqlColumnDeclaration colDef,
                           boolean first, SqlIdentifier afterColumn, String sql, SqlParserPos pos){
        super(pos);
        this.tableName = tableName;
        this.originTableName = tableName;
        this.colName = colName;
        this.colDef = colDef;
        this.first = first;
        this.afterColumn = afterColumn;
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
        final SqlWriter.Frame frame = writer.startList(SqlWriter.FrameTypeEnum.SELECT, "MODIFY COLUMN", "");

        colName.unparse(writer, leftPrec, rightPrec);
        colDef.unparse(writer, leftPrec, rightPrec);

        if (first) {
            writer.keyword("FIRST");
        } else if (null != afterColumn) {
            writer.keyword("AFTER");
            afterColumn.unparse(writer, leftPrec, rightPrec);
        }

        writer.endList(frame);
    }

    public SqlNode getTableName() {
        return tableName;
    }

    public SqlIdentifier getColName() {
        return colName;
    }

    public SqlColumnDeclaration getColDef() {
        return colDef;
    }

    public boolean isFirst() {
        return first;
    }

    public SqlIdentifier getAfterColumn() {
        return afterColumn;
    }

    public String getSourceSql() {
        return sourceSql;
    }

    public SqlIdentifier getOriginTableName() {
        return originTableName;
    }

    @Override
    public SqlAlterSpecification replaceTableName(SqlIdentifier newTableName) {
        return new SqlModifyColumn(newTableName,
            colName,
            colDef,
            first,
            afterColumn,
            sourceSql,
            getParserPosition());
    }

    @Override
    public SqlAlterSpecification removeAfterColumn() {
        return new SqlModifyColumn((SqlIdentifier) tableName,
            colName,
            colDef,
            first,
            null,
            genSourceSqlWithOutAfter(sourceSql),
            getParserPosition());
    }

    @Override
    public boolean supportFileStorage() { return true;}
}
