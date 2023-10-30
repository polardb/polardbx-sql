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

import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.Arrays;
import java.util.List;

/**
 * @author wenki
 */
public class SqlDropForeignKey extends SqlAlterSpecification {
    private static final SqlOperator OPERATOR =
        new SqlSpecialOperator("DROP FOREIGN KEY", SqlKind.DROP_FOREIGN_KEY);

    /**
     * Creates a SqlAlterTableDropIndex.
     */
    public SqlDropForeignKey(SqlIdentifier indexName, SqlIdentifier tableName, SqlIdentifier constraint, String sql,
                             SqlParserPos pos, boolean pushDown) {
        super(pos);
        this.tableName = tableName;
        this.originTableName = tableName;
        this.sourceSql = sql;
        this.indexName = indexName;
        this.constraint = constraint;
        this.pushDown = pushDown;
    }

    private SqlNode tableName;
    final private SqlIdentifier indexName;
    final private SqlIdentifier originTableName;
    private SqlIdentifier constraint;
    final private String sourceSql;
    private boolean pushDown = false;

    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    @Override
    public List<SqlNode> getOperandList() {
        return Arrays.asList(tableName);
    }

    public void setTargetTable(SqlNode tableName) {
        this.tableName = tableName;
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        final SqlWriter.Frame frame = writer.startList(SqlWriter.FrameTypeEnum.SELECT, "DROP FOREIGN KEY", "");

        indexName.unparse(writer, leftPrec, rightPrec);

        writer.endList(frame);
    }

    public SqlNode getTableName() {
        return tableName;
    }

    public String getSourceSql() {
        return sourceSql;
    }

    public SqlIdentifier getOriginTableName() {
        return originTableName;
    }

    public SqlIdentifier getIndexName() {
        return indexName;
    }

    public SqlIdentifier getConstraint() {
        return constraint;
    }

    public SqlIdentifier setConstraint(SqlIdentifier constraint) {
        return this.constraint = constraint;
    }

    public boolean isPushDown() {
        return pushDown;
    }

    public void setPushDown(boolean pushDown) {
        this.pushDown = pushDown;
    }
}
