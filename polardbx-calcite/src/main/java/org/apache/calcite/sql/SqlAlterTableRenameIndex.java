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
 * @date 2019/1/28 12:43 PM
 */
public class SqlAlterTableRenameIndex extends SqlAlterSpecification {

    private static final SqlOperator OPERATOR = new SqlSpecialOperator("RENAME INDEX", SqlKind.ALTER_RENAME_INDEX);
    final private SqlIdentifier      originIndexName;
    final private SqlIdentifier      originNewIndexName;
    final private SqlIdentifier      indexName;
    final private SqlIdentifier      newIndexName;
    final private String             sourceSql;
    private SqlNode                  tableName;

    /**
     * Creates a SqlAlterTableRenameIndex.
     *
     * @param indexName
     * @param tableName
     * @param pos
     */
    public SqlAlterTableRenameIndex(SqlIdentifier tableName, SqlIdentifier indexName, SqlIdentifier newIndexName,
                                    String sql, SqlParserPos pos){
        super(pos);
        this.tableName = tableName;
        this.originIndexName = indexName;
        this.originNewIndexName = newIndexName;
        this.indexName = indexName;
        this.newIndexName = newIndexName;
        this.sourceSql = sql;
    }

    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    @Override
    public List<SqlNode> getOperandList() {
        return Arrays.asList(tableName, indexName);
    }

    public void setTargetTable(SqlNode tableName) {
        this.tableName = tableName;
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        final SqlWriter.Frame frame = writer.startList(SqlWriter.FrameTypeEnum.SELECT, "RENAME INDEX", "");

        indexName.unparse(writer, leftPrec, rightPrec);

        writer.endList(frame);
    }

    public SqlNode getTableName() {
        return tableName;
    }

    public SqlIdentifier getIndexName() {
        return indexName;
    }

    public String getSourceSql() {
        return sourceSql;
    }

    public SqlIdentifier getOriginIndexName() {
        return originIndexName;
    }

    public SqlIdentifier getOriginNewIndexName() {
        return originNewIndexName;
    }

    public SqlIdentifier getNewIndexName() {
        return newIndexName;
    }

    public String getNewIndexNameStr() {
        return newIndexName.getLastName();
    }
}
