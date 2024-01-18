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
import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.List;

public class SqlAlterTableDropFile extends SqlAlterSpecification {
    private static final SqlOperator OPERATOR =
        new SqlSpecialOperator("DROP FILE", SqlKind.DROP_FILE);

    private SqlNode tableName;
    final private SqlIdentifier originTableName;
    final private List<SqlIdentifier> fileNames;
    final private String sourceSql;

    public SqlAlterTableDropFile(SqlIdentifier tableName, List<SqlIdentifier> fileNames , String sql, SqlParserPos pos) {
        super(pos);
        this.tableName = tableName;
        this.originTableName = tableName;
        this.fileNames = fileNames;
        this.sourceSql = sql;
    }

    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        final SqlWriter.Frame frame = writer.startList(SqlWriter.FrameTypeEnum.SELECT, "DROP FILE", "");

        final SqlWriter.Frame fileFrame = writer.startList(SqlWriter.FrameTypeEnum.SELECT, "", "");
        int i = 0;
        for (SqlIdentifier sqlIdentifier : fileNames) {
            sqlIdentifier.unparse(writer, leftPrec, rightPrec);
            i++;
            if (i < fileNames.size()) {
                writer.sep(",");
            }
        }

        writer.endList(fileFrame);

        writer.endList(frame);
    }

    @Override
    public List<SqlNode> getOperandList() {
        List<SqlNode> operands = ImmutableList.<SqlNode>builder()
            .add(tableName).addAll(fileNames)
            .build();
        return operands;
    }

    public SqlNode getTableName() {
        return tableName;
    }

    public SqlIdentifier getOriginTableName() {
        return originTableName;
    }

    public List<SqlIdentifier> getFileNames() {
        return fileNames;
    }

    public String getSourceSql() {
        return sourceSql;
    }

    @Override
    public boolean supportFileStorage() { return true;}
}
