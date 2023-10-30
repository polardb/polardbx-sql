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
 * Created by zhuqiwei.
 *
 * @author zhuqiwei
 */
public class SqlAlterTableAlterIndex extends SqlAlterSpecification {
    final private SqlIdentifier tableName;
    final private SqlIdentifier indexName;
    private String indexVisibility = null;
    private boolean isAlterIndexVisibility = false;

    private static final SqlOperator OPERATOR =
        new SqlSpecialOperator("ALTER INDEX VISIBILITY", SqlKind.ALTER_INDEX_VISIBILITY);

    public SqlAlterTableAlterIndex(SqlIdentifier tableName, SqlIdentifier indexName, SqlParserPos pos) {
        super(pos);
        this.tableName = tableName;
        this.indexName = indexName;
    }

    public String getIndexVisibility() {
        return indexVisibility;
    }

    public void setIndexVisibility(String indexVisibility) {
        this.indexVisibility = indexVisibility;
        this.isAlterIndexVisibility = true;
    }

    public boolean isAlterIndexVisibility() {
        return isAlterIndexVisibility;
    }

    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    @Override
    public List<SqlNode> getOperandList() {
        return Arrays.asList(tableName, indexName);
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        final SqlWriter.Frame frame = writer.startList(SqlWriter.FrameTypeEnum.SELECT, "ALTER INDEX ", "");

        indexName.unparse(writer, leftPrec, rightPrec);

        if (this.isAlterIndexVisibility && this.indexVisibility != null) {
            writer.keyword(this.indexVisibility);
        }

        writer.endList(frame);
    }

    public SqlIdentifier getTableName() {
        return tableName;
    }

    public SqlIdentifier getIndexName() {
        return indexName;
    }

    public String getVisibility() {
        return this.indexVisibility;
    }
}
