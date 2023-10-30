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

import java.util.List;

public class SqlAlterTableReorgPartition extends SqlAlterSpecification {

    private static final SqlOperator OPERATOR =
        new SqlSpecialOperator("REORGANIZE PARTITION", SqlKind.REORGANIZE_PARTITION);

    private final List<SqlNode> names;
    private final List<SqlNode> partitions;
    protected final boolean isSubPartition;

    private SqlAlterTableGroup parent;

    public SqlAlterTableReorgPartition(SqlParserPos pos, List<SqlNode> names, List<SqlNode> partitions,
                                       boolean isSubPartition) {
        super(pos);
        this.names = names;
        this.partitions = partitions;
        this.isSubPartition = isSubPartition;
    }

    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    @Override
    public List<SqlNode> getOperandList() {
        return partitions;
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        final SqlWriter.Frame frame = writer.startList(SqlWriter.FrameTypeEnum.SELECT, "REORGANIZE", "");

        writer.keyword(isSubPartition ? "SUBPARTITION" : "PARTITION");

        unparse(writer, leftPrec, rightPrec, names);

        writer.keyword("INTO");

        final SqlWriter.Frame partFrame = writer.startList(SqlWriter.FrameTypeEnum.SELECT, "(", ")");

        unparse(writer, leftPrec, rightPrec, partitions);

        writer.endList(partFrame);

        writer.endList(frame);
    }

    private void unparse(SqlWriter writer, int leftPrec, int rightPrec, List<SqlNode> sqlNodes) {
        int i = 0;
        for (SqlNode sqlNode : sqlNodes) {
            sqlNode.unparse(writer, leftPrec, rightPrec);
            i++;
            if (i < sqlNodes.size()) {
                writer.sep(",");
            }
        }
    }

    public boolean isSubPartition() {
        return isSubPartition;
    }

    public List<SqlNode> getNames() {
        return names;
    }

    public List<SqlNode> getPartitions() {
        return partitions;
    }

    public SqlAlterTableGroup getParent() {
        return parent;
    }

    public void setParent(SqlAlterTableGroup parent) {
        this.parent = parent;
    }
}

