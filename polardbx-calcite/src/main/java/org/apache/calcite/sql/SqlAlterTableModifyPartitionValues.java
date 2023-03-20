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

/**
 * Created by luoyanxin.
 *
 * @author luoyanxin
 */
public class SqlAlterTableModifyPartitionValues extends SqlAlterSpecification {

    private static final SqlOperator OPERATOR =
        new SqlSpecialOperator("MODIFY PARTITION VALUES", SqlKind.MODIFY_PARTITION);

    protected final SqlPartition partition;
    protected final boolean isAdd;
    protected final boolean isDrop;

    protected SqlNode parent;

    public SqlAlterTableModifyPartitionValues(SqlParserPos pos, SqlPartition partitionDef, boolean isAdd) {
        super(pos);
        this.partition = partitionDef;
        this.isAdd = isAdd;
        this.isDrop = !isAdd;
    }

    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    @Override
    public List<SqlNode> getOperandList() {
        return ImmutableList.of(partition);
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        final SqlWriter.Frame frame = writer.startList(SqlWriter.FrameTypeEnum.SELECT, isAdd ? "ADD" : "DROP", "");

        partition.unparse(writer, leftPrec, rightPrec);

        writer.endList(frame);
    }

    public boolean isAdd() {
        return isAdd;
    }

    public boolean isDrop() {
        return isDrop;
    }

    public SqlPartition getPartition() {
        return partition;
    }

    public SqlNode getParent() {
        return parent;
    }

    public void setParent(SqlNode parent) {
        this.parent = parent;
    }
}
