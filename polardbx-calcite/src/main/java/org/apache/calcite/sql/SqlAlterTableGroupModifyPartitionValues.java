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
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;

import java.util.ArrayList;
import java.util.List;

/**
 * @author chenghui.lch
 */
public class SqlAlterTableGroupModifyPartitionValues extends SqlAlterTableModifyPartitionValues {

    private static final SqlOperator OPERATOR =
        new SqlSpecialOperator("MODIFY PARTITION VALUES", SqlKind.MODIFY_PARTITION);
    protected SqlNode parent;

    public SqlAlterTableGroupModifyPartitionValues(SqlParserPos pos,
                                                   SqlPartition partitionAst, boolean isAdd,
                                                   boolean isSubPartition) {
        super(pos, partitionAst, isAdd, isSubPartition);
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

    @Override
    public void validate(SqlValidator validator, SqlValidatorScope scope) {
        validator.setColumnReferenceExpansion(false);

        List<SqlNode> partDefs = new ArrayList<>();
        partDefs.add(partition);
        int partColCnt = -1;
        SqlPartitionBy.validatePartitionDefs(validator, scope, partDefs, partColCnt, -1, true, false);

    }
}
