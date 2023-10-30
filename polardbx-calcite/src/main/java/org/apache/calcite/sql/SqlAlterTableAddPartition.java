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

import com.alibaba.polardbx.common.utils.GeneralUtil;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by luoyanxin.
 *
 * @author luoyanxin
 */
public class SqlAlterTableAddPartition extends SqlAlterSpecification {

    private static final SqlOperator OPERATOR = new SqlSpecialOperator("ADD PARTITION", SqlKind.ADD_PARTITION);

    protected final List<SqlNode> partitions;

    protected SqlNode parent;

    protected boolean isSubPartition;

    protected String algorithm = null;

    public SqlAlterTableAddPartition(SqlParserPos pos, List<SqlNode> partitions, boolean isSubPartition) {
        super(pos);
        this.partitions = partitions;
        this.isSubPartition = isSubPartition;
    }

    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    @Override
    public List<SqlNode> getOperandList() {
        return this.partitions;
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        final SqlWriter.Frame frame = writer.startList(SqlWriter.FrameTypeEnum.SELECT, "ADD", "");

        writer.keyword("PARTITION");
        final SqlWriter.Frame partFrame = writer.startList(SqlWriter.FrameTypeEnum.SELECT, "(", ")");
        int i = 0;
        for (SqlNode sqlNode : partitions) {
            sqlNode.unparse(writer, leftPrec, rightPrec);
            i++;
            if (i < partitions.size()) {
                writer.sep(",");
            }
        }
        writer.endList(partFrame);

        writer.endList(frame);
    }

    public List<SqlNode> getPartitions() {
        return partitions;
    }

    public SqlNode getParent() {
        return parent;
    }

    public void setParent(SqlNode parent) {
        this.parent = parent;
    }

    public boolean isSubPartition() {
        return isSubPartition;
    }

    public void setSubPartition(boolean subPartition) {
        isSubPartition = subPartition;
    }

    public String getAlgorithm() {
        return algorithm;
    }

    public void setAlgorithm(String algorithm) {
        this.algorithm = algorithm;
    }

    @Override
    public void validate(SqlValidator validator, SqlValidatorScope scope) {
        validator.setColumnReferenceExpansion(false);
        if (GeneralUtil.isNotEmpty(partitions)) {
            List<SqlNode> partDefs = new ArrayList<>();
            partDefs.addAll(partitions);
            int partColCnt = -1;
            SqlPartitionBy.validatePartitionDefs(validator, scope, partDefs, partColCnt, -1, true, false);
        }
    }
}
