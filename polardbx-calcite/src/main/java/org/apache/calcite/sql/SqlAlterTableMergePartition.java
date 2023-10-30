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

import java.util.ArrayList;
import java.util.List;

/**
 * Created by luoyanxin.
 *
 * @author luoyanxin
 */
public class SqlAlterTableMergePartition extends SqlAlterSpecification {

    private static final SqlOperator OPERATOR = new SqlSpecialOperator("MERGE PARTITION", SqlKind.MERGE_PARTITION);
    private final SqlNode targetPartitionName;
    private final List<SqlNode> oldPartitions;
    private final boolean subPartitionsMerge;

    public SqlAlterTableMergePartition(SqlParserPos pos, SqlNode targetPartitionName, List<SqlNode> oldPartitions,
                                       boolean subPartitionsMerge) {
        super(pos);
        this.targetPartitionName = targetPartitionName;
        this.oldPartitions = oldPartitions == null ? new ArrayList<>() : oldPartitions;
        this.subPartitionsMerge = subPartitionsMerge;
    }

    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    @Override
    public List<SqlNode> getOperandList() {
        return null;
    }

    public SqlNode getTargetPartitionName() {
        return targetPartitionName;
    }

    public List<SqlNode> getOldPartitions() {
        return oldPartitions;
    }

    public boolean isSubPartitionsMerge() {
        return subPartitionsMerge;
    }
}

