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
public class SqlAlterTableGroupMovePartition extends SqlAlterSpecification {

    private static final SqlOperator OPERATOR = new SqlSpecialOperator("MOVE PARTITION", SqlKind.MOVE_PARTITION);
    private final SqlNode targetStorageId;
    private final List<SqlNode> oldPartitions;
    private SqlAlterTableGroup parent;

    public SqlAlterTableGroupMovePartition(SqlParserPos pos, SqlNode targetStorageId, List<SqlNode> oldPartitions) {
        super(pos);
        this.targetStorageId = targetStorageId;
        this.oldPartitions = oldPartitions == null ? new ArrayList<>() : oldPartitions;
    }

    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    @Override
    public List<SqlNode> getOperandList() {
        return null;
    }

    public SqlAlterTableGroup getParent() {
        return parent;
    }

    public void setParent(SqlAlterTableGroup parent) {
        this.parent = parent;
    }

    public SqlNode getTargetStorageId() {
        return targetStorageId;
    }

    public List<SqlNode> getOldPartitions() {
        return oldPartitions;
    }
}