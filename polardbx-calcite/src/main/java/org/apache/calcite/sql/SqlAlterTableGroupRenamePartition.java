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
import org.apache.calcite.util.Pair;

import java.util.List;

/**
 * Created by luoyanxin.
 *
 * @author luoyanxin
 */
public class SqlAlterTableGroupRenamePartition extends SqlAlterSpecification {

    private static final SqlOperator OPERATOR = new SqlSpecialOperator("RENAME PARTITION", SqlKind.ALTER_TABLEGROUP_RENAME_PARTITION);

    private final List<Pair<String, String>> changePartitionsPair;

    private SqlAlterTableGroup parent;

    public SqlAlterTableGroupRenamePartition(SqlParserPos pos, List<Pair<String, String>> changePartitionsPair) {
        super(pos);
        this.changePartitionsPair = changePartitionsPair;
    }

    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    public void setParent(SqlAlterTableGroup parent) {
        this.parent = parent;
    }

    @Override
    public List<SqlNode> getOperandList() {
        return null;
    }

    public List<Pair<String, String>> getChangePartitionsPair() {
        return changePartitionsPair;
    }

    public SqlAlterTableGroup getParent() {
        return parent;
    }
}
