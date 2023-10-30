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

import com.alibaba.polardbx.common.utils.Pair;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.List;

/**
 * Created by luoyanxin.
 *
 * @author luoyanxin
 */
public class SqlAlterTableRenamePartition extends SqlAlterSpecification {

    private static final SqlOperator OPERATOR =
        new SqlSpecialOperator("RENAME PARTITION", SqlKind.RENAME_PARTITION);

    private final List<Pair<String, String>> changePartitionsPair;
    private final boolean subPartitionsRename;


    public SqlAlterTableRenamePartition(SqlParserPos pos, List<Pair<String, String>> changePartitionsPair, boolean subPartitionsRename) {
        super(pos);
        this.changePartitionsPair = changePartitionsPair;
        this.subPartitionsRename = subPartitionsRename;
    }

    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    @Override
    public List<SqlNode> getOperandList() {
        return null;
    }

    public List<Pair<String, String>> getChangePartitionsPair() {
        return changePartitionsPair;
    }

    public boolean isSubPartitionsRename() {
        return subPartitionsRename;
    }
}
