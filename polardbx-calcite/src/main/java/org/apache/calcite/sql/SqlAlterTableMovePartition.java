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
import java.util.Map;
import java.util.Set;

/**
 * Created by luoyanxin.
 *
 * @author luoyanxin
 */
public class SqlAlterTableMovePartition extends SqlAlterSpecification {

    private static final SqlOperator OPERATOR = new SqlSpecialOperator("MOVE PARTITION", SqlKind.MOVE_PARTITION);

    private Map<String, Set<String>> targetPartitions;
    private final boolean subPartitionsMoved;

    public Map<String, String> getLocalities() {
        return localities;
    }

    public void setLocalities(Map<String, String> localities) {
        this.localities = localities;
    }

    private Map<String, String> localities;

    public SqlAlterTableMovePartition(SqlParserPos pos,
                                      Map<String, Set<String>> targetPartitions,
                                      boolean subPartitionsMoved,
                                      Map<String, String> localities) {
        super(pos);
        this.targetPartitions = targetPartitions;
        this.subPartitionsMoved = subPartitionsMoved;
        this.localities = localities;
    }

    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    @Override
    public List<SqlNode> getOperandList() {
        return null;
    }

    public Map<String, Set<String>> getTargetPartitions() {
        return targetPartitions;
    }

    public void setTargetPartitions(Map<String, Set<String>> targetPartitions) {
        this.targetPartitions = targetPartitions;
    }

    @Override
    public boolean supportFileStorage() { return true;}

    public boolean isSubPartitionsMoved() {
        return subPartitionsMoved;
    }

}
