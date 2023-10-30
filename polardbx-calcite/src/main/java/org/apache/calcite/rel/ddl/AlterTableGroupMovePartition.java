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

package org.apache.calcite.rel.ddl;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlDdl;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Created by luoyanxin.
 *
 * @author luoyanxin
 */
public class AlterTableGroupMovePartition extends AlterTableGroupDdl {
    Map<String, Set<String>> targetPartitions;
    boolean subPartitionsMoved;

    protected AlterTableGroupMovePartition(RelOptCluster cluster, RelTraitSet traits, SqlDdl ddl,
                                           RelDataType rowType,
                                           String tableGroupName,
                                           Map<String, Set<String>> targetPartitions,
                                           boolean subPartitionsMoved) {
        super(cluster, traits, ddl, rowType);
        this.tableGroupName = tableGroupName;
        this.sqlNode = ddl;
        this.setTableName(new SqlIdentifier(tableGroupName, SqlParserPos.ZERO));
        this.targetPartitions = targetPartitions;
        this.subPartitionsMoved = subPartitionsMoved;
    }

    public static AlterTableGroupMovePartition create(RelOptCluster cluster, RelTraitSet traits, SqlDdl ddl,
                                                      RelDataType rowType,
                                                      String tableGroupName,
                                                      Map<String, Set<String>> targetPartitions,
                                                      boolean subPartitionsMoved) {

        return new AlterTableGroupMovePartition(cluster, traits, ddl, rowType, tableGroupName, targetPartitions,
            subPartitionsMoved);
    }

    @Override
    public AlterTableGroupMovePartition copy(
        RelTraitSet traitSet, List<RelNode> inputs) {
        assert traitSet.containsIfApplicable(Convention.NONE);
        return new AlterTableGroupMovePartition(this.getCluster(), traitSet, this.ddl, rowType, tableGroupName,
            targetPartitions, subPartitionsMoved);
    }

    @Override
    public String getTableGroupName() {
        return tableGroupName;
    }

    public Map<String, Set<String>> getTargetPartitions() {
        return targetPartitions;
    }

    public void setTargetPartitions(Map<String, Set<String>> targetPartitions) {
        this.targetPartitions = targetPartitions;
    }

    public boolean isSubPartitionsMoved() {
        return subPartitionsMoved;
    }

    public void setSubPartitionsMoved(boolean subPartitionsMoved) {
        this.subPartitionsMoved = subPartitionsMoved;
    }
}
