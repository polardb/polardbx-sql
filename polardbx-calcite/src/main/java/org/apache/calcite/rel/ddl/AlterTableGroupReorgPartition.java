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
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlDdl;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.List;
import java.util.Map;

public class AlterTableGroupReorgPartition extends AlterTableGroupDdl {
    final Map<Integer, Map<SqlNode, RexNode>> partRexInfoCtxByLevel;

    protected AlterTableGroupReorgPartition(RelOptCluster cluster, RelTraitSet traits, SqlDdl ddl,
                                            RelDataType rowType,
                                            Map<Integer, Map<SqlNode, RexNode>> partRexInfoCtxByLevel,
                                            String tableGroupName) {
        super(cluster, traits, ddl, rowType);
        this.partRexInfoCtxByLevel = partRexInfoCtxByLevel;
        this.tableGroupName = tableGroupName;
        this.sqlNode = ddl;
        this.setTableName(new SqlIdentifier(tableGroupName, SqlParserPos.ZERO));
    }

    public static AlterTableGroupReorgPartition create(RelOptCluster cluster, RelTraitSet traits, SqlDdl ddl,
                                                       RelDataType rowType,
                                                       Map<Integer, Map<SqlNode, RexNode>> partRexInfoCtxByLevel,
                                                       String tableGroupName) {

        return new AlterTableGroupReorgPartition(cluster, traits, ddl, rowType, partRexInfoCtxByLevel,
            tableGroupName);
    }

    @Override
    public AlterTableGroupReorgPartition copy(
        RelTraitSet traitSet, List<RelNode> inputs) {
        assert traitSet.containsIfApplicable(Convention.NONE);
        return new AlterTableGroupReorgPartition(this.getCluster(), traitSet, this.ddl, rowType,
            partRexInfoCtxByLevel, tableGroupName);
    }

    public Map<Integer, Map<SqlNode, RexNode>> getPartRexInfoCtxByLevel() {
        return partRexInfoCtxByLevel;
    }

    @Override
    public String getTableGroupName() {
        return tableGroupName;
    }
}
