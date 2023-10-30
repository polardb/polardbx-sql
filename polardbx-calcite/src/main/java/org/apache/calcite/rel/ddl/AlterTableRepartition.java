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
import org.apache.calcite.rel.core.DDL;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlDdl;
import org.apache.calcite.sql.SqlNode;

import java.util.List;
import java.util.Map;

/**
 * @author wumu
 */
public class AlterTableRepartition extends DDL {
    protected Map<SqlNode, RexNode> allRexExprInfo;

    protected AlterTableRepartition(RelOptCluster cluster, RelTraitSet traits, SqlDdl ddl, SqlNode tableName,
                                    Map<SqlNode, RexNode> allRexExprInfo) {
        super(cluster, traits, null);
        this.sqlNode = ddl;
        this.allRexExprInfo = allRexExprInfo;
        this.setTableName(tableName);
    }

    public static AlterTableRepartition create(RelOptCluster cluster, SqlDdl ddl, SqlNode tableName,
                                               Map<SqlNode, RexNode> allRexExprInfo) {
        return new AlterTableRepartition(cluster, cluster.traitSet(), ddl, tableName, allRexExprInfo);
    }

    @Override
    public AlterTableRepartition copy(
        RelTraitSet traitSet, List<RelNode> inputs) {
        assert traitSet.containsIfApplicable(Convention.NONE);
        return new AlterTableRepartition(this.getCluster(), traitSet, this.ddl, getTableName(), allRexExprInfo);
    }

    public Map<SqlNode, RexNode> getAllRexExprInfo() {
        return allRexExprInfo;
    }
}
