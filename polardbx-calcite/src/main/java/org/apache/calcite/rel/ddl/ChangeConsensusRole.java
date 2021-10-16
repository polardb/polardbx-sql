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
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlChangeConsensusRole;
import org.apache.calcite.sql.SqlDdl;

import java.util.List;

/**
 * @author moyi
 * @since 2021/03
 */
public class ChangeConsensusRole extends DDL {

    public ChangeConsensusRole(RelOptCluster cluster, RelTraitSet traitSet, SqlDdl ddl, RelDataType rowType){
        super(cluster, traitSet, ddl, rowType);
    }

    public static ChangeConsensusRole create(SqlChangeConsensusRole changeRole, RelDataType rowType, RelOptCluster cluster) {
        return new ChangeConsensusRole(cluster, cluster.traitSetOf(Convention.NONE), changeRole, rowType);
    }

    @Override
    public ChangeConsensusRole copy(RelTraitSet traitSet, List<RelNode> inputs) {
        assert traitSet.containsIfApplicable(Convention.NONE);
        return new ChangeConsensusRole(this.getCluster(), traitSet, ((ChangeConsensusRole) inputs.get(0)).getAst(), rowType);
    }
}
