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
import org.apache.calcite.sql.SqlAlterJoinGroup;
import org.apache.calcite.sql.SqlDdl;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.List;

/**
 * Created by luoyanxin.
 *
 * @author luoyanxin
 */
public class AlterJoinGroup extends DDL {
    protected AlterJoinGroup(RelOptCluster cluster, RelTraitSet traits,
                             SqlDdl ddl, RelDataType rowType) {
        super(cluster, traits, ddl, rowType);
        this.sqlNode = ddl;
        this.setTableName(new SqlIdentifier("-", SqlParserPos.ZERO));
    }

    public static AlterJoinGroup create(SqlAlterJoinGroup sqlAlterJoinGroup, RelDataType rowType,
                                        RelOptCluster cluster) {
        return new AlterJoinGroup(cluster, cluster.traitSetOf(Convention.NONE), sqlAlterJoinGroup, rowType);
    }

    @Override
    public AlterJoinGroup copy(RelTraitSet traitSet, List<RelNode> inputs) {
        assert traitSet.containsIfApplicable(Convention.NONE);
        return new AlterJoinGroup(this.getCluster(), traitSet, ((AlterJoinGroup) inputs.get(0)).getAst(), rowType);
    }
}
