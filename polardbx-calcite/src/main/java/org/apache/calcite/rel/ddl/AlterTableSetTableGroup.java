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
import org.apache.calcite.sql.SqlDdl;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;

import java.util.List;

/**
 * Created by luoyanxin.
 *
 * @author luoyanxin
 */
public class AlterTableSetTableGroup extends DDL {
    final String tableGroupName;
    final List<SqlIdentifier> objectNames;
    final boolean force;
    final boolean implicit;

    protected AlterTableSetTableGroup(RelOptCluster cluster, RelTraitSet traits, SqlDdl ddl,
                                      RelDataType rowType,
                                      List<SqlIdentifier> objectNames,
                                      SqlNode tableName,
                                      String tableGroupName,
                                      boolean implicit,
                                      boolean force) {
        super(cluster, traits, ddl, rowType);
        this.tableGroupName = tableGroupName;
        this.sqlNode = ddl;
        this.objectNames = objectNames;
        this.setTableName(tableName);
        this.implicit = implicit;
        this.force = force;
    }

    public static AlterTableSetTableGroup create(RelOptCluster cluster, RelTraitSet traits, SqlDdl ddl,
                                                 RelDataType rowType, List<SqlIdentifier> objectNames,
                                                 SqlNode tableName,
                                                 String tableGroupName, boolean implicit, boolean force) {

        return new AlterTableSetTableGroup(cluster, traits, ddl, rowType, objectNames, tableName, tableGroupName,
            implicit, force);
    }

    @Override
    public AlterTableSetTableGroup copy(
        RelTraitSet traitSet, List<RelNode> inputs) {
        assert traitSet.containsIfApplicable(Convention.NONE);
        return new AlterTableSetTableGroup(this.getCluster(), traitSet, this.ddl, rowType, this.objectNames,
            getTableName(), tableGroupName, implicit, force);
    }

    public String getTableGroupName() {
        return tableGroupName;
    }

    public List<SqlIdentifier> getObjectNames() {
        return objectNames;
    }

    public boolean isForce() {
        return force;
    }

    public boolean isImplicit() {
        return implicit;
    }
}
