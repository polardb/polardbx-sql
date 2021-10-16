/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
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
import org.apache.calcite.sql.SqlNode;

import java.util.List;

public final class DropIndex extends DDL {

    protected DropIndex(RelOptCluster cluster, RelTraitSet traits, SqlNode sqlNode, SqlNode tableNameNode) {
        super(cluster, traits, null);
        this.sqlNode = sqlNode;
        this.setTableName(tableNameNode);
    }

    public static DropIndex create(RelOptCluster cluster, SqlNode sqlNode, SqlNode tableName) {
        return new DropIndex(cluster, cluster.traitSet(), sqlNode, tableName);
    }

    @Override
    public DropIndex copy(
        RelTraitSet traitSet, List<RelNode> inputs) {
        assert traitSet.containsIfApplicable(Convention.NONE);
        return new DropIndex(this.getCluster(), traitSet, this.sqlNode, getTableName());
    }

}
