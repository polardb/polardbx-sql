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
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;

import java.util.List;

public final class DropMaterializedView extends DDL {

    private final String schemaName;

    private final String viewName;

    protected DropMaterializedView(RelOptCluster cluster, RelTraitSet traits, String schemaName, String viewName) {
        super(cluster, traits, null);
        this.schemaName = schemaName;
        this.viewName = viewName;
    }

    public static DropMaterializedView create(RelOptCluster cluster, String schemaName, String tableName) {
        return new DropMaterializedView(cluster, cluster.traitSet(), schemaName, tableName);
    }

    @Override
    public DropMaterializedView copy(
        RelTraitSet traitSet, List<RelNode> inputs) {
        assert traitSet.containsIfApplicable(Convention.NONE);
        return new DropMaterializedView(this.getCluster(), traitSet, this.schemaName, this.viewName);
    }

    public String getViewName() {
        return viewName;
    }

    @Override
    public String getSchemaName() {
        return schemaName;
    }

    @Override
    public SqlKind kind() {
        return SqlKind.DROP_MATERIALIZED_VIEW;
    }
}
