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

package com.alibaba.polardbx.optimizer.core.rel.ddl;

import com.alibaba.polardbx.optimizer.core.DrdsConvention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.DDL;

import java.util.List;

/**
 * @author dylan
 */
public class LogicalDropMaterializedView extends DDL {

    private final String schemaName;

    private final String viewName;

    private final boolean ifExists;

    public LogicalDropMaterializedView(RelOptCluster cluster, String schemaName, String viewName, boolean ifExists) {
        super(cluster, cluster.traitSetOf(DrdsConvention.INSTANCE), null);
        this.schemaName = schemaName;
        this.viewName = viewName;
        this.ifExists = ifExists;
    }

    @Override
    public LogicalDropMaterializedView copy(
        RelTraitSet traitSet, List<RelNode> inputs) {
        assert traitSet.containsIfApplicable(DrdsConvention.INSTANCE);
        return new LogicalDropMaterializedView(this.getCluster(), schemaName, viewName, ifExists);
    }

    public String getViewName() {
        return viewName;
    }

    public boolean isIfExists() {
        return ifExists;
    }

    @Override
    public String getSchemaName() {
        return schemaName;
    }

    @Override
    public RelWriter explainTermsForDisplay(RelWriter pw) {
        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append("DROP MATERIALIZED VIEW ");
        if (ifExists) {
            sqlBuilder.append("IF EXISTS ");
        }
        sqlBuilder.append("`").append(schemaName).append("`.`").append(viewName).append("`");
        return pw.item("sql", sqlBuilder.toString());
    }
}

