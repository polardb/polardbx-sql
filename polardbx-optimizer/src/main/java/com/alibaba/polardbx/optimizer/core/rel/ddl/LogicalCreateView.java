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
import org.apache.calcite.rel.externalize.RelDrdsWriter;
import org.apache.calcite.sql.SqlNode;

import java.util.List;

/**
 * @author dylan
 */
public final class LogicalCreateView extends DDL {

    private final String schemaName;

    private final String viewName;

    private final boolean replace;

    private final List<String> columnList;

    private final SqlNode definition;

    public LogicalCreateView(RelOptCluster cluster, boolean replace, String schemaName, String viewName,
                             List<String> columnList,
                             SqlNode definition) {
        super(cluster, cluster.traitSetOf(DrdsConvention.INSTANCE), null);
        this.schemaName = schemaName;
        this.viewName = viewName;
        this.replace = replace;
        this.columnList = columnList;
        this.definition = definition;
    }

    public LogicalCreateView copy(
        RelTraitSet traitSet, List<RelNode> inputs) {
        assert traitSet.containsIfApplicable(DrdsConvention.INSTANCE);
        return new LogicalCreateView(this.getCluster(), replace, schemaName, viewName, columnList, definition);
    }

    public String getSchemaName() {
        return schemaName;
    }

    public String getViewName() {
        return viewName;
    }

    public List<String> getColumnList() {
        return columnList;
    }

    public SqlNode getDefinition() {
        return definition;
    }

    public boolean isReplace() {
        return replace;
    }

    @Override
    public RelWriter explainTermsForDisplay(RelWriter pw) {

        StringBuilder sqlBuilder = new StringBuilder();

        if (replace) {
            sqlBuilder.append("REPLACE VIEW ");
        } else {
            sqlBuilder.append("CREATE VIEW ");
        }
        sqlBuilder.append("`").append(schemaName).append("`.`").append(viewName).append("`");

        if (columnList != null && !columnList.isEmpty()) {
            sqlBuilder.append("(").append(String.join(",", columnList)).append(")");
        }

        sqlBuilder.append(" AS ").append(definition);

        pw.item(RelDrdsWriter.REL_NAME, "REPLACE VIEW ");

        return pw.item("sql", sqlBuilder.toString());
    }

}
