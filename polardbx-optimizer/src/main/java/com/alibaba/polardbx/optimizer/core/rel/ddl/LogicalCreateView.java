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

import com.alibaba.polardbx.druid.sql.SQLUtils;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.core.DrdsConvention;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.DDL;
import org.apache.calcite.rel.ddl.CreateFileStorage;
import org.apache.calcite.rel.ddl.CreateFunction;
import org.apache.calcite.rel.ddl.CreateJavaFunction;
import org.apache.calcite.rel.ddl.CreateView;
import org.apache.calcite.rel.externalize.RelDrdsWriter;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlCreateJavaFunction;
import org.apache.calcite.sql.SqlCreateView;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;

import java.util.List;
import java.util.stream.Collectors;

/**
 * @author dylan
 */
public final class LogicalCreateView extends BaseDdlOperation {

    private final boolean replace;

    private final boolean alter;

    private final List<String> columnList;

    private final SqlNode definition;

    public LogicalCreateView(DDL ddl) {
        super(ddl);

        final SqlCreateView sqlCreateView = (SqlCreateView) relDdl.getSqlNode();

        SqlNodeList columns = sqlCreateView.getColumnList();
        List<String> columnList = null;
        if (columns != null && columns.size() > 0) {
            columnList = columns.getList().stream().map(x -> x.toString()).map(SQLUtils::normalizeNoTrim)
                .collect(Collectors.toList());
        }
        this.replace = sqlCreateView.isReplace();
        this.alter = sqlCreateView.isAlter();
        this.columnList = columnList;
        this.definition = sqlCreateView.getQuery();
    }

    public static LogicalCreateView create(CreateView createView) {
        return new LogicalCreateView(createView);
    }

    @Override
    public boolean isSupportedByFileStorage() {
        return true;
    }

    @Override
    public boolean isSupportedByBindFileStorage() {
        return true;
    }

    public String getSchemaName() {
        return schemaName;
    }

    public String getViewName() {
        return getTableName();
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

    public boolean isAlter() {
        return alter;
    }

    @Override
    public RelWriter explainTermsForDisplay(RelWriter pw) {
        StringBuilder sqlBuilder = new StringBuilder();

        if (replace) {
            sqlBuilder.append("REPLACE VIEW ");
        } else {
            sqlBuilder.append("CREATE VIEW ");
        }
        sqlBuilder.append("`").append(schemaName).append("`.`").append(getViewName()).append("`");

        if (columnList != null && !columnList.isEmpty()) {
            sqlBuilder.append("(").append(String.join(",", columnList)).append(")");
        }

        sqlBuilder.append(" AS ").append(definition);

        pw.item(RelDrdsWriter.REL_NAME, "REPLACE VIEW ");

        return pw.item("sql", sqlBuilder.toString());
    }
}
