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

import com.alibaba.polardbx.common.exception.TddlNestableRuntimeException;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.CursorMeta;
import com.alibaba.polardbx.optimizer.core.dialect.DbType;
import com.alibaba.polardbx.optimizer.core.rel.BaseQueryOperation;
import com.alibaba.polardbx.optimizer.utils.CalciteUtils;
import com.alibaba.polardbx.optimizer.utils.RelUtils;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.DDL;
import org.apache.calcite.rel.externalize.RelDrdsWriter;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlDdl;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.util.Util;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public abstract class BaseDdlOperation extends BaseQueryOperation {

    public DDL relDdl;
    protected String tableName;
    private String phyTable;

    protected Map<String, List<List<String>>> targetTablesHintCache;

    public BaseDdlOperation(RelOptCluster cluster, RelTraitSet traitSet, SqlDdl sqlDdl, RelDataType rowType) {
        super(cluster, traitSet, RelUtils.toNativeSqlLine(sqlDdl), sqlDdl, DbType.MYSQL);
        this.rowType = rowType;
        this.cursorMeta = CursorMeta.build(CalciteUtils.buildColumnMeta(rowType, "Ddl"));
    }

    protected BaseDdlOperation(RelOptCluster cluster, RelTraitSet traits, DDL ddl) {
        this(cluster, traits, (SqlDdl) ddl.getSqlNode(), ddl.getRowType());
        this.relDdl = ddl;
    }

    public BaseDdlOperation(DDL ddl) {
        this(ddl.getCluster(), ddl.getTraitSet(), ddl);
        final SqlIdentifier tableName = (SqlIdentifier) ddl.getTableName();
        if (tableName.isSimple()) {
            this.tableName = tableName.getSimple();
            this.schemaName = PlannerContext.getPlannerContext(ddl).getSchemaName();
        } else {
            final String schemaName = tableName.names.get(0);
            if (OptimizerContext.getContext(schemaName) == null) {
                throw new TddlNestableRuntimeException("Unknown database " + schemaName);
            }
            this.tableName = Util.last(tableName.names);
            this.schemaName = schemaName;
        }
    }

    @Override
    public String getSchemaName() {
        return this.schemaName;
    }

    @Override
    public void setSchemaName(String schemaName) {
        this.schemaName = schemaName;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getPhyTable() {
        return phyTable;
    }

    public void setPhyTable(String phyTable) {
        this.phyTable = phyTable;
    }

    public SqlNode getTableNameNode() {
        return relDdl.getTableName();
    }

    @Override
    public SqlKind getKind() {
        return relDdl.sqlNode.getKind();
    }

    public Map<String, List<List<String>>> getTargetTablesHintCache() {
        return targetTablesHintCache;
    }

    public void setTargetTablesHintCache(Map<String, List<List<String>>> targetTablesHintCache) {
        this.targetTablesHintCache = targetTablesHintCache;
    }

    @Override
    public Pair<String, Map<Integer, ParameterContext>> getDbIndexAndParam(Map<Integer, ParameterContext> param,
                                                                           ExecutionContext executionContext) {
        param = new HashMap<>();
        return Pair.of("", param);
    }

    @Override
    public SqlNodeList getHints() {
        return ((SqlDdl) relDdl.sqlNode).getHints();
    }

    @Override
    public RelNode setHints(SqlNodeList hints) {
        ((SqlDdl) relDdl.sqlNode).setHints(hints);
        return this;
    }

    @Override
    public RelWriter explainTermsForDisplay(RelWriter pw) {
        pw.item(RelDrdsWriter.REL_NAME, getExplainName());
        pw.item("sql", this.sqlTemplate);
        return pw;
    }

    @Override
    protected String getExplainName() {
        return "LogicalDdl";
    }

    @Override
    protected RelDataType deriveRowType() {
        return relDdl.getRowType();
    }

}
