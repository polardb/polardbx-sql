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

package com.alibaba.polardbx.optimizer.core.rel.dal;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.alibaba.polardbx.optimizer.core.dialect.DbType;
import com.alibaba.polardbx.optimizer.utils.CalciteUtils;
import com.alibaba.polardbx.optimizer.utils.RelUtils;
import com.alibaba.polardbx.optimizer.core.CursorMeta;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.externalize.RelDrdsWriter;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlDal;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlShow;
import org.apache.calcite.sql.parser.SqlParserPos;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.optimizer.core.rel.BaseQueryOperation;
import com.alibaba.polardbx.optimizer.utils.ExplainUtils;

/**
 * @author chenmo.cm
 */
public abstract class BaseDalOperation extends BaseQueryOperation {

    protected final RelDataType rowType;
    protected Map<String, List<List<String>>> targetTable;
    protected List<String> tableNames = new ArrayList<>();
    protected String phyTable;
    protected boolean removeDbPrefix = true;

    public BaseDalOperation(RelOptCluster cluster, RelTraitSet traitSet, SqlNode nativeSqlNode, RelDataType rowType,
                            String dbIndex, String phyTable, String schemaName) {
        this(cluster,
            traitSet,
            RelUtils.toNativeSqlLine(nativeSqlNode),
            nativeSqlNode,
            DbType.MYSQL,
            rowType,
            null,
            null,
            dbIndex,
            phyTable,
            schemaName);
    }

    public BaseDalOperation(RelOptCluster cluster, RelTraitSet traitSet, SqlNode nativeSqlNode, RelDataType rowType,
                            Map<String, List<List<String>>> targetTable, List<String> tableNames, String schemaName) {
        this(cluster,
            traitSet,
            RelUtils.toNativeSqlLine(nativeSqlNode),
            nativeSqlNode,
            DbType.MYSQL,
            rowType,
            targetTable,
            tableNames,
            null,
            null,
            schemaName);
    }

    public BaseDalOperation(RelOptCluster cluster, RelTraitSet traitSet, String sqlTemplate, SqlNode nativeSqlNode,
                            DbType dbType, RelDataType rowType, Map<String, List<List<String>>> targetTable,
                            List<String> tableNames, String dbIndex, String phyTable, String schemaName) {
        super(cluster, traitSet, sqlTemplate, nativeSqlNode, dbType);
        this.rowType = rowType;
        this.tableNames = tableNames;
        this.targetTable = targetTable;
        this.dbIndex = dbIndex;
        this.phyTable = phyTable;
        this.kind = kind();
        this.schemaName = schemaName;
        this.cursorMeta = CursorMeta.build(CalciteUtils.buildColumnMeta(rowType, "Dal"));
    }

    @Override
    protected RelDataType deriveRowType() {
        return rowType;
    }

    @Override
    public RelWriter explainTermsForDisplay(RelWriter pw) {
        if (single()) {
            return super.explainTermsForDisplay(pw);
        } else {
            pw.item(RelDrdsWriter.REL_NAME, getExplainName());
            pw.item("node", ExplainUtils.compressName(targetTable.keySet()));
            pw.item("sql", this.sqlTemplate);
            return pw;
        }
    }

    public List<RelNode> getInput(Map<Integer, ParameterContext> params) {
        final SqlDal dal = (SqlDal) this.nativeSqlNode;
        // remove db name by default
        if (isRemoveDbPrefix()) {
            dal.setDbName((SqlNode) null);
        }
        final SqlNode tableName = dal.getTableName();
        if (null != tableName && TStringUtil.isNotBlank(phyTable)) {
            dal.setTableName(phyTable);
            this.sqlTemplate = RelUtils.toNativeSqlLine(dal);
        }
        return ImmutableList.of(PhyDal.create(this, dbIndex, phyTable));
    }

    @Override
    public RelNode getInput(int i) {
        return getInput(ImmutableMap.of()).get(i);
    }

    public SqlKind kind() {
        if (getNativeSqlNode() instanceof SqlShow) {
            return ((SqlShow) getNativeSqlNode()).getShowKind();
        } else {
            return getNativeSqlNode().getKind();
        }
    }

    @Override
    public SqlKind getKind() {
        return getNativeSqlNode().getKind();
    }

    public boolean single() {
        return null == targetTable || targetTable.isEmpty();
    }

    public Map<String, List<List<String>>> getTargetTable() {
        return targetTable;
    }

    public void setTargetTable(Map<String, List<List<String>>> targetTable) {
        this.targetTable = targetTable;
    }

    @Override
    public SqlNodeList getHints() {
        return ((SqlDal) getNativeSqlNode()).getHints();
    }

    @Override
    public RelNode setHints(SqlNodeList hints) {
        ((SqlDal) getNativeSqlNode()).setHints(hints);
        return this;
    }

    public String getPhyTable() {
        return phyTable;
    }

    public void setPhyTable(String phyTable) {
        this.phyTable = phyTable;
    }

    public SqlNode getPhyTableNode() {
        return new SqlIdentifier(phyTable, SqlParserPos.ZERO);
    }

    public boolean isRemoveDbPrefix() {
        return removeDbPrefix;
    }

    public void setRemoveDbPrefix(boolean removeDbPrefix) {
        this.removeDbPrefix = removeDbPrefix;
    }
}
