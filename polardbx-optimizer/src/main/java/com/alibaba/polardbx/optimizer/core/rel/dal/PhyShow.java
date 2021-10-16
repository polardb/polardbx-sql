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

import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.dialect.DbType;
import com.alibaba.polardbx.optimizer.utils.RelUtils;
import com.alibaba.polardbx.optimizer.utils.ExplainUtils;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.dal.Show;
import org.apache.calcite.rel.externalize.RelDrdsWriter;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlDal;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlShow;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.commons.collections.CollectionUtils;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

/**
 * @author chenmo.cm
 */
public class PhyShow extends BaseDalOperation {

    public PhyShow(RelOptCluster cluster, RelTraitSet traitSet, SqlNode nativeSqlNode, RelDataType rowType,
                   String dbIndex, String phyTable, String schemaName) {
        super(cluster, traitSet, nativeSqlNode, rowType, dbIndex, phyTable, schemaName);
    }

    public PhyShow(RelOptCluster cluster, RelTraitSet traitSet, SqlNode nativeSqlNode, RelDataType rowType,
                   String dbIndex, String phyTable) {
        super(cluster, traitSet, nativeSqlNode, rowType, dbIndex, phyTable, null);
    }

    public PhyShow(RelOptCluster cluster, RelTraitSet traitSet, SqlNode nativeSqlNode, RelDataType rowType,
                   Map<String, List<List<String>>> targetTable, List<String> tableNames, String schemaName) {
        super(cluster, traitSet, nativeSqlNode, rowType, targetTable, tableNames, schemaName);
    }

    public PhyShow(RelOptCluster cluster, RelTraitSet traitSet, String sqlTemplate, SqlNode nativeSqlNode,
                   DbType dbType, RelDataType rowType, Map<String, List<List<String>>> targetTable,
                   List<String> tableNames, String dbIndex, String phyTable, String schemaName) {
        super(cluster,
            traitSet,
            sqlTemplate,
            nativeSqlNode,
            dbType,
            rowType,
            targetTable,
            tableNames,
            dbIndex,
            phyTable,
            schemaName);
    }

    public static PhyShow create(Show show, String dbIndex, String phyTable, String schemaName) {
        return new PhyShow(show.getCluster(),
            show.getTraitSet(),
            show.getAst(),
            show.getRowType(),
            dbIndex,
            phyTable,
            schemaName);
    }

    public static PhyShow create(BaseDalOperation show, Map<String, List<List<String>>> targetTable,
                                 List<String> tableNames) {
        return new PhyShow(show.getCluster(),
            show.getTraitSet(),
            show.getNativeSqlNode(),
            show.getRowType(),
            targetTable,
            tableNames,
            show.getSchemaName());
    }

    @Override
    public List<RelNode> getInput(Map<Integer, ParameterContext> params) {
        // remove db name by default
        if (isRemoveDbPrefix()) {
            final SqlDal sqlDal = (SqlDal) this.nativeSqlNode;
            if (null != sqlDal.getDbName()) {
                sqlDal.setDbName((SqlNode) null);
                this.sqlTemplate = RelUtils.toNativeSqlLine(sqlDal);
            }
        }

        if (single()) {
            return super.getInput(params);
        }

        final List<RelNode> result = new LinkedList<>();

        final SqlShow show = (SqlShow) this.nativeSqlNode;
        final SqlNode tableName = show.getTableName();
        for (Entry<String, List<List<String>>> entry : targetTable.entrySet()) {
            final String group = entry.getKey();

            if (null != tableName && CollectionUtils.isNotEmpty(entry.getValue())) {
                for (List<String> phyTables : entry.getValue()) {
                    final SqlShow tmpShow = (SqlShow) show.clone(SqlParserPos.ZERO);

                    tmpShow.setTableName(phyTables.get(0));

                    PhyShow subShow = new PhyShow(getCluster(),
                        getTraitSet(),
                        tmpShow,
                        this.rowType,
                        group,
                        phyTables.get(0),
                        schemaName);

                    result.add(subShow);
                } // end of for
            } else {
                PhyShow subShow = new PhyShow(getCluster(),
                    getTraitSet(),
                    this.sqlTemplate,
                    show,
                    this.dbType,
                    this.rowType,
                    null,
                    null,
                    group,
                    "",
                    schemaName);

                result.add(subShow);
            } // end of else
        } // end of for

        return result;
    }

    @Override
    public Pair<String, Map<Integer, ParameterContext>> getDbIndexAndParam(Map<Integer, ParameterContext> param,
                                                                           ExecutionContext executionContext) {
        return Pair.of(dbIndex, null);
    }

    @Override
    public RelWriter explainTermsForDisplay(RelWriter pw) {
        pw.item(RelDrdsWriter.REL_NAME, getExplainName());
        if (single()) {
            pw.item("node", dbIndex);
        } else {
            if (targetTable.entrySet().iterator().next().getValue().size() > 0) {
                final String table = ExplainUtils.getPhyTableString(tableNames, targetTable);
                pw.item("table", table);
            } else {
                final String node = ExplainUtils.compressName(targetTable.keySet());
                pw.item("node", node);
            }
        }
        pw.item("sql", this.sqlTemplate);
        return pw;
    }

    public List<String> getTableNames() {
        return tableNames;
    }

    public void setTableNames(List<String> tableNames) {
        this.tableNames = tableNames;
    }

    @Override
    public PhyShow copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new PhyShow(getCluster(),
            traitSet,
            sqlTemplate,
            nativeSqlNode,
            dbType,
            rowType,
            targetTable,
            tableNames,
            dbIndex,
            phyTable,
            schemaName);
    }
}
