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

import com.alibaba.polardbx.common.jdbc.BytesSql;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.dialect.DbType;
import com.alibaba.polardbx.optimizer.utils.RelUtils;
import com.alibaba.polardbx.optimizer.utils.ExplainUtils;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.externalize.RelDrdsWriter;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlDal;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOptimizeTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.commons.collections.CollectionUtils;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

/**
 * @author chenmo.cm
 */
public class PhyDal extends BaseDalOperation {

    public PhyDal(RelOptCluster cluster, RelTraitSet traitSet, SqlNode nativeSqlNode, RelDataType rowType,
                  String dbIndex, String phyTable, String schemaName) {
        super(cluster, traitSet, nativeSqlNode, rowType, dbIndex, phyTable, schemaName);
    }

    public PhyDal(RelOptCluster cluster, RelTraitSet traitSet, SqlNode nativeSqlNode, RelDataType rowType,
                  Map<String, List<List<String>>> targetTable, List<String> tableNames, String schemaName) {
        super(cluster, traitSet, nativeSqlNode, rowType, targetTable, tableNames, schemaName);
    }

    public PhyDal(RelOptCluster cluster, RelTraitSet traitSet, BytesSql sqlTemplate, SqlNode nativeSqlNode,
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

    public static PhyDal create(BaseDalOperation dal, String dbIndex, String phyTable) {
        return new PhyDal(dal.getCluster(),
            dal.getTraitSet(),
            dal.getNativeSqlNode(),
            dal.getRowType(),
            dbIndex,
            phyTable,
            null);
    }

    public static PhyDal create(BaseDalOperation dal, Map<String, List<List<String>>> targetTable,
                                List<String> tableNames) {
        return new PhyDal(dal.getCluster(),
            dal.getTraitSet(),
            dal.getNativeSqlNode(),
            dal.getRowType(),
            targetTable,
            tableNames,
            dal.getSchemaName());
    }

    @Override
    public List<RelNode> getInput(Map<Integer, ParameterContext> params) {
        // remove db name by default
        if (isRemoveDbPrefix()) {
            final SqlDal sqlDal = (SqlDal) this.nativeSqlNode;
            sqlDal.setDbName((SqlNode) null);
        }

        if (single()) {
            return super.getInput(params);
        }

        List<RelNode> result = new LinkedList<>();

        final SqlDal dal = (SqlDal) this.nativeSqlNode;
        final SqlNode tableName = dal.getTableName();
        for (Entry<String, List<List<String>>> entry : targetTable.entrySet()) {
            final String group = entry.getKey();

            if (null != tableName && CollectionUtils.isNotEmpty(entry.getValue())) {
                for (List<String> phyTables : entry.getValue()) {
                    final SqlDal tmpDal = (SqlDal) dal.clone(SqlParserPos.ZERO);

                    tmpDal.setTableName(phyTables.get(0));

                    PhyDal subDal = new PhyDal(getCluster(),
                        getTraitSet(),
                        tmpDal,
                        this.rowType,
                        group,
                        phyTables.get(0),
                        schemaName);

                    result.add(subDal);
                } // end of for
            } else {
                PhyDal subDal = new PhyDal(getCluster(),
                    getTraitSet(),
                    this.bytesSql,
                    this.nativeSqlNode,
                    this.dbType,
                    this.rowType,
                    null,
                    this.tableNames,
                    group,
                    "",
                    schemaName);
                result.add(subDal);
            } // end of else
        }

        return result;
    }

    @Override
    public Pair<String, Map<Integer, ParameterContext>> getDbIndexAndParam(Map<Integer, ParameterContext> param,
                                                                           ExecutionContext executionContext) {
        param = new HashMap<>();
        return Pair.of(dbIndex, param);
    }

    @Override
    public RelWriter explainTermsForDisplay(RelWriter pw) {
        pw.item(RelDrdsWriter.REL_NAME, getExplainName());
        BytesSql sql = this.bytesSql;
        if (single()) {
            pw.item("node", dbIndex);
        } else {
            if (targetTable.entrySet().iterator().next().getValue().size() > 0) {
                String table = "";
                if (compressTableName()) {
                    table = ExplainUtils.getPhyTableString(tableNames, targetTable);
                } else if (null != getNativeSqlNode() && getNativeSqlNode().getKind() == SqlKind.OPTIMIZE_TABLE) {
                    table = TStringUtil.join(this.tableNames, ",");

                    final SqlOptimizeTable oldOt = (SqlOptimizeTable) this.getNativeSqlNode();
                    SqlOptimizeTable newOt = SqlOptimizeTable.create(SqlParserPos.ZERO,
                        this.tableNames,
                        oldOt.isNoWriteToBinlog(),
                        oldOt.isLocal());
                    sql = RelUtils.toNativeBytesSql(newOt);
                }
                pw.item("table", table);
            } else {
                final String node = ExplainUtils.compressName(targetTable.keySet());
                pw.item("node", node);
            }
        }
        pw.item("sql", sql.display());
        return pw;
    }

    public boolean compressTableName() {
        final SqlNode sqlNode = getNativeSqlNode();
        if (null == sqlNode) {
            return true;
        }

        final SqlKind kind = sqlNode.getKind();
        return kind != SqlKind.OPTIMIZE_TABLE || this.tableNames.size() <= 1;
    }
}
