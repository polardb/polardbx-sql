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

package com.alibaba.polardbx.optimizer.core.rel;

import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.dialect.DbType;
import com.alibaba.polardbx.optimizer.utils.PlannerUtils;
import com.alibaba.polardbx.optimizer.utils.RelUtils;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.Parameters;
import com.alibaba.polardbx.common.utils.Pair;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlInsert;
import org.apache.calcite.sql.SqlNode;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author minggong.zm 2018-01-18
 */
public class PhyTableInsertBuilder {

    /**
     * SQL Template, containing all values.
     */
    private SqlNode sqlTemplate;

    /**
     * SQL 参数
     */
    private Parameters parameterSettings;
    private final LogicalInsert parent;
    private final DbType dbType;
    private String schemaName;
    private ExecutionContext executionContext;

    public PhyTableInsertBuilder(SqlNode sqlTemplate, ExecutionContext executionContext, LogicalInsert parent,
                                 DbType dbType, String schemaName) {

        this.executionContext = executionContext;
        this.sqlTemplate = sqlTemplate;
        this.parameterSettings = executionContext.getParams();
        this.parent = parent;
        this.dbType = dbType;
        this.schemaName = schemaName;
    }

    public List<RelNode> build(List<PhyTableInsertSharder.PhyTableShardResult> shardResult) {

        List<RelNode> phyTableModifys = new ArrayList<>();
        String logicalTableName = parent.getLogicalTableName();

        boolean needBuildEveryTime = !isAllShardResultSame(shardResult);
        PhyTableOperation phyTableModify = null;
        for (PhyTableInsertSharder.PhyTableShardResult result : shardResult) {
            final String phyTableName = result.getPhyTableName();
            if (!needBuildEveryTime && phyTableModify != null) {
                // Copy the first one, only changing groupName and tableName.
                phyTableModify = new PhyTableOperation(phyTableModify);
                phyTableModify.setDbIndex(result.getGroupName());
                phyTableModify.setTableNames(ImmutableList.<List<String>>of(Lists.newArrayList(phyTableName)));

                // Replace tableName in params
                Map<Integer, ParameterContext> newParams = new HashMap<>(phyTableModify.getParam());
                newParams.put(1, PlannerUtils.buildParameterContextForTableName(phyTableName, 1));
                phyTableModify.setParam(newParams);
            } else {

                if (executionContext != null && executionContext.getLoadDataContext() != null && executionContext
                    .getLoadDataContext().isUseBatch()
                    && !executionContext.getLoadDataContext().isSwapColumns()
                    && !executionContext.getLoadDataContext().isGsiInsertTurn()) {
                    //load data mode, use batch parameters for performance
                    //Condition: column list of load data(insert) same with the column list of the table
                    Pair<SqlInsert, List<Map<Integer, ParameterContext>>> sqlInsertAndParam =
                        buildSqlInsertBatchParam((SqlInsert) sqlTemplate,
                            result.getValueIndices(),
                            phyTableName);
                    SqlInsert sqlInsert = sqlInsertAndParam.getKey();
                    List<Map<Integer, ParameterContext>> batchParamters = sqlInsertAndParam.getValue();
                    String sql = RelUtils.toNativeSql(sqlInsert, dbType);
                    phyTableModify = buildPhyTableModify(result.getGroupName(),
                        phyTableName,
                        logicalTableName,
                        sql,
                        sqlInsert,
                        null,
                        batchParamters);
                } else {
                    Pair<SqlInsert, Map<Integer, ParameterContext>> sqlInsertAndParam =
                        buildSqlInsertAndParam((SqlInsert) sqlTemplate,
                            result.getValueIndices(),
                            phyTableName);

                    SqlInsert sqlInsert = sqlInsertAndParam.getKey();
                    Map<Integer, ParameterContext> params = sqlInsertAndParam.getValue();
                    String sql = RelUtils.toNativeSql(sqlInsert, dbType);
                    phyTableModify = buildPhyTableModify(result.getGroupName(),
                        phyTableName,
                        logicalTableName,
                        sql,
                        sqlInsert,
                        params,
                        null);
                }
            }

            phyTableModifys.add(phyTableModify);
        }

        return phyTableModifys;
    }

    /**
     * If it's inserting into broadcast table, or inserting with routing hint,
     * all the shardResults are the same, there's no need to build SqlInsert
     * twice.
     */
    private boolean isAllShardResultSame(List<PhyTableInsertSharder.PhyTableShardResult> shardResult) {
        for (PhyTableInsertSharder.PhyTableShardResult result : shardResult) {
            if (result.getValueIndices() != null) {
                return false;
            }
        }
        return true;
    }

    private PhyTableOperation buildPhyTableModify(String groupIndex, String phyTableName, String logicalTableName,
                                                  String sql, SqlInsert sqlInsert,
                                                  Map<Integer, ParameterContext> params,
                                                  List<Map<Integer, ParameterContext>> batchParams) {
        PhyTableOperation phyTableModify =
            new PhyTableOperation(parent.getCluster(), parent.getTraitSet(), parent.getRowType(), null, parent);
        phyTableModify.setKind(parent.getOperation().toSqlKind());
        phyTableModify.setDbIndex(groupIndex);
        List<String> tableNames = Lists.newArrayList(phyTableName);
        phyTableModify.setTableNames(ImmutableList.of(tableNames));

        phyTableModify.setSqlTemplate(sql);
        phyTableModify.setNativeSqlNode(sqlInsert);
        phyTableModify.setParam(params);
        phyTableModify.setBatchParameters(batchParams);
        phyTableModify.setLogicalTableNames(ImmutableList.of(logicalTableName));

        RelUtils.changeRowType(phyTableModify, parent.getRowType());
        return phyTableModify;
    }

    /**
     * Create a new SqlInsert by sqlTemplate and sharded values.
     *
     * @param sqlTemplate SqlInsert containing all values.
     * @param valueIndices indices of rows sharded to one physical table.
     */
    private Pair<SqlInsert, Map<Integer, ParameterContext>> buildSqlInsertAndParam(SqlInsert sqlTemplate,
                                                                                   List<Integer> valueIndices,
                                                                                   String tableName) {

        Map<Integer, ParameterContext> outputParams = new HashMap<>();
        outputParams.put(1, PlannerUtils.buildParameterContextForTableName(tableName, 1));
        BuildInsertValuesVisitor visitor = new BuildInsertValuesVisitor(valueIndices, parameterSettings, outputParams);
        SqlInsert newSqlInsert = visitor.visit(sqlTemplate);

        return new Pair<>(newSqlInsert, outputParams);
    }

    private Pair<SqlInsert, List<Map<Integer, ParameterContext>>> buildSqlInsertBatchParam(SqlInsert sqlTemplate,
                                                                                           List<Integer> valueIndices,
                                                                                           String tableName) {
        FillTargetTableNameVisitor targetTableNameVisitor =
            new FillTargetTableNameVisitor(Lists.newArrayList(tableName));
        SqlInsert newSqlInsert = (SqlInsert) targetTableNameVisitor.visit(sqlTemplate);
        List<Map<Integer, ParameterContext>> batchParams = new ArrayList<>();
        for (int index : valueIndices) {
            batchParams.add(parameterSettings.getBatchParameters().get(index));
        }
        return new Pair<>(newSqlInsert, batchParams);
    }
}
