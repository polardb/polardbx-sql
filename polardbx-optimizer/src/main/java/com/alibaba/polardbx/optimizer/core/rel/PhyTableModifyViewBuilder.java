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
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.optimizer.memory.MemoryAllocatorCtx;
import org.apache.calcite.rel.AbstractRelNode;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlNode;
import org.apache.commons.collections.CollectionUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @author minggong.zm 2018-03-06
 */
public class PhyTableModifyViewBuilder extends PhyOperationBuilderCommon {

    /**
     * SQL Template, tableName has been parameterized.
     */
    private SqlNode sqlTemplate;
    /**
     * <pre>
     * key: GroupName
     * values: List of TableNames
     * </pre>
     */
    private Map<String, List<List<String>>> targetTables;
    private final Map<Integer, ParameterContext> params;
    private final RelNode parent;
    private final DbType dbType;
    private final List<Integer> paramIndex;
    private final List<String> logicalTableNames;
    private final String schemaName;
    private ExecutionContext executionContext;

    public PhyTableModifyViewBuilder(SqlNode sqlTemplate, Map<String, List<List<String>>> targetTables,
                                     ExecutionContext executionContext, LogicalView parent, DbType dbType,
                                     String schemaName) {
        this(sqlTemplate, targetTables, executionContext, parent, dbType, parent.getTableNames(), schemaName);
    }

    public PhyTableModifyViewBuilder(SqlNode sqlTemplate, Map<String, List<List<String>>> targetTables,
                                     ExecutionContext executionContext, RelNode parent, DbType dbType,
                                     List<String> logicalTableNames, String schemaName) {
        this.executionContext = executionContext;
        this.sqlTemplate = sqlTemplate;
        this.targetTables = targetTables;
        this.params = executionContext.getParams() == null ? null : executionContext.getParams().getCurrentParameter();
        this.parent = parent;
        this.dbType = dbType;
        this.paramIndex = PlannerUtils.getDynamicParamIndex(sqlTemplate);
        this.logicalTableNames = logicalTableNames;
        this.schemaName = schemaName;
    }

    public List<RelNode> build() {

        String sqlTemplateStr = RelUtils.toNativeSql(sqlTemplate, dbType);
        ShardPlanMemoryContext shardPlanMemoryContext = buildShardPlanMemoryContext(parent,
            sqlTemplateStr,
            (AbstractRelNode) parent,
            this.params,
            this.targetTables,
            this.executionContext);
        MemoryAllocatorCtx maOfPlanBuildingPool = shardPlanMemoryContext.memoryAllocator;
        if (maOfPlanBuildingPool != null) {
            long phyOpMemSize = shardPlanMemoryContext.phyOpMemSize;
            maOfPlanBuildingPool.allocateReservedMemory(phyOpMemSize * shardPlanMemoryContext.allShardCount);
        }
        List<RelNode> phyTableScans = new ArrayList<>();
        for (Map.Entry<String, List<List<String>>> t : targetTables.entrySet()) {
            String group = t.getKey();
            List<List<String>> tableNames = t.getValue();
            for (List<String> subTableNames : tableNames) {
                buildOnePhyTableOperatorForModify(sqlTemplateStr,
                    maOfPlanBuildingPool,
                    phyTableScans,
                    group,
                    subTableNames);
            }
        }
        return phyTableScans;
    }

    /**
     * 构建 SQL 对应的参数信息
     */
    private Map<Integer, ParameterContext> buildParams(List<String> tableNames) {
        Preconditions.checkArgument(CollectionUtils.isNotEmpty(tableNames));
        return PlannerUtils.buildParam(tableNames, this.params, paramIndex);
    }

    private void buildOnePhyTableOperatorForModify(String sqlTemplateStr, MemoryAllocatorCtx maOfPlanBuildingPool,
                                                   List<RelNode> phyTableScans, String group,
                                                   List<String> subTableNames) {

        PhyTableOperation phyTableModify =
            new PhyTableOperation(parent.getCluster(), parent.getTraitSet(), parent.getRowType(), null, parent);
        phyTableModify.setDbIndex(group);
        phyTableModify.setLogicalTableNames(logicalTableNames);
        phyTableModify.setTableNames(ImmutableList.of(subTableNames));
        phyTableModify.setKind(sqlTemplate.getKind());
        phyTableModify.setSchemaName(schemaName);
        phyTableModify.setSqlTemplate(sqlTemplateStr);
        phyTableModify.setNativeSqlNode(sqlTemplate);
        phyTableModify.setDbType(dbType);
        phyTableModify.setParam(buildParams(subTableNames));
        phyTableModify.setMemoryAllocator(maOfPlanBuildingPool);
        phyTableScans.add(phyTableModify);
    }
}
