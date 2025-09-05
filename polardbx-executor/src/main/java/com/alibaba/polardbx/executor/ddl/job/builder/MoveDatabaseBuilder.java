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

package com.alibaba.polardbx.executor.ddl.job.builder;

import com.alibaba.polardbx.optimizer.config.table.ScaleOutPlanUtil;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.PhyDdlTableOperation;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.MoveDatabaseItemPreparedData;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.MoveDatabasePreparedData;
import org.apache.calcite.rel.core.DDL;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/**
 * Created by luoyanxin.
 *
 * @author luoyanxin
 */
public class MoveDatabaseBuilder {

    protected final DDL relDdl;
    protected final MoveDatabasePreparedData preparedData;
    protected final ExecutionContext executionContext;

    protected Map<String, List<PhyDdlTableOperation>> logicalTablesPhysicalPlansMap =
        new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
    protected Map<String, TreeMap<String, List<List<String>>>> tablesTopologyMap =
        new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
    protected Map<String, Map<String, Set<String>>> sourceTablesTopology = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
    protected Map<String, Map<String, Set<String>>> targetTablesTopology = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
    protected Map<String, MoveDatabaseItemPreparedData> tablesPreparedData =
        new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
    protected Map<String, List<PhyDdlTableOperation>> discardTableSpacePhysicalPlansMap =
        new TreeMap<>(String.CASE_INSENSITIVE_ORDER);

    public MoveDatabaseBuilder(DDL ddl, MoveDatabasePreparedData preparedData,
                               ExecutionContext executionContext) {
        this.relDdl = ddl;
        this.preparedData = preparedData;
        this.executionContext = executionContext;
    }

    public MoveDatabaseBuilder build() {
        buildTablesPhysicalPlans();
        return this;
    }

    public void buildTablesPhysicalPlans() {
        List<String> gsiTables = new ArrayList<>();
        List<String> logicTableNames =
            ScaleOutPlanUtil.getLogicalTables(preparedData.getSchemaName(), gsiTables, executionContext);
        //普通表先完成 然后GSI表
        logicTableNames.addAll(gsiTables);

        for (String tableName : logicTableNames) {
            MoveDatabaseItemPreparedData moveDatabaseItemPreparedData =
                new MoveDatabaseItemPreparedData(preparedData.getSchemaName(), preparedData.getSourceTargetGroupMap(),
                    tableName);

            MoveDatabaseItemBuilder itemBuilder =
                new MoveDatabaseItemBuilder(relDdl, moveDatabaseItemPreparedData, executionContext);
            List<PhyDdlTableOperation> phyDdlTableOperations = itemBuilder.build().getPhysicalPlans();
            tablesTopologyMap.put(tableName, itemBuilder.getTableTopology());
            sourceTablesTopology.put(tableName, itemBuilder.getSourcePhyTables());
            targetTablesTopology.put(tableName, itemBuilder.getTargetPhyTables());
            logicalTablesPhysicalPlansMap.put(tableName, phyDdlTableOperations);
            tablesPreparedData.put(tableName, moveDatabaseItemPreparedData);

            AlterTableDiscardTableSpaceBuilder discardTableSpaceBuilder =
                AlterTableDiscardTableSpaceBuilder.createBuilder(
                    preparedData.getSchemaName(), tableName, itemBuilder.getTableTopology(), executionContext);
            discardTableSpacePhysicalPlansMap.put(tableName, discardTableSpaceBuilder.build().getPhysicalPlans());
        }

    }

    public Map<String, List<PhyDdlTableOperation>> getLogicalTablesPhysicalPlansMap() {
        return logicalTablesPhysicalPlansMap;
    }

    public Map<String, TreeMap<String, List<List<String>>>> getTablesTopologyMap() {
        return tablesTopologyMap;
    }

    public Map<String, Map<String, Set<String>>> getSourceTablesTopology() {
        return sourceTablesTopology;
    }

    public Map<String, Map<String, Set<String>>> getTargetTablesTopology() {
        return targetTablesTopology;
    }

    public Map<String, List<PhyDdlTableOperation>> getDiscardTableSpacePhysicalPlansMap() {
        return discardTableSpacePhysicalPlansMap;
    }

    public Map<String, MoveDatabaseItemPreparedData> getTablesPreparedData() {
        return tablesPreparedData;
    }

    public DDL getRelDdl() {
        return relDdl;
    }

    public ExecutionContext getExecutionContext() {
        return executionContext;
    }
}