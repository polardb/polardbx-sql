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

package com.alibaba.polardbx.executor.ddl.job.builder.gsi;

import com.alibaba.polardbx.executor.ddl.job.builder.DdlPhyPlanBuilder;
import com.alibaba.polardbx.executor.ddl.job.builder.DropTableBuilder;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.PhyDdlTableOperation;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.DropTablePreparedData;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.gsi.DropGlobalIndexPreparedData;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.gsi.DropTableWithGsiPreparedData;
import org.apache.calcite.rel.core.DDL;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class DropTableWithGsiBuilder {

    protected final DDL relDdl;
    protected final DropTableWithGsiPreparedData preparedData;
    protected final ExecutionContext executionContext;

    protected TreeMap<String, List<List<String>>> primaryTableTopology;
    protected List<PhyDdlTableOperation> primaryTablePhysicalPlans;

    protected Map<String, Map<String, List<List<String>>>> indexTableTopologyMap = new LinkedHashMap<>();
    protected Map<String, List<PhyDdlTableOperation>> indexTablePhysicalPlansMap = new LinkedHashMap<>();

    public DropTableWithGsiBuilder(DDL ddl, DropTableWithGsiPreparedData preparedData,
                                   ExecutionContext executionContext) {
        this.relDdl = ddl;
        this.preparedData = preparedData;
        this.executionContext = executionContext;
    }

    public void build() {
        buildPrimaryTablePhysicalPlans();
        buildIndexTablePhysicalPlans();
    }

    public TreeMap<String, List<List<String>>> getPrimaryTableTopology() {
        return primaryTableTopology;
    }

    public List<PhyDdlTableOperation> getPrimaryTablePhysicalPlans() {
        return primaryTablePhysicalPlans;
    }

    public Map<String, Map<String, List<List<String>>>> getIndexTableTopologyMap() {
        return indexTableTopologyMap;
    }

    public Map<String, List<PhyDdlTableOperation>> getIndexTablePhysicalPlansMap() {
        return indexTablePhysicalPlansMap;
    }

    protected void buildPrimaryTablePhysicalPlans() {
        DropTablePreparedData primaryTablePreparedData = preparedData.getPrimaryTablePreparedData();
        DdlPhyPlanBuilder primaryTableBuilder =
            new DropTableBuilder(relDdl, primaryTablePreparedData, executionContext).build();
        this.primaryTablePhysicalPlans = primaryTableBuilder.getPhysicalPlans();
        this.primaryTableTopology = primaryTableBuilder.getTableTopology();
    }

    private void buildIndexTablePhysicalPlans() {
        for (Map.Entry<String, DropGlobalIndexPreparedData> entry :
            preparedData.getIndexTablePreparedDataMap().entrySet()) {
            buildIndexTablePhysicalPlans(entry.getKey(), entry.getValue());
        }
    }

    protected void buildIndexTablePhysicalPlans(String indexTableName,
                                                DropGlobalIndexPreparedData indexTablePreparedData) {
        DdlPhyPlanBuilder indexTableBuilder =
            new DropGlobalIndexBuilder(relDdl, indexTablePreparedData, executionContext).build();
        this.indexTablePhysicalPlansMap.put(indexTableName, indexTableBuilder.getPhysicalPlans());
    }

}
