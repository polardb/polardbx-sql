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
import com.alibaba.polardbx.executor.ddl.job.builder.TruncateTableBuilder;
import com.alibaba.polardbx.executor.ddl.job.converter.DdlJobDataConverter;
import com.alibaba.polardbx.executor.ddl.job.converter.PhysicalPlanData;
import com.alibaba.polardbx.executor.utils.failpoint.FailPoint;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.PhyDdlTableOperation;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.TruncateTablePreparedData;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.gsi.TruncateGlobalIndexPreparedData;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.gsi.TruncateTableWithGsiPreparedData;
import lombok.val;
import org.apache.calcite.rel.core.DDL;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class TruncateTableWithGsiBuilder extends DdlPhyPlanBuilder {

    private final DDL relDdl;
    private final TruncateTableWithGsiPreparedData preparedData;
    private final ExecutionContext executionContext;

    private Map<String, List<List<String>>> primaryTableTopology;
    private List<PhyDdlTableOperation> primaryTablePhysicalPlans;

    private Map<String, Map<String, List<List<String>>>> indexTableTopologyMap = new LinkedHashMap<>();
    private Map<String, List<PhyDdlTableOperation>> indexTablePhysicalPlansMap = new LinkedHashMap<>();

    public TruncateTableWithGsiBuilder(DDL ddl, TruncateTableWithGsiPreparedData preparedData,
                                       ExecutionContext executionContext) {
        super(ddl, preparedData, executionContext);
        this.relDdl = ddl;
        this.preparedData = preparedData;
        this.executionContext = executionContext;
    }

    @Override
    public TruncateTableWithGsiBuilder build() {
        super.build();
        return this;
    }

    @Override
    protected void buildTableRuleAndTopology() {
    }

    @Override
    protected void buildPhysicalPlans() {
        TruncateTablePreparedData primaryTablePreparedData = preparedData.getPrimaryTablePreparedData();
        DdlPhyPlanBuilder primaryTableBuilder =
            new TruncateTableBuilder(relDdl, primaryTablePreparedData, executionContext).build();
        this.primaryTableTopology = primaryTableBuilder.getTableTopology();
        this.primaryTablePhysicalPlans = primaryTableBuilder.getPhysicalPlans();

        buildIndexTablePhysicalPlans();
    }

    @Override
    public PhysicalPlanData genPhysicalPlanData() {
        Objects.requireNonNull(primaryTableTopology);
        Objects.requireNonNull(primaryTablePhysicalPlans);

        FailPoint.injectRandomExceptionFromHint(executionContext);
        FailPoint.injectRandomSuspendFromHint(executionContext);

        return DdlJobDataConverter.convertToPhysicalPlanData(primaryTableTopology, primaryTablePhysicalPlans);
    }

    public Map<String, PhysicalPlanData> genIndexPlanData() {
        Map<String, PhysicalPlanData> gsiPhyTruncatePlanMap = new HashMap<>();

        for (val stringMapEntry : indexTableTopologyMap.entrySet()) {
            final String gsiName = stringMapEntry.getKey();
            final Map<String, List<List<String>>> gsiTopology = stringMapEntry.getValue();
            final List<PhyDdlTableOperation> gsiTablePhysicalPlans = indexTablePhysicalPlansMap.get(gsiName);
            PhysicalPlanData gsiPhyTruncatePlan =
                DdlJobDataConverter.convertToPhysicalPlanData(gsiTopology, gsiTablePhysicalPlans);
            gsiPhyTruncatePlanMap.put(gsiName, gsiPhyTruncatePlan);
        }
        return gsiPhyTruncatePlanMap;
    }

    public Map<String, List<List<String>>> getPrimaryTableTopology() {
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

    private void buildIndexTablePhysicalPlans() {
        for (Map.Entry<String, TruncateGlobalIndexPreparedData> entry :
            preparedData.getIndexTablePreparedDataMap().entrySet()) {
            buildIndexTablePhysicalPlans(entry.getKey(), entry.getValue());
        }
    }

    private void buildIndexTablePhysicalPlans(String indexTableName,
                                              TruncateGlobalIndexPreparedData indexTablePreparedData) {
        DdlPhyPlanBuilder indexTableBuilder =
            new TruncateGlobalIndexBuilder(relDdl, indexTablePreparedData, executionContext).build();
        this.indexTableTopologyMap.put(indexTableName, indexTableBuilder.getTableTopology());
        this.indexTablePhysicalPlansMap.put(indexTableName, indexTableBuilder.getPhysicalPlans());
    }

}
