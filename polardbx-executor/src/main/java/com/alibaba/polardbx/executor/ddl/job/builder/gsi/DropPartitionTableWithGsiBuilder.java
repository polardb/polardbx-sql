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

import com.alibaba.polardbx.executor.ddl.job.builder.DropPartitionTableBuilder;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.DropTablePreparedData;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.gsi.DropGlobalIndexPreparedData;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.gsi.DropTableWithGsiPreparedData;
import org.apache.calcite.rel.core.DDL;

public class DropPartitionTableWithGsiBuilder extends DropTableWithGsiBuilder {

    public DropPartitionTableWithGsiBuilder(DDL ddl, DropTableWithGsiPreparedData preparedData,
                                            ExecutionContext executionContext) {
        super(ddl, preparedData, executionContext);
    }

    @Override
    protected void buildPrimaryTablePhysicalPlans() {
        DropTablePreparedData primaryTablePreparedData = preparedData.getPrimaryTablePreparedData();
        DropPartitionTableBuilder primaryTableBuilder =
            new DropPartitionTableBuilder(relDdl, primaryTablePreparedData, executionContext);
        primaryTableBuilder.build();
        this.primaryTablePhysicalPlans = primaryTableBuilder.getPhysicalPlans();
        this.primaryTableTopology = primaryTableBuilder.getTableTopology();
    }

    @Override
    protected void buildIndexTablePhysicalPlans(String indexTableName,
                                                DropGlobalIndexPreparedData indexTablePreparedData) {
        DropGlobalIndexBuilder indexTableBuilder =
            new DropPartitionGlobalIndexBuilder(relDdl, indexTablePreparedData, executionContext);
        indexTableBuilder.build();
        this.indexTablePhysicalPlansMap.put(indexTableName, indexTableBuilder.getPhysicalPlans());
    }

}
