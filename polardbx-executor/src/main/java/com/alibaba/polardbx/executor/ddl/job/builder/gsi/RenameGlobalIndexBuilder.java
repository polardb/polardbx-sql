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
import com.alibaba.polardbx.executor.ddl.job.builder.RenameTableBuilder;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.gsi.RenameGlobalIndexPreparedData;
import org.apache.calcite.rel.core.DDL;

public class RenameGlobalIndexBuilder extends DdlPhyPlanBuilder {

    private final RenameGlobalIndexPreparedData gsiPreparedData;

    private RenameTableBuilder renameTableBuilder;

    public RenameGlobalIndexBuilder(DDL ddl,
                                    RenameGlobalIndexPreparedData gsiPreparedData,
                                    ExecutionContext executionContext) {
        super(ddl, gsiPreparedData.getIndexTablePreparedData(), executionContext);
        this.gsiPreparedData = gsiPreparedData;
    }

    @Override
    protected void buildTableRuleAndTopology() {
        renameTableBuilder =
            new RenameTableBuilder(relDdl, gsiPreparedData.getIndexTablePreparedData(), executionContext);
        renameTableBuilder.buildTableRuleAndTopology();

        this.tableRule = renameTableBuilder.getTableRule();
        this.tableTopology = renameTableBuilder.getTableTopology();
    }

    @Override
    protected void buildPhysicalPlans() {
        buildSqlTemplate();
        buildPhysicalPlans(gsiPreparedData.getIndexTableName());
    }

}
