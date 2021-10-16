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

import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.TruncateTablePreparedData;
import com.alibaba.polardbx.optimizer.partition.PartitionInfoUtil;
import org.apache.calcite.rel.core.DDL;

public class TruncatePartitionTableBuilder extends TruncateTableBuilder {

    public TruncatePartitionTableBuilder(DDL ddl, TruncateTablePreparedData preparedData,
                                         ExecutionContext executionContext) {
        super(ddl, preparedData, executionContext);
    }

    @Override
    public TruncatePartitionTableBuilder build() {
        if (partitionInfo == null) {
            partitionInfo = OptimizerContext.getContext(preparedData.getSchemaName()).getPartitionInfoManager()
                .getPartitionInfo(ddlPreparedData.getTableName());
        }
        super.build();
        return this;
    }

    @Override
    public void buildTableRuleAndTopology() {
        this.tableTopology = PartitionInfoUtil.buildTargetTablesFromPartitionInfo(partitionInfo);
    }
}
