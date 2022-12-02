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

package com.alibaba.polardbx.executor.ddl.job.builder.tablegroup;

import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.AlterTableGroupDropPartitionPreparedData;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.rel.core.DDL;

import java.util.List;

public class AlterTableDropPartitionBuilder extends AlterTableGroupDropPartitionBuilder {

    public AlterTableDropPartitionBuilder(DDL ddl, AlterTableGroupDropPartitionPreparedData preparedData,
                                          ExecutionContext executionContext) {
        super(ddl, preparedData, executionContext);
    }

    @Override
    public List<String> getAllTableNames() {
        return ImmutableList.of(preparedData.getTableName());
    }
}
