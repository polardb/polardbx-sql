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

package com.alibaba.polardbx.executor.archive.reader;

import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.metadata.RelColumnOrigin;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.util.ImmutableBitSet;

import java.io.IOException;
import java.util.List;

public class OSSTableReader implements BatchReader<OSSPhysicalTableReadResult> {
    private OSSReadOption OSSReadOption;
    private ExecutionContext executionContext;
    private List<AggregateCall> aggCalls;
    private ImmutableBitSet group;
    private RelDataType dataType;
    List<RelColumnOrigin> aggColumns;

    public OSSTableReader(OSSReadOption OSSReadOption, ExecutionContext executionContext,
                          LogicalAggregate agg, List<RelColumnOrigin> aggColumns) {
        this.OSSReadOption = OSSReadOption;
        this.executionContext = executionContext;
        if (agg != null) {
            this.aggCalls = agg.getAggCallList();
            this.group = agg.getGroupSet();
            this.dataType = agg.getRowType();
        } else {
            this.aggCalls = null;
            this.group = null;
            this.dataType = null;
        }
        this.aggColumns = aggColumns;
    }

    @Override
    public OSSPhysicalTableReadResult readBatch() {
        return new OSSPhysicalTableReadResult(OSSReadOption, executionContext, aggCalls, aggColumns, group, dataType);
    }

    @Override
    public boolean isClosed() {
        return false;
    }

    @Override
    public void close() throws IOException {

    }
}
