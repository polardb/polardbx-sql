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

package com.alibaba.polardbx.executor.mpp.operator.factory;

import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.executor.mpp.execution.buffer.OutputBuffer;
import com.alibaba.polardbx.executor.mpp.execution.buffer.PagesSerdeFactory;
import com.alibaba.polardbx.executor.mpp.operator.PartitionedOutputCollector;
import com.alibaba.polardbx.executor.mpp.operator.TaskOutputCollector;
import com.alibaba.polardbx.executor.mpp.planner.PartitionShuffleHandle;
import com.alibaba.polardbx.executor.mpp.planner.PartitioningScheme;
import com.alibaba.polardbx.executor.mpp.planner.PlanFragment;
import com.alibaba.polardbx.executor.mpp.planner.SerializeDataType;
import com.alibaba.polardbx.executor.operator.ConsumerExecutor;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;

import java.util.List;

public class OutputExecutorFactory implements ConsumeExecutorFactory {
    private PlanFragment fragment;
    private OutputBuffer outputBuffer;
    private PagesSerdeFactory pagesSerdeFactory;

    public OutputExecutorFactory(PlanFragment fragment, OutputBuffer outputBuffer, PagesSerdeFactory pagesSerdeFactory
    ) {
        this.fragment = fragment;
        this.outputBuffer = outputBuffer;
        this.pagesSerdeFactory = pagesSerdeFactory;
    }

    @Override
    public ConsumerExecutor createExecutor(ExecutionContext context, int index) {
        PartitioningScheme partitioningScheme = fragment.getPartitioningScheme();
        List<DataType> inputType = fragment.getTypes();
        List<DataType> outputType = SerializeDataType.convertToDataType(fragment.getOutputTypes());

        if (partitioningScheme.getShuffleHandle().isSinglePartition() || partitioningScheme.getPartitionMode()
            .equals(PartitionShuffleHandle.PartitionShuffleMode.BROADCAST)
            || partitioningScheme.getPartitionCount() == 1) {
            return new TaskOutputCollector(inputType, outputType, outputBuffer,
                this.pagesSerdeFactory.createPagesSerde(outputType, context), context);
        } else {
            int chunkLimit = context.getParamManager().getInt(ConnectionParams.CHUNK_SIZE);
            return new PartitionedOutputCollector(partitioningScheme.getPartitionCount(),
                partitioningScheme.getPrunePartitions(), partitioningScheme.getFullPartCount(),
                inputType, partitioningScheme.isRemotePairWise(), outputType, partitioningScheme.getPartChannels(),
                outputBuffer, this.pagesSerdeFactory,
                chunkLimit, context);
        }
    }
}
