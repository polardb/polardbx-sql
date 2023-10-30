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

package com.alibaba.polardbx.executor.ddl.job.task.basic.oss;

import com.alibaba.fastjson.annotation.JSONCreator;
import com.alibaba.polardbx.common.ArchiveMode;
import com.alibaba.polardbx.common.Engine;
import com.alibaba.polardbx.common.ddl.newengine.DdlTaskState;
import com.alibaba.polardbx.executor.ddl.job.converter.PhysicalPlanData;
import com.alibaba.polardbx.executor.ddl.job.task.RemoteExecutableDdlTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.partition.pruning.PhysicalPartitionInfo;
import lombok.Getter;
import org.apache.commons.lang.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

@Getter
@TaskName(name = "CreateOssTableGenerateDataMppTask")
public class CreateOssTableGenerateDataMppTask extends CreateOssTableGenerateDataTask implements
    RemoteExecutableDdlTask {

    private int totalTaskNumber;

    private int serialNumber;

    @JSONCreator
    public CreateOssTableGenerateDataMppTask(String schemaName, String logicalTableName,
                                             PhysicalPlanData physicalPlanData,
                                             String loadTableSchema, String loadTableName, Engine tableEngine,
                                             ArchiveMode archiveMode, int totalTaskNumber, int serialNumber) {
        super(schemaName, logicalTableName, physicalPlanData, loadTableSchema, loadTableName, tableEngine, archiveMode);
        this.totalTaskNumber = totalTaskNumber;
        this.serialNumber = serialNumber;
        onExceptionTryRollback();
    }

    @Override
    protected void beforeTransaction(ExecutionContext executionContext) {
        updateTaskStateInNewTxn(DdlTaskState.DIRTY);
    }

    @Override
    protected List<PhysicalPartitionInfo> getFlattenedPartitionInfo(
        Map<String, List<PhysicalPartitionInfo>> partitionInfoMap) {
        List<PhysicalPartitionInfo> partitionInfos = new ArrayList<>();
        partitionInfoMap.values().forEach(partitionInfos::addAll);
        return OSSTaskUtils.getOrderedPartitionInfo(partitionInfos, totalTaskNumber, serialNumber);
    }

    @Override
    public Optional<String> chooseServer() {
        if (forbidRemoteDdlTask()) {
            return Optional.empty();
        }
        return OSSTaskUtils.chooseRemoteNode(taskId);
    }

    protected String remark() {
        String partitions =
            getFlattenedPartitionInfo(physicalPlanData.getPhysicalPartitionTopology()).stream()
                .map(x -> x.getPartName()).collect(
                    Collectors.joining(","));
        if (StringUtils.isEmpty(partitions)) {
            partitions = "empty";
        }
        return String.format("|%s.%s, task SN: %d/%d, partitions: %s", loadTableSchema, loadTableName,
            serialNumber, totalTaskNumber, partitions);
    }
}
