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
import com.alibaba.polardbx.executor.ddl.job.task.RemoteExecutableDdlTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.optimizer.partition.pruning.PhysicalPartitionInfo;
import lombok.Getter;

import java.util.List;
import java.util.Optional;

@Getter
@TaskName(name = "FileValidationMPPTask")
public class FileValidationMppTask extends FileValidationTask implements RemoteExecutableDdlTask {

    /**
     * same definition as {@link ArchiveOSSTableDataMppTask}
     */
    private final Integer totalTaskNumber;
    private final Integer serialNumber;

    @JSONCreator
    public FileValidationMppTask(String schemaName, String logicalTableName,
                                 String loadTableSchema, String loadTableName,
                                 String localPartitionName, Integer totalTaskNumber, Integer serialNumber) {
        super(schemaName, logicalTableName, loadTableSchema, loadTableName, localPartitionName);
        this.totalTaskNumber = totalTaskNumber;
        this.serialNumber = serialNumber;
    }

    @Override
    protected List<PhysicalPartitionInfo> getFlattenedPartitionInfo(String schema, String table) {
        return OSSTaskUtils.getOrderedPartitionInfo(schema, table, totalTaskNumber, serialNumber);
    }

    protected String remark() {
        // find local partition lower bound && upper bound
        return String.format("|%s.%s, task SN: %d/%d", loadTableSchema, loadTableName,
            serialNumber, totalTaskNumber);
    }

    @Override
    public Optional<String> chooseServer() {
        if (forbidRemoteDdlTask()) {
            return Optional.empty();
        }
        return OSSTaskUtils.chooseRemoteNode(taskId);
    }
}
