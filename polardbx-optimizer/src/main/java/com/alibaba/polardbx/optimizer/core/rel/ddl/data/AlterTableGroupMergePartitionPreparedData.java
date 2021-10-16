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

package com.alibaba.polardbx.optimizer.core.rel.ddl.data;

import com.alibaba.polardbx.gms.topology.GroupDetailInfoExRecord;
import com.alibaba.polardbx.optimizer.config.table.ComplexTaskMetaManager;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlPartition;
import org.apache.calcite.util.Pair;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class AlterTableGroupMergePartitionPreparedData extends AlterTableGroupBasePreparedData {

    public AlterTableGroupMergePartitionPreparedData() {
    }

    /*
    key: target partition
    value: partitions to be merge
    */
    private Map<String, List<String>> mergePartitions;
    /*
    key: target partition
    value: storage instance
    */
    private Pair<String, String> targetStorageInstPair;

    public Map<String, List<String>> getMergePartitions() {
        return mergePartitions;
    }

    public void setMergePartitions(Map<String, List<String>> mergePartitions) {
        this.mergePartitions = mergePartitions;
        List<String> oldPartitions = new ArrayList<>();
        List<String> newPartitions = new ArrayList<>();
        for (Map.Entry<String, List<String>> entry : mergePartitions.entrySet()) {
            newPartitions.add(entry.getKey());
            oldPartitions.addAll(entry.getValue());
        }
        setOldPartitionNames(oldPartitions);
        setNewPartitionNames(newPartitions);
    }

    public Pair<String, String> getTargetStorageInstPair() {
        return targetStorageInstPair;
    }

    public void setTargetStorageInstPair(Pair<String, String> targetStorageInstPair) {
        this.targetStorageInstPair = targetStorageInstPair;
    }
}
