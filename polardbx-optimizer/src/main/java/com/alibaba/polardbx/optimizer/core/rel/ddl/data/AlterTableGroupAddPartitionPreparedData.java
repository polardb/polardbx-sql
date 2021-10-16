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

import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlPartition;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class AlterTableGroupAddPartitionPreparedData extends AlterTableGroupBasePreparedData {

    public AlterTableGroupAddPartitionPreparedData() {
    }

    private List<SqlPartition> newPartitions;
    private boolean includeFullPartitionDefinition;
    private List<String> targetStorageInstIds;
    private Map<SqlNode, RexNode> partBoundExprInfo;

    public void setSplitPartitions(List<String> splitPartitions) {
        setOldPartitionNames(splitPartitions);
    }

    public List<SqlPartition> getNewPartitions() {
        return newPartitions;
    }

    public void setNewPartitions(List<SqlPartition> newPartitions) {
        this.newPartitions = newPartitions;
        List<String> newPartitionNames = new ArrayList<>();
        for (SqlPartition sqlPartition : newPartitions) {
            newPartitionNames.add(sqlPartition.getName().toString());
        }
        setNewPartitionNames(newPartitionNames);
    }

    public boolean isIncludeFullPartitionDefinition() {
        return includeFullPartitionDefinition;
    }

    public void setIncludeFullPartitionDefinition(boolean includeFullPartitionDefinition) {
        this.includeFullPartitionDefinition = includeFullPartitionDefinition;
    }

    public List<String> getTargetStorageInstIds() {
        return targetStorageInstIds;
    }

    public void setTargetStorageInstIds(List<String> targetStorageInstIds) {
        this.targetStorageInstIds = targetStorageInstIds;
    }

    public Map<SqlNode, RexNode> getPartBoundExprInfo() {
        return partBoundExprInfo;
    }

    public void setPartBoundExprInfo(
        Map<SqlNode, RexNode> partBoundExprInfo) {
        this.partBoundExprInfo = partBoundExprInfo;
    }
}
