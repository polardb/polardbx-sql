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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class AlterTableGroupExtractPartitionPreparedData extends AlterTableGroupBasePreparedData {

    private Map<SqlNode, RexNode> partBoundExprInfo;
    private List<SqlNode> hotKeys;
    private List<SqlPartition> newPartitions;
    List<Long[]> splitPoints;

    public AlterTableGroupExtractPartitionPreparedData() {
    }

    public Map<SqlNode, RexNode> getPartBoundExprInfo() {
        return partBoundExprInfo;
    }

    public void setPartBoundExprInfo(
        Map<SqlNode, RexNode> partBoundExprInfo) {
        this.partBoundExprInfo = partBoundExprInfo;
    }

    public List<SqlNode> getHotKeys() {
        return hotKeys;
    }

    public void setHotKeys(List<SqlNode> hotKeys) {
        this.hotKeys = hotKeys;
    }

    public List<SqlPartition> getNewPartitions() {
        return newPartitions;
    }

    public void setNewPartitions(List<SqlPartition> newPartitions) {
        this.newPartitions = newPartitions;
        List<String> newPartitionNames = new ArrayList<>();
        Map<String, String> newPartitionLocalities = new HashMap<>();
        for (SqlPartition sqlPartition : newPartitions) {
            newPartitionNames.add(sqlPartition.getName().toString());
            newPartitionLocalities.put(sqlPartition.getName().toString(), sqlPartition.getLocality());
        }
        setNewPartitionNames(newPartitionNames);
        setNewPartitionLocalities(newPartitionLocalities);
    }

    public void setSplitPartitions(List<String> splitPartitions) {
        setOldPartitionNames(splitPartitions);
    }

    public List<Long[]> getSplitPoints() {
        return splitPoints;
    }

    public void setSplitPoints(List<Long[]> splitPoints) {
        this.splitPoints = splitPoints;
    }
}
