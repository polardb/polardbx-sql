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

import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.gms.util.PartitionNameUtil;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlPartition;
import org.apache.calcite.sql.SqlSubPartition;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class AlterTableGroupSplitPartitionPreparedData extends AlterTableGroupBasePreparedData {

    public AlterTableGroupSplitPartitionPreparedData() {
    }

    private List<SqlPartition> newPartitions;

    private boolean includeFullPartitionDefinition;
    private List<String> targetStorageInstIds;
    private Map<SqlNode, RexNode> partBoundExprInfo;
    private SqlNode atVal;
    private boolean splitSubPartition;

    public List<String> getSplitPartitions() {
        return getOldPartitionNames();
    }

    public void setSplitPartitions(List<String> splitPartitions) {
        setOldPartitionNames(splitPartitions);
    }

    public List<SqlPartition> getNewPartitions() {
        return newPartitions;
    }

    public void setNewPartitions(List<SqlPartition> newPartitions) {
        this.newPartitions = newPartitions;
        List<String> newPartitionNames = new ArrayList<>();
        Map<String, String> newPartitionLocalities = new HashMap<>();
        for (SqlPartition sqlPartition : newPartitions) {
            String partitionName = ((SqlIdentifier) (sqlPartition).getName()).getSimple();
            if (GeneralUtil.isNotEmpty(sqlPartition.getSubPartitions())) {
                for (SqlNode sqlNode : sqlPartition.getSubPartitions()) {
                    String subPartitionName = ((SqlIdentifier) ((SqlSubPartition) sqlNode).getName()).getSimple();
                    newPartitionNames.add(subPartitionName);
                }
            } else {
                newPartitionNames.add(partitionName);
            }
            newPartitionLocalities.put(sqlPartition.getName().toString(), sqlPartition.getLocality());
        }
        setNewPartitionNames(newPartitionNames);
        setNewPartitionLocalities(newPartitionLocalities);
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

    public SqlNode getAtVal() {
        return atVal;
    }

    public void setAtVal(SqlNode atVal) {
        this.atVal = atVal;
    }

    public boolean isSplitSubPartition() {
        return splitSubPartition;
    }

    public void setSplitSubPartition(boolean splitSubPartition) {
        this.splitSubPartition = splitSubPartition;
    }
}
