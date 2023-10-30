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

import com.alibaba.polardbx.common.utils.Pair;

import java.util.ArrayList;
import java.util.List;

public class AlterTableGroupSetLocalityPreparedData extends DdlPreparedData {

    public AlterTableGroupSetLocalityPreparedData() {
    }

    private String tableGroupName;

    private List<String> drainNodeList;

    private String targetLocality;

    private String sourceSql;

    public Boolean getNeedToGetTableGroupLock() {
        return needToGetTableGroupLock;
    }

    public void setNeedToGetTableGroupLock(Boolean needToGetTableGroupLock) {
        this.needToGetTableGroupLock = needToGetTableGroupLock;
    }

    private Boolean needToGetTableGroupLock;

    private Boolean withRebalance;

    private String rebalanceSql;

    private Boolean withMergeTableGroup;

    public Boolean getWithMergeTableGroup() {
        return withMergeTableGroup;
    }

    public void setWithMergeTableGroup(Boolean withMergeTableGroup) {
        this.withMergeTableGroup = withMergeTableGroup;
    }

    public List<String> getMergeTableGroupSql() {
        return mergeTableGroupSql;
    }

    public void setMergeTableGroupSql(List<String> mergeTableGroupSql) {
        this.mergeTableGroupSql = mergeTableGroupSql;
    }

    private List<String> mergeTableGroupSql;

    public String getTableGroupName() {
        return tableGroupName;
    }

    public void setTableGroupName(String tableGroupName) {
        this.tableGroupName = tableGroupName;
    }

    public String getSourceSql() {
        return sourceSql;
    }

    public void setSourceSql(String sourceSql) {
        this.sourceSql = sourceSql;
    }

    public List<String> getDrainNodeList() {
        return this.drainNodeList;
    }

    public void setDrainNodeList(List<String> drainNodeList) {
        this.drainNodeList = drainNodeList;
    }

    public Boolean getWithRebalance() {
        return this.withRebalance;
    }

    public void setWithRebalance(Boolean withRebalance) {
        this.withRebalance = withRebalance;
    }

    public String getTargetLocality() {
        return targetLocality;
    }

    public void setTargetLocality(String targetLocality) {
        this.targetLocality = targetLocality;
    }

    public String getRebalanceSql() {
        return rebalanceSql;
    }

    public void setRebalanceSql(String rebalanceSql) {
        this.rebalanceSql = rebalanceSql;
    }
}
