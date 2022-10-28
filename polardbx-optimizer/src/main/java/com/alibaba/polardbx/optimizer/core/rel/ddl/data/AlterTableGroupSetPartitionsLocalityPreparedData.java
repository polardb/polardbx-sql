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

import java.util.List;

public class AlterTableGroupSetPartitionsLocalityPreparedData extends DdlPreparedData {

    public AlterTableGroupSetPartitionsLocalityPreparedData() {
    }

    private String tableGroupName;

    private List<String> drainNodeList;

    private String partition;

    private String targetLocality;

    private String sourceSql;

    private Boolean withRebalance;

    private String rebalanceSql;

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

    public String getPartition() {
        return partition;
    }

    public void setPartition(String partition) {
        this.partition = partition;
    }
}
