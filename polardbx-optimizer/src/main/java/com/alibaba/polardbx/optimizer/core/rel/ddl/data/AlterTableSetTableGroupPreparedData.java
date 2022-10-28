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

import java.util.Map;
import java.util.TreeMap;

public class AlterTableSetTableGroupPreparedData extends DdlPreparedData {

    public AlterTableSetTableGroupPreparedData() {
    }

    private String tableGroupName;
    private String sourceSql;
    private String originalTableGroup;
    private Long tableVersion;
    private String primaryTableName;
    private String originalJoinGroup;
    private boolean alignPartitionNameFirst = false;
    private boolean repartition = false;
    private boolean force = false;
    private Map<String, String> partitionNamesMap = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);

    public String getTableGroupName() {
        return tableGroupName;
    }

    public void setTableGroupName(String tableGroupName) {
        this.tableGroupName = tableGroupName;
    }

    public String getOriginalTableGroup() {
        return originalTableGroup;
    }

    public void setOriginalTableGroup(String originalTableGroup) {
        this.originalTableGroup = originalTableGroup;
    }

    public String getOriginalJoinGroup() {
        return originalJoinGroup;
    }

    public void setOriginalJoinGroup(String originalJoinGroup) {
        this.originalJoinGroup = originalJoinGroup;
    }

    public String getSourceSql() {
        return sourceSql;
    }

    public void setSourceSql(String sourceSql) {
        this.sourceSql = sourceSql;
    }

    public Long getTableVersion() {
        return tableVersion;
    }

    public void setTableVersion(Long tableVersion) {
        this.tableVersion = tableVersion;
    }

    public String getPrimaryTableName() {
        return primaryTableName;
    }

    public void setPrimaryTableName(String primaryTableName) {
        this.primaryTableName = primaryTableName;
    }

    public boolean isAlignPartitionNameFirst() {
        return alignPartitionNameFirst;
    }

    public void setAlignPartitionNameFirst(boolean alignPartitionNameFirst) {
        this.alignPartitionNameFirst = alignPartitionNameFirst;
    }

    public boolean isRepartition() {
        return repartition;
    }

    public void setRepartition(boolean repartition) {
        this.repartition = repartition;
    }

    public Map<String, String> getPartitionNamesMap() {
        return partitionNamesMap;
    }

    public boolean isForce() {
        return force;
    }

    public void setForce(boolean force) {
        this.force = force;
    }
}
