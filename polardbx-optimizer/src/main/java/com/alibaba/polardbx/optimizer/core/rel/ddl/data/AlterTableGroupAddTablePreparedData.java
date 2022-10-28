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

import com.alibaba.polardbx.gms.tablegroup.TableGroupConfig;

import java.util.Map;
import java.util.Set;

public class AlterTableGroupAddTablePreparedData extends DdlPreparedData {

    public AlterTableGroupAddTablePreparedData() {
    }

    private String tableGroupName;
    // if the tableGroup is empty(no table), then we will choose one table
    // in the add list as reference table, other tables's location will align
    // with the reference table
    private String referenceTable;
    private Set<String> tables;
    private TableGroupConfig tableGroupConfig;
    private Map<String, Long> tableVersions;
    private boolean force = false;

    public String getTableGroupName() {
        return tableGroupName;
    }

    public void setTableGroupName(String tableGroupName) {
        this.tableGroupName = tableGroupName;
    }

    public Set<String> getTables() {
        return tables;
    }

    public void setTables(Set<String> tables) {
        this.tables = tables;
    }

    public TableGroupConfig getTableGroupConfig() {
        return tableGroupConfig;
    }

    public void setTableGroupConfig(TableGroupConfig tableGroupConfig) {
        this.tableGroupConfig = tableGroupConfig;
    }

    public String getReferenceTable() {
        return referenceTable;
    }

    public void setReferenceTable(String referenceTable) {
        this.referenceTable = referenceTable;
    }

    public Map<String, Long> getTableVersions() {
        return tableVersions;
    }

    public void setTableVersions(Map<String, Long> tableVersions) {
        this.tableVersions = tableVersions;
    }

    public boolean isForce() {
        return force;
    }

    public void setForce(boolean force) {
        this.force = force;
    }
}
