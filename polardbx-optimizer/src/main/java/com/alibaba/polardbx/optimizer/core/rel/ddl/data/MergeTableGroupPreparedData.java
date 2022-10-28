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
import com.alibaba.polardbx.gms.tablegroup.TableGroupConfig;

import java.util.Map;
import java.util.Set;

/**
 * Created by luoyanxin.
 *
 * @author luoyanxin
 */
public class MergeTableGroupPreparedData extends DdlPreparedData {

    public MergeTableGroupPreparedData(String schemaName, String targetTableGroupName, Set<String> sourceTableGroups,
                                       Map<String, TableGroupConfig> tableGroupConfigMap,
                                       Map<String, Map<String, Long>> tablesVersion,
                                       Set<String> physicalGroups, Map<String, String> dbInstMap, boolean force) {
        super(schemaName, "");
        this.targetTableGroupName = targetTableGroupName;
        this.sourceTableGroups = sourceTableGroups;
        this.tableGroupConfigMap = tableGroupConfigMap;
        this.tablesVersion = tablesVersion;
        this.physicalGroups = physicalGroups;
        this.dbInstMap = dbInstMap;
        this.force = force;
    }

    private final String targetTableGroupName;
    private final Set<String> sourceTableGroups;
    private final Map<String, TableGroupConfig> tableGroupConfigMap;
    private final Map<String, Map<String, Long>> tablesVersion;
    private final Set<String> physicalGroups;
    private final Map<String, String> dbInstMap;
    private final boolean force;

    public Map<String, Map<String, Long>> getTablesVersion() {
        return tablesVersion;
    }

    public String getTargetTableGroupName() {
        return targetTableGroupName;
    }

    public Set<String> getSourceTableGroups() {
        return sourceTableGroups;
    }

    public Set<String> getPhysicalGroups() {
        return physicalGroups;
    }

    public Map<String, TableGroupConfig> getTableGroupConfigMap() {
        return tableGroupConfigMap;
    }

    public Map<String, String> getDbInstMap() {
        return dbInstMap;
    }

    public boolean isForce() {
        return force;
    }
}
