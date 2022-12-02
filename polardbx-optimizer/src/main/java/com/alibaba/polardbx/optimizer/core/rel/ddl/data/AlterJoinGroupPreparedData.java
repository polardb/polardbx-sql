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

import java.util.Map;
import java.util.Set;

/**
 * Created by luoyanxin.
 *
 * @author luoyanxin
 */
public class AlterJoinGroupPreparedData extends DdlPreparedData {

    public AlterJoinGroupPreparedData(String schemaName, String joinGroupName, boolean isAdd,
                                      Map<String, Long> tablesVersion,
                                      Map<String, Pair<Set<String>, Set<String>>> tableGroupInfos) {
        super(schemaName, "");
        this.joinGroupName = joinGroupName;
        this.isAdd = isAdd;
        this.tablesVersion = tablesVersion;
        this.tableGroupInfos = tableGroupInfos;
    }

    private final String joinGroupName;
    private final boolean isAdd;
    private final Map<String, Long> tablesVersion;
    /**
     * key:tableGroupName
     * value:
     * key:tables be altered
     * value:all tables in current tableGroup
     */
    private final Map<String, Pair<Set<String>, Set<String>>> tableGroupInfos;

    public String getJoinGroupName() {
        return joinGroupName;
    }

    public boolean isAdd() {
        return isAdd;
    }

    public Map<String, Long> getTablesVersion() {
        return tablesVersion;
    }

    public Map<String, Pair<Set<String>, Set<String>>> getTableGroupInfos() {
        return tableGroupInfos;
    }
}
