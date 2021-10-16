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

import java.util.List;
import java.util.Map;

public class RefreshTopologyPreparedData {

    Map<String, Pair<TableGroupConfig, Map<String, List<Pair<String, String>>>>> dbTableGroupAndInstGroupInfo;
    Map<String, RefreshDbTopologyPreparedData> allRefreshTopologyPreparedData;

    public RefreshTopologyPreparedData() {
    }

    public Map<String, Pair<TableGroupConfig, Map<String, List<Pair<String, String>>>>> getDbTableGroupAndInstGroupInfo() {
        return dbTableGroupAndInstGroupInfo;
    }

    public void setDbTableGroupAndInstGroupInfo(
        Map<String, Pair<TableGroupConfig, Map<String, List<Pair<String, String>>>>> dbTableGroupAndInstGroupInfo) {
        this.dbTableGroupAndInstGroupInfo = dbTableGroupAndInstGroupInfo;
    }

    public Map<String, RefreshDbTopologyPreparedData> getAllRefreshTopologyPreparedData() {
        return allRefreshTopologyPreparedData;
    }

    public void setAllRefreshTopologyPreparedData(
        Map<String, RefreshDbTopologyPreparedData> allRefreshTopologyPreparedData) {
        this.allRefreshTopologyPreparedData = allRefreshTopologyPreparedData;
    }
}
