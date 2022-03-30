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

import java.util.List;
import java.util.Map;

public class RefreshDbTopologyPreparedData extends AlterTableGroupBasePreparedData {

    //key: storage_inst, pair:group and physicalDbName
    Map<String, List<Pair<String, String>>> instGroupDbInfo;

    public RefreshDbTopologyPreparedData() {
    }

    public Map<String, List<Pair<String, String>>> getInstGroupDbInfo() {
        return instGroupDbInfo;
    }

    public void setInstGroupDbInfo(
        Map<String, List<Pair<String, String>>> instGroupDbInfo) {
        this.instGroupDbInfo = instGroupDbInfo;
    }
}
