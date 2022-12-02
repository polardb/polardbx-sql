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

package com.alibaba.polardbx.optimizer.utils;

import java.util.List;
import java.util.Map;

public class TargetTableInfo {

    protected Map<GroupConnId, Map<String, List<List<String>>>> grpConnIdTargetTablesMap;
    protected Map<String, List<GroupConnId>> grpToConnSetMap;

    public TargetTableInfo() {
    }

    public Map<GroupConnId, Map<String, List<List<String>>>> getGrpConnIdTargetTablesMap() {
        return grpConnIdTargetTablesMap;
    }

    public void setGrpConnIdTargetTablesMap(
        Map<GroupConnId, Map<String, List<List<String>>>> grpConnIdTargetTablesMap) {
        this.grpConnIdTargetTablesMap = grpConnIdTargetTablesMap;
    }

    public Map<String, List<GroupConnId>> getGrpToConnSetMap() {
        return grpToConnSetMap;
    }

    public void setGrpToConnSetMap(
        Map<String, List<GroupConnId>> grpToConnSetMap) {
        this.grpToConnSetMap = grpToConnSetMap;
    }
}
