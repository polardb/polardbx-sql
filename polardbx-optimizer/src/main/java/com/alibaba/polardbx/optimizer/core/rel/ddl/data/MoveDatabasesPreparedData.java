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
import java.util.Map;

/**
 * Created by luoyanxin.
 *
 * @author luoyanxin
 */
public class MoveDatabasesPreparedData extends DdlPreparedData {

    public MoveDatabasesPreparedData(Map<String, Map<String, List<String>>> logicalDbStorageGroups, String sourceSql) {
        this.logicalDbStorageGroups = logicalDbStorageGroups;
        this.sourceSql = sourceSql;
    }

    private String sourceSql;
    /**
     * logicalDbStorageGroups
     * key: logicalDBName
     * values: <key:storageInst, value:sourceGroupNames>
     */
    private Map<String, Map<String, List<String>>> logicalDbStorageGroups;

    public String getSourceSql() {
        return sourceSql;
    }

    public void setSourceSql(String sourceSql) {
        this.sourceSql = sourceSql;
    }

    public Map<String, Map<String, List<String>>> getLogicalDbStorageGroups() {
        return logicalDbStorageGroups;
    }

    public void setLogicalDbStorageGroups(Map<String, Map<String, List<String>>> logicalDbStorageGroups) {
        this.logicalDbStorageGroups = logicalDbStorageGroups;
    }
}
