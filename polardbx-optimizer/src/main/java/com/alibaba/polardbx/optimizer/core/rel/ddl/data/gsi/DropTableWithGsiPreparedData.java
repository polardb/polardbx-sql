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

package com.alibaba.polardbx.optimizer.core.rel.ddl.data.gsi;

import com.alibaba.polardbx.optimizer.core.rel.ddl.data.DdlPreparedData;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.DropTablePreparedData;
import org.jetbrains.annotations.NotNull;

import java.util.LinkedHashMap;
import java.util.Map;

public class DropTableWithGsiPreparedData extends DdlPreparedData {

    private DropTablePreparedData primaryTablePreparedData;
    private Map<String, DropGlobalIndexPreparedData> indexTablePreparedDataMap = new LinkedHashMap<>();

    public DropTablePreparedData getPrimaryTablePreparedData() {
        return primaryTablePreparedData;
    }

    public void setPrimaryTablePreparedData(DropTablePreparedData primaryTablePreparedData) {
        this.primaryTablePreparedData = primaryTablePreparedData;
    }

    public Map<String, DropGlobalIndexPreparedData> getIndexTablePreparedDataMap() {
        return indexTablePreparedDataMap;
    }

    public DropGlobalIndexPreparedData getIndexTablePreparedData(String indexTableName) {
        return indexTablePreparedDataMap.get(indexTableName);
    }

    public void addIndexTablePreparedData(DropGlobalIndexPreparedData indexTablePreparedData) {
        String indexTableName = indexTablePreparedData.getIndexTableName();
        this.indexTablePreparedDataMap.put(indexTableName, indexTablePreparedData);
    }

    public boolean hasGsi() {
        return indexTablePreparedDataMap.size() > 0;
    }

    public void setDdlVersionId(@NotNull Long versionId) {
        super.setDdlVersionId(versionId);
        if (null != indexTablePreparedDataMap) {
            indexTablePreparedDataMap.values().forEach(p -> p.setDdlVersionId(versionId));
        }
    }

}
