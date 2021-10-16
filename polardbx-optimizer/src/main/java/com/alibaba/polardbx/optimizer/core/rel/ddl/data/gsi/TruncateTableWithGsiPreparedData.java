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

import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalCreateTable;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.DdlPreparedData;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.TruncateTablePreparedData;

import java.util.LinkedHashMap;
import java.util.Map;

public class TruncateTableWithGsiPreparedData extends DdlPreparedData {

    private TruncateTablePreparedData primaryTablePreparedData;
    private Map<String, TruncateGlobalIndexPreparedData> indexTablePreparedDataMap = new LinkedHashMap<>();

    private LogicalCreateTable logicalCreateTable;
    private String tmpTableSuffix;
    private Map<String, String> tmpIndexTableMap;

    public TruncateTablePreparedData getPrimaryTablePreparedData() {
        return primaryTablePreparedData;
    }

    public void setPrimaryTablePreparedData(TruncateTablePreparedData primaryTablePreparedData) {
        this.primaryTablePreparedData = primaryTablePreparedData;
    }

    public Map<String, TruncateGlobalIndexPreparedData> getIndexTablePreparedDataMap() {
        return indexTablePreparedDataMap;
    }

    public TruncateGlobalIndexPreparedData getIndexTablePreparedData(String indexTableName) {
        return indexTablePreparedDataMap.get(indexTableName);
    }

    public void addIndexTablePreparedData(TruncateGlobalIndexPreparedData indexTablePreparedData) {
        String indexTableName = indexTablePreparedData.getIndexTableName();
        this.indexTablePreparedDataMap.put(indexTableName, indexTablePreparedData);
    }

    public LogicalCreateTable getLogicalCreateTable() {
        return logicalCreateTable;
    }

    public void setLogicalCreateTable(LogicalCreateTable logicalCreateTable) {
        this.logicalCreateTable = logicalCreateTable;
    }

    public String getTmpTableSuffix() {
        return tmpTableSuffix;
    }

    public void setTmpTableSuffix(String tmpTableSuffix) {
        this.tmpTableSuffix = tmpTableSuffix;
    }

    public Map<String, String> getTmpIndexTableMap() {
        return tmpIndexTableMap;
    }

    public void setTmpIndexTableMap(Map<String, String> tmpIndexTableMap) {
        this.tmpIndexTableMap = tmpIndexTableMap;
    }

    public String getPrimaryTableName() {
        return primaryTablePreparedData.getTableName();
    }

    public String getSchemaName() {
        return primaryTablePreparedData.getSchemaName();
    }

    public boolean hasGsi() {
        return indexTablePreparedDataMap.size() > 0;
    }

}
