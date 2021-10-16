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

package com.alibaba.polardbx.optimizer.sharding.result;

import com.alibaba.polardbx.common.model.sqljep.Comparative;
import com.alibaba.polardbx.common.utils.CaseInsensitive;

import java.util.Map;
import java.util.TreeMap;

/**
 * @author chenghui.lch
 */
public class PlanShardInfo {

    /**
     * key: schemaName.tableName
     * val: the shard info of table
     */
    protected Map<String, RelShardInfo> allRelShardInfo;
    ExtractionResult er;

    public PlanShardInfo() {
        allRelShardInfo = new TreeMap<>(CaseInsensitive.CASE_INSENSITIVE_ORDER);
    }

    public Map<String, Map<String, Comparative>> getAllTableComparative(String schemaName) {
        Map<String, Map<String, Comparative>> allTableCompInfo = new TreeMap<>(CaseInsensitive.CASE_INSENSITIVE_ORDER);
        allRelShardInfo.forEach((k, v) -> {
            String dbName = v.getSchemaName();
            if (dbName.equalsIgnoreCase(schemaName)) {
                Map<String, Comparative> tmpAllComps = new TreeMap<>(CaseInsensitive.CASE_INSENSITIVE_ORDER);
                tmpAllComps.putAll(v.getAllComps());
                allTableCompInfo.put(v.getTableName(), tmpAllComps);
            }
        });
        return allTableCompInfo;
    }

    public Map<String, Map<String, Comparative>> getAllTableFullComparative(String schemaName) {
        Map<String, Map<String, Comparative>> allTableCompInfo = new TreeMap<>(CaseInsensitive.CASE_INSENSITIVE_ORDER);
        allRelShardInfo.forEach((k, v) -> {
            String dbName = v.getSchemaName();
            if (dbName.equalsIgnoreCase(schemaName)) {
                Map<String, Comparative> tmpAllFullComps = new TreeMap<>(CaseInsensitive.CASE_INSENSITIVE_ORDER);
                tmpAllFullComps.putAll(v.getAllFullComps());
                allTableCompInfo.put(v.getTableName(), tmpAllFullComps);
            }
        });
        return allTableCompInfo;
    }

    public void putRelShardInfo(String schemaName, String tableName, RelShardInfo relShardInfo) {
        allRelShardInfo.put(buildFullTableNameKey(schemaName, tableName), relShardInfo);
    }

    public RelShardInfo getRelShardInfo(String schemaName, String tableName) {
        RelShardInfo relShardInfo = allRelShardInfo.get(buildFullTableNameKey(schemaName, tableName));
        return relShardInfo;
    }

    private String buildFullTableNameKey(String schemaName, String tableName) {
        StringBuilder sb = new StringBuilder();
        sb.append(schemaName);
        sb.append(".");
        sb.append(tableName);
        return sb.toString().toLowerCase();
    }

    public void setEr(ExtractionResult er) {
        this.er = er;
    }

    public ExtractionResult getEr() {
        return er;
    }
}
