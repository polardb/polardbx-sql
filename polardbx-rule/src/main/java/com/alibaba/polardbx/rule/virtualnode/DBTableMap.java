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

package com.alibaba.polardbx.rule.virtualnode;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.google.common.collect.Maps;

/**
 * 构造group的一致性hash的slot
 * 
 * @author <a href="junyu@taobao.com">junyu</a>
 * @version 1.0
 * @since 1.6
 * @date 2011-6-2 03:12:39
 */
public class DBTableMap extends WrappedLogic implements VirtualNodeMap {

    private Map<String/* slot number */, String/* group_0 */> dbContext  = Maps.newConcurrentMap();
    private Map<String, String>                               dbTableMap = Maps.newHashMap();
    private String                                            logicTable;

    private Map<String/* group */, PartitionFunction>         parFuncMap;

    public void init() {
        this.initDbTableMap(this.dbTableMap);
        if (null != dbTableMap && dbTableMap.size() > 0) {
            dbContext = extraReverseMap(dbTableMap);
            addLogicTableAndSplitorToKey();
        } else {
            throw new IllegalArgumentException("no dbTableMap config at all");
        }
    }

    public String getValue(String tableSuffix) {
        return dbContext.get(tableSuffix);
    }

    private void addLogicTableAndSplitorToKey() {
        if (tableSlotKeyFormat != null) {
            ConcurrentHashMap<String, String> reKeyMap = new ConcurrentHashMap<String, String>();
            for (Map.Entry<String, String> entry : dbContext.entrySet()) {
                String newKey = super.wrapValue(entry.getKey());
                reKeyMap.put(newKey, entry.getValue());
            }
            dbContext.clear();
            dbContext = reKeyMap;
        } else if (this.logicTable != null) {
            ConcurrentHashMap<String, String> reKeyMap = new ConcurrentHashMap<String, String>();
            for (Map.Entry<String, String> entry : dbContext.entrySet()) {
                StringBuilder sb = new StringBuilder(this.logicTable);
                sb.append(tableSplitor);
                sb.append(entry.getKey());
                reKeyMap.put(sb.toString(), entry.getValue());
            }
            dbContext.clear();
            dbContext = reKeyMap;
        } else {
            // throw new
            // RuntimeException("TableRule no tableSlotKeyFormat property and logicTable is null");
        }
    }

    private void initDbTableMap(Map<String, String> dbTableMap) {
        if (this.parFuncMap == null || this.parFuncMap.isEmpty()) {
            return;
        }

        for (String key : this.parFuncMap.keySet()) {
            PartitionFunction parFunc = this.parFuncMap.get(key);
            String value = this.buildDbTableMapValue(parFunc);
            dbTableMap.put(key, value);
        }
    }

    private String buildDbTableMapValue(PartitionFunction parFunc) {
        StringBuilder valueBuilder = new StringBuilder();
        int[] partitionCount = parFunc.getCount();
        int[] partitionLength = parFunc.getLength();
        int firstValue = parFunc.getFirstValue();

        for (int i = 0; i < partitionCount.length; i++) {
            for (int j = 0; j < partitionCount[i]; j++) {
                int key = firstValue + partitionLength[i];
                firstValue = key;
                valueBuilder.append(key);
                valueBuilder.append(",");
            }
        }

        valueBuilder.deleteCharAt(valueBuilder.length() - 1);
        return valueBuilder.toString();
    }

    // ===================== setter / getter ====================

    public void setDbTableMap(Map<String, String> dbTableMap) {
        this.dbTableMap = dbTableMap;
    }

    public void setLogicTable(String logicTable) {
        this.logicTable = logicTable;
    }

    public void setParFuncMap(Map<String, PartitionFunction> parFuncMap) {
        this.parFuncMap = parFuncMap;
    }
}
