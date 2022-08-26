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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.google.common.collect.Maps;

/**
 * 构造table的一致性hash的slot
 * 
 * @version 1.0
 * @since 1.6
 * @date 2011-6-2 03:13:08
 */
public class TableSlotMap extends WrappedLogic implements VirtualNodeMap {

    private String                                                   logicTable;

    protected Map<String/* slot number */, String/* table suffix */> tableContext = Maps.newConcurrentMap();
    private Map<String/* table suffix */, String/* slot string */>   tableSlotMap = Maps.newHashMap();

    private PartitionFunction                                        keyPartitionFunction;
    private PartitionFunction                                        valuePartitionFunction;

    public void init() {
        this.initTableSlotMap(tableSlotMap);
        if (null != tableSlotMap && tableSlotMap.size() > 0) {
            tableContext = extraReverseMap(tableSlotMap);
        } else {
            throw new IllegalArgumentException("no tableSlotMap config at all");
        }
    }

    private void initTableSlotMap(Map<String, String> tableSlotMap) {
        if (this.keyPartitionFunction == null || this.valuePartitionFunction == null) {
            return;
        }

        List<String> keys = this.buildTableSlotMapKeys();
        List<String> values = this.buildTableSlotMapValues();

        for (int i = 0; i < keys.size(); i++) {
            tableSlotMap.put(keys.get(i), values.get(i));
        }
    }

    public String getValue(String key) {
        String suffix = tableContext.get(key);
        // TODO 增加虚拟节点使用统计
        // TotalStatMonitor.virtualSlotIncrement(buildLogKey(key));
        if (super.tableSlotKeyFormat != null) {
            return super.wrapValue(suffix);
        } else if (logicTable != null) {
            StringBuilder sb = new StringBuilder();
            sb.append(logicTable);
            sb.append(tableSplitor);
            sb.append(suffix);
            return sb.toString();
        } else {
            // throw new
            // RuntimeException("TableRule no tableSlotKeyFormat property and logicTable is null");
            return suffix;
        }
    }

    // ======================== helper method ======================

    private List<String> buildTableSlotMapKeys() {
        List<String> result = new ArrayList<String>();
        int[] partitionCount = this.keyPartitionFunction.getCount();
        int[] partitionLength = this.keyPartitionFunction.getLength();
        int firstValue = this.keyPartitionFunction.getFirstValue();

        for (int i = 0; i < partitionCount.length; i++) {
            for (int j = 0; j < partitionCount[i]; j++) {
                int key = firstValue + partitionLength[i];
                firstValue = key;
                result.add(String.valueOf(key));
            }
        }

        return result;
    }

    private List<String> buildTableSlotMapValues() {
        List<String> result = new ArrayList<String>();
        int[] partitionCount = this.valuePartitionFunction.getCount();
        int[] partitionLength = this.valuePartitionFunction.getLength();
        int firstValue = this.valuePartitionFunction.getFirstValue();

        for (int i = 0; i < partitionCount.length; i++) {
            for (int j = 0; j < partitionCount[i]; j++) {
                int startValue = firstValue;
                int endValue = startValue + partitionLength[i] - 1;
                firstValue += partitionLength[i];
                result.add(String.valueOf(startValue) + "-" + String.valueOf(endValue));
            }
        }

        return result;
    }

    public String buildLogKey(String key) {
        if (logicTable != null) {
            StringBuilder sb = new StringBuilder(logicTable);
            sb.append("_slot_");
            sb.append(key);
            return sb.toString();
        } else {
            throw new RuntimeException("TableRule no logicTable at all,can not happen!!");
        }
    }

    // ===================== setter / getter ====================

    public void setTableSlotMap(Map<String, String> tableSlotMap) {
        this.tableSlotMap = tableSlotMap;
    }

    public void setLogicTable(String logicTable) {
        this.logicTable = logicTable;
    }

    public void setKeyPartitionFunction(PartitionFunction keyPartitionFunction) {
        this.keyPartitionFunction = keyPartitionFunction;
    }

    public void setValuePartitionFunction(PartitionFunction valuePartitionFunction) {
        this.valuePartitionFunction = valuePartitionFunction;
    }

}
