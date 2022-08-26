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

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import com.alibaba.polardbx.common.model.lifecycle.AbstractLifecycle;
import com.alibaba.polardbx.common.model.sqljep.Comparative;
import com.alibaba.polardbx.rule.enumerator.handler.CloseIntervalFieldsEnumeratorHandler;
import com.alibaba.polardbx.rule.enumerator.handler.IntegerPartDiscontinousRangeEnumerator;

/**
 * @version 1.0
 * @since 1.6
 * @date 2011-8-20 02:58:34
 */
public class WrappedLogic extends AbstractLifecycle {

    private static final String                  SLOT_PIECE_SPLIT   = ",";
    private static final String                  RANGE_SUFFIX_SPLIT = "-";
    private CloseIntervalFieldsEnumeratorHandler enumerator         = new IntegerPartDiscontinousRangeEnumerator();
    protected String                             valuePrefix;                                                      // 无getter/setter
    protected String                             valueSuffix;                                                      // 无getter/setter
    protected int                                valueAlignLen      = 0;                                           // 无getter/setter
    protected String                             tableSlotKeyFormat = null;

    public void setTableSlotKeyFormat(String tableSlotKeyFormat) {
        if (tableSlotKeyFormat == null) {
            return;
        }

        this.tableSlotKeyFormat = tableSlotKeyFormat;

        int index0 = tableSlotKeyFormat.indexOf('{');
        if (index0 == -1) {
            this.valuePrefix = tableSlotKeyFormat;
            return;
        }
        int index1 = tableSlotKeyFormat.indexOf('}', index0);
        if (index1 == -1) {
            this.valuePrefix = tableSlotKeyFormat;
            return;
        }
        this.valuePrefix = tableSlotKeyFormat.substring(0, index0);
        this.valueSuffix = tableSlotKeyFormat.substring(index1 + 1);
        this.valueAlignLen = index1 - index0 - 1;// {0000}中0的个数
    }

    protected String wrapValue(String value) {
        StringBuilder sb = new StringBuilder();
        if (valuePrefix != null) {
            sb.append(valuePrefix);
        }

        if (valueAlignLen > 1) {
            int k = valueAlignLen - value.length();
            for (int i = 0; i < k; i++) {
                sb.append("0");
            }
        }
        sb.append(value);
        if (valueSuffix != null) {
            sb.append(valueSuffix);
        }
        return sb.toString();
    }

    /**
     * <pre>
     * 参数oriMap的value格式为 <b>0,1,2-6</b> 0,1表示2个槽,'-'表示一个范围
     * 
     * 此函数中将范围枚举成一个个槽,并将槽变为key,原本的key变为value
     * 
     * example 1:key为 1 value为 1,2,3-6
     * 
     * 返回结果为 1->1,2->1,3->1,4->1,5->1,6->1
     * 
     * example 2:key为db_group_1 value为1,2 db_group_2 value为3,4-6
     * 返回结果为 1->db_group_1,2->db_group_1
     * 3->db_group_2,4->db_group_2,5->db_group_3,2->db_group_2
     * 
     * <b>
     * 暂时不支持任何形式的value格式化.即_0000,0001之类的字符串,只接受
     * 数学形式上的integer,long
     * 
     * 后续改进
     * </b>
     * </pre>
     * 
     * @param tableMap
     * @return
     */
    protected Map<String, String> extraReverseMap(Map<String, String> oriMap) {
        ConcurrentHashMap<String, String> slotMap = new ConcurrentHashMap<String, String>();
        for (Map.Entry<String, String> entry : oriMap.entrySet()) {
            String[] pieces = entry.getValue().trim().split(SLOT_PIECE_SPLIT);
            for (String piece : pieces) {
                String[] range = piece.trim().split(RANGE_SUFFIX_SPLIT);
                if (range.length == 2) {
                    Comparative start = new Comparative(Comparative.GreaterThanOrEqual, Integer.valueOf(range[0]));
                    Comparative end = new Comparative(Comparative.LessThanOrEqual, Integer.valueOf(range[1]));
                    int cumulativeTimes = Integer.valueOf(range[1]) - Integer.valueOf(range[0]);
                    Set<Object> result = new HashSet<Object>();
                    enumerator.mergeFeildOfDefinitionInCloseInterval(start, end, result, cumulativeTimes, 1);
                    for (Object v : result) {
                        slotMap.put(String.valueOf(v), entry.getKey());
                    }
                } else if (range.length == 1) {
                    slotMap.put(piece, entry.getKey());
                } else {
                    throw new IllegalArgumentException("slot config error,slot piece:" + piece);
                }
            }
        }
        return slotMap;
    }
}
