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

package com.alibaba.polardbx.rule.model;

import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;

import org.apache.commons.lang.builder.ToStringBuilder;

import com.taobao.tddl.common.utils.TddlToStringStyle;

/**
 * <pre>
 * 存放列名->sourceKey的映射，比如支持id in (xx)时，根据param计算后得出了目标库地址，可将该记录的sourceKey的发送到目标库上进行执行.
 *
 * * 保存列名(通常是拆分键)及其经过规则路由计算后的可路由到目标分片的条件的值
 *
 * <pre>
 *  例如， id in (1,2,3,4)  可路由到 tb1(1,3)  和 tb2(2,4), 那么就会有这样的结果：
 *           tb1 ---> Field（  sourceKeys = id:1,3 ）
 *           tb2 ---> Field（  sourceKeys = id:2,4 ）
 *
 * 实质上就是记录了一个记录被路由到的分片，是由哪些列的哪些枚举值计算出来的
 *
 * </pre>
 *
 * @author shenxun
 */
public class Field {

    public static final Field EMPTY_FIELD = new Field(0);

    /**
     * 保存列名(通常是拆分键)及其经过规则路由计算后的可路由到目标分片的条件的值
     *
     * <pre>
     *  例如， id in (1,2,3,4)  可路由到 tb1(1,3)  和 tb2(2,4), 那么就会有这样的结果：
     *           tb1 ---> Field（  sourceKeys = id:1,3 ）
     *           tb2 ---> Field（  sourceKeys = id:2,4 ）
     *
     * 实质上就是记录了一个记录被路由到的分片，是由哪些列的哪些枚举值计算出来的
     *
     * </pre>
     */
    private Map<String/* 列名 */, Set<Object>/* 得到该结果的描点值名 */> sourceKeys;

    public Field(int capacity) {
        sourceKeys = new TreeMap<String, Set<Object>>(String.CASE_INSENSITIVE_ORDER);
    }

    public boolean equals(Object obj, Map<String, String> alias) {
        // 用于比较两个field是否相等。field包含多个列，那么多列内的每一个值都应该能找到对应的值才算相等。
        if (!(obj instanceof Field)) {
            return false;
        }
        Map<String, Set<Object>> target = ((Field) obj).sourceKeys;
        for (Entry<String, Set<Object>> entry : sourceKeys.entrySet()) {
            String srcKey = entry.getKey();
            if (alias.containsKey(srcKey)) {
                srcKey = alias.get(srcKey);
            }
            Set<Object> targetValueSet = target.get(srcKey);
            Set<Object> sourceValueSet = entry.getValue();
            for (Object srcValue : sourceValueSet) {
                boolean eq = false;
                for (Object tarValue : targetValueSet) {
                    if (tarValue.equals(srcValue)) {
                        eq = true;
                    }
                }
                if (!eq) {
                    return false;
                }
            }
        }
        return true;
    }

    public Map<String, Set<Object>> getSourceKeys() {
        return sourceKeys;
    }

    public void setSourceKeys(Map<String, Set<Object>> sourceKeys) {
        this.sourceKeys.putAll(sourceKeys);
    }

    @Override
    public String toString() {
        return ToStringBuilder.reflectionToString(this, TddlToStringStyle.DEFAULT_STYLE);
    }
}
