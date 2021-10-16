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

package com.alibaba.polardbx.optimizer.index;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * @author dylan
 */
public class PartitionRuleSet {

    // {schema -> table -> column}
    Map<String, Map<String, HumanReadableRule>> m;

    public PartitionRuleSet() {
        this.m = new HashMap<>();
    }

    public void addPartitionRule(String schemaName, String tableName, HumanReadableRule humanReadableRule) {
        schemaName = schemaName.toLowerCase();
        tableName = tableName.toLowerCase();
        Map<String, HumanReadableRule> t = m.get(schemaName);
        if (t == null) {
            t = new HashMap<>();
            m.put(schemaName, t);
        }
        t.put(tableName, humanReadableRule);
    }

    public HumanReadableRule getRule(String schemaName, String tableName) {
        return m.get(schemaName.toLowerCase()).get(tableName.toLowerCase());
    }

    public Collection<HumanReadableRule> getPartitionPolicys(String schemaName) {
        Map<String, HumanReadableRule> map = m.get(schemaName.toLowerCase());
        Map<String, HumanReadableRule> digestMap = new HashMap<>();
        for (HumanReadableRule rule : map.values()) {
            digestMap.put(rule.getPartitionPolicyDigest(), rule);
        }
        return digestMap.values();
    }
}
