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

package com.alibaba.polardbx.rule;

import java.util.List;

/**
 * @author shicai.xsc 2018/8/10 下午8:59
 * @since 5.0.0.0
 */
public class GroupMappingRule {
    private String keyValue;
    private String group;
    private List<MappingRule> mappingRules;

    public GroupMappingRule() {
    }

    public GroupMappingRule(String keyValue, String group, List<MappingRule> mappingRules) {
        this.keyValue = keyValue;
        this.group = group;
        this.mappingRules = mappingRules;
    }

    public String getKeyValue() {
        return keyValue;
    }

    public void setKeyValue(String keyValue) {
        this.keyValue = keyValue;
    }

    public String getGroup() {
        return group;
    }

    public void setGroup(String group) {
        this.group = group;
    }

    public List<MappingRule> getMappingRules() {
        return mappingRules;
    }

    public void setMappingRules(List<MappingRule> mappingRules) {
        this.mappingRules = mappingRules;
    }
}
