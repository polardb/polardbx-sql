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

import com.alibaba.polardbx.common.model.sqljep.Comparative;
import com.alibaba.polardbx.rule.TableRule;
import com.alibaba.polardbx.rule.enums.RuleType;

/**
 * ${DESCRIPTION}
 *
 * @author hongxi.chx
 * @create 2018-09-20 17:32
 */
public class ShardExtContext {

    private Map<RuleType, String> ruleTypeColumnMap;
    private Comparative           comparative;
    //
    private RuleType              currentRuleType;

    private TableRule rule;

    public ShardExtContext(Map<RuleType, String> ruleTypeColumnMap, RuleType currentRuleType, Comparative comparative,
                           TableRule rule){
        this.ruleTypeColumnMap = ruleTypeColumnMap;
        this.currentRuleType = currentRuleType;
        this.comparative = comparative;
        this.rule = rule;
    }

    public Map<RuleType, String> getRuleTypeColumnMap() {
        return ruleTypeColumnMap;
    }

    public String getShardDbColumn() {
        if (ruleTypeColumnMap == null) return null;
        return ruleTypeColumnMap.get(RuleType.DB_RULE_TYPE);
    }

    public String getShardTableColumn() {
        if (ruleTypeColumnMap == null) return null;
        return ruleTypeColumnMap.get(RuleType.TB_RULE_TYPE);
    }

    public String getCurrentColumn() {
        if (ruleTypeColumnMap == null) return null;
        return ruleTypeColumnMap.get(currentRuleType);
    }

    public Comparative getComparative() {
        return comparative;
    }

    public void setComparative(Comparative comparative) {
        this.comparative = comparative;
    }

    public RuleType getCurrentRuleType() {
        return currentRuleType;
    }

    public TableRule getRule() {
        return rule;
    }
}
