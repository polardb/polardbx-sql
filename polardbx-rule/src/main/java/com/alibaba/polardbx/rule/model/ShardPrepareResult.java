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

import com.alibaba.polardbx.rule.MappingRule;

/**
 * ${DESCRIPTION}
 *
 * @author hongxi.chx
 * @create 2018-09-25 17:32
 */
public class ShardPrepareResult {
    private ShardMatchedRuleType matchedDBType;
    private ShardMatchedRuleType matchedTBType;
    private MappingRule          matchedMappingRule;

    public ShardPrepareResult(ShardMatchedRuleType matchedDBType, ShardMatchedRuleType matchedTBType) {
        this(matchedDBType,matchedTBType,null);
    }

    public ShardPrepareResult(ShardMatchedRuleType matchedDBType, ShardMatchedRuleType matchedTBType, MappingRule matchedMappingRule) {
        this.matchedDBType = matchedDBType;
        this.matchedTBType = matchedTBType;
        this.matchedMappingRule = matchedMappingRule;
    }

    public ShardMatchedRuleType getMatchedDBType() {
        return matchedDBType;
    }

    public ShardMatchedRuleType getMatchedTBType() {
        return matchedTBType;
    }

    public MappingRule getMatchedMappingRule() {
        return matchedMappingRule;
    }
}
