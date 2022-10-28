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

package com.alibaba.polardbx.rule.enumerator;

import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.alibaba.polardbx.rule.MappingRule;
import com.alibaba.polardbx.rule.Rule.RuleColumn;
import com.alibaba.polardbx.rule.enums.RuleType;

/**
 * ${DESCRIPTION}
 *
 * @author hongxi.chx
 */
public class EnumGeneratorImpl implements Enumerator {

    private final Map<String, Set<MappingRule>> extMappingRules;
    private Enumerator enumeratorCommon;
    private RuleType ruleType;

    public EnumGeneratorImpl(RuleType ruleType,
                             Map<String, Set<MappingRule>> extMappingRules, RuleColumn ruleColumnParams) {
        this.ruleType = ruleType;
        this.extMappingRules = extMappingRules;
        this.enumeratorCommon = new RuleEnumeratorImpl(ruleColumnParams);

    }

    @Override
    public Set<Object> getEnumeratedValue(Comparable condition, Integer cumulativeTimes,
                                          Comparable<?> atomicIncrementValue, boolean needMergeValueInCloseInterval) {
        Set<Object> enumeratedSet = Sets.newHashSet();
        List<Enumerator> enumerators = getEnumeratorList();
        enumInner(condition,
            cumulativeTimes,
            atomicIncrementValue,
            extMappingRules,
            needMergeValueInCloseInterval,
            enumeratedSet,
            enumerators);
        return enumeratedSet;
    }

    private void enumInner(Comparable condition, Integer cumulativeTimes, Comparable<?> atomicIncrementValue,
                           Map<String, Set<MappingRule>> extMappingRules, boolean needMergeValueInCloseInterval,
                           Set<Object> enumeratedSet, List<Enumerator> enumerators) {
        for (int i = 0; i < enumerators.size(); i++) {
            Enumerator enumerator = enumerators.get(i);
            final Set<Object> enumeratedValue = enumerator.getEnumeratedValue(condition,
                cumulativeTimes,
                atomicIncrementValue,
                needMergeValueInCloseInterval);
            if (enumeratedValue != null) {
                enumeratedSet.addAll(enumeratedValue);
            }
        }
    }

    public List<Enumerator> getEnumeratorList() {
        List<Enumerator> enumerators = Lists.newArrayList();
        enumerators.add(enumeratorCommon);
        return enumerators;
    }

}
