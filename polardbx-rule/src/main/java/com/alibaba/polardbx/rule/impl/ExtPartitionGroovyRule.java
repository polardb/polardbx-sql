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

package com.alibaba.polardbx.rule.impl;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import com.alibaba.polardbx.rule.ExtRule;
import com.alibaba.polardbx.rule.MappingRule;
import com.alibaba.polardbx.rule.VirtualTableSupport;
import com.alibaba.polardbx.rule.enums.RuleType;
import com.alibaba.polardbx.rule.model.AdvancedParameter;
import com.alibaba.polardbx.rule.utils.RuleUtils;
import com.alibaba.polardbx.rule.utils.sample.Samples;
import com.alibaba.polardbx.rule.utils.sample.SamplesCtx;
import org.apache.commons.lang.StringUtils;

import com.alibaba.polardbx.common.model.sqljep.Comparative;

/**
 * ${DESCRIPTION}
 *
 * @author hongxi.chx
 */
public class ExtPartitionGroovyRule extends WrappedGroovyRule implements ExtRule<String> {

    public ExtPartitionGroovyRule(VirtualTableSupport tableRule, String expression, String wrapPattern,
                                  String extraPackagesStr,
                                  Map<String, Set<MappingRule>> extKeyToMappingRules, boolean lazyInit) {
        super(tableRule, expression, wrapPattern, extraPackagesStr, lazyInit);
        this.extKeyToMappingRules = extKeyToMappingRules;
    }

    @Override
    public String eval(Map<String, Object> columnValues, Object outerContext, Map<String, Object> calcParams) {
        return super.eval(columnValues, outerContext, calcParams);
    }

    @Override
    public String eval(Map<String, Object> columnValues, Object outerContext, Map<String, Object> calcParams,
                       RuleType ruleType) {
        if (ruleType == RuleType.DB_RULE_TYPE) {
            for (Iterator<String> iterator = columnValues.keySet().iterator(); iterator.hasNext(); ) {
                String key = iterator.next();
                final Object o = columnValues.get(key);
                if (o instanceof MappingRule) {
                    return ((MappingRule) o).getDb();
                } else if (o instanceof Set) {
                    final Object next = ((Set) o).iterator().next();
                    if (next instanceof MappingRule) {
                        return ((MappingRule) next).getDb();
                    }
                }
            }
        } else if (ruleType == RuleType.TB_RULE_TYPE) {
            for (Iterator<String> iterator = columnValues.keySet().iterator(); iterator.hasNext(); ) {
                String key = iterator.next();
                final Object o = columnValues.get(key);
                String tb = null;
                if (o instanceof MappingRule) {
                    tb = ((MappingRule) o).getTb();
                } else if (o instanceof Set) {
                    final Object next = ((Set) o).iterator().next();
                    if (next instanceof MappingRule) {
                        tb = ((MappingRule) next).getTb();
                    }
                }
                if (StringUtils.isEmpty(tb)) {
                    return super.eval(columnValues, outerContext, calcParams);
                } else {
                    return tb;
                }
            }
        }
        return super.eval(columnValues, outerContext, calcParams);
    }

    @Override
    public Map<String, Samples> calculate(Map<String, Comparative> sqlArgs, Object ctx, Object outerCtx) {
        return super.calculate(sqlArgs, ctx, outerCtx);
    }

    @Override
    public Map<String, Samples> calculate(Map<String, Comparative> sqlArgs, Object ctx, Object outerCtx,
                                          Map<String, Object> calcParams) {
        Map<String, Set<Object>> enumerates = getEnumerates(sqlArgs, ctx);
        Map<String, Samples> res = new HashMap<>(1);
        for (Map<String, Object> sample : new Samples(enumerates)) { // 遍历笛卡尔抽样
            String value = this.eval(sample, outerCtx, calcParams);
            if (value == null) {
                throw new IllegalArgumentException("rule eval resulte is null! rule:" + this.expression);
            }
            Samples evalSamples = res.get(value);
            if (evalSamples == null) {
                evalSamples = new Samples(sample.keySet());
                res.put(value, evalSamples);
            }
            evalSamples.addSample(sample);
        }
        return res;
    }

    @Override
    public Map<String, ?> calculate(Map<String, Comparative> sqlArgs, Object ctx, Object outerCtx,
                                    Map<String, Object> calcParams,
                                    RuleType ruleType) {
        Map<String, Set<Object>> enumerates = getEnumerates(sqlArgs, ctx, ruleType);
        Map<String, Samples> res = new HashMap<>(1);
        for (Map<String, Object> sample : new Samples(enumerates)) { // 遍历笛卡尔抽样
            String value = this.eval(sample, outerCtx, calcParams, ruleType);
            if (value == null) {
                throw new IllegalArgumentException("rule eval resulte is null! rule:" + this.expression);
            }
            Samples evalSamples = res.get(value);
            if (evalSamples == null) {
                evalSamples = new Samples(sample.keySet());
                res.put(value, evalSamples);
            }
            evalSamples.addSample(sample);
        }
        return res;
    }

    @Override
    public Set<String> calculateNoTrace(Map<String, Comparative> sqlArgs, Object ctx, Object outerContext,
                                        Map<String, Object> calcParams,
                                        RuleType ruleType) {
        Map<String, Set<Object>> enumerates = getEnumerates(sqlArgs, ctx, ruleType);
        Set<String> res = new HashSet<String>(1);
        for (Map<String, Object> sample : new Samples(enumerates)) { // 遍历笛卡尔抽样
            String value = this.eval(sample, outerContext, calcParams, ruleType);
            if (value == null) {
                if (ruleType == RuleType.TB_RULE_TYPE) {
                    return null;
                }
                throw new IllegalArgumentException("rule eval result is null! rule:" + this.expression);
            }
            res.add(value);
        }
        return res;
    }

    @Override
    public Set<String> calculateNoTrace(Map<String, Comparative> sqlArgs, Object ctx, Object outerContext,
                                        RuleType ruleType) {
        return this.calculateNoTrace(sqlArgs, ctx, outerContext, null, ruleType);
    }

    /**
     * 计算一下枚举值
     */
    protected Map<String, Set<Object>> getEnumerates(Map<String, Comparative> sqlArgs, Object ctx, RuleType ruleType) {
        Set<AdvancedParameter> thisParam = RuleUtils.cast(this.parameterSet);
        Map<String, Set<Object>> enumerates;
        SamplesCtx samplesCtx = (SamplesCtx) ctx;
        if (samplesCtx != null) { // thread local中存在上下文
            Samples commonSamples = samplesCtx.samples;
            if (samplesCtx.dealType == SamplesCtx.replace) {
                Set<AdvancedParameter> withoutCommon = new HashSet<AdvancedParameter>(thisParam.size());
                for (AdvancedParameter p : thisParam) {
                    if (!commonSamples.getSubColumSet().contains(p.key)) { // 找出非公共列
                        withoutCommon.add(p);
                    }
                }
                // 非公共列使用自己的枚举值，公共列使用上下文中，实现replace
                enumerates = RuleUtils.getSamplingField(sqlArgs,
                    thisParam,
                    ((WrappedGroovyRule) this).getExtKeyToMappingRules(),
                    ruleType);
            } else if (samplesCtx.dealType == SamplesCtx.merge) {
                enumerates = RuleUtils.getSamplingField(sqlArgs,
                    thisParam,
                    ((WrappedGroovyRule) this).getExtKeyToMappingRules(),
                    ruleType);
                for (String diffType : commonSamples.getSubColumSet()) {
                    enumerates.get(diffType).addAll(commonSamples.getColumnEnumerates(diffType));
                }
            } else {
                throw new IllegalStateException("Should not happen! SamplesCtx.dealType has a new Enum?");
            }
        } else {
            enumerates = RuleUtils.getSamplingField(sqlArgs,
                thisParam,
                ((WrappedGroovyRule) this).getExtKeyToMappingRules(),
                ruleType);
        }
        return enumerates;
    }

}
