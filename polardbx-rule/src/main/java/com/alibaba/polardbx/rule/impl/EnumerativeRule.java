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

import com.alibaba.polardbx.common.model.sqljep.Comparative;
import com.alibaba.polardbx.rule.MappingRule;
import com.alibaba.polardbx.rule.utils.AdvancedParameterParser;
import com.alibaba.polardbx.rule.utils.sample.Samples;
import com.alibaba.polardbx.rule.utils.sample.SamplesCtx;
import com.alibaba.polardbx.rule.model.AdvancedParameter;
import com.alibaba.polardbx.rule.utils.RuleUtils;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * 通过描点枚举的方式实现比较树匹配
 *
 * @author linxuan
 */
public abstract class EnumerativeRule<T> extends ExpressionRule<T> {

    public static final String REAL_TABLE_NAME_KEY = "REAL_TABLE_NAME";

    public EnumerativeRule(String expression) {
        super(expression);
    }

    /**
     * 解决了一个问题：若规则中同一个列出现多次，必需在列名第一次出现的地方写全参数，后续的出现可以不写
     */
    protected String parseParam(String paramInDoller, Map<String, RuleColumn> parameters) {
        RuleColumn ruleColumn = null;
        if (paramInDoller.indexOf(",") == -1) {
            // 如果没有其他参数，直接从parameters中取
            // 这里必需RuleColumn的对于Key的处理方式一致，否则将取不到，这是一个风险点
            ruleColumn = parameters.get(paramInDoller.trim());
        }

        if (ruleColumn == null) {
            ruleColumn = AdvancedParameterParser.getAdvancedParamByParamTokenNew(paramInDoller, true, this);
            parameters.put(ruleColumn.key, ruleColumn);
        }
        return replace(ruleColumn);
    }

    /**
     * 允许子类根据{@linkplain RuleColumn}生成执行表达式
     */
    abstract protected String replace(RuleColumn ruleColumn);

    public Map<T, Samples> calculate(Map<String, Comparative> sqlArgs, Object ctx, Object outerCtx) {
        return calculate(sqlArgs, ctx, outerCtx, null);
    }

    public Set<T> calculateNoTrace(Map<String, Comparative> sqlArgs, Object ctx, Object outerCtx) {
        return calculateNoTrace(sqlArgs, ctx, outerCtx, null);
    }

    public T calculateVnodeNoTrace(String key, Object ctx, Object outerCtx) {
        return calculateVnodeNoTrace(key, ctx, outerCtx, null);
    }

    public Map<T, Samples> calculate(Map<String, Comparative> sqlArgs, Object ctx, Object outerCtx,
                                     Map<String, Object> calcParams) {
        Map<String, Set<Object>> enumerates = getEnumerates(sqlArgs, ctx);
        Map<T, Samples> res = new HashMap<T, Samples>(1);
        for (Map<String, Object> sample : new Samples(enumerates)) { // 遍历笛卡尔抽样
            T value = this.eval(sample, outerCtx, calcParams);
            if (value == null) {
                throw new IllegalArgumentException("rule eval result is null! rule:" + this.expression);
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

    public Set<T> calculateNoTrace(Map<String, Comparative> sqlArgs, Object ctx, Object outerCtx,
                                   Map<String, Object> calcParams) {
        Map<String, Set<Object>> enumerates = getEnumerates(sqlArgs, ctx);
        Set<T> res = new HashSet<T>(1);
        for (Map<String, Object> sample : new Samples(enumerates)) { // 遍历笛卡尔抽样
            T value = this.eval(sample, outerCtx, calcParams);
            if (value == null) {
                throw new IllegalArgumentException("rule eval resulte is null! rule:" + this.expression);
            }
            res.add(value);
        }
        return res;
    }

    public T calculateVnodeNoTrace(String key, Object ctx, Object outerCtx, Map<String, Object> calcParams) {
        Map<String, Object> sample = new HashMap<String, Object>(1);
        sample.put(REAL_TABLE_NAME_KEY, key);
        T value = this.eval(sample, outerCtx, calcParams);
        if (value == null) {
            throw new IllegalArgumentException("rule eval resulte is null! rule:" + this.expression);
        }
        return value;
    }

    /**
     * 计算一下枚举值
     */
    protected Map<String, Set<Object>> getEnumerates(Map<String, Comparative> sqlArgs, Object ctx) {
        Set<AdvancedParameter> thisParam = RuleUtils.cast(this.parameterSet);
        Map<String, Set<Object>> enumerates;
        SamplesCtx samplesCtx = (SamplesCtx) ctx;
        Map<String, Set<MappingRule>> extKeyToMappingRules = null;
        if (this instanceof WrappedGroovyRule) {
            extKeyToMappingRules = ((WrappedGroovyRule) this).getExtKeyToMappingRules();
        }

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
                enumerates = RuleUtils.getSamplingField(sqlArgs, withoutCommon, extKeyToMappingRules, null);
                for (String name : commonSamples.getSubColumSet()) {
                    enumerates.put(name, commonSamples.getColumnEnumerates(name)); // 公共列只使用上一层规则的描点，值输入的分流窄化问题
                }
            } else if (samplesCtx.dealType == SamplesCtx.merge) {
                enumerates = RuleUtils.getSamplingField(sqlArgs, thisParam, extKeyToMappingRules, null);
                for (String diffType : commonSamples.getSubColumSet()) {
                    enumerates.get(diffType).addAll(commonSamples.getColumnEnumerates(diffType));
                }
            } else {
                throw new IllegalStateException("Should not happen! SamplesCtx.dealType has a new Enum?");
            }
        } else {
            enumerates = RuleUtils.getSamplingField(sqlArgs, thisParam, extKeyToMappingRules, null);
        }
        return enumerates;
    }

}
