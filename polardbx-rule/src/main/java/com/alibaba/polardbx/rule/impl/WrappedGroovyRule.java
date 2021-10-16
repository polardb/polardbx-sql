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

import com.google.common.collect.Maps;
import com.alibaba.polardbx.rule.MappingRule;
import com.alibaba.polardbx.rule.VirtualTableSupport;

import java.util.Map;
import java.util.Set;

public class WrappedGroovyRule extends GroovyRule<String> {

    protected String valuePrefix;              // 无getter/setter
    protected String valueSuffix;              // 无getter/setter
    protected int valueAlignLen = 0; // 无getter/setter

    protected Map<String, Set<MappingRule>> extKeyToMappingRules = Maps.newHashMap();

    public WrappedGroovyRule(VirtualTableSupport tableRule, String expression, String wrapPattern, boolean lazyInit) {
        super(tableRule, expression, lazyInit);
        setValueWrappingPattern(wrapPattern);
    }

    public WrappedGroovyRule(VirtualTableSupport tableRule, String expression, String wrapPattern,
                             String extraPackagesStr, boolean lazyInit) {
        super(tableRule, expression, extraPackagesStr, lazyInit);
        setValueWrappingPattern(wrapPattern);
    }

    public WrappedGroovyRule(VirtualTableSupport tableRule, String expression, String wrapPattern,
                             String extraPackagesStr, Map<String, Set<MappingRule>> extKeyToMappingRules,
                             boolean lazyInit) {
        super(tableRule, expression, extraPackagesStr, lazyInit);
        setValueWrappingPattern(wrapPattern);
        this.extKeyToMappingRules = extKeyToMappingRules;
        if (extKeyToMappingRules == null) {
            this.extKeyToMappingRules = Maps.newHashMap();
        }
    }

    private void setValueWrappingPattern(String wrapPattern) {
        int index0 = wrapPattern.indexOf('{');
        if (index0 == -1) {
            this.valuePrefix = wrapPattern;
            return;
        }
        int index1 = wrapPattern.indexOf('}', index0);
        if (index1 == -1) {
            this.valuePrefix = wrapPattern;
            return;
        }
        this.valuePrefix = wrapPattern.substring(0, index0);
        this.valueSuffix = wrapPattern.substring(index1 + 1);
        this.valueAlignLen = index1 - index0 - 1; // {0000}中0的个数
    }

    /**
     * 用来判断是否是占位符还是常量
     */
    public int getValueAlignLen() {
        return valueAlignLen;
    }

    protected static String wrapValue(String prefix, String suffix, int len, String value) {
        StringBuilder sb = new StringBuilder();
        if (prefix != null) {
            sb.append(prefix);
        }
        if (len > 1) {
            int k = len - value.length();
            for (int i = 0; i < k; i++) {
                sb.append("0");
            }
        }
        sb.append(value);
        if (suffix != null) {
            sb.append(suffix);
        }
        return sb.toString();
    }

    @Override
    public String eval(Map<String, Object> columnValues, Object outerContext) {
        return eval(columnValues, outerContext, null);
    }

    @Override
    public String eval(Map<String, Object> columnValues, Object outerContext, Map<String, Object> calcParams) {
        String value = String.valueOf(super.eval(columnValues, outerContext, null));
        return wrapValue(valuePrefix, valueSuffix, valueAlignLen, value);
    }

    public Map<String, Set<MappingRule>> getExtKeyToMappingRules() {
        return extKeyToMappingRules;
    }
}
