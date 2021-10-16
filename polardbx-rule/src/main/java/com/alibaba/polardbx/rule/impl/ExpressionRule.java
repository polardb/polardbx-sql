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

import com.alibaba.polardbx.rule.Rule;

import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 通过表达式来表达规则的基类
 *
 * @author linxuan
 */
public abstract class ExpressionRule<T> implements Rule<T> {

    private static final Pattern DOLLER_PATTERN;

    static {
        // 提供一个可配置的机会，但是在绝大多数默认的情况下，不想影响接口层次和代码结构
        String regex = System.getProperty("com.alibaba.polardbx.rule.columnParamRegex", "#.*?#");
        DOLLER_PATTERN = Pattern.compile(regex);
    }

    /**
     * 当前规则需要用到的参数
     */
    protected Map<String, RuleColumn> parameters;
    protected Set<RuleColumn> parameterSet;    // 规则列

    /**
     * 当前规则需要用到的表达式
     */
    protected String expression;
    protected final String originExpression; // 原始的表达式

    public ExpressionRule(String expression) {
        this.originExpression = expression;
        this.expression = expression;
    }

    protected void initRuleExpression() {
        this.parameters = Collections.unmodifiableMap(parse());
        this.parameterSet = new HashSet<RuleColumn>(parameters.size());
        this.parameterSet.addAll(parameters.values());
    }

    private Map<String, RuleColumn> parse() {
        Map<String, RuleColumn> parameters = new TreeMap<String, RuleColumn>(String.CASE_INSENSITIVE_ORDER);
        Matcher matcher = DOLLER_PATTERN.matcher(expression);
        int start = 0;
        StringBuffer sb = new StringBuffer();
        while (matcher.find(start)) {
            String realParam = matcher.group();
            realParam = realParam.substring(1, realParam.length() - 1);
            sb.append(expression.substring(start, matcher.start()));
            sb.append(parseParam(realParam, parameters));
            start = matcher.end();
        }
        sb.append(expression.substring(start));
        expression = sb.toString();
        return parameters;
    }

    /**
     * 子类将paramInDoller解析为RuleColumn，加入到parameters中，并返回替换后的字串
     *
     * @return 替换后的字串
     */
    abstract protected String parseParam(String paramInDoller, Map<String, RuleColumn> parameters);

    public Map<String, RuleColumn> getRuleColumns() {
        return parameters;
    }

    public Set<RuleColumn> getRuleColumnSet() {
        return parameterSet;
    }

    public String getExpression() {
        return originExpression;
    }

    /**
     * originExpression相同则相同，eclipse生成
     */
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        ExpressionRule other = (ExpressionRule) obj;
        if (originExpression == null) {
            if (other.originExpression != null) {
                return false;
            }
        } else if (!originExpression.equals(other.originExpression)) {
            return false;
        }
        return true;
    }

    /**
     * eclipse生成
     */
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((originExpression == null) ? 0 : originExpression.hashCode());
        return result;
    }

    public Map<String, RuleColumn> getParameters() {
        return parameters;
    }

}
