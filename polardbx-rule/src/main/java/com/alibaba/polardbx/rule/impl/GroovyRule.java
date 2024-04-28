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

import com.alibaba.polardbx.common.utils.convertor.Convertor;
import com.alibaba.polardbx.common.utils.convertor.ConvertorHelper;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.rule.MappingRule;
import com.alibaba.polardbx.rule.VirtualTableSupport;
import com.alibaba.polardbx.rule.impl.groovy.ShardingFunction;
import com.alibaba.polardbx.rule.model.AdvancedParameter;
import com.alibaba.polardbx.rule.model.AdvancedParameter.AtomIncreaseType;
import com.alibaba.polardbx.rule.utils.RuleUtils;
import com.alibaba.polardbx.rule.utils.SimpleRuleProcessor;
import groovy.lang.GroovyClassLoader;
import org.codehaus.groovy.control.CompilationFailedException;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 基于groovy实现
 *
 * @author jianghang 2013-11-4 下午3:50:29
 * @since 5.0.0
 */
public class GroovyRule<T> extends EnumerativeRule<T> {

    private static final Logger logger = LoggerFactory.getLogger(GroovyRule.class);
    // 应用置入的上下文，可以用在evel的groovy脚本里
    private static final String IMPORT_EXTRA_PARAMETER_CONTEXT =
        "import com.alibaba.polardbx.rule.impl.groovy.ExtraParameterContext;";
    private static final String IMPORT_STATIC_METHOD =
        "import static com.alibaba.polardbx.rule.impl.groovy.GroovyStaticMethod.*;";
    private static final Pattern RETURN_WHOLE_WORD_PATTERN = Pattern.compile("\\breturn\\b",
        Pattern.CASE_INSENSITIVE);
    // 全字匹配

    protected String extraPackagesStr;
    protected ShardingFunction shardingFunction;
    protected GroovyClassLoader loader;
    protected Convertor stringToNumberConvert = null;

    protected VirtualTableSupport tableRule = null;

    public GroovyRule(String expression, boolean lazyInit) {
        this(null, expression, null, lazyInit);
    }

    public GroovyRule(VirtualTableSupport tableRule, String expression, boolean lazyInit) {
        this(tableRule, expression, null, lazyInit);
    }

    public GroovyRule(VirtualTableSupport tableRule, String expression, String extraPackagesStr, boolean lazyInit) {
        super(expression);

        this.tableRule = tableRule;
        initRuleExpression();
        if (extraPackagesStr == null) {
            this.extraPackagesStr = "";
        } else {
            this.extraPackagesStr = extraPackagesStr;
        }

        if (!lazyInit) {
            initGroovy();
        }

    }

    private void initGroovy() {
        if (expression == null) {
            throw new IllegalArgumentException("未指定 expression");
        }

        SimpleRuleProcessor.SimpleRule parse = SimpleRuleProcessor.parseDB(originExpression); // 检查下原始rule规则
        if (parse == null) {
            parse = SimpleRuleProcessor.parseTB(originExpression);
        }

        if (parse != null) {
            stringToNumberConvert = ConvertorHelper.getInstance().getConvertor(String.class, Long.class);
            final SimpleRuleProcessor.SimpleRule rule = parse;
            // 存在简单规则，生成硬编码java调用
            shardingFunction = new ShardingFunction() {

                @Override
                public Object eval(Map map, Object outerCtx) {
                    Object obj = map.get(rule.column);
                    if (obj instanceof MappingRule) {
                        return ((MappingRule) obj).getDb();
                    } else if (obj instanceof Set) {
                        for (Iterator iterator = ((Set) obj).iterator(); iterator.hasNext(); ) {
                            Object value = iterator.next();
                            if (value instanceof MappingRule) {
                                return ((MappingRule) value).getDb();
                            }
                        }
                    }
                    if (rule.type == AtomIncreaseType.NUMBER) {
                        if (obj == null) {
                            obj = 0L;
                        }
                        if (obj instanceof Number) {
                            return String.valueOf((((Number) obj).longValue() % rule.tbCount) / rule.dbCount);
                        } else if (obj instanceof String) {
                            // 处理字符串转码
                            return String.valueOf(
                                (((Long) stringToNumberConvert.convert(obj, Long.class)).longValue() % rule.tbCount)
                                    / rule.dbCount);
                        } else {
                            throw new IllegalArgumentException(obj + "不是一个合法的数字类型");
                        }
                    } else if (rule.type == AtomIncreaseType.NUMBER_ABS) {
                        if (obj == null) {
                            obj = 0L;
                        }
                        if (obj instanceof Number) {
                            return String
                                .valueOf(RuleUtils.safeAbs(((Number) obj).longValue() % rule.tbCount / rule.dbCount));
                        } else if (obj instanceof String) {
                            // 处理字符串转码
                            return String.valueOf(
                                (RuleUtils.safeAbs(
                                    ((Long) stringToNumberConvert.convert(obj, Long.class)).longValue() % rule.tbCount)
                                    / rule.dbCount));
                        } else {
                            throw new IllegalArgumentException(obj + "不是一个合法的数字类型");
                        }
                    } else if (rule.type == AtomIncreaseType.STRING) {
                        if (obj == null) {
                            obj = "0";
                        }
                        if (obj instanceof String || obj instanceof Number) {
                            return String.valueOf(RuleUtils.safeAbs((obj.hashCode() % rule.tbCount) / rule.dbCount));
                        } else {
                            throw new IllegalArgumentException(obj + "不是一个合法的字符串类型");
                        }

                    } else {
                        throw new IllegalArgumentException(obj + "不是一个合法的简单规则类型");
                    }
                }

            };
        } else {
            loader = new GroovyClassLoader(GroovyRule.class.getClassLoader());
            String groovyRule = getGroovyRule(expression, extraPackagesStr);
            Class<?> c_groovy;
            try {
                c_groovy = loader.parseClass(groovyRule);
            } catch (CompilationFailedException e) {
                throw new IllegalArgumentException(groovyRule, e);
            }

            try {
                // 新建类实例
                Object ruleObj = c_groovy.newInstance();
                if (ruleObj instanceof ShardingFunction) {
                    shardingFunction = (ShardingFunction) ruleObj;
                } else {
                    throw new IllegalArgumentException("should not be here");
                }
                // 获取方法

            } catch (Throwable t) {
                throw new IllegalArgumentException("实例化规则对象失败", t);
            }
        }
    }

    /**
     * 通过为expression来动态拼JAVA代码，实时地弄一个ShardingFunction类的实现
     *
     * <pre>
     * import com.alibaba.polardbx.rule.impl.groovy.ExtraParameterContext;
     * import static com.alibaba.polardbx.rule.impl.groovy.GroovyStaticMethod.*;
     * public class RULE implements com.alibaba.polardbx.rule.impl.groovy.ShardingFunction {
     *
     *      public Object eval(Map map, Object outerCtx) {
     *
     *           return "expression";
     *
     *      };
     * }
     *
     * 或
     *
     * import com.alibaba.polardbx.rule.impl.groovy.ExtraParameterContext;
     * import static com.alibaba.polardbx.rule.impl.groovy.GroovyStaticMethod.*;
     * public class RULE implements com.alibaba.polardbx.rule.impl.groovy.ShardingFunction {
     *
     *      public Object eval(Map map, Object outerCtx) {
     *
     *           return "expression" + "";
     *
     *      };
     * }
     * </pre>
     */
    protected static String getGroovyRule(String expression, String extraPackagesStr) {
        expression = expression.replace("Math.abs", "com.alibaba.polardbx.rule.impl.groovy.RuleUtils.safeAbs");
        StringBuffer sb = new StringBuffer();
        sb.append(extraPackagesStr);
        sb.append(IMPORT_STATIC_METHOD);
        sb.append(IMPORT_EXTRA_PARAMETER_CONTEXT);
        sb.append("public class RULE implements com.alibaba.polardbx.rule.impl.groovy.ShardingFunction").append("{");
        sb.append("public Object eval(Map map, Object outerCtx) {");
        Matcher returnMarcher = RETURN_WHOLE_WORD_PATTERN.matcher(expression);
        if (!returnMarcher.find()) {
            sb.append("return ");
            sb.append(expression);
            sb.append("+\"\";};}");
        } else {
            sb.append(expression);
            sb.append(";};}");
        }
        if (logger.isDebugEnabled()) {
            logger.debug(sb.toString());
        }
        return sb.toString();
    }

    /**
     * 替换成(map.get("name"));以在运算时通过列名取得参数值（描点值）
     */
    @Override
    protected String replace(RuleColumn ruleColumn) {
        if (true) {

            // 针对null值,使用0值替代
            // return new
            // StringBuilder("defaultIfNull(map.get(\"").append(ruleColumn.key).append("\"),0L)").toString();

            // 针对null值,使用0值替代
            String callPreProcessShardKeyValueStr = buildPreProcessShardKeyValueCallerStr(ruleColumn);
            return new StringBuilder(callPreProcessShardKeyValueStr).toString();

        } else {
            return new StringBuilder("(map.get(\"").append(ruleColumn.key).append("\"))").toString();
        }
    }

    public String buildPreProcessShardKeyValueCallerStr(RuleColumn ruleColumn) {

        String atomIncreaseTypeStr = "";
        if (ruleColumn instanceof AdvancedParameter) {
            AdvancedParameter apRuleCol = (AdvancedParameter) ruleColumn;
            AtomIncreaseType type = apRuleCol.atomicIncreateType;
            atomIncreaseTypeStr = type.toString();
            if (type == AtomIncreaseType.STRNUM) {
                atomIncreaseTypeStr = type.toString();
            }
        }

        Map<String, String> callerOtherParamsMap = new HashMap<String, String>();
        callerOtherParamsMap.put("ruleColumnKey", ruleColumn.key);
        callerOtherParamsMap.put("ruleColumnType", atomIncreaseTypeStr);

        String paramHashMapStr = buildCallerParamHashMapStr(callerOtherParamsMap);

        StringBuilder callPreProcessShardKeyValueStrSb = new StringBuilder("");
        callPreProcessShardKeyValueStrSb.append("preProcessShardKeyValue(map, \"");
        callPreProcessShardKeyValueStrSb.append(ruleColumn.key);
        callPreProcessShardKeyValueStrSb.append("\", \"");
        callPreProcessShardKeyValueStrSb.append(atomIncreaseTypeStr);
        callPreProcessShardKeyValueStrSb.append("\", ");
        callPreProcessShardKeyValueStrSb.append(paramHashMapStr);
        callPreProcessShardKeyValueStrSb.append(")");
        return callPreProcessShardKeyValueStrSb.toString();
    }

    protected String buildCallerParamHashMapStr(Map<String, String> callerOtherParamsMap) {

        String paramHashMapStrPrefix = "new java.util.HashMap(){{";
        String paramHashMapStrSuffix = "}}";

        StringBuilder paramHashMapPutParamsSb = new StringBuilder("");
        String paramHashMapPutParams = "";

        for (Map.Entry<String, String> paramsItem : callerOtherParamsMap.entrySet()) {
            String paramKey = paramsItem.getKey();
            String paramVal = paramsItem.getValue();

            paramHashMapPutParamsSb.append("put(\"");
            paramHashMapPutParamsSb.append(paramKey);
            paramHashMapPutParamsSb.append("\", \"");
            paramHashMapPutParamsSb.append(paramVal);
            paramHashMapPutParamsSb.append("\");");

        }
        paramHashMapPutParams = paramHashMapPutParamsSb.toString();

        StringBuilder paramHashMapStrSb = new StringBuilder("");
        paramHashMapStrSb.append(paramHashMapStrPrefix).append(paramHashMapPutParams).append(paramHashMapStrSuffix);
        return paramHashMapStrSb.toString();
    }

    /**
     * <pre>
     * 调用groovy的方法：
     *
     *  public Object eval(Map map,Map ctx) {
     *
     *      ...
     *
     *  }");
     * </pre>
     */
    @Override
    public T eval(Map<String, Object> columnValues, Object outerCtx) {
        return eval(columnValues, outerCtx, null);
    }

    /**
     * <pre>
     * 调用groovy的方法：
     *
     *  // map: 拆分键及其对应值
     *  // ctx: tableRule上下文, 大部分情况为null
     *  // calcParams: 规则动态计算时的参数
     *  public Object eval(Map map, Map ctx, Map calcParams) {
     *
     *      ...
     *
     *  }");
     * </pre>
     */
    @Override
    public T eval(Map<String, Object> columnValues, Object outerCtx, Map<String, Object> calcParams) {
        try {
            if (shardingFunction == null) {
                synchronized (this) {
                    if (shardingFunction == null) {
                        initGroovy();
                    }
                }
            }
            T value = (T) shardingFunction.eval(columnValues, outerCtx);
            if (value == null) {
                throw new IllegalArgumentException("rule eval resulte is null! rule:" + this.expression);
            }
            return value;
        } catch (Throwable t) {
            throw new IllegalArgumentException("调用方法失败: " + expression, t);
        }
    }

    @Override
    public String toString() {
        return new StringBuilder("GroovyRule{expression=").append(expression)
            .append(", parameters=")
            .append(parameters)
            .append("}")
            .toString();
    }

    public VirtualTableSupport getTableRule() {
        return tableRule;
    }
}
