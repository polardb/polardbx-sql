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

package com.alibaba.polardbx.rule.utils;

import com.alibaba.polardbx.common.exception.TddlNestableRuntimeException;
import com.alibaba.polardbx.common.model.sqljep.Comparative;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.rule.MappingRule;
import com.alibaba.polardbx.rule.TableRule;
import com.alibaba.polardbx.rule.enumerator.EnumGeneratorImpl;
import com.alibaba.polardbx.rule.enumerator.Enumerator;
import com.alibaba.polardbx.rule.enumerator.RuleEnumeratorImpl;
import com.alibaba.polardbx.rule.enums.RuleType;
import com.alibaba.polardbx.rule.exception.TddlRuleException;
import com.alibaba.polardbx.rule.model.AdvancedParameter;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class RuleUtils {

    private static final Enumerator enumerator = new RuleEnumeratorImpl();

    public static void notNull(Object obj, String message) {
        if (obj == null) {
            throw new IllegalArgumentException(message);
        }
    }

    public static boolean isEmpty(Collection collection) {
        return collection == null || collection.isEmpty();
    }

    public static boolean isNotEmpty(Collection collection) {
        return !isEmpty(collection);
    }

    public static boolean isEmpty(Map map) {
        return map == null || map.isEmpty();
    }

    public static boolean isNotEmpty(Map map) {
        return !isEmpty(map);
    }

    public static float safeAbs(float val) {
        val = Math.abs(val);
        if (val < 0 || val == Float.NaN) {
            val = 0;
        }
        return val;
    }

    public static double safeAbs(double val) {
        val = Math.abs(val);
        if (val < 0 || val == Double.NaN) {
            val = 0;
        }
        return val;
    }

    public static int safeAbs(int val) {
        val = Math.abs(val);
        if (val < 0) {
            val = Integer.MAX_VALUE;
        }
        return val;
    }

    public static long safeAbs(long val) {
        val = Math.abs(val);
        if (val < 0) {
            val = Long.MAX_VALUE;
        }
        return val;
    }

    /**
     * 返回对应column的枚举值
     */
    public static Map<String, Set<Object>> getSamplingField(Map<String, Comparative> argumentsMap,
                                                            Set<AdvancedParameter> param) {
        // 枚举以后的columns与他们的描点之间的对应关系
        Map<String, Set<Object>> enumeratedMap = new HashMap<String, Set<Object>>(param.size());
        for (AdvancedParameter entry : param) {
            String key = entry.key;
            // 当前enumerator中指定当前规则是否需要处理交集问题。
            try {
                if (argumentsMap.containsKey(key)) {
                    // 四种情况，新规则匹配|不匹配，老规则匹配|不匹配

                    Enumerator enumerator = new RuleEnumeratorImpl(entry);
                    Set<Object> samplingField = enumerator.getEnumeratedValue(argumentsMap.get(key),
                        entry.cumulativeTimes,
                        entry.atomicIncreateValue,
                        entry.needMergeValueInCloseInterval);
                    if (samplingField.isEmpty()) {
                        enumeratedMap.put(key, entry.enumerateRange((TableRule) null));
                    } else {
                        enumeratedMap.put(key, samplingField);
                    }
                } else {
                    // 如果sql中不存在，则进行全表扫
                    // 因为这里的枚举不是从规则来的，所以不需要处理特殊的mmdd_i和dd_i的时间函数
                    enumeratedMap.put(key, entry.enumerateRange((TableRule) null));
                }
            } catch (UnsupportedOperationException e) {
                throw new UnsupportedOperationException("当前列分库分表出现错误，出现错误的列名是:" + entry.key, e);
            }
        }

        return enumeratedMap;
    }

    /**
     * 返回对应column的枚举值
     */
    public static Map<String, Set<Object>> getSamplingField(Map<String, Comparative> argumentsMap,
                                                            Set<AdvancedParameter> param,
                                                            Map<String, Set<MappingRule>> extMappingRules,
                                                            RuleType ruleType) {
        // 枚举以后的columns与他们的描点之间的对应关系
        Map<String, Set<Object>> enumeratedMap = new HashMap<>(param.size());
        for (AdvancedParameter entry : param) {
            String key = entry.key;
            // 当前enumerator中指定当前规则是否需要处理交集问题。
            try {
                if (argumentsMap.containsKey(key)) {
                    Enumerator enumerator = new EnumGeneratorImpl(ruleType, extMappingRules, entry);
                    final Set<Object> samplingField = enumerator.getEnumeratedValue(argumentsMap.get(key),
                        entry.cumulativeTimes,
                        entry.atomicIncreateValue,
                        entry.needMergeValueInCloseInterval);
                    enumeratedMap.put(key, samplingField);

                    if (samplingField.isEmpty()) {
                        enumeratedMap.put(key, entry.enumerateRangeWithHot((TableRule) null, extMappingRules));
                    } else {
                        enumeratedMap.put(key, samplingField);
                    }

                } else {
                    // 如果sql中不存在，则进行全表扫
                    // 因为这里的枚举不是从规则来的，所以不需要处理特殊的mmdd_i和dd_i的时间函数
                    enumeratedMap.put(key, entry.enumerateRangeWithHot((TableRule) null, extMappingRules));
                }
            } catch (UnsupportedOperationException e) {
                throw new UnsupportedOperationException("当前列分库分表出现错误，出现错误的列名是:" + entry.key, e);
            }
        }

        return enumeratedMap;
    }

    public static <T> T cast(Object obj) {
        return (T) obj;
    }

    public static void logOnly(Logger logger, String errMsg) {
        logOnly(logger, errMsg, null);
    }

    public static void logOnly(Logger logger, String errMsg, Throwable t) {
        if (errMsg == null) {
            errMsg = "Hit exception without specific error message.";
        }
        if (t != null) {
            logger.error(errMsg, t);
        } else {
            logger.error(errMsg);
        }
    }

    public static TddlRuleException logAndThrow(Logger logger, String errMsg) {
        throw logAndThrow(logger, errMsg, null);
    }

    public static TddlRuleException logAndThrow(Logger logger, String errMsg, Throwable t) {
        if (errMsg == null) {
            errMsg = "Hit exception without specific error message.";
        }
        if (t != null) {
            logger.error(errMsg, t);
            throw new TddlNestableRuntimeException(errMsg, t);
        } else {
            logger.error(errMsg);
            throw new TddlNestableRuntimeException(errMsg);
        }
    }

    public static String mockEmptyRule() {
        return "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
            + "<!DOCTYPE beans PUBLIC \"-//SPRING//DTD BEAN//EN\" \"http://www.springframework.org/dtd/spring-beans.dtd\">\n"
            + "<beans>\n"
            + "  <bean class=\"com.alibaba.polardbx.rule.VirtualTableRoot\" id=\"vtabroot\" init-method=\"init\">\n"
            + "    <property name=\"dbType\" value=\"MYSQL\" />\n"
            + "    <property name=\"tableRules\">\n"
            + "      <map>\n"
            + "      </map>\n"
            + "    </property>\n"
            + "  </bean>\n"
            + "\n"
            + "</beans>";
    }
}
