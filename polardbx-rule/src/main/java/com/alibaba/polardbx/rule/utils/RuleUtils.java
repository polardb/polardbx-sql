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
import com.alibaba.polardbx.common.model.sqljep.ComparativeAND;
import com.alibaba.polardbx.common.model.sqljep.ExtComparative;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.rule.MappingRule;
import com.alibaba.polardbx.rule.TableRule;
import com.alibaba.polardbx.rule.enumerator.EnumGeneratorImpl;
import com.alibaba.polardbx.rule.enumerator.Enumerator;
import com.alibaba.polardbx.rule.enumerator.RuleEnumeratorImpl;
import com.alibaba.polardbx.rule.enums.RuleType;
import com.alibaba.polardbx.rule.exception.TddlRuleException;
import com.alibaba.polardbx.rule.model.ShardExtContext;
import com.alibaba.polardbx.rule.model.ShardMatchedRuleType;
import com.alibaba.polardbx.rule.model.ShardPrepareResult;
import com.alibaba.polardbx.rule.model.AdvancedParameter;
import org.apache.commons.lang.StringUtils;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
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
                                                            ShardPrepareResult shardPrepareResult, RuleType ruleType) {
        // 枚举以后的columns与他们的描点之间的对应关系
        Map<String, Set<Object>> enumeratedMap = new HashMap<>(param.size());
        for (AdvancedParameter entry : param) {
            String key = entry.key;
            // 当前enumerator中指定当前规则是否需要处理交集问题。
            try {
                if (argumentsMap.containsKey(key)) {
                    Enumerator enumerator = new EnumGeneratorImpl(shardPrepareResult, ruleType, extMappingRules, entry);
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

    /**
     *
     */
    public static ShardPrepareResult prepareMatchedRule(ShardExtContext shardExtContext) {
        final TableRule rule = shardExtContext.getRule();
        // 此处代码测试时可注释，更容易复现问题
        if (rule == null) {
            return new ShardPrepareResult(ShardMatchedRuleType.DB, ShardMatchedRuleType.TB);
        }
        final List<MappingRule> extPartitions = rule.getExtPartitions();
        if (extPartitions == null || extPartitions.size() == 0) {
            return new ShardPrepareResult(ShardMatchedRuleType.DB, ShardMatchedRuleType.TB);
        }
        // 枚举以后的columns与他们的描点之间的对应关系
        ShardMatchedRuleType matchedDBType = ShardMatchedRuleType.MIXED_DB;
        ShardMatchedRuleType matchedTBType = ShardMatchedRuleType.MIXED_TB;
        final Comparative comparative = shardExtContext.getComparative();
        if (comparative == null) {
            return new ShardPrepareResult(matchedDBType, matchedTBType);
        }
        Comparative comparative1 = null;
        Comparative comparative2 = null;
        if (comparative instanceof ComparativeAND) {
            final List<Comparative> list = ((ComparativeAND) comparative).getList();
            if (list.size() != 2) {
                return new ShardPrepareResult(matchedDBType, matchedTBType);
            }
            comparative1 = list.get(0);
            comparative2 = list.get(1);
        } else if (comparative instanceof ExtComparative) {
            comparative1 = comparative;
            comparative2 = comparative;
        } else {
            return new ShardPrepareResult(matchedDBType, matchedTBType);
        }

        if (comparative1.getComparison() != Comparative.Equivalent
            || comparative2.getComparison() != Comparative.Equivalent) {
            return new ShardPrepareResult(matchedDBType, matchedTBType);
        }

        // 只走新逻辑唯一库表
        if (comparative1 instanceof ExtComparative && comparative2 instanceof ExtComparative) {
            String columnName1 = ((ExtComparative) comparative1).getColumnName();
            String columnName2 = ((ExtComparative) comparative2).getColumnName();
            final String shardDbColumn = shardExtContext.getShardDbColumn();
            final String shardTableColumn = shardExtContext.getShardTableColumn();
            // 分库不分表、不分库分表情况
            if (shardDbColumn == null && shardTableColumn == null) {
                return new ShardPrepareResult(matchedDBType, matchedTBType);
            }
            // 分库字段为空可能只有分表规则
            if (shardDbColumn == null) {
                return new ShardPrepareResult(matchedDBType, matchedTBType);
            }

            // 仅仅当条件不同时交换条件使用
            if (shardDbColumn != null && !shardDbColumn.equalsIgnoreCase(shardTableColumn)) {// 分库分表字段不同
                if (!columnName1.equalsIgnoreCase(shardDbColumn)) {
                    Comparative comparativeTmp = comparative1;
                    comparative1 = comparative2;
                    comparative2 = comparativeTmp;
                    columnName1 = ((ExtComparative) comparative1).getColumnName();
                    columnName2 = ((ExtComparative) comparative2).getColumnName();
                }
            }

            /**
             * 单库分表不支持热点映射，所以目前不需要判断shardDbColumn是否为空，
             * 其他情况增加了分库部分表后需要额外判断是否走热点或继承
             */
            if (StringUtils.isNotEmpty(shardTableColumn)) {
                if (!columnName1.equalsIgnoreCase(shardDbColumn) || !columnName2.equalsIgnoreCase(shardTableColumn)) {
                    return new ShardPrepareResult(matchedDBType, matchedTBType);
                }
            } else {
                /**
                 * <pre>
                 * 分库分表：
                 *    字段相同：此时如果匹配到了规则 shardTableColumn 一定不为空
                 *    字段不同：此时只有db条件，则走混合规则（匹配热点规则和普通规则的集合）
                 * 分库不分表：
                 *    只有分库字段，走匹配热点映射逻辑
                 * </pre>
                 */
                if (rule.getRuleDbCount() > 1 && rule.getRuleTbCount() / rule.getRuleDbCount() > 1) {
                    return new ShardPrepareResult(matchedDBType, matchedTBType);
                }

            }
            // 此条件判断有误，本意是用于判断要同时包含分库字段和分表字段，但是遇到分库分表字段相同时，则无法判断，故此判断存在逻辑问题<-此逻辑问题已于10月18号修复
            // 1. 针对配到到dbkey和tbkey的直接走指定热点库
            for (int i = 0; i < extPartitions.size(); i++) {
                final MappingRule mappingRule = extPartitions.get(i);
                if (comparative1.compareToIgnoreType(mappingRule.getDbKeyValue()) == 0
                    && comparative2.compareToIgnoreType(mappingRule.getTbKeyValue()) == 0) {
                    return new ShardPrepareResult(ShardMatchedRuleType.HOT_DB, ShardMatchedRuleType.HOT_TB,
                        mappingRule);
                }
            }
            // 2. 针对匹配到dbkey热点的，但是没tbkey的（1. tbKeyValue is
            // null；2.tbKeyValue有但是没有匹配到），
            // 直接走此hot库，此处匹配单库，需要规则继承判断表的去向，需要走唯一表
            // 2.1 匹配继承规则
            // 2.2 热点不匹配，直接走普通库查询，此时仍然可以唯一匹配库列表
            for (int i = 0; i < extPartitions.size(); i++) {
                final MappingRule mappingRule = extPartitions.get(i);
                if (comparative1.compareToIgnoreType(mappingRule.getDbKeyValue()) == 0) {
                    if (StringUtils.isEmpty(mappingRule.getTbKeyValue())) {
                        return new ShardPrepareResult(ShardMatchedRuleType.HOT_DB, ShardMatchedRuleType.TB,
                            mappingRule);
                    }
                }
            }
            // 此处不返回，继续走完for循环，但是此处会走混合规则的全库表扫描
            // 3. 针对匹配到db但是没有匹配到tb的或没匹配到db、tb热点的，直接走普通库查询，此时仍然可以唯一匹配库表
            matchedDBType = ShardMatchedRuleType.DB;
            matchedTBType = ShardMatchedRuleType.TB;

        }
        // 只有分库，需要判定是热点库继承规则，还是普通库，如果是热点库走指定dbkey、tbkey的热点库同时，还要走默认库；//如果没匹配到热点库，走普通规则
        return new ShardPrepareResult(matchedDBType, matchedTBType);
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
}
