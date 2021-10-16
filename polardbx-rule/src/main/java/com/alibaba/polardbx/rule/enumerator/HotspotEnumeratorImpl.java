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

import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import com.alibaba.polardbx.common.exception.NotSupportException;
import com.alibaba.polardbx.common.model.sqljep.Comparative;
import com.alibaba.polardbx.common.model.sqljep.ComparativeAND;
import com.alibaba.polardbx.common.model.sqljep.ComparativeBaseList;
import com.alibaba.polardbx.common.model.sqljep.ComparativeOR;
import com.alibaba.polardbx.rule.MappingRule;
import com.alibaba.polardbx.rule.Rule.RuleColumn;
import com.alibaba.polardbx.rule.enumerator.handler.CloseIntervalFieldsEnumeratorHandler;
import com.alibaba.polardbx.rule.exception.TddlRuleException;

/**
 * 针对热点数据的枚举匹配实现
 *
 * @author hongxi.chx 2018-12-06 下午5:57:27
 * @since 5.0.0
 */
public class HotspotEnumeratorImpl extends BaseEnumerator implements Enumerator {

    private static final String DEFAULT_ENUMERATOR = "DEFAULT_ENUMERATOR";
    private boolean isDebug = false;

    private final Map<String, Set<MappingRule>> extMappingRules;

    public HotspotEnumeratorImpl(Map<String, Set<MappingRule>> extMappingRules, RuleColumn ruleColumnParams) {
        super(ruleColumnParams);
        this.extMappingRules = extMappingRules;

    }

    public Set<Object> getEnumeratedValue(Comparable condition, Integer cumulativeTimes, Comparable<?> atomIncrValue,
                                          boolean needMergeValueInCloseInterval) {
        Set<Object> retValue = null;
        if (!isDebug) {
            retValue = new HashSet<Object>();
        } else {
            retValue = new TreeSet<Object>();
        }
        try {
            processForExtPartition(condition,
                retValue,
                cumulativeTimes,
                atomIncrValue,
                extMappingRules,
                needMergeValueInCloseInterval);
        } catch (EnumerationInterruptException e) {
            processAllPassableFields(e.getComparative(),
                retValue,
                cumulativeTimes,
                atomIncrValue,
                needMergeValueInCloseInterval);
            retValue.addAll(extMappingRules.values());
        }
        return retValue;
    }

    // ======================== 委托调用handler进行处理==========================

    /**
     * 函数的目标是返回全部可能的值，主要用于无限的定义域的处理，一般的说，对于部分连续部分不连续的函数曲线。
     * 这个值应该是从任意一个值开始，按照原子自增值与倍数穷举出该函数的y的一个变化周期中x对应的变化周期的所有点即可。
     */
    private void processAllPassableFields(Comparative source, Set<Object> retValue, Integer cumulativeTimes,
                                          Comparable<?> atomIncrValue, boolean needMergeValueInCloseInterval) {
        if (!needMergeValueInCloseInterval) {
            throw new EnumerationFailedException("请打开规则的needMergeValueInCloseInterval选项，以支持分库分表条件中使用> < >= <=");
        }

        // 重构 现在这种架构下，id =? id in (?,?,?)都能走最短路径，但如果有多个 id > ? and id < ? or id>
        // ? and id<? 则要从map中查多次。不过因为这种情况比较少，因此可以忽略
        CloseIntervalFieldsEnumeratorHandler closeIntervalFieldsEnumeratorHandler =
            getCloseIntervalEnumeratorHandlerByComparative(source,
                needMergeValueInCloseInterval);
        closeIntervalFieldsEnumeratorHandler.processAllPassableFields(source, retValue, cumulativeTimes, atomIncrValue);
    }

    /**
     * 穷举出从from到to中的所有值，根据自增value
     */
    private void mergeFeildOfDefinitionInCloseInterval(Comparative from, Comparative to, Set<Object> retValue,
                                                       Integer cumulativeTimes, Comparable<?> atomIncrValue,
                                                       boolean needMergeValueInCloseInterval) {
        if (!needMergeValueInCloseInterval) {
            throw new IllegalArgumentException("请打开规则的needMergeValueInCloseInterval选项，以支持分库分表条件中使用> < >= <=");
        }

        // 重构 现在这种架构下，id =? id in (?,?,?)都能走最短路径
        // 但如果有多个 id > ? and id < ? or id > ? and id<?
        // 则要从map中查多次。不过因为这种情况比较少，因此可以忽略
        CloseIntervalFieldsEnumeratorHandler closeIntervalFieldsEnumeratorHandler =
            getCloseIntervalEnumeratorHandlerByComparative(from,
                needMergeValueInCloseInterval);
        closeIntervalFieldsEnumeratorHandler.mergeFeildOfDefinitionInCloseInterval(from,
            to,
            retValue,
            cumulativeTimes,
            atomIncrValue);
    }

    /**
     * 根据传入的参数决定使用哪类枚举器
     */
    private CloseIntervalFieldsEnumeratorHandler getCloseIntervalEnumeratorHandlerByComparative(Comparative comp,
                                                                                                boolean needMergeValueInCloseInterval) {
        if (!needMergeValueInCloseInterval) {
            //return enumeratorMap.get(DEFAULT_ENUMERATOR);
            return getEnumeratorMapByClassName(DEFAULT_ENUMERATOR);
        }
        if (comp == null) {
            throw new IllegalArgumentException("comp is null");
        }

        Object value = comp.getValue();
        if (value instanceof ComparativeBaseList) {
            ComparativeBaseList comparativeBaseList = (ComparativeBaseList) value;
            for (Comparative comparative : comparativeBaseList.getList()) {
                return getCloseIntervalEnumeratorHandlerByComparative(comparative, needMergeValueInCloseInterval);
            }

            throw new NotSupportException();// 不可能到这一步
        } else if (value instanceof Comparative) {
            return getCloseIntervalEnumeratorHandlerByComparative(comp, needMergeValueInCloseInterval);
        } else {
            // 表明是一个comparative对象
            //CloseIntervalFieldsEnumeratorHandler enumeratorHandler = enumeratorMap.get(value.getClass().getName());

            CloseIntervalFieldsEnumeratorHandler enumeratorHandler =
                getEnumeratorMapByClassName(value.getClass().getName());

            if (enumeratorHandler != null) {
                return enumeratorHandler;
            } else {
                //return enumeratorMap.get(DEFAULT_ENUMERATOR);
                return getEnumeratorMapByClassName(DEFAULT_ENUMERATOR);
            }
        }
    }

    // ======================== helper method ==================

    private Comparative valid2varableInAndIsNotComparativeBaseList(Object arg) {
        if (arg instanceof ComparativeBaseList) {
            throw new TddlRuleException("ComparativeAND only support two args");
        }

        if (arg instanceof Comparative) {
            Comparative comp = ((Comparative) arg);
            int comparison = comp.getComparison();
            if (comparison == 0) {
                // 0的时候意味着这个非ComparativeBaseList的Comparative是个纯粹的包装对象。
                return valid2varableInAndIsNotComparativeBaseList(comp.getValue());
            } else {
                // 其他就是有意义的值对象了
                return comp;
            }
        } else {
            // 否则就是基本对象，应该用等于包装
            // return new Comparative(Comparative.Equivalent,arg);
            throw new IllegalArgumentException("input value is not a comparative: " + arg);
        }
    }

    private boolean containsEquvilentRelation(Comparative comp) {
        int comparasion = comp.getComparison();
        if (comparasion == Comparative.Equivalent || comparasion == Comparative.GreaterThanOrEqual
            || comparasion == Comparative.LessThanOrEqual) {
            return true;
        }
        return false;
    }

    /**
     * 是否属于一个区间
     */
    private boolean isCloseInterval(Comparative from, Comparative to) {
        int fromComparasion = from.getComparison();
        int toComparasion = to.getComparison();
        // 本来想简单通过数值比大小，但发现里面还有not in,like这类的标记，还是保守点写清楚
        if ((fromComparasion == Comparative.GreaterThan || fromComparasion == Comparative.GreaterThanOrEqual)
            && (toComparasion == Comparative.LessThan || toComparasion == Comparative.LessThanOrEqual)) {
            return true;
        } else {
            return false;
        }

    }

    /**
     * 处理一个and条件中 x > 1 and x = 3 类似这样的情况，因为前面已经对from 和 to 相等的情况作了处理
     * 因此这里只需要处理不等的情况中的上述问题。 同时也处理了x = 1 and x = 2这种情况。以及x = 1 and x>2 和x < 1
     * and x =2这种情况
     */
    protected static Object compareAndGetIntersactionOneValue(Comparative from, Comparative to) {
        // x = from and x <= to
        if (from.getComparison() == Comparative.Equivalent) {
            if (to.getComparison() == Comparative.LessThan || to.getComparison() == Comparative.LessThanOrEqual) {
                return from.getValue();
            }
        }

        // x <= from and x = to
        if (to.getComparison() == Comparative.Equivalent) {
            if (from.getComparison() == Comparative.GreaterThan
                || from.getComparison() == Comparative.GreaterThanOrEqual) {
                return to.getValue();
            }
        }

        return null;
    }

    public boolean isDebug() {
        return isDebug;
    }

    public void setDebug(boolean isDebug) {
        this.isDebug = isDebug;
    }

    private void processForExtPartition(Object condition, Set<Object> retValue, Integer cumulativeTimes,
                                        Comparable<?> atomIncrValue, Map<String, Set<MappingRule>> extKeyMappingRules,
                                        boolean needMergeValueInCloseInterval) {
        if (condition == null) {
            retValue.add(null);
        } else if (condition instanceof ComparativeOR) {
            processComparativeORForExtPartition((ComparativeOR) condition,
                retValue,
                cumulativeTimes,
                atomIncrValue,
                extKeyMappingRules,
                needMergeValueInCloseInterval);
        } else if (condition instanceof ComparativeAND) {

            processComparativeAndContainingBaseListForExtPartition((ComparativeAND) condition,
                retValue,
                cumulativeTimes,
                atomIncrValue,
                extKeyMappingRules,
                needMergeValueInCloseInterval);
        } else if (condition instanceof Comparative) {
            processComparativeOneForExtPartition((Comparative) condition,
                retValue,
                cumulativeTimes,
                atomIncrValue,
                extKeyMappingRules,
                needMergeValueInCloseInterval);
        } else {
            retValue.add(condition);
        }
    }

    private void processComparativeORForExtPartition(Comparable<?> condition, Set<Object> retValue,
                                                     Integer cumulativeTimes, Comparable<?> atomIncrValue,
                                                     Map<String, Set<MappingRule>> extKeyMappingRules,
                                                     boolean needMergeValueInCloseInterval) {
        List<Comparative> orList = ((ComparativeOR) condition).getList();
        for (Comparative comp : orList) {
            // 递归调用
            processForExtPartition(comp,
                retValue,
                cumulativeTimes,
                atomIncrValue,
                extKeyMappingRules,
                needMergeValueInCloseInterval);

        }
    }

    private void processComparativeAndContainingBaseListForExtPartition(Comparable<?> condition,
                                                                        Set<Object> retValue,
                                                                        Integer cumulativeTimes,
                                                                        Comparable<?> atomIncrValue,
                                                                        Map<String, Set<MappingRule>> extKeyMappingRules,
                                                                        boolean needMergeValueInCloseInterval) {
        List<Comparative> andList = ((ComparativeAND) condition).getList();
        // 多余两个感觉没什么实际的意义，碰到了再处理
        if (andList.size() == 2) {
            Comparable<?> arg1 = andList.get(0);
            Comparable<?> arg2 = andList.get(1);

            if (!(arg1 instanceof ComparativeBaseList) && !(arg2 instanceof ComparativeBaseList)) {
                processComparativeAndForExtPartition(condition,
                    retValue,
                    cumulativeTimes,
                    atomIncrValue,
                    extKeyMappingRules,
                    needMergeValueInCloseInterval);
            } else {
                // 来到这步，说明arg1 与 arg2 至少有一个还是ANR 或 OR 的逻辑条件，而不是布尔条件

                // 获取arg1的枚举值
                Set<Object> retValueForArg1 = new HashSet<Object>();
                try {
                    processForExtPartition(arg1,
                        retValueForArg1,
                        cumulativeTimes,
                        atomIncrValue,
                        extKeyMappingRules,
                        needMergeValueInCloseInterval);
                } catch (EnumerationInterruptException e) {
                    // 还有后续条件，需要catch住异常
                    processAllPassableFields(e.getComparative(),
                        retValueForArg1,
                        cumulativeTimes,
                        atomIncrValue,
                        needMergeValueInCloseInterval);
                    retValue.addAll(extKeyMappingRules.values());
                }

                // 获取arg2的枚举值
                Set<Object> retValueForArg2 = new HashSet<Object>();
                try {
                    processForExtPartition(arg2,
                        retValueForArg2,
                        cumulativeTimes,
                        atomIncrValue,
                        extKeyMappingRules,
                        needMergeValueInCloseInterval);
                } catch (EnumerationInterruptException e) {
                    // 还有后续条件，需要catch住异常
                    processAllPassableFields(e.getComparative(),
                        retValueForArg2,
                        cumulativeTimes,
                        atomIncrValue,
                        needMergeValueInCloseInterval);
                    retValue.addAll(extKeyMappingRules.values());
                }
                // 求两者的枚举值的交集
                Set<Object> finalRetValue = new HashSet<Object>();

                finalRetValue.addAll(retValueForArg1);
                finalRetValue.retainAll(retValueForArg2);
                if (!finalRetValue.isEmpty()) {
                    retValue.addAll(finalRetValue);
                } else {
                    // deal with scenarios like : cumulativeTimes = 4, left
                    // operator is x > 7, right operator is x < 18
                    if (retValueForArg1.size() == cumulativeTimes) {
                        retValue.addAll(retValueForArg2);
                    } else if (retValueForArg2.size() == cumulativeTimes) {
                        retValue.addAll(retValueForArg1);
                    } else {
                        // 求交集后，ComparativeAND的枚举集最终为空
                        // throw new
                        // TddlRuleException("ComparativeAND leads to an empty enumeration set");
                    }
                }

            }
        } else {
            throw new TddlRuleException("ComparativeAND only support two args");
        }
    }

    private void processComparativeAndForExtPartition(Comparable<?> condition, Set<Object> retValue,
                                                      Integer cumulativeTimes, Comparable<?> atomIncrValue,
                                                      Map<String, Set<MappingRule>> extKeyMappingRules,
                                                      boolean needMergeValueInCloseInterval) {
        List<Comparative> andList = ((ComparativeAND) condition).getList();
        // 多余两个感觉没什么实际的意义，碰到了再处理
        if (andList.size() == 2) {
            Comparable<?> arg1 = andList.get(0);
            Comparable<?> arg2 = andList.get(1);
            Comparative compArg1 = valid2varableInAndIsNotComparativeBaseList(arg1);
            Comparative compArg2 = valid2varableInAndIsNotComparativeBaseList(arg2);

            int compResult = 0;
            // deal with not equivalent, retValue collection is dominated by the
            // other arg.
            if (compArg1.getComparison() == Comparative.NotEquivalent) {
                processForExtPartition(compArg2,
                    retValue,
                    cumulativeTimes,
                    atomIncrValue,
                    extKeyMappingRules,
                    needMergeValueInCloseInterval);
                return;
            } else if (compArg2.getComparison() == Comparative.NotEquivalent) {
                processForExtPartition(compArg1,
                    retValue,
                    cumulativeTimes,
                    atomIncrValue,
                    extKeyMappingRules,
                    needMergeValueInCloseInterval);
                return;
            }

            try {
                compArg1.setValue(EnumeratorUtils.toPrimaryValue(compArg1.getValue()));
                compArg2.setValue(EnumeratorUtils.toPrimaryValue(compArg2.getValue()));
                if (compArg1.getValue() instanceof Comparable && compArg2.getValue() instanceof Comparable) {
                    compResult = ((Comparable) compArg1.getValue()).compareTo(compArg2.getValue());
                } else {
                    throw new EnumerationFailedException("not Comparable");
                }
            } catch (NullPointerException e) {
                throw new IllegalArgumentException("ComparativeAnd args is null", e);
            }

            if (compResult == 0) {
                // 即 compArg1 == compArg2

                // 值相等，如果都含有=关系，那么还有个公共点，否则一个公共点都没有
                int comparasion1 = compArg1.getComparison();
                int comparasion2 = compArg2.getComparison();

                if (comparasion1 == comparasion2) {
                    // 出现重复的比较符,比如A=8 AND B=8
                    processForExtPartition(compArg1,
                        retValue,
                        cumulativeTimes,
                        atomIncrValue,
                        extKeyMappingRules,
                        needMergeValueInCloseInterval);
                } else if (comparasion1 == Comparative.GreaterThan && comparasion2 == Comparative.GreaterThanOrEqual) {
                    // 出现 A>8 AND B>=8
                    processForExtPartition(compArg2,
                        retValue,
                        cumulativeTimes,
                        atomIncrValue,
                        extKeyMappingRules,
                        needMergeValueInCloseInterval);
                } else if (comparasion1 == Comparative.GreaterThanOrEqual && comparasion2 == Comparative.GreaterThan) {
                    // 出现 A>=8 AND B>8
                    processForExtPartition(compArg1,
                        retValue,
                        cumulativeTimes,
                        atomIncrValue,
                        extKeyMappingRules,
                        needMergeValueInCloseInterval);
                } else if (comparasion1 == Comparative.LessThan && comparasion2 == Comparative.LessThanOrEqual) {
                    // 出现 A<8 AND B<=8
                    processForExtPartition(compArg2,
                        retValue,
                        cumulativeTimes,
                        atomIncrValue,
                        extKeyMappingRules,
                        needMergeValueInCloseInterval);
                } else if (comparasion1 == Comparative.LessThanOrEqual && comparasion2 == Comparative.LessThan) {
                    // 出现 A<=8 AND B<8
                    processForExtPartition(compArg1,
                        retValue,
                        cumulativeTimes,
                        atomIncrValue,
                        extKeyMappingRules,
                        needMergeValueInCloseInterval);
                } else if (containsEquvilentRelation(compArg1) && containsEquvilentRelation(compArg2)) {
                    // 走到这里一定是 A>=8 AND B<8 这一类情况
                    Comparative comparativeTmp = (Comparative) compArg1.clone();
                    comparativeTmp.setComparison(Comparative.Equivalent);
                    processForExtPartition(comparativeTmp,
                        retValue,
                        cumulativeTimes,
                        atomIncrValue,
                        extKeyMappingRules,
                        needMergeValueInCloseInterval);
                } else {
                    // 无交集, 比如 A>8 AND B<8 , 暂时不做处理
                }
            } else if (compResult < 0) {
                // 即 compArg1 < compArg2

                // arg1 < arg2
                processTwoDifferentArgsInComparativeAndForExtPartition(retValue,
                    compArg1,
                    compArg2,
                    cumulativeTimes,
                    atomIncrValue,
                    extKeyMappingRules,
                    needMergeValueInCloseInterval);

            } else {
                // 即 compArg1 > compArg2

                // compResult>0
                // arg1 > arg2
                processTwoDifferentArgsInComparativeAndForExtPartition(retValue,
                    compArg2,
                    compArg1,
                    cumulativeTimes,
                    atomIncrValue,
                    extKeyMappingRules,
                    needMergeValueInCloseInterval);

            }
        } else {
            throw new TddlRuleException("ComparativeAND only support two args");
        }
    }

    private void processComparativeOneForExtPartition(Comparative comparative, Set<Object> retValue,
                                                      Integer cumulativeTimes, Comparable<?> atomIncrValue,
                                                      Map<String, Set<MappingRule>> extKeyToMappingRules,
                                                      boolean needMergeValueInCloseInterval) {
        Comparative comp = (Comparative) comparative;
        int comparison = comp.getComparison();
        if (extKeyToMappingRules == null || extKeyToMappingRules.size() == 0) {
            return;
        }
        switch (comparison) {
        case 0:
            // 为0 的时候表示纯粹的包装对象。
            processForExtPartition(comp.getValue(),
                retValue,
                cumulativeTimes,
                atomIncrValue,
                extKeyToMappingRules,
                needMergeValueInCloseInterval);
            break;
        case Comparative.Equivalent:
            // 等于关系，直接放在collection
            for (Iterator<String> iterator = extKeyToMappingRules.keySet().iterator(); iterator.hasNext(); ) {
                String dbKey = iterator.next();
                if (comparative.compareToIgnoreType(dbKey) == 0) {
                    retValue.add(extKeyToMappingRules.get(dbKey));
                }
            }
            break;
        case Comparative.GreaterThan:
            for (Iterator<String> iterator = extKeyToMappingRules.keySet().iterator(); iterator.hasNext(); ) {
                String dbKey = iterator.next();
                if (comparative.compareToIgnoreType(dbKey) > 0) {
                    retValue.add(extKeyToMappingRules.get(dbKey));
                }
            }
            break;
        case Comparative.GreaterThanOrEqual:
            for (Iterator<String> iterator = extKeyToMappingRules.keySet().iterator(); iterator.hasNext(); ) {
                String dbKey = iterator.next();
                if (comparative.compareToIgnoreType(dbKey) >= 0) {
                    retValue.add(extKeyToMappingRules.get(dbKey));
                }
            }
            break;
        case Comparative.LessThan:
            for (Iterator<String> iterator = extKeyToMappingRules.keySet().iterator(); iterator.hasNext(); ) {
                String dbKey = iterator.next();
                if (comparative.compareToIgnoreType(dbKey) < 0) {
                    retValue.add(extKeyToMappingRules.get(dbKey));
                }
            }
            break;
        case Comparative.LessThanOrEqual:
            for (Iterator<String> iterator = extKeyToMappingRules.keySet().iterator(); iterator.hasNext(); ) {
                String dbKey = iterator.next();
                if (comparative.compareToIgnoreType(dbKey) <= 0) {
                    retValue.add(extKeyToMappingRules.get(dbKey));
                }
            }
            break;
        case Comparative.NotEquivalent:
            // 各种需要全取的情况
            throw new EnumerationInterruptException(comp);
        default:
            throw new NotSupportException();
        }
        return;
    }

    /**
     * 处理在一个and条件中的两个不同的argument
     */
    private void processTwoDifferentArgsInComparativeAndForExtPartition(Set<Object> retValue,
                                                                        Comparative from,
                                                                        Comparative to,
                                                                        Integer cumulativeTimes,
                                                                        Comparable<?> atomIncrValue,
                                                                        Map<String, Set<MappingRule>> extKeyToMappingRules,
                                                                        boolean needMergeValueInCloseInterval) {
        if (extKeyToMappingRules == null || extKeyToMappingRules.size() == 0) {
            // do nothing
        } else if (isCloseInterval(from, to)) { // 处理 1 < x < 3的情况
            Set<Object> fromSet = new HashSet<>();
            processComparativeOneForExtPartition(from,
                fromSet,
                cumulativeTimes,
                atomIncrValue,
                extKeyToMappingRules,
                needMergeValueInCloseInterval);
            Set<Object> toSet = new HashSet<>();
            processComparativeOneForExtPartition(from,
                toSet,
                cumulativeTimes,
                atomIncrValue,
                extKeyToMappingRules,
                needMergeValueInCloseInterval);
            final boolean b = fromSet.retainAll(toSet);
            retValue.addAll(fromSet);
        } else {
            // 处理 1 < x = 3 或者 1 = x <= 3
            Object temp = compareAndGetIntersactionOneValueForExtPartition(from,
                to,
                cumulativeTimes,
                atomIncrValue,
                extKeyToMappingRules,
                needMergeValueInCloseInterval);
            // 处理其他情况
            if (temp != null) {
                retValue.add(temp);
            } else {
                // 闭区间已经处理过，x >= ? and x = ? 或者 x <= ? and x = ?有交的也处理过，纯粹的> 和
                // <已经被转化为 >= 以及<=
                // 这里主要处理三类情况 x <= 3 and x>=5 这类，
                if (from.getComparison() == Comparative.LessThanOrEqual
                    || from.getComparison() == Comparative.LessThan) {
                    if (to.getComparison() == Comparative.LessThanOrEqual
                        || to.getComparison() == Comparative.LessThan) {
                        // 处理 x <= 3 and x <= 5
                        processAllPassableFields(from,
                            retValue,
                            cumulativeTimes,
                            atomIncrValue,
                            needMergeValueInCloseInterval);
                        retValue.addAll(extKeyToMappingRules.values());
                    } else {
                        // to为GreaterThanOrEqual,或者为Equals 那么是个开区间，无交集
                        // do nothing.
                    }
                } else if (to.getComparison() == Comparative.GreaterThanOrEqual
                    || to.getComparison() == Comparative.GreaterThan) {
                    if (from.getComparison() == Comparative.GreaterThanOrEqual
                        || from.getComparison() == Comparative.GreaterThan) {
                        // 处理 x >= 3 and x >= 5
                        processAllPassableFields(to,
                            retValue,
                            cumulativeTimes,
                            atomIncrValue,
                            needMergeValueInCloseInterval);
                        retValue.addAll(extKeyToMappingRules.values());
                    } else {
                        // from为LessThanOrEqual，或者为Equals,为开区间，无交集
                        // do nothing.
                    }
                }
            }
        }
    }

    /**
     * 处理一个and条件中 x > 1 and x = 3 类似这样的情况，因为前面已经对from 和 to 相等的情况作了处理
     * 因此这里只需要处理不等的情况中的上述问题。 同时也处理了x = 1 and x = 2这种情况。以及x = 1 and x>2 和x < 1
     * and x =2这种情况
     */
    protected Object compareAndGetIntersactionOneValueForExtPartition(Comparative from,
                                                                      Comparative to,
                                                                      Integer cumulativeTimes,
                                                                      Comparable<?> atomIncrValue,
                                                                      Map<String, Set<MappingRule>> extKeyToMappingRules,
                                                                      boolean needMergeValueInCloseInterval) {
        Set<Object> retValue = new HashSet<>();
        // x = from and x <= to
        if (from.getComparison() == Comparative.Equivalent) {
            if (to.getComparison() == Comparative.LessThan || to.getComparison() == Comparative.LessThanOrEqual) {
                processComparativeOneForExtPartition(from,
                    retValue,
                    cumulativeTimes,
                    atomIncrValue,
                    extKeyToMappingRules,
                    needMergeValueInCloseInterval);
            }
        }

        // x <= from and x = to
        if (to.getComparison() == Comparative.Equivalent) {
            if (from.getComparison() == Comparative.GreaterThan
                || from.getComparison() == Comparative.GreaterThanOrEqual) {
                processComparativeOneForExtPartition(to,
                    retValue,
                    cumulativeTimes,
                    atomIncrValue,
                    extKeyToMappingRules,
                    needMergeValueInCloseInterval);
            }
        }
        if (retValue.size() > 0) {
            return retValue.iterator().next();
        }
        return null;
    }

}
