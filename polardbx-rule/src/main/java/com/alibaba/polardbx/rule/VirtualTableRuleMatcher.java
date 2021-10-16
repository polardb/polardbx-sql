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

package com.alibaba.polardbx.rule;

import com.alibaba.polardbx.rule.enumerator.EnumerationFailedException;
import com.alibaba.polardbx.rule.enums.RuleType;
import com.alibaba.polardbx.rule.model.ShardExtContext;
import com.alibaba.polardbx.rule.model.ShardMatchedRuleType;
import com.alibaba.polardbx.rule.model.ShardPrepareResult;
import com.alibaba.polardbx.rule.model.TargetDB;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.model.sqljep.Comparative;
import com.alibaba.polardbx.common.model.sqljep.ComparativeMapChoicer;
import com.alibaba.polardbx.common.model.sqljep.ComparativeOR;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.rule.Rule.RuleColumn;
import com.alibaba.polardbx.rule.impl.VirtualNodeGroovyRule;
import com.alibaba.polardbx.rule.model.AdvancedParameter;
import com.alibaba.polardbx.rule.model.Field;
import com.alibaba.polardbx.rule.model.MatcherResult;
import com.alibaba.polardbx.rule.utils.CalcParamsAttribute;
import com.alibaba.polardbx.rule.utils.RuleUtils;
import com.alibaba.polardbx.rule.utils.sample.Samples;
import com.alibaba.polardbx.rule.utils.sample.SamplesCtx;
import org.apache.commons.lang.StringUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * <pre>
 * 1.在分库列和分表列没有交集的情况下，各自计算，结果做笛卡尔组合。
 *
 * 2.在分库列和分表列有交集，公共列的附加参数完全相同的情况下，公共列库表规则的描点总是相同的。
 *   表的描点要经过库规则计算后按库分类，在每个库内，只对产生该库的描点值进行表规则计算来确定表
 *   否则可能会产生不在本库的表。例如库%2表%8：
 *       id%2=0--db0：t0,t2,t4,t6
 *       id%2=1--db1: t1,t3,t5,t7
 *   表的描点0-7需要先经过库规则计算，按库结果分成两类db0:0,2,4,6和db1:1,3,5,7再分表进行表规则计算
 *
 * 3.在分库列和分表列 列名相同 类型不同 的情况下，各自描点取并集，再按2的方式计算。比如库1_month 表1_day，
 *   单月db0，双月db1；一天一张表。则time in（2月5日 3月8日 4月20日）正确结果应该是：
 *       db0: t_5,t_20
 *       db1: t_8
 *   上述例子是描点相同的情况。对于描点不同的情况例如 5月31日<=time<=6月2日。
 *   库的描点是5月31日，6月1日共2个；表是5月31日、6月1日、6月2日共3个，正确的结果应该是：
 *       db0: t_1,t_2
 *       db1: t_31
 *   上述列子是表的描点多于库，同时包含了库的描点。表的描点必须经过库规则分类再计算。
 *   对于表的描点不包含全部库描点的情况，例如：库描点a,b,c表描点b,c,d ....
 *
 * 4.不考虑库表规则的公共列（参数）在类型相同的情况下，迭代次数的不同。
 *   列名和类型相同，配不同的迭代次数是意义不大的，例如库%2表%8，计算库的时候2个描点，计算表需要8个描点，
 *   但是由于是同一个列，表的8个描点需要全部走一遍库规则，进行按库分离聚合再计算表，才能保证结果的正确：
 *   如果库的迭代次数大于表的，例如库id%8 表id%3, 统一按库的描点表规则会多计算几次
 *       DB0:t0,t1,t2; DB1:t0,t1,t2
 *   忽略这种情况，所以公共列（参数）在类型相同时，要求应用按最大迭代次数配成一致。
 * </pre>
 *
 * @author linxuan
 */
public class VirtualTableRuleMatcher {

    public MatcherResult match(ComparativeMapChoicer choicer, List<Object> args, VirtualTableRule<String, String> rule,
                               boolean forceAllowFullTableScan) {
        return match(choicer, args, rule, forceAllowFullTableScan, null);
    }

    public MatcherResult match(ComparativeMapChoicer choicer, List<Object> args, VirtualTableRule<String, String> rule,
                               boolean forceAllowFullTableScan, Map<String, Object> calcParams) {
        return match(null, choicer, args, rule, false, forceAllowFullTableScan, calcParams);
    }

    /**
     * <pre>
     * 基本思路：
     * 1. 根据参数，选择匹配的{@linkplain Rule}.
     *    a. 如果同时匹配多个，优先返回第一个
     *    b. 如果没有匹配，检查是否允许全表扫描
     * 2. 根据db和table rule的情况进行计算
     *    a. db rule不存在或者不匹配，单独计算table后 +　所有db
     *    b. tb rule不存在或者不匹配，单独计算db后　+ 所有table
     *    c. db/tb都存在
     *      i.　rule中不存在交集字段，单独计算db + 单独计算table.
     *          (遇上virtual node，根据虚拟table节点，转化为实际table表，再根据db和实际table的映射，提取实际db)
     *      ii. rule中存在交集字段
     *          1. 提取交集字段，将db+tb的枚举值采取merge，优先计算出db
     *          2. 遍历db的枚举结果，将db+tb的枚举值采取replace，即以db的枚举值为准，计算出tb
     * </pre>
     *
     * @param args sql的参数列表
     * @param rule sql中虚拟表名对应的规则
     */
    public MatcherResult match(String vtab, ComparativeMapChoicer choicer, List<Object> args,
                               VirtualTableRule<String, String> rule, boolean needSourceKey,
                               boolean forceAllowFullTableScan, Map<String, Object> calcParams) {
        ShardPrepareResult shardPrepareResult = null;
        try {
            // 所有库表规则中用到的列和对应的比较树,作为一个规则链匹配时绑定参数的缓存。
            // 一开始就把所有的取出来不够优化。可能有些是不需要取的。也就省了些绑定参数的操作
            Map<String, Comparative> allRuleArgs = new HashMap<String, Comparative>(2);
            Map<String, Comparative> dbRuleArgs = new HashMap<String, Comparative>(2); // 匹配的规则所对应的列名和比较树
            Map<String, Comparative> tbRuleArgs = new HashMap<String, Comparative>(2); // 匹配的规则所对应的列名和比较树

            // 竟然不需要defaultDbValue和defaultTbValue了
            Rule<String> dbRule = findMatchedRule(vtab,
                allRuleArgs,
                rule.getDbShardRules(),
                dbRuleArgs,
                choicer,
                args,
                rule,
                forceAllowFullTableScan);
            Rule<String> tbRule = findMatchedRule(vtab,
                allRuleArgs,
                rule.getTbShardRules(),
                tbRuleArgs,
                choicer,
                args,
                rule,
                forceAllowFullTableScan);

            final String comDbTb = CalcParamsAttribute.COM_DB_TB;
            Map<String, Map<String, Comparative>> o = null;
            if (calcParams != null) {
                o = (Map<String, Map<String, Comparative>>) calcParams.get(comDbTb);
            }
            Map<String, Comparative> allRuleColumnArgs1 = Maps.newHashMap();
            List objs1 = Lists.newArrayList();
            if (o != null) {
                for (int i = 0; i < rule.getShardColumns().size(); i++) {
                    Comparative comparative = getComparative(rule.getShardColumns().get(i),
                        allRuleColumnArgs1,
                        (ComparativeMapChoicer) calcParams.get(CalcParamsAttribute.SHARD_CHOISER),
                        objs1);
                    if (!o.containsKey(vtab)) {
                        o.put(vtab, new HashMap<>());
                    }
                    o.get(vtab).put(rule.getShardColumns().get(i), comparative);
                }
                String dbRuleColumn = null;
                String tbRuleColumn = null;
                if (dbRule != null) {
                    dbRuleColumn = dbRule.getRuleColumns().keySet().iterator().next();
                }
                if (tbRule != null) {
                    tbRuleColumn = tbRule.getRuleColumns().keySet().iterator().next();
                }

                if (rule instanceof TableRule) {
                    Map<RuleType, String> ruleTypeColumnMap = new HashMap<>();
                    ruleTypeColumnMap.put(RuleType.DB_RULE_TYPE, dbRuleColumn);
                    ruleTypeColumnMap.put(RuleType.TB_RULE_TYPE, tbRuleColumn);
                    final Comparative next;
                    final Map<String, Comparative> stringComparativeMap = o.get(vtab);
                    if (stringComparativeMap != null && o.get(vtab).values().iterator().hasNext()) {
                        next = o.get(vtab).values().iterator().next();
                    } else {
                        next = null;
                    }
                    // TB 此处需要提前准备比较的条件，不能放到 compare 的内部，
                    // 因为：
                    // 1. 只有提前计算才能进行后面的 o1 操作，否则只能遍历；
                    // 2. 提前计算好，避免扩展 compare 的实现
                    final ShardExtContext shardExtContext = new ShardExtContext(ruleTypeColumnMap,
                        null,
                        next,
                        (TableRule) rule);
                    shardPrepareResult = RuleUtils.prepareMatchedRule(shardExtContext);
                }
            } else {
                shardPrepareResult = RuleUtils.prepareMatchedRule(new ShardExtContext(Maps.newHashMap(),
                    null,
                    null,
                    null));
            }

            if (needSourceKey && (isComparativeNeedSourceKey(dbRuleArgs) || isComparativeNeedSourceKey(tbRuleArgs))) {
                // 暂时不支持虚拟节点
                return matchWithSourceKey(dbRule,
                    tbRule,
                    dbRuleArgs,
                    tbRuleArgs,
                    rule,
                    forceAllowFullTableScan,
                    calcParams,
                    shardPrepareResult);
            } else {
                return matchNoSourceKey(dbRule,
                    tbRule,
                    dbRuleArgs,
                    tbRuleArgs,
                    rule,
                    forceAllowFullTableScan,
                    calcParams,
                    shardPrepareResult);
            }
        } catch (EnumerationFailedException e) {
            // 尝试去掉参数做一次全表查询
            // if (needSourceKey) {
            // // 暂时不支持虚拟节点
            // return matchWithSourceKey(null, null, new HashMap(), new
            // HashMap(), rule, forceAllowFullTableScan);
            // } else {
            return matchNoSourceKey(null,
                null,
                new HashMap(),
                new HashMap(),
                rule,
                forceAllowFullTableScan,
                calcParams,
                shardPrepareResult);
            // }
        }
    }

    public MatcherResult match(String vtab, ComparativeMapChoicer choicer, List<Object> args,
                               VirtualTableRule<String, String> rule, boolean needSourceKey,
                               boolean forceAllowFullTableScan) {
        return match(vtab, choicer, args, rule, needSourceKey, forceAllowFullTableScan, null);
    }

    private boolean isComparativeNeedSourceKey(Map<String, Comparative> ruleArgs) {
        if (ruleArgs == null || ruleArgs.isEmpty()) {
            return false;
        }

        for (Comparative comp : ruleArgs.values()) {
            if (comp instanceof ComparativeOR) {
                // 如果出现OR规则，一定的可能性就是IN条件
                return true;
            }
        }

        return false;
    }

    private MatcherResult matchNoSourceKey(Rule<String> dbRule, Rule<String> tbRule,
                                           Map<String, Comparative> dbRuleArgs, Map<String, Comparative> tbRuleArgs,
                                           VirtualTableRule<String, String> rule, boolean forceAllowFullTableScan,
                                           Map<String, Object> calcParams, ShardPrepareResult shardPrepareResult) {
        // 所有库表规则中用到的列和对应的比较树,作为一个规则链匹配时绑定参数的缓存。
        // 一开始就把所有的取出来不够优化。可能有些是不需要取的。也就省了些绑定参数的操作
        Object outerCtx = rule.getOuterContext();

        Map<String, Set<String>> topology;
        Map<String, Set<String>> topologyOfRule = rule.getActualTopology(calcParams);

        if (dbRule == null && tbRule == null) {
            // 若无库规则，静态拓扑里面只有一个库，则无论没表规则也好，全表扫也好，结果都和静态拓扑一样
            // 若有库规则，那么是全库扫，则无论没表规则也好，全表扫也好，结果仍然和静态拓扑一样！！

            // topology = rule.getActualTopology(); // 不猜不知道，世界真奇妙

            topology = topologyOfRule;

        } else if (dbRule == null) {
            Set<String> tbValues = calculateNoTrace(tbRule,
                tbRuleArgs,
                outerCtx,
                calcParams,
                null,
                shardPrepareResult,
                RuleType.TB_RULE_TYPE);
            // 1.无库规则 2.有但是没匹配到 ；这时表规则一定不为空，且和库规则全无关联，自己算自己的
            // topology = new HashMap<String,
            // Set<String>>(rule.getActualTopology().size());

            topology = new HashMap<>(topologyOfRule.size());
            final Map<String, Set<String>> actualTopology = rule.getActualTopology();
            for (String dbValue : actualTopology.keySet()) {
                Set<String> newTbValues = new HashSet();
                for (String value : tbValues) {
                    if (actualTopology.get(dbValue).contains(value)) {
                        newTbValues.add(value);
                    }
                }
                if (newTbValues.size() > 0) {
                    topology.put(dbValue, newTbValues);
                }
            }
        } else if (tbRule == null) {
            // dbRule is VirtualNodeGroovyRule, just do the FullTableScan;
            if (dbRule instanceof VirtualNodeGroovyRule) {
                topology = rule.getActualTopology();
            } else {
                // 1.无表规则 2.有但是没匹配到 ；这时库规则一定不为空，且和表规则全无关联，自己算自己的
                Set<String> dbValues = calculateNoTrace(dbRule,
                    dbRuleArgs,
                    outerCtx,
                    calcParams,
                    null,
                    shardPrepareResult,
                    RuleType.DB_RULE_TYPE);
                topology = new HashMap<String, Set<String>>(dbValues.size());
                for (String dbValue : dbValues) {
                    Set<String> tables = topologyOfRule.get(dbValue);
                    if (tables != null && !tables.isEmpty()) {
                        // 分几种情况处理，分库分表热点映射，分库不分表热点映射，分库分表非热点映射，分库部分表非热点映射
                        if (shardPrepareResult != null && shardPrepareResult.getMatchedMappingRule() != null) {
                            if (shardPrepareResult.getMatchedDBType() == ShardMatchedRuleType.HOT_DB
                                && shardPrepareResult.getMatchedTBType() == ShardMatchedRuleType.HOT_TB
                                && StringUtils.isNotEmpty(shardPrepareResult.getMatchedMappingRule().getTb())) {
                                final MappingRule matchedMappingRule = shardPrepareResult.getMatchedMappingRule();
                                tables = new HashSet<>();
                                tables.add(matchedMappingRule.getTb());
                            } else if (shardPrepareResult.getMatchedDBType() == ShardMatchedRuleType.HOT_DB
                                && shardPrepareResult.getMatchedTBType() == ShardMatchedRuleType.TB) {
                                if (rule.getTbRulesStrs() == null || rule.getTbRulesStrs().length == 0) {
                                    if ((rule instanceof TableRule)
                                        && ((TableRule) rule).getVirtualTbName()
                                        .equalsIgnoreCase(rule.getTbNamePattern())) {
                                        String tableName = rule.getTbNamePattern();
                                        tables = new HashSet<>();
                                        tables.add(tableName);
                                    }
                                }

                            } else if (shardPrepareResult.getMatchedDBType() == ShardMatchedRuleType.DB
                                && shardPrepareResult.getMatchedTBType() == ShardMatchedRuleType.TB) {
                                if (rule.getTbRulesStrs() == null || rule.getTbRulesStrs().length == 0) {
                                    if ((rule instanceof TableRule)
                                        && ((TableRule) rule).getVirtualTbName()
                                        .equalsIgnoreCase(rule.getTbNamePattern())) {
                                        String tableName = rule.getTbNamePattern();
                                        tables = new HashSet<>();
                                        tables.add(tableName);
                                    }
                                }

                            }
                            topology.put(dbValue, tables);
                        } else {
                            topology.put(dbValue, tables);
                        }

                        topology.put(dbValue, tables);
                    } else {
                        String tableName = rule.getTbNamePattern();
                        Set<String> tbValues = new HashSet<String>();
                        tbValues.add(tableName);
                        topology.put(dbValue, tbValues);
                    }
                }
            }
        } else {
            // 看表规则和库规则的列有无交集
            Set<String> commonSet = getCommonColumnSet(dbRule, tbRule);
            String[] commonColumn = commonSet == null ? null : commonSet.toArray(new String[commonSet.size()]);
            if (commonColumn == null || commonColumn.length == 0) {
                // 无交集
                // modify by junyu,2011-9-23,增加虚拟节点
                Set<String> tbValues = calculateNoTrace(tbRule,
                    tbRuleArgs,
                    outerCtx,
                    calcParams,
                    null,
                    shardPrepareResult,
                    RuleType.TB_RULE_TYPE);
                Set<String> dbValues = null;
                if (dbRule instanceof VirtualNodeGroovyRule) {// add by xiaoying
                    // 此处新版规则不会走到，可以忽略
                    // 说明dbRule是虚拟映射
                    topology = new HashMap<String, Set<String>>();
                    for (String tab : tbValues) {
                        String db = dbRule.calculateVnodeNoTrace(tab, null, outerCtx, calcParams);
                        if (!topology.containsKey(db)) {
                            Set<String> tbSet = new HashSet<String>();
                            tbSet.add(tab);
                            topology.put(db, tbSet);
                        } else {
                            topology.get(db).add(tab);
                        }
                    }
                } else {
                    Set<String> calculate = calculateNoTrace(dbRule,
                        dbRuleArgs,
                        outerCtx,
                        calcParams,
                        null,
                        shardPrepareResult,
                        RuleType.DB_RULE_TYPE);
                    dbValues = calculate;
                    final Map<String, Set<String>> actualTopology = rule.getActualTopology();
                    topology = new HashMap<String, Set<String>>(dbValues.size());
                    for (String dbValue : dbValues) {
                        Set<String> newTbValues = new HashSet();
                        for (String value : tbValues) {
                            if (actualTopology.get(dbValue).contains(value)) {
                                newTbValues.add(value);
                            }
                        }
                        if (newTbValues.size() > 0) {
                            topology.put(dbValue, newTbValues);
                        }
                    }

                }
            } else {
                topology = crossNoSourceKey1(dbRule,
                    dbRuleArgs,
                    tbRule,
                    tbRuleArgs,
                    commonColumn,
                    outerCtx,
                    calcParams,
                    shardPrepareResult);
            }
        }

        return new MatcherResult(buildTargetDbList(topology, topologyOfRule), dbRuleArgs, tbRuleArgs);
    }

    /**
     * @param dbRule 分库规则
     * @param tbRule 分表规则
     * @param dbRuleArgs 分库规则的涉及到的条件（即包含拆分键的条件）
     * @param tbRuleArgs 分表规则的涉及到的条件（即包含拆分键的条件）
     */
    private MatcherResult matchWithSourceKey(Rule<String> dbRule, Rule<String> tbRule,
                                             Map<String, Comparative> dbRuleArgs, Map<String, Comparative> tbRuleArgs,
                                             VirtualTableRule<String, String> rule, boolean forceAllowFullTableScan,
                                             Map<String, Object> calcParams, ShardPrepareResult shardPrepareResult) {

        Object outerCtx = rule.getOuterContext();

        Map<String, Set<String>> topologyOfRule = rule.getActualTopology(calcParams);
        Map<String, Map<String, Field>> topology;
        if (dbRule == null && tbRule == null) {
            // 若无库规则，静态拓扑里面只有一个库，则无论没表规则也好，全表扫也好，结果都和静态拓扑一样
            // 若有库规则，那么是全库扫，则无论没表规则也好，全表扫也好，结果仍然和静态拓扑一样！！
            Map<String, Set<String>> actualTopology = topologyOfRule;
            topology = new HashMap<String, Map<String, Field>>(actualTopology.size());
            for (Map.Entry<String, Set<String>> e : topologyOfRule.entrySet()) {
                topology.put(e.getKey(), toMapField(e.getValue()));
            }
        } else if (dbRule == null) {
            // 1.无库规则 2.有但是没匹配到 ；这时表规则一定不为空，且和库规则全无关联，自己算自己的
            Map<String, Samples> tbValues = RuleUtils.cast(calculate(tbRule,
                tbRuleArgs,
                outerCtx,
                calcParams,
                null,
                shardPrepareResult,
                RuleType.TB_RULE_TYPE));
            topology = new HashMap<String, Map<String, Field>>(topologyOfRule.size());
            for (String dbValue : topologyOfRule.keySet()) {
                topology.put(dbValue, toMapField(tbValues));
            }
        } else if (tbRule == null) {
            // if dbRule is VirtualNodeGroovyRule,just do the fullTableScan
            if (dbRule instanceof VirtualNodeGroovyRule) {
                topology = new HashMap<String, Map<String, Field>>(rule.getActualTopology().size());
                for (Map.Entry<String, Set<String>> e : rule.getActualTopology().entrySet()) {
                    topology.put(e.getKey(), toMapField(e.getValue()));
                }
            } else {
                // 1.无表规则 2.有但是没匹配到 ；这时库规则一定不为空，且和表规则全无关联，自己算自己的
                // bug fix by leiwen.zh: needIdInGroup不支持只分库的场景
                Map<String, Samples> dbValues = RuleUtils.cast(calculate(dbRule,
                    dbRuleArgs,
                    outerCtx,
                    calcParams,
                    null,
                    shardPrepareResult,
                    RuleType.DB_RULE_TYPE));
                topology = new HashMap<String, Map<String, Field>>(dbValues.size());
                for (String dbValue : dbValues.keySet()) {
                    // 不分表,pattern即为实际表名
                    // String tableName = rule.getTbNamePattern();
                    Set<String> tables = topologyOfRule.get(dbValue);
                    // 不分表,Sample与分库保持一致即可
                    Samples samples = dbValues.get(dbValue);
                    if (tables != null && !tables.isEmpty()) {
                        Map<String, Samples> tbValues = new HashMap<String, Samples>();
                        for (String tableName : tables) {
                            tbValues.put(tableName, samples);
                        }
                        topology.put(dbValue, toMapField(tbValues));
                    } else {
                        String tableName = rule.getTbNamePattern();
                        Map<String, Samples> tbValues = new HashMap<String, Samples>();
                        tbValues.put(tableName, samples);
                        topology.put(dbValue, toMapField(tbValues));
                    }
                }
            }
        } else {
            // 看表规则和库规则的列有无交集
            Set<String> commonSet = getCommonColumnSet(dbRule, tbRule);
            String[] commonColumn = commonSet == null ? null : commonSet.toArray(new String[commonSet.size()]);
            if (commonColumn == null || commonColumn.length == 0) {
                // 无交集
                // modify by junyu,2011-10-24,原本没有加这个，需要测试下id in group 优化的部分
                if (dbRule instanceof VirtualNodeGroovyRule) {
                    Map<String, Samples> tbValues = RuleUtils.cast(tbRule.calculate(tbRuleArgs,
                        null,
                        outerCtx,
                        calcParams));
                    // 说明dbRule是虚拟映射
                    topology = new HashMap<String, Map<String, Field>>();
                    Map<String, Map<String, Samples>> templogy = new HashMap<String, Map<String, Samples>>();
                    for (Map.Entry<String, Samples> entry : tbValues.entrySet()) {
                        String db = dbRule.calculateVnodeNoTrace(entry.getKey(), null, outerCtx, calcParams);
                        if (!topology.containsKey(db)) {
                            Map<String, Samples> tbSet = new HashMap<String, Samples>();
                            tbSet.put(entry.getKey(), entry.getValue());
                            templogy.put(db, tbSet);
                        } else {
                            templogy.get(db).put(entry.getKey(), entry.getValue());
                        }
                    }

                    for (Map.Entry<String, Map<String, Samples>> entry : templogy.entrySet()) {
                        topology.put(entry.getKey(), toMapField(entry.getValue()));
                    }
                } else {
                    Set<String> dbValues = calculateNoTrace(dbRule,
                        dbRuleArgs,
                        outerCtx,
                        calcParams,
                        null,
                        shardPrepareResult,
                        RuleType.DB_RULE_TYPE);
                    Map<String, Samples> tbValues = RuleUtils.cast(calculate(tbRule,
                        tbRuleArgs,
                        outerCtx,
                        calcParams,
                        null,
                        shardPrepareResult,
                        RuleType.TB_RULE_TYPE));
                    topology = new HashMap<String, Map<String, Field>>(dbValues.size());
                    final Map<String, Set<String>> actualTopology = rule.getActualTopology();
                    for (String dbValue : dbValues) {
                        Map<String, Samples> newTbValues = new HashMap<>();
                        for (String value : tbValues.keySet()) {
                            if (actualTopology.get(dbValue).contains(value)) {
                                newTbValues.put(value, tbValues.get(value));
                            }

                        }
                        if (newTbValues.size() > 0) {
                            topology.put(dbValue, toMapField(newTbValues));
                        }
                    }

                }
            } else { // 有交集
                topology = crossWithSourceKey1(dbRule,
                    dbRuleArgs,
                    tbRule,
                    tbRuleArgs,
                    commonColumn,
                    outerCtx,
                    calcParams,
                    shardPrepareResult);
            }
        }

        return new MatcherResult(buildTargetDbListWithSourceKey(topology, topologyOfRule), dbRuleArgs, tbRuleArgs);
    }

    private Map<String, Set<String>> crossNoSourceKey1(Rule<String> matchedDbRule,
                                                       Map<String, Comparative> matchedDbRuleArgs,
                                                       Rule<String> matchedTbRule,
                                                       Map<String, Comparative> matchedTbRuleArgs,
                                                       String[] commonColumn, Object outerCtx,
                                                       Map<String, Object> calcParams,
                                                       ShardPrepareResult shardPrepareResult) {
        SamplesCtx dbRuleCtx = null;
        // 对于表规则中与库规则列名相同而自增类型不同的列，将其表枚举结果加入库规则的枚举集
        Set<AdvancedParameter> mergeInCommon = diffTypeOrOptionalInCommon(matchedDbRule,
            matchedTbRule,
            commonColumn,
            matchedDbRuleArgs,
            matchedTbRuleArgs);
        if (mergeInCommon != null && !mergeInCommon.isEmpty()) {
            // 公共列包含有枚举类型不同的列，例如库是1_month，表示1_day
            Map<String, Set<Object>> tbTypes = RuleUtils.getSamplingField(matchedTbRuleArgs, mergeInCommon);
            dbRuleCtx = new SamplesCtx(new Samples(tbTypes), SamplesCtx.merge);
        }
        Map<String, ?> calculate = null;
        final RuleType dbRuleType = RuleType.DB_RULE_TYPE;
        calculate = calculate(matchedDbRule,
            matchedDbRuleArgs,
            outerCtx,
            calcParams,
            dbRuleCtx,
            shardPrepareResult,
            dbRuleType);
        Map<String, Samples> dbValues = RuleUtils.cast(calculate);
        Map<String, Set<String>> topology = new HashMap<String, Set<String>>(dbValues.size());
        for (Map.Entry<String, Samples> e : dbValues.entrySet()) {
            SamplesCtx tbRuleCtx = new SamplesCtx(e.getValue().subSamples(commonColumn), SamplesCtx.replace);
            Set<String> tbValues = null;
            final RuleType tbRuleType = RuleType.TB_RULE_TYPE;
            tbValues = calculateNoTrace(matchedTbRule,
                matchedTbRuleArgs,
                outerCtx,
                tbRuleCtx,
                shardPrepareResult,
                tbRuleType);
            topology.put(e.getKey(), tbValues);
        }
        return topology;
    }

    // TODO xiaoying
    private Set<String> calculateNoTrace(Rule<String> matchedDbRule, Map<String, Comparative> matchedDbRuleArgs,
                                         Object outerCtx, Map<String, Object> calcParams, SamplesCtx dbRuleCtx,
                                         ShardPrepareResult shardPrepareResult, RuleType dbRuleType) {
        Set<String> calculate;
        if (matchedDbRule instanceof ExtRule) {
            calculate = ((ExtRule) matchedDbRule).calculateNoTrace(matchedDbRuleArgs,
                dbRuleCtx,
                outerCtx,
                calcParams,
                shardPrepareResult,
                dbRuleType);
        } else {
            calculate = matchedDbRule.calculateNoTrace(matchedDbRuleArgs, dbRuleCtx, outerCtx, calcParams);
        }
        return calculate;
    }

    private Set<String> calculateNoTrace(Rule<String> matchedTbRule, Map<String, Comparative> matchedTbRuleArgs,
                                         Object outerCtx, SamplesCtx tbRuleCtx, ShardPrepareResult shardPrepareResult,
                                         RuleType tbRuleType) {
        Set<String> tbValues;
        if (matchedTbRule instanceof ExtRule) {
            tbValues = ((ExtRule) matchedTbRule).calculateNoTrace(matchedTbRuleArgs,
                tbRuleCtx,
                outerCtx,
                shardPrepareResult,
                tbRuleType);
        } else {
            tbValues = matchedTbRule.calculateNoTrace(matchedTbRuleArgs, tbRuleCtx, outerCtx);
        }
        return tbValues;
    }

    private Map<String, ?> calculate(Rule<String> matchedDbRule, Map<String, Comparative> matchedDbRuleArgs,
                                     Object outerCtx, Map<String, Object> calcParams, SamplesCtx dbRuleCtx,
                                     ShardPrepareResult shardPrepareResult, RuleType dbRuleType) {
        Map<String, ?> calculate;
        if (matchedDbRule instanceof ExtRule) {
            calculate = ((ExtRule) matchedDbRule).calculate(matchedDbRuleArgs,
                dbRuleCtx,
                outerCtx,
                calcParams,
                shardPrepareResult,
                dbRuleType);
        } else {
            calculate = matchedDbRule.calculate(matchedDbRuleArgs, dbRuleCtx, outerCtx, calcParams);
        }
        return calculate;
    }

    private Map<String, Map<String, Field>> crossWithSourceKey1(Rule<String> matchedDbRule,
                                                                Map<String, Comparative> matchedDbRuleArgs,
                                                                Rule<String> matchedTbRule,
                                                                Map<String, Comparative> matchedTbRuleArgs,
                                                                String[] commonColumn, Object outerCtx,
                                                                Map<String, Object> calcParams) {
        return crossWithSourceKey1(matchedDbRule,
            matchedDbRuleArgs,
            matchedTbRule,
            matchedTbRuleArgs,
            commonColumn,
            outerCtx,
            calcParams,
            null);
    }

    private Map<String, Map<String, Field>> crossWithSourceKey1(Rule<String> matchedDbRule,
                                                                Map<String, Comparative> matchedDbRuleArgs,
                                                                Rule<String> matchedTbRule,
                                                                Map<String, Comparative> matchedTbRuleArgs,
                                                                String[] commonColumn, Object outerCtx,
                                                                Map<String, Object> calcParams,
                                                                ShardPrepareResult shardPrepareResult) {
        SamplesCtx dbRuleCtx = null; // 对于表规则中与库规则列名相同而自增类型不同的列，将其表枚举结果加入库规则的枚举集
        Set<AdvancedParameter> mergeInCommon = diffTypeOrOptionalInCommon(matchedDbRule,
            matchedTbRule,
            commonColumn,
            matchedDbRuleArgs,
            matchedTbRuleArgs);
        if (mergeInCommon != null && !mergeInCommon.isEmpty()) {
            // 公共列包含有枚举类型不同的列，例如库是1_month，表示1_day
            Map<String, Set<Object>> tbTypes = RuleUtils.getSamplingField(matchedTbRuleArgs, mergeInCommon);
            dbRuleCtx = new SamplesCtx(new Samples(tbTypes), SamplesCtx.merge);
        }
        if (calcParams == null) {
            calcParams = new HashMap<>();
        }
        Map<String, ?> calculate = null;
        final RuleType dbRuleType = RuleType.DB_RULE_TYPE;
        calculate = calculate(matchedDbRule,
            matchedDbRuleArgs,
            outerCtx,
            calcParams,
            dbRuleCtx,
            shardPrepareResult,
            dbRuleType);
        Map<String, Samples> dbValues = RuleUtils.cast(calculate);
        Map<String, Map<String, Field>> topology = new HashMap<String, Map<String, Field>>(dbValues.size());
        for (Map.Entry<String, Samples> e : dbValues.entrySet()) {
            SamplesCtx tbRuleCtx = new SamplesCtx(e.getValue().subSamples(commonColumn), SamplesCtx.replace);

            Map<String, Samples> tbValues = null;
            final RuleType tbRuleType = RuleType.TB_RULE_TYPE;
            Map<String, ?> calculateTb = calculate(matchedTbRule,
                matchedTbRuleArgs,
                outerCtx,
                calcParams,
                tbRuleCtx,
                shardPrepareResult,
                tbRuleType);
            tbValues = RuleUtils.cast(calculateTb);
            topology.put(e.getKey(), toMapField(tbValues));
        }
        return topology;
    }

    private Map<String, Set<String>> crossNoSourceKey2(Rule<String> matchedDbRule,
                                                       Map<String, Comparative> matchedDbRuleArgs,
                                                       Rule<String> matchedTbRule,
                                                       Map<String, Comparative> matchedTbRuleArgs,
                                                       Set<String> commonSet, Object outerCtx,
                                                       Map<String, Object> calcParams) {
        return crossNoSourceKey2(matchedDbRule,
            matchedDbRuleArgs,
            matchedTbRule,
            matchedTbRuleArgs,
            commonSet,
            outerCtx,
            null,
            null);
    }

    private Map<String, Set<String>> crossNoSourceKey2(Rule<String> matchedDbRule,
                                                       Map<String, Comparative> matchedDbRuleArgs,
                                                       Rule<String> matchedTbRule,
                                                       Map<String, Comparative> matchedTbRuleArgs,
                                                       Set<String> commonSet, Object outerCtx,
                                                       Map<String, Object> calcParams,
                                                       ShardPrepareResult shardPrepareResult) {
        // 有交集
        String[] commonColumn = commonSet == null ? null : commonSet.toArray(new String[commonSet.size()]);
        Set<AdvancedParameter> dbParams = RuleUtils.cast(matchedDbRule.getRuleColumnSet());
        Set<AdvancedParameter> tbParams = RuleUtils.cast(matchedTbRule.getRuleColumnSet());
        Map<String, Set<Object>> dbEnumerates = RuleUtils.getSamplingField(matchedDbRuleArgs, dbParams);
        Set<AdvancedParameter> mergeInCommon = diffTypeOrOptionalInCommon(matchedDbRule,
            matchedTbRule,
            commonColumn,
            matchedDbRuleArgs,
            matchedTbRuleArgs);
        if (mergeInCommon != null && !mergeInCommon.isEmpty()) {
            // 将自增类型不同的公共列的表枚举值加入库枚举值中
            Map<String, Set<Object>> diifTypeTbEnumerates =
                RuleUtils.getSamplingField(matchedTbRuleArgs, mergeInCommon);
            for (Map.Entry<String, Set<Object>> e : diifTypeTbEnumerates.entrySet()) {
                dbEnumerates.get(e.getKey()).addAll(e.getValue());
            }
        }
        Set<AdvancedParameter> tbOnly = new HashSet<AdvancedParameter>();
        for (AdvancedParameter param : tbParams) {
            if (!commonSet.contains(param.key)) {
                tbOnly.add(param);
            }
        }

        Map<String, Set<String>> topology = new HashMap<String, Set<String>>();
        if (tbOnly.isEmpty()) {
            // 分库列完全包含了分表列
            for (Map<String, Object> dbSample : new Samples(dbEnumerates)) { // 遍历笛卡尔抽样
                String dbIndex = matchedDbRule.eval(dbSample, outerCtx, calcParams);
                String tbName = matchedTbRule.eval(dbSample, outerCtx);
                addToTopology(dbIndex, tbName, topology);
            }
        } else {
            Map<String, Set<Object>> tbEnumerates = RuleUtils.getSamplingField(matchedTbRuleArgs, tbOnly);// 只有表的枚举
            Samples tbSamples = new Samples(tbEnumerates);
            for (Map<String, Object> dbSample : new Samples(dbEnumerates)) { // 遍历库笛卡尔抽样
                String dbIndex = matchedDbRule.eval(dbSample, outerCtx, calcParams);
                for (Map<String, Object> tbSample : tbSamples) { // 遍历表中单独列的笛卡尔抽样
                    // dbSample.putAll(tbSample);
                    // String tbName = matchedTbRule.eval(dbSample, outerCtx);
                    // modify by jianghang at 2013-11-18，应该是以dbSample为主构造枚举值才对
                    tbSample.putAll(dbSample);
                    String tbName = matchedTbRule.eval(tbSample, outerCtx);
                    addToTopology(dbIndex, tbName, topology);
                }
            }
        }
        return topology;
    }

    private Map<String, Map<String, Field>> crossWithSourceKey2(Rule<String> matchedDbRule,
                                                                Map<String, Comparative> matchedDbRuleArgs,
                                                                Rule<String> matchedTbRule,
                                                                Map<String, Comparative> matchedTbRuleArgs,
                                                                Set<String> commonSet, Object outerCtx,
                                                                Map<String, Object> calcParams,
                                                                ShardPrepareResult shardPrepareResult) {
        // 有交集
        String[] commonColumn = commonSet == null ? null : commonSet.toArray(new String[commonSet.size()]);
        Set<AdvancedParameter> dbParams = RuleUtils.cast(matchedDbRule.getRuleColumnSet());
        Set<AdvancedParameter> tbParams = RuleUtils.cast(matchedTbRule.getRuleColumnSet());
        Map<String, Set<Object>> dbEnumerates = RuleUtils.getSamplingField(matchedDbRuleArgs, dbParams);
        Set<AdvancedParameter> mergeInCommon = diffTypeOrOptionalInCommon(matchedDbRule,
            matchedTbRule,
            commonColumn,
            matchedDbRuleArgs,
            matchedTbRuleArgs);
        if (mergeInCommon != null && !mergeInCommon.isEmpty()) {
            // 将自增类型不同的公共列的表枚举值加入库枚举值中
            Map<String, Set<Object>> diifTypeTbEnumerates =
                RuleUtils.getSamplingField(matchedTbRuleArgs, mergeInCommon);
            for (Map.Entry<String, Set<Object>> e : diifTypeTbEnumerates.entrySet()) {
                dbEnumerates.get(e.getKey()).addAll(e.getValue());
            }
        }
        Set<AdvancedParameter> tbOnly = new HashSet<AdvancedParameter>();
        for (AdvancedParameter param : tbParams) {
            if (!commonSet.contains(param.key)) {
                tbOnly.add(param);
            }
        }

        Map<String, Map<String, Field>> topology = new HashMap<String, Map<String, Field>>();
        if (tbOnly.isEmpty()) {
            // 分库列完全包含了分表列
            for (Map<String, Object> dbSample : new Samples(dbEnumerates)) { // 遍历笛卡尔抽样
                String dbIndex = matchedDbRule.eval(dbSample, outerCtx, calcParams);
                String tbName = matchedTbRule.eval(dbSample, outerCtx);
                addToTopologyWithSource(dbIndex, tbName, topology, dbSample, tbParams);
            }
        } else {
            Map<String, Set<Object>> tbEnumerates = RuleUtils.getSamplingField(matchedTbRuleArgs, tbOnly);// 只有表的枚举
            Samples tbSamples = new Samples(tbEnumerates);
            for (Map<String, Object> dbSample : new Samples(dbEnumerates)) { // 遍历库笛卡尔抽样
                String dbIndex = matchedDbRule.eval(dbSample, outerCtx, calcParams);
                for (Map<String, Object> tbSample : tbSamples) { // 遍历表中单独列的笛卡尔抽样
                    // dbSample.putAll(tbSample);
                    // String tbName = matchedTbRule.eval(dbSample, outerCtx);
                    // modify by jianghang at 2013-11-18，应该是以dbSample为主构造枚举值才对
                    tbSample.putAll(dbSample);
                    String tbName = matchedTbRule.eval(tbSample, outerCtx);
                    addToTopologyWithSource(dbIndex, tbName, topology, dbSample, tbParams);
                }
            }
        }
        return topology;
    }

    private static void addToTopology(String dbIndex, String tbName, Map<String, Set<String>> topology) {
        Set<String> tbNames = topology.get(dbIndex);
        if (tbNames == null) {
            tbNames = new HashSet<String>();
            topology.put(dbIndex, tbNames);
        }
        tbNames.add(tbName);
    }

    private static void addToTopologyWithSource(String dbIndex, String tbName,
                                                Map<String, Map<String, Field>> topology, Map<String, Object> tbSample,
                                                Set<AdvancedParameter> tbParams) {
        Map<String, Field> tbNames = topology.get(dbIndex);
        if (tbNames == null) {
            tbNames = new HashMap<String, Field>();
            topology.put(dbIndex, tbNames);
        }
        Field f = tbNames.get(tbName);
        if (f == null) {
            f = new Field(tbParams.size());
            tbNames.put(tbName, f);
        }
        for (AdvancedParameter ap : tbParams) {
            Set<Object> set = f.getSourceKeys().get(ap.key);
            if (set == null) {
                set = new HashSet<Object>();
            }
            set.add(tbSample.get(ap.key));
        }
    }

    private Map<String, Field> toMapField(Map<String/* rule计算结果 */, Samples/* 得到该结果的样本 */> values) {
        Map<String, Field> res = new HashMap<String, Field>(values.size());
        for (Map.Entry<String, Samples> e : values.entrySet()) {
            Field f = new Field(e.getValue().size());
            f.setSourceKeys(e.getValue().getColumnEnumerates());
            res.put(e.getKey(), f);
        }
        return res;
    }

    private Map<String, Field> toMapField(Set<String> values) {
        Map<String, Field> res = new HashMap<String, Field>(values.size());
        for (String valule : values) {
            res.put(valule, null);
        }
        return res;
    }

    private List<TargetDB> buildTargetDbList(Map<String, Set<String>> topology,
                                             Map<String, Set<String>> originTopology) {
        List<TargetDB> targetDbList = new ArrayList<TargetDB>(topology.size());

        for (Map.Entry<String, Set<String>> e : topology.entrySet()) {
            TargetDB db = new TargetDB();
            Map<String, Field> tableNames = new HashMap<String, Field>(e.getValue().size());
            for (String tbName : e.getValue()) {
                if (originTopology.get(e.getKey()).contains(tbName)) {
                    tableNames.put(tbName, null);
                }
            }
            db.setDbIndex(e.getKey());
            db.setTableNames(tableNames);
            targetDbList.add(db);
        }
        return targetDbList;
    }

    private List<TargetDB> buildTargetDbListWithSourceKey(Map<String, Map<String, Field>> topology,
                                                          Map<String, Set<String>> originTopology) {
        List<TargetDB> targetDbList = new ArrayList<TargetDB>(topology.size());
        for (Map.Entry<String, Map<String, Field>> e : topology.entrySet()) {
            TargetDB db = new TargetDB();
            db.setDbIndex(e.getKey());
            Map<String, Field> tableNames = new HashMap<String, Field>(e.getValue().size());
            final Map<String, Field> value = e.getValue();
            for (String tbName : e.getValue().keySet()) {
                if (originTopology.get(e.getKey()).contains(tbName)) {
                    tableNames.put(tbName, e.getValue().get(tbName));
                }
            }
            db.setTableNames(tableNames);
            targetDbList.add(db);
        }
        return targetDbList;
    }

    private static <T> Rule<T> findMatchedRule(String vtab, Map<String, Comparative> allRuleColumnArgs,
                                               List<Rule<T>> shardRules, Map<String, Comparative> matchArgs,
                                               ComparativeMapChoicer choicer, List<Object> args,
                                               VirtualTableRule<String, String> rule, boolean forceAllowFullTableScan) {
        Rule<T> matchedRule = null;
        if (shardRules != null && shardRules.size() != 0) {
            matchedRule = findMatchedRule(allRuleColumnArgs, shardRules, matchArgs, choicer, args);
            if (matchedRule == null) {
                // 有分库或分表规则，但是没有匹配到，是否执行全部扫描
                if (!(rule.isAllowFullTableScan() || forceAllowFullTableScan)) {
                    List<String> shardColumns = new ArrayList<String>(shardRules.size());
                    for (Rule<T> r : shardRules) {
                        Set<String> columnSet = new HashSet<String>();
                        for (RuleColumn rc : r.getRuleColumnSet()) {
                            columnSet.add(rc.key);
                        }
                        shardColumns.add(TStringUtil.join(columnSet, ","));
                    }
                    throw new TddlRuntimeException(ErrorCode.ERR_CONTAINS_NO_SHARDING_KEY,
                        vtab,
                        TStringUtil.join(shardColumns, ";"));
                }
            }
        }
        return matchedRule;
    }

    /**
     * @return 返回两个规则的公共列
     */
    private static Set<String> getCommonColumnSet(Rule<String> matchedDbRule, Rule<String> matchedTbRule) {
        Set<String> res = null;
        for (String key : matchedDbRule.getRuleColumns().keySet()) {
            if (matchedTbRule.getRuleColumns().containsKey(key)) {
                if (res == null) {
                    res = new HashSet<String>(1);
                }
                res.add(key);
            }
        }
        return res;
    }

    /**
     * <pre>
     * 1.  dbRule中和tbRule中存在相同的列，并且当前处于optional状态
     * 2.  tbRule中和dbRule列名相同而自增类型不用的AdvancedParameter对象
     * </pre>
     */
    private static Set<AdvancedParameter> diffTypeOrOptionalInCommon(Rule<String> dbRule, Rule<String> tbRule,
                                                                     String[] commonColumn,
                                                                     Map<String, Comparative> matchedDbRuleArgs,
                                                                     Map<String, Comparative> matchedTbRuleArgs) {
        Set<AdvancedParameter> mergeInCommon = null;
        for (String common : commonColumn) {
            AdvancedParameter dbap = (AdvancedParameter) dbRule.getRuleColumns().get(common);
            AdvancedParameter tbap = (AdvancedParameter) tbRule.getRuleColumns().get(common);
            boolean isOptional = matchedDbRuleArgs.containsKey(common) == false
                && matchedTbRuleArgs.containsKey(common) == false;
            if (dbap.atomicIncreateType != tbap.atomicIncreateType || isOptional
                || dbap.cumulativeTimes != tbap.cumulativeTimes) {
                if (mergeInCommon == null) {
                    mergeInCommon = new HashSet<AdvancedParameter>(0);
                }
                mergeInCommon.add(tbap);
            }
        }
        return mergeInCommon;
    }

    /**
     * <pre>
     * 规则一：#a# #b?#
     * 规则二：#a# #c?#
     * 规则三：#b?# #d?#
     *
     * 参数为(a，c)，则选规则二;
     * 参数为(a，d)则选规则一;
     * 参数为(b) 则选规则三
     * </pre>
     */
    private static <T> Rule<T> findMatchedRule(Map<String, Comparative> allRuleColumnArgs, List<Rule<T>> rules,
                                               Map<String, Comparative> matchArgs, ComparativeMapChoicer choicer,
                                               List<Object> args) {
        // 优先匹配必选列
        for (int i = 0; i < rules.size(); i++) {
            Rule<T> r = rules.get(i);
            matchArgs.clear();
            for (RuleColumn ruleColumn : r.getRuleColumns().values()) {
                Comparative comparative =
                    getComparative(ruleColumn.key.toLowerCase(), allRuleColumnArgs, choicer, args);
                if (comparative == null) {
                    break;
                }
                matchArgs.put(ruleColumn.key, comparative);
            }
            if (matchArgs.size() == r.getRuleColumns().size()) {
                return r; // 完全匹配
            }
        }

        // 匹配必选列 + 可选列
        for (int i = 0; i < rules.size(); i++) {
            Rule<T> r = rules.get(i);
            matchArgs.clear();
            int mandatoryColumnCount = 0;
            for (RuleColumn ruleColumn : r.getRuleColumns().values()) {
                if (ruleColumn.optional) {
                    continue;
                }

                mandatoryColumnCount++;
                Comparative comparative = getComparative(ruleColumn.key, allRuleColumnArgs, choicer, args);
                if (comparative == null) {
                    break;
                }
                matchArgs.put(ruleColumn.key, comparative);
            }

            if (mandatoryColumnCount != 0 && matchArgs.size() == mandatoryColumnCount) {
                return r; // 必选列匹配
            }
        }

        // 针对没有必选列的规则如：rule=..#a?#..#b?#.. 并且只有a或者b列在sql中有
        for (int i = 0; i < rules.size(); i++) {
            Rule<T> r = rules.get(i);
            matchArgs.clear();
            for (RuleColumn ruleColumn : r.getRuleColumns().values()) {
                if (!ruleColumn.optional) {
                    break; // 如果当前规则有必选项，直接跳过,因为走到这里必选列已经不匹配了
                }

                Comparative comparative = getComparative(ruleColumn.key, allRuleColumnArgs, choicer, args);
                if (comparative != null) {
                    matchArgs.put(ruleColumn.key, allRuleColumnArgs.get(ruleColumn.key));
                }
            }

            if (matchArgs.size() != 0) {
                return r; // 第一个全是可选列的规则，并且args包含该规则的部分可选列
            }
        }

        // add by jianghang at 2013-11-18
        // 如果还没有匹配规则，则可能一种情况就是所有的Rule都不满足，最后再查找一次规则中所有列都为可选的进行返回，按规则顺序返回第一个
        boolean isAllOptional = true;
        for (int i = 0; i < rules.size(); i++) {
            Rule<T> r = rules.get(i);
            for (RuleColumn ruleColumn : r.getRuleColumns().values()) {
                if (!ruleColumn.optional) {
                    isAllOptional = false;
                    break;// 如果当前规则有必选项，直接跳过,因为走到这里必选列已经不匹配了
                }
            }

            if (isAllOptional) {
                return r;
            }
        }

        return null;
    }

    private static Comparative getComparative(String colName, Map<String, Comparative> allRuleColumnArgs,
                                              ComparativeMapChoicer comparativeMapChoicer, List<Object> args) {
        Comparative comparative = allRuleColumnArgs.get(colName); // 先从缓存中获取
        if (comparative == null) {
            comparative = comparativeMapChoicer.getColumnComparative(args, colName);
            if (comparative != null) {
                allRuleColumnArgs.put(colName, comparative); // 放入缓存
            }
        }
        return comparative;
    }

}
