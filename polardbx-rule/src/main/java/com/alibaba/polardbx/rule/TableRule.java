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

import com.alibaba.polardbx.common.model.DBType;
import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.rule.Rule.RuleColumn;
import com.alibaba.polardbx.rule.exception.TddlRuleException;
import com.alibaba.polardbx.rule.impl.EnumerativeRule;
import com.alibaba.polardbx.rule.impl.ExtPartitionGroovyRule;
import com.alibaba.polardbx.rule.meta.RangeHash1Meta;
import com.alibaba.polardbx.rule.meta.ShardFunctionMeta;
import com.alibaba.polardbx.rule.model.AdvancedParameter;
import com.alibaba.polardbx.rule.model.AdvancedParameter.AtomIncreaseType;
import com.alibaba.polardbx.rule.utils.CalcParamsAttribute;
import com.alibaba.polardbx.rule.utils.CoverRuleProcessor;
import com.alibaba.polardbx.rule.utils.GroovyRuleShardFuncFinder;
import com.alibaba.polardbx.rule.utils.RuleUtils;
import com.alibaba.polardbx.rule.utils.ShardFunctionMetaUtils;
import com.alibaba.polardbx.rule.utils.SimpleRuleProcessor;
import com.alibaba.polardbx.rule.utils.sample.Samples;
import com.alibaba.polardbx.rule.virtualnode.DBTableMap;
import com.alibaba.polardbx.rule.virtualnode.TableSlotMap;
import org.apache.commons.lang.ObjectUtils;
import org.apache.commons.lang.StringUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.regex.Matcher;

import static com.alibaba.polardbx.common.ddl.newengine.DdlConstants.MAX_TABLE_NAME_LENGTH_MYSQL_ALLOWS;
import static com.alibaba.polardbx.common.ddl.newengine.DdlConstants.RANDOM_SUFFIX_LENGTH_OF_PHYSICAL_TABLE_NAME;

/**
 * 描述一个逻辑表怎样分库分表，允许指定逻辑表名和db/table的{@linkplain Rule}规则
 * <p>
 *
 * @author linxuan
 * @author jianghang 2013-11-4 下午5:33:51
 * @since 5.0.0
 */

public class TableRule extends VirtualTableSupport implements VirtualTableRule {

    /** =================================================== **/
    /** == 原始的配置字符串 == **/
    /**
     * ===================================================
     **/
    protected String dbNamePattern;                          // item_{0000}_dbkey
    protected String tbNamePattern;                          // item_{0000}
    protected String[] dbRules;                                // rule配置字符串
    protected String[] tbRules;                                // rule配置字符串
    protected List<String> extraPackages;                          // 自定义用户包

    protected boolean allowFullTableScan = false; // 是否允许全表扫描

    /**
     * 虚拟节点映射
     */
    protected TableSlotMap tableSlotMap;
    protected DBTableMap dbTableMap;
    protected String tableSlotKeyFormat;

    /**
     * <pre>
     *      key: groupKey
     *      val: set of phy tb
     * </pre>
     */
    protected Map<String, Set<String>> staticTopology;                         // 静态拓扑,通过正常拓扑计算不出来的拓扑内容

    /**
     * ============ 运行时变量 =================
     **/

    protected List<Rule<String>> dbShardRules;
    protected List<Rule<String>> tbShardRules;
    protected List<String> shardColumns;                           // 分库分表字段
    protected List<String> upperShardColumns;                      // 大写的分库分表字段

    protected List<String> dbPartitionKeys;
    protected List<String> tbPartitionKeys;

    /**
     *
     */
    protected Object outerContext;

    protected CoverRuleProcessor coverRuleProcessor;
    /**
     * 是否是个广播表，optimizer模块需要用他来标记某个表是否需要进行复制
     */
    protected boolean broadcast = false;

    /**
     * 相同的join group 应该具有相同的切分规则
     */
    protected String joinGroup = null;
    // 特殊规则优化
    protected boolean simple = false;

    protected int ruleDbCount;

    protected int extDbCount;

    protected int ruleTbCount;

    protected int extTbCount;

    protected boolean randomTableNamePatternEnabled = true;
    protected String existingRandomSuffixForRecovery = null;
    protected String tableNamePrefixForShadowTable = null;
    protected final long version;
    protected int isActiveTopologyInited;

    /**
     * <pre>
     *  SimpleRule类型的规则模板：
     *      1. ((#{0},1,{1}#).longValue() % {2}).intdiv({3})
     *      2. (#{0},1,{1}#.hashCode().abs().longValue() % {2}).intdiv({3})
     *
     *  dbCount 是简单规则枚举的总分库数，
     *  tbCount 是简单规则枚举的总分表数；
     *
     *  在SimpleRule类型规则的表达式中，dbCount 与 tbCount 是直接通过正则表达式从简单规则表达式中提取出来，
     *  因此，dbCount 与 tbCount  与 与真实的 库表数量不一定完全一致（例如，字符串枚举可能会有产生重复），
     *  （例如，
     *      对于对映射规则、冷热规则，dbCount可能也不是真实的分库数
     *   ）
     *   dbCount 与 tbCount 目前主要用在两个地方：
     *   （1）计算简单规则的路由计算，简单规则通过提取规则表达式的值，走JAVA而不是走Groovy来计算路由；
     *   （2）show create table 时回接分库分表数；
     *   （3）Join下推判断分库分表规则是否完全一致。
     *
     *  问题：dbCount、tbCount 与 acutalDbCount、actualTbCount 的区别：
     *     acutalDbCount 和 actualTbCount 是TDDL根据拆分规则的实际枚举数目，
     *
     *     acutalDbCount 是实际的规则枚举的总分库数，
     *     actualTbCount 是实际的规则枚举的总分表数；
     *
     *     而 dbCount 和 tbCount 不是。
     * --------------------------------
     *  在非SimpleRule类型的表达式中，由于其分库分表数目是根据规则枚举结果算出来的,
     *  所以，dbCount、tbCount 直接由 acutalDbCount、actualTbCount赋值，
     *  总与acutalDbCount、actualTbCount 保持一致
     *
     *
     * </pre>
     */

    // SimpleRule 场景下 tbCount/dbCount 与真实的 库表数量不一致~
    protected int tbCount;
    protected int dbCount;
    protected AtomIncreaseType partitionType;
    protected String[] tbNames;
    protected String[] dbNames;
    protected boolean coverRule = false; // 规则是否可以被覆盖更新,默认false,对于noloop不断滚动的规则可以覆盖
    protected Integer start;
    protected Integer end;

    protected String[] tbNamesOfExtraDb;
    protected String[] dbNamesOfExtraDb;

    /**
     * <pre>
     * 专用于保存直接调用groovy函数的非参数化的复杂规则（通常是非简单规则或时间类规则居多）的所使用的groovy拆分函数的类型
     * ， 这个参数主要用于判断拆分规则的等价判断
     * </pre>
     */
    // 分库用的Groovy函数
    protected String dbGroovyShardMethodName;

    // 分表用的Groovy函数
    protected String tbGroovyShardMethodName;

    /**
     * 用于描述分库函数的元数据，dbShardFunctionMetaMap 保存的是XML规则的关于DB切分函数的元数据内容,
     * dbShardFunctionMeta 由 dbShardFunctionMetaMap 反射生成
     */
    protected Map<String, Object> dbShardFunctionMetaMap;
    protected ShardFunctionMeta dbShardFunctionMeta;

    /**
     * 用于描述分库函数的元数据, tbShardFunctionMetaMap
     * 保存的是XML规则的关于DB切分函数的元数据内容，tbShardFunctionMeta 由 tbShardFunctionMetaMap
     * 反射生成
     */
    protected Map<String, Object> tbShardFunctionMetaMap;
    protected ShardFunctionMeta tbShardFunctionMeta;

    // 从 actualTopology 里计算出来的实际分库分表数量，用于 SHOW CREATE TABLE
    protected int actualTbCount;                          // 总分表数
    protected int actualDbCount;                          // 总分库数

    protected List<MappingRule> extPartitions;
    protected Map<String, Set<MappingRule>> extDbKeyToMappingRules;
    protected Map<String, Set<MappingRule>> extTbKeyToMappingRules;

    protected Map<String, Set<String>> extTopology;

    public TableRule() {
        this.version = 0;
    }

    public TableRule(long version) {
        this.version = version;
    }

    public long getVersion() {
        return this.version;
    }

    @Override
    public void doInit() {
        // 初始化虚拟节点
        if (tableSlotMap != null) {
            tableSlotMap.setTableSlotKeyFormat(tableSlotKeyFormat);
            tableSlotMap.setLogicTable(this.virtualTbName);
            tableSlotMap.init();
        }

        if (dbTableMap != null) {
            dbTableMap.setTableSlotKeyFormat(tableSlotKeyFormat);
            dbTableMap.setLogicTable(this.virtualTbName);
            dbTableMap.init();
        }

        if (outerContext == null) {
            // // 初始化规则计算的上下文，这个非常重要
            // outerContext = new HashMap<String, Object>();
            // ((Map<String, Object>)
            // outerContext).put(OuterContextAttribute.NEED_ROUTE_FOR_EXTRA_DB,
            // this);
        }

        if (extPartitions != null) {
            buildExtMapping();
        }

        // 处理一下占位符
        replaceWithParam(this.dbRules, dbRuleParames != null ? dbRuleParames : ruleParames);
        replaceWithParam(this.tbRules, tbRuleParames != null ? tbRuleParames : ruleParames);

        // 构造一下Rule对象
        String packagesStr = buildExtraPackagesStr(extraPackages);
        if (dbShardRules == null) { // 如果未设置Rule对象，基于string生成rule对象
            setDbShardRules(convertToRuleArray(dbRules,
                dbNamePattern,
                packagesStr,
                dbTableMap,
                tableSlotMap,
                extDbKeyToMappingRules,
                false));
        }
        if (tbShardRules == null) {
            setTbShardRules(convertToRuleArray(tbRules,
                tbNamePattern,
                packagesStr,
                dbTableMap,
                tableSlotMap,
                extTbKeyToMappingRules,
                true));
        }

        if (tbShardRules == null || tbShardRules.size() == 0) {
            if (this.tbNamePattern == null) {
                // 表规则没有，tbKeyPattern为空
                this.tbNamePattern = this.virtualTbName;
            }
        }

        // 构造一下分区字段
        buildShardColumns();
        if (customTopology != null) {
            // 依赖tb/db name,需要在init方法中计算
            buildStaticTopology(customTopology);
        }

        SimpleRuleProcessor.process(this);

        if (!isLazyInit()) {
            // 基于rule计算一下拓扑结构
            initActualTopology();

        }

        if (broadcast) {
            if (!RuleUtils.isEmpty(dbShardRules) || !RuleUtils.isEmpty(tbShardRules)) {
                throw new IllegalArgumentException("broadcast table not support shard rule");
            }
        }

        if (dbShardFunctionMetaMap != null) {
            dbShardFunctionMeta = getShardFunctionMetaObject(dbShardFunctionMetaMap);
            dbShardFunctionMeta.initShardFuncionMeta(this);
        }

        if (tbShardFunctionMetaMap != null) {
            tbShardFunctionMeta = getShardFunctionMetaObject(tbShardFunctionMetaMap);
            tbShardFunctionMeta.initShardFuncionMeta(this);
        }

        GroovyRuleShardFuncFinder.process(this);

    }

    public boolean varifyTableShardFuncMetaInfo() {

        List<String> shardColumns = getShardColumns();

        if (shardColumns.size() == 1) {

            String shardColumn = shardColumns.get(0);

            ShardFunctionMeta dbShardFunctionMeta = getDbShardFunctionMeta();
            ShardFunctionMeta tbShardFunctionMeta = getTbShardFunctionMeta();

            if (dbShardFunctionMeta != null && tbShardFunctionMeta != null) {

                String dbCreateTableFuncAndParams = dbShardFunctionMeta.buildCreateTablePartitionFunctionStr();
                String tbCreateTableFuncAndParams = tbShardFunctionMeta.buildCreateTablePartitionFunctionStr();

                if (!dbCreateTableFuncAndParams.equals(tbCreateTableFuncAndParams)) {
                    throw new TddlRuleException(
                        String.format("Not support to both different parition function on the same column `%s`",
                            shardColumn));
                }
            } else {

                if (GroovyRuleShardFuncFinder.groovyDateMethodFunctionSet.contains(dbGroovyShardMethodName)
                    && GroovyRuleShardFuncFinder.groovyDateMethodFunctionSet.contains(tbGroovyShardMethodName)) {
                    // 分库分表的拆分函数都是时间类的拆分函数

                    if (!dbGroovyShardMethodName.equalsIgnoreCase(tbGroovyShardMethodName)) {
                        // 如果分库函数与分表函数不一样，但拆分键一样，这样不允许建表
                        if (dbPartitionKeys.size() == 1 && tbPartitionKeys.size() == 1) {
                            String dbKey = dbPartitionKeys.get(0).toLowerCase();
                            String tbKey = tbPartitionKeys.get(0).toLowerCase();
                            if (dbKey.equalsIgnoreCase(tbKey)) {
                                throw new TddlRuleException(String.format(
                                    "Not support to use two different db/tb shard functions on the same column `%s`",
                                    shardColumn));
                            }
                        }
                    }
                } else {
                    if (dbRules != null && tbRules != null) {
                        String dbShardInfoStr =
                            String.format("%s_%s", dbShardFunctionMeta != null, dbGroovyShardMethodName);
                        String tbShardInfoStr =
                            String.format("%s_%s", tbShardFunctionMeta != null, tbGroovyShardMethodName);
                        if (!dbShardInfoStr.equals(tbShardInfoStr)) {
                            throw new TddlRuleException(String.format(
                                "Not support to use two different db/tb shard functions on the same column `%s`",
                                shardColumn));
                        }
                    }
                }

            }
        }
        return true;

    }

    public ShardFunctionMeta getShardFunctionMetaObject(Map<String, Object> shardFuncMetaMap) {
        String classUrl = null;
        Class classObj = null;
        ShardFunctionMeta shardFunctionMetaObj = null;
        try {
            classUrl = (String) shardFuncMetaMap.get(ShardFunctionMetaUtils.CLASS_URL);
            if (classUrl == null) {
                throw new IllegalArgumentException("shardFunctionMetaMap must contain property of class url.");
            }
            classUrl = RuleCompatibleHelper.compatibleShardFunctionMeta(classUrl);
            classObj = Class.forName(classUrl);
            shardFunctionMetaObj =
                (ShardFunctionMeta) ShardFunctionMetaUtils.convertMapToShardFunctionMeta(shardFuncMetaMap,
                    classObj);
        } catch (Throwable e) {
            // DRDS模式下，忽略报错
            logger.error(String.format("failed to build shardFunctionMeta, its tbNamePattern is %s, classUrl is %s",
                tbNamePattern,
                classUrl == null ? "null" : classUrl),
                e);
        }
        return shardFunctionMetaObj;
    }

    public void initActualTopology() {
        if (actualTopology != null) {
            showTopology(false);
            return; // 用户显式设置的优先
        }
        if (logger.isDebugEnabled()) {
            logger.debug("Initializing topology of virtual table " + this.virtualTbName);
        }
        actualTopology = new TreeMap<String, Set<String>>();
        actualTopologyOfExtraDb = new TreeMap<String, Set<String>>();
        if (RuleUtils.isEmpty(dbShardRules) && RuleUtils.isEmpty(tbShardRules)) {

            // 啥规则都没有

            // 该表没有做任何拆分
            Set<String> tbs = new TreeSet<String>();
            tbs.add(this.tbNamePattern);
            actualTopology.put(this.dbNamePattern, tbs);
            if (ConfigDataMode.isFastMock()) {
                dbCount = 1;
                tbCount = 1;
            }
        } else if (RuleUtils.isEmpty(dbShardRules)) {

            // 没有库规则

            // 该表分表不分库
            Set<String> tbs = new TreeSet<String>();
            for (Rule<String> tbRule : tbShardRules) {
                tbs.addAll(vbvRule(tbRule, getEnumerates(tbRule), null));
            }
            actualTopology.put(this.dbNamePattern, tbs);

        } else if (RuleUtils.isEmpty(tbShardRules)) {

            // 没有表规则

            // 该表分库不分表
            Set<String> tbs = new TreeSet<String>();
            tbs.add(this.tbNamePattern);
            for (Rule<String> dbRule : dbShardRules) {
                for (String dbIndex : vbvRule(dbRule, getEnumerates(dbRule), null)) {
                    actualTopology.put(dbIndex, tbs);
                }
            }
        } else {

            // 库表规则都有

            // 该表不但分库，而且分表
            if (checkRangeHash()) {
                valueByValue(this.actualTopology,
                    this.actualTopologyOfExtraDb,
                    dbShardRules.get(0),
                    tbShardRules.get(0),
                    false);
            } else {
                for (Rule<String> dbRule : dbShardRules) {
                    for (Rule<String> tbRule : tbShardRules) {
                        if (this.tableSlotMap != null && this.dbTableMap != null) {

                            // 存在vnode

                            valueByValue(this.actualTopology, this.actualTopologyOfExtraDb, dbRule, tbRule, true);

                        } else if (this.isSimple()) {

                            // 简单规则，需要走简单规则的枚举逻辑

                            generateActualTopologyBySimpleRule();

                        } else {
                            // 复杂规则，需要走规则的枚举逻辑

                            valueByValue(this.actualTopology, this.actualTopologyOfExtraDb, dbRule, tbRule, false);
                        }
                    }
                }
            }
        }

        if (staticTopology != null) {
            // 规则中手工配置了静态表拓扑

            // 合并静态拓扑
            for (Map.Entry<String, Set<String>> entry : staticTopology.entrySet()) {
                Set<String> values = actualTopology.get(entry.getKey());
                if (values != null) {
                    values.addAll(entry.getValue());
                } else {
                    actualTopology.put(entry.getKey(), entry.getValue());
                }
            }
        }

        ruleDbCount = actualTopology.keySet().size();

        int tempTbCount = 0;
        for (Map.Entry<String, Set<String>> entry : actualTopology.entrySet()) {
            Set<String> values = actualTopology.get(entry.getKey());
            tempTbCount += values.size();
        }

        ruleTbCount = tempTbCount;

        if (extPartitions != null) {
            buildExtTopology(actualTopology);
        }

        // 基于拓扑的计算结果，计算dbCount与tbCount
        int dbCountVal = actualTopology.keySet().size();
        int tbCountVal = 0;
        for (Map.Entry<String, Set<String>> actualTopologyItem : actualTopology.entrySet()) {
            tbCountVal += actualTopologyItem.getValue().size();
        }
        this.actualDbCount = dbCountVal;
        this.actualTbCount = tbCountVal;

        extDbCount = dbCountVal - ruleDbCount;
        extTbCount = tbCountVal - tempTbCount;
        if (extTopology != null) {
            for (Iterator<String> iterator = extTopology.keySet().iterator(); iterator.hasNext(); ) {
                String next = iterator.next();
                final Set<String> strings = extTopology.get(next);
                if (strings == null || strings.size() == 0) {
                    continue;
                }
                if (actualTopology.containsKey(next)) {
                    actualTopology.get(next).addAll(strings);
                } else {
                    actualTopology.put(next, strings);
                }
            }
        }

        if (!isSimple()) {
            // 在DRDS模式下，非简单规则(一般都是新型拆分规则及按时间分表滚动的规则)，dbCount与tbCount分别为用拓扑实际计算出来的分库数与分表数
            dbCount = actualDbCount;
            tbCount = actualTbCount;
        }

        if (logger.isDebugEnabled()) {
            logger.debug("Initialized topology of virtual table " + this.virtualTbName);
            showTopology(true);
        }
    }

    private boolean checkRangeHash() {
        if (this.dbShardFunctionMeta != null && this.tbShardFunctionMeta != null
            && this.dbShardFunctionMeta instanceof RangeHash1Meta
            && this.tbShardFunctionMeta instanceof RangeHash1Meta) {
            if (((RangeHash1Meta) this.dbShardFunctionMeta).getShardKeys()
                .equals(((RangeHash1Meta) this.tbShardFunctionMeta).getShardKeys())) {
                return true;
            }
        }
        if (dbShardFunctionMetaMap != null && tbShardFunctionMetaMap != null) {
            if ("RangeHash1Meta".equals(dbShardFunctionMetaMap.get("classUrl"))
                && "RangeHash1Meta".equals(tbShardFunctionMetaMap.get("classUrl"))) {
                if (dbShardFunctionMetaMap.get("shardKeys").equals(tbShardFunctionMetaMap.get("shardKeys"))) {
                    return true;
                }
            }
        }
        return false;
    }

    private void generateActualTopologyBySimpleRule() {

        String[] tmpTbNames = this.tbNames;
        String[] tmpDbNames = this.dbNames;

        for (int tbIndex = 0; tbIndex < tmpTbNames.length; tbIndex++) {
            String actualTableName = tmpTbNames[tbIndex];
            String actualDbName = tmpDbNames[tbIndex];

            Set<String> tbNamesOfOneDb = this.actualTopology.get(actualDbName);
            if (tbNamesOfOneDb == null) {
                tbNamesOfOneDb = new HashSet<String>(this.tbCount / this.dbCount);
                this.actualTopology.put(actualDbName, tbNamesOfOneDb);
            }
            tbNamesOfOneDb.add(actualTableName);
        }
    }

    // ==================== build topology =====================

    /**
     * // 计算一下规则，并返回计算结果
     * 基于传入的枚举样本samples，应用传入的规则参数rule，计算出这些枚举值所对应的物理分表名或（物理库名），并返回集合
     */
    private Set<String> vbvRule(Rule<String> rule, Samples samples, Map<String, Object> calcParams) {
        Set<String> finalRouteResult = new TreeSet<String>();
        for (Map<String, Object> sample : samples) {

            // tbs.add(rule.eval(sample, null));

            String phyName = rule.eval(sample, outerContext, calcParams);
            finalRouteResult.add(phyName);
        }
        return finalRouteResult;
    }

    /**
     * // 计算一下规则，并返回计算结果
     * 基于传入的枚举值集合enumerates，应用传入的规则参数rule，计算出这些枚举值所对应的物理分表名或（物理库名），并返回集合
     */
    private Set<String> vbvRule(Rule<String> rule, Map<String, Set<Object>> enumerates,
                                Map<String, Object> calcParams) {
        Set<String> finalRouteResult = new TreeSet<String>();
        Samples samples = new Samples(enumerates);
        for (Map<String, Object> sample : samples) {
            // tbs.add(rule.eval(sample, null));
            String phyName = rule.eval(sample, outerContext, calcParams);
            finalRouteResult.add(phyName);
        }
        return finalRouteResult;
    }

    /**
     * 计算一下规则，同时记录对应计算参数，通常是计算分库规则用
     * <p>
     * <pre>
     * 基于传入的枚举值集合enumerates，应用传入的规则参数rule，
     * 计算出这些枚举值所对应的物理分表名或（物理库名），
     * 并返回这样的对应关系：
     *
     *   Map<String, Samples>:
     *      key: groupKey
     *      val: 经过规则计算后产生能路由到这个groupKey的枚举值的集合
     * </pre>
     */
    private Map<String, Samples> vbvTrace(Rule<String> rule, Map<String, Set<Object>> enumerates,
                                          Map<String, Object> calcParams) {

        /**
         * <pre>
         *  key: groupKey
         *  val: 经过规则计算后产生能路由到这个groupKey的枚举值的集合
         * </pre>
         */
        Map<String, Samples> db2Samples = new TreeMap<String, Samples>();

        Samples dbSamples = new Samples(enumerates);
        for (Map<String, Object> sample : dbSamples) {

            // String v = rule.eval(sample, null);
            String phyName = rule.eval(sample, outerContext, calcParams);

            Samples s = db2Samples.get(phyName);
            if (s == null) {
                s = new Samples(sample.keySet());
                db2Samples.put(phyName, s);
            }
            s.addSample(sample);
        }
        return db2Samples;
    }

    /**
     * 复杂规则的初始化枚举方式
     */
    private void valueByValue(Map<String, Set<String>> topology, Map<String, Set<String>> topologyForExtraDb,
                              Rule<String> dbRule, Rule<String> tbRule, boolean isVnode) {

        /**
         * <pre>
         *  根据分库规则，从分库规则的初始枚举定义中，获取分库键的枚举值
         *
         *  key: 分库键
         *  val: 分库键的初始化的枚举值
         * </pre>
         */
        Map<String/* 列名 */, Set<Object>> dbEnumerates = getEnumerates(dbRule);

        /**
         * <pre>
         *  根据分表规则，从分表规则的初始枚举定义中，获取分表键的枚举值
         *   key: 分表键
         *   val: 分表键的初始化的枚举值
         * </pre>
         */
        Map<String/* 列名 */, Set<Object>> tbEnumerates = getEnumerates(tbRule);

        // 以table rule列为基准
        Set<AdvancedParameter> params = RuleUtils.cast(tbRule.getRuleColumnSet());
        for (AdvancedParameter tbap : params) {

            if (dbEnumerates.containsKey(tbap.key)) {

                // 库表规则的公共列名，表枚举值要涵盖所有库枚举值跨越的范围= =!

                /**
                 * 获取与分表键相同的分库键的对应的枚举值集合
                 */
                Set<Object> dbEnumValSetOfDbAdvancedParameter = dbEnumerates.get(tbap.key);

                /**
                 * tbValuesBasedONdbValue保存着本次分表键tbap.key的所有枚举值值
                 */
                Set<Object> tbValuesBasedOnDbValue = new HashSet<Object>();

                /**
                 * 针对每一个分库键的枚举值，让分表键基于分库键的这个枚举值生成该分表键对应的枚举集合
                 */
                for (Object dbValue : dbEnumValSetOfDbAdvancedParameter) {

                    /**
                     * 让分表键根据dbValue作为枚举基点，进行值枚举，并产生分表键的枚举集合
                     */
                    Set<Object> tbEnumValSetOfTbApBaseOnDbVal = tbap.enumerateRange(dbValue);
                    tbValuesBasedOnDbValue.addAll(tbEnumValSetOfTbApBaseOnDbVal);
                }

                /**
                 * 将tbValuesBasedONdbValue保存着本次分表键tbap.key的所有枚举值值，将其加到分库键的枚举集合中
                 */
                dbEnumValSetOfDbAdvancedParameter.addAll(tbValuesBasedOnDbValue);

            } else {

                // 库表规则没有公共列名

                Set<Object> tbEnumValSetOfTbAp = tbEnumerates.get(tbap.key);

                /**
                 * 将分表键的枚举集合也同样添加到分库的键的枚举集合中，这里为什么要这样做？
                 */
                dbEnumerates.put(tbap.key, tbEnumValSetOfTbAp);
            }
        }

        // 有虚拟节点的话按照虚拟节点计算
        if (isVnode) {
            Samples tabSamples = new Samples(tbEnumerates);
            Set<String> tbs = new TreeSet<String>();
            for (Map<String, Object> sample : tabSamples) {
                // String value = tbRule.eval(sample, null);
                String value = tbRule.eval(sample, outerContext);
                tbs.add(value);
            }

            for (String table : tbs) {
                Map<String, Object> sample = new HashMap<String, Object>(1);
                sample.put(EnumerativeRule.REAL_TABLE_NAME_KEY, table);
                // String db = dbRule.eval(sample, null);
                String db = dbRule.eval(sample, outerContext);
                if (topology.get(db) == null) {
                    Set<String> tabs = new HashSet<String>();
                    tabs.add(table);
                    topology.put(db, tabs);
                } else {
                    topology.get(db).add(table);
                }
            }

            return;
        } else {
            // 没有虚拟节点按正常走
            buildTopogolyByDbEnumerates(topology, dbRule, tbRule, dbEnumerates, null);
        }
    }

    private void buildTopogolyByDbEnumerates(Map<String, Set<String>> topology, Rule<String> dbRule,
                                             Rule<String> tbRule, Map<String, Set<Object>> dbEnumerates,
                                             Map<String, Object> calcParams) {
        /**
         * 获取groupKey与 经过规则计算后产生能路由到这个groupKey的枚举值的集合 的对应关系。
         *
         * <pre>
         *  dbRouteResultSet key: groupKey
         *  dbRouteResultSet val: 经过规则计算后产生能路由到这个groupKey的枚举值的集合
         *
         *  dbRouteResultSet这个对像可以根据groupKey获取所有能够路由到它的对应枚举输入值集合
         * </pre>
         */
        Map<String, Samples> dbRouteResultSet = vbvTrace(dbRule, dbEnumerates, calcParams);

        /**
         * 对于每一个分库及其对应的枚举值，
         */
        for (Map.Entry<String/* groupkey */, Samples/* groupkey对应的枚举值 */> dbRouteResult : dbRouteResultSet.entrySet()) {

            String groupKey = dbRouteResult.getKey();
            Samples samplesOfGroupKey = dbRouteResult.getValue();

            Set<String> tbSetOfOneGroup = topology.get(groupKey);
            if (tbSetOfOneGroup == null) {

                /**
                 * 获取单个分库之下的所有物理分表的枚举结果
                 */
                tbSetOfOneGroup = vbvRule(tbRule, samplesOfGroupKey, null);

                /**
                 * 将获取物理分表计算结果保存到拓扑中
                 */
                topology.put(groupKey, tbSetOfOneGroup);

            } else {

                /**
                 * 获取单个分库之下的所有物理分表的枚举结果
                 */
                Set<String> tbRouteResultSet = vbvRule(tbRule, samplesOfGroupKey, null);

                /**
                 * 将获取物理分表计算结果保存到拓扑中，会自动去重
                 */
                tbSetOfOneGroup.addAll(tbRouteResultSet);
            }
        }
    }

    public String getDbGroovyShardMethodName() {
        return dbGroovyShardMethodName;
    }

    public void setDbGroovyShardMethodName(String dbGroovyShardMethodName) {
        this.dbGroovyShardMethodName = dbGroovyShardMethodName;
    }

    public String getTbGroovyShardMethodName() {
        return tbGroovyShardMethodName;
    }

    public void setTbGroovyShardMethodName(String tbGroovyShardMethodName) {
        this.tbGroovyShardMethodName = tbGroovyShardMethodName;
    }

    class SpecialDateMethodProcessor {

        private Boolean isSpecialDate = null;
        private Rule rule;

        public SpecialDateMethodProcessor(Rule rule) {
            this.rule = rule;
        }

        public void process(AdvancedParameter ap) {
            if (ap.atomicIncreateType.isTime()) {
                /**
                 * 判断当前规则中是否包含特殊的日期函数如mmdd_i和dd_i，这两个函数要求最大到366和31，否则从当前时间进行
                 * 枚举可能会无法达到这个值 只处理一次
                 */
                if (isSpecialDate == null) {
                    isSpecialDate = SimpleRuleProcessor.isSpecialDateMethod(rule.getExpression());
                }
                ap.setIsSpecialDateMethod(isSpecialDate);
            }
        }
    }

    /**
     * 根据#id,1,32|512_64#中第三段定义的遍历值范围，枚举规则中每个列的遍历值
     */
    private Map<String/* 列名 */, Set<Object>/* 列值 */> getEnumerates(Rule rule) {

        /**
         * 获取规则定义中所有的拆分键，有些情况下拆分键多于一个，每一个拆分列就是一个AdvancedParameter对象
         */
        Set<AdvancedParameter> params = RuleUtils.cast(rule.getRuleColumnSet());

        Map<String/* 列名 */, Set<Object>/* 列值 */> enumerates = new HashMap<String, Set<Object>>(params.size());

        SpecialDateMethodProcessor dateProcessor = new SpecialDateMethodProcessor(rule);

        for (AdvancedParameter ap : params) {
            dateProcessor.process(ap);

            /**
             * 对于规则定义中每一个拆分键，让它根据本地规则进行枚举描点，枚举描点是基本规则定义，并得到枚举结果
             */
            Set<Object> enumResult = ap.enumerateRange(this);

            /**
             * 将每一个拆分列得到的枚举结果进行保存
             */
            enumerates.put(ap.key, enumResult);
        }
        return enumerates;
    }

    private String buildExtraPackagesStr(List<String> extraPackages) {
        StringBuilder ep = new StringBuilder("");
        if (extraPackages != null) {
            int packNum = extraPackages.size();
            for (int i = 0; i < packNum; i++) {
                ep.append("import ");
                ep.append(extraPackages.get(i));
                ep.append(";");
            }
        }
        return ep.toString();
    }

    private void buildShardColumns() {
        Set<String> shardColumns = new TreeSet<String>();
        Set<String> dbPartitionKeys = new TreeSet<String>();
        Set<String> tbPartitionKeys = new TreeSet<String>();

        Set<String> shardColumnsUpper = new TreeSet<String>();
        Set<String> dbPartitionKeysUpper = new TreeSet<String>();
        Set<String> tbPartitionKeysUpper = new TreeSet<String>();

        if (null != dbShardRules) {
            for (Rule<String> rule : dbShardRules) {
                Map<String, RuleColumn> columRule = rule.getRuleColumns();
                if (null != columRule && !columRule.isEmpty()) {
                    for (String shardKey : columRule.keySet()) {
                        String shardKeyStr = shardKey;
                        String shardKeyStrUpper = shardKey.toUpperCase();
                        if (!shardColumnsUpper.contains(shardKeyStrUpper)) {
                            shardColumns.add(shardKeyStr);
                            shardColumnsUpper.add(shardKeyStrUpper);
                        }

                        if (!dbPartitionKeysUpper.contains(shardKeyStrUpper)) {
                            dbPartitionKeys.add(shardKeyStr);
                            dbPartitionKeysUpper.add(shardKeyStrUpper);
                        }

                    }

                }
            }
        }

        if (null != tbShardRules) {
            for (Rule<String> rule : tbShardRules) {
                Map<String, RuleColumn> columRule = rule.getRuleColumns();
                if (null != columRule && !columRule.isEmpty()) {
                    for (String shardKey : columRule.keySet()) {

                        String shardKeyStr = shardKey;
                        String shardKeyStrUpper = shardKey.toUpperCase();

                        if (!shardColumnsUpper.contains(shardKeyStrUpper)) {
                            shardColumns.add(shardKeyStr);
                            shardColumnsUpper.add(shardKeyStrUpper);
                        }

                        if (!tbPartitionKeysUpper.contains(shardKeyStrUpper)) {
                            tbPartitionKeys.add(shardKeyStr);
                            tbPartitionKeysUpper.add(shardKeyStrUpper);
                        }
                    }
                }
            }
        }

        this.shardColumns = new ArrayList<String>(shardColumns);// 没有表配置，代表没有走分区
        this.upperShardColumns = new ArrayList<String>(shardColumns.size());
        for (String column : shardColumns) {
            this.upperShardColumns.add(column.toUpperCase());
        }
        this.dbPartitionKeys = new ArrayList<String>(dbPartitionKeys);// 没有表配置，代表没有走分区
        this.tbPartitionKeys = new ArrayList<String>(tbPartitionKeys);// 没有表配置，代表没有走分区
    }

    // ===================== setter / getter ====================

    public void setDbRuleArray(List<String> dbRules) {
        // 若类型改为String[],spring会自动以逗号分隔，变态！
        dbRules = trimRuleString(dbRules);
        this.dbRules = dbRules.toArray(new String[dbRules.size()]);
    }

    public List<String> getDbPartitionKeys() {
        return dbPartitionKeys;
    }

    public List<String> getTbPartitionKeys() {
        return tbPartitionKeys;
    }

    public void setTbRuleArray(List<String> tbRules) {
        // 若类型改为String[],spring会自动以逗号分隔，变态！
        tbRules = trimRuleString(tbRules);
        this.tbRules = tbRules.toArray(new String[tbRules.size()]);
    }

    public void setDbRules(String dbRules) {
        if (this.dbRules == null) {
            // 优先级比dbRuleArray低
            // this.dbRules = dbRules.split("\\|");
            this.dbRules = new String[] {dbRules.trim()}; // 废掉|分隔符，没人用且容易造成混乱
        }
    }

    public void setTbRules(String tbRules) {
        if (this.tbRules == null) {
            // 优先级比tbRuleArray低
            // this.tbRules = tbRules.split("\\|");
            this.tbRules = new String[] {tbRules.trim()}; // 废掉|分隔符，没人用且容易造成混乱
        }
    }

    @Override
    public void setRuleParames(String ruleParames) {
        if (ruleParames.indexOf('|') != -1) {
            // 优先用|线分隔,因为有些规则表达式中会有逗号
            this.ruleParames = ruleParames.split("\\|");
        } else {
            this.ruleParames = ruleParames.split(",");
        }
    }

    public void setDbNamePattern(String dbKeyPattern) {
        this.dbNamePattern = dbKeyPattern;
    }

    public void setTbNamePattern(String tbKeyPattern) {
        this.tbNamePattern = tbKeyPattern;
    }

    public void setExtraPackages(List<String> extraPackages) {
        this.extraPackages = extraPackages;
    }

    public void setOuterContext(Object outerContext) {
        this.outerContext = outerContext;
    }

    public void setAllowFullTableScan(boolean allowFullTableScan) {
        this.allowFullTableScan = allowFullTableScan;
    }

    public boolean isCoverRule() {
        return coverRule;
    }

    public void setCoverRule(boolean coverRule) {
        this.coverRule = coverRule;
    }

    public CoverRuleProcessor getCoverRuleProcessor() {
        return coverRuleProcessor;
    }

    public void setCoverRuleProcessor(CoverRuleProcessor coverRuleProcessor) {
        this.coverRuleProcessor = coverRuleProcessor;
    }

    @Override
    public void setDbType(DBType dbType) {
        this.dbType = dbType;
    }

    public void setDbShardRules(List<Rule<String>> dbShardRules) {
        this.dbShardRules = dbShardRules;
    }

    public void setTbShardRules(List<Rule<String>> tbShardRules) {
        this.tbShardRules = tbShardRules;
    }

    @Override
    public DBType getDbType() {
        return dbType;
    }

    @Override
    public List getDbShardRules() {
        return dbShardRules;
    }

    @Override
    public List getTbShardRules() {
        return tbShardRules;
    }

    @Override
    public Object getOuterContext() {
        return outerContext;
    }

    @Override
    public TableSlotMap getTableSlotMap() {
        return tableSlotMap;
    }

    @Override
    public DBTableMap getDbTableMap() {
        return dbTableMap;
    }

    @Override
    public boolean isAllowFullTableScan() {
        return allowFullTableScan;
    }

    @Override
    public String getTbNamePattern() {
        return tbNamePattern;
    }

    @Override
    public String getDbNamePattern() {
        return dbNamePattern;
    }

    @Override
    public String[] getDbRuleStrs() {
        return dbRules;
    }

    @Override
    public String[] getTbRulesStrs() {
        return tbRules;
    }

    public void setDbTableMap(DBTableMap dbTableMap) {
        this.dbTableMap = dbTableMap;
    }

    public void setTableSlotMap(TableSlotMap tableSlotMap) {
        this.tableSlotMap = tableSlotMap;
    }

    public void setTableSlotKeyFormat(String tableSlotKeyFormat) {
        this.tableSlotKeyFormat = tableSlotKeyFormat;
    }

    @Override
    public boolean isBroadcast() {
        return broadcast;
    }

    public void setBroadcast(boolean broadcast) {
        this.broadcast = broadcast;
    }

    @Override
    public String getJoinGroup() {
        return joinGroup;
    }

    public void setJoinGroup(String joinGroup) {
        this.joinGroup = joinGroup;
    }

    @Override
    public List<String> getShardColumns() {
        return shardColumns;
    }

    @Override
    public List<String> getUpperShardColumns() {
        return upperShardColumns;
    }

    @Override
    public Map<String, Set<String>> getActualTopology() {
        if (actualTopology == null) {
            synchronized (this) {
                if (actualTopology == null) {
                    // 基于rule计算一下拓扑结构
                    initActualTopology();
                }
            }
        }

        return actualTopology;
    }

    @Override
    public Map<String, Set<String>> getActualTopology(Map topologyParams) {
        final Map<String, Set<String>> actualTopology = getActualTopology();
        if (topologyParams == null) {
            return actualTopology;
        }

        Boolean shardForExtraDb = (Boolean) topologyParams.get(CalcParamsAttribute.SHARD_FOR_EXTRA_DB);
        if (shardForExtraDb == null || !shardForExtraDb) {
            return actualTopology;
        }

        if (actualTopology == null) {
            synchronized (this) {
                if (actualTopology == null) {
                    // 基于rule计算一下拓扑结构
                    initActualTopology();
                }
            }
        }

        return actualTopologyOfExtraDb;
    }

    // public Map<String, Set<String>> getActualTopology(Map<String, Object>
    // topologyParams) {
    // if (topologyParams == null) {
    // return getActualTopology();
    // }
    // return null;
    // }

    public int getDbCount() {
        return dbCount;
    }

    public void setDbCount(int dbCount) {
        this.dbCount = dbCount;
    }

    public AtomIncreaseType getPartitionType() {
        return partitionType;
    }

    public void setPartitionType(AtomIncreaseType partitionType) {
        this.partitionType = partitionType;
    }

    public boolean isSimple() {
        return simple;
    }

    public void setSimple(boolean simple) {
        this.simple = simple;
    }

    public void setDbNames(String[] dbNames) {
        this.dbNames = dbNames;
    }

    public String[] getDbNames() {
        return dbNames;
    }

    public int getTbCount() {
        return tbCount;
    }

    public void setTbCount(int tbCount) {
        this.tbCount = tbCount;
    }

    public String[] getTbNames() {
        return tbNames;
    }

    public void setTbNames(String[] tbNames) {
        this.tbNames = tbNames;
    }

    public Integer getStart() {
        return start;
    }

    public void setStart(Integer start) {
        this.start = start;
    }

    public Integer getEnd() {
        return end;
    }

    public void setEnd(Integer end) {
        this.end = end;
    }

    public void setActualTopology(Map actualTopology) {
        Map<String, Set<String>> topologys = new TreeMap<String, Set<String>>();
        for (Object obj : actualTopology.entrySet()) {
            Map.Entry entry = (Map.Entry) obj;
            String key = ObjectUtils.toString(entry.getKey());
            Object value = entry.getValue();
            if (value instanceof Set) {
                // 兼容一下以前的Map<String,Set<String>>的接口
                Set<String> tables = new HashSet<String>();
                for (Object subValue : (Set) value) {
                    tables.add(ObjectUtils.toString(subValue));
                }

                topologys.put(key, tables);
            } else {
                buildTopology(topologys, key, ObjectUtils.toString(value));
            }
        }

        this.actualTopology = topologys;
    }

    /**
     * <pre>
     * For setting extpartition first time, we need to initialize the topology information, multiple calls do not guarantee the correctness.
     * </pre>
     */
    public void initExtTopology() {
        buildExtMapping();
        buildExtTopology(this.actualTopology);
    }

    public Map<String, Set<String>> getStaticTopology() {
        return staticTopology;
    }

    public String extractRandomSuffix() {
        String randomSuffix = null;

        if (TStringUtil.isNotEmpty(tbNamePattern)
            && (tbNamePattern.length() > RANDOM_SUFFIX_LENGTH_OF_PHYSICAL_TABLE_NAME)) {

            int randomSuffixStartIndex = 0, randomSuffixEndIndex = 0;
            int leftCurlyBraceIndex = TStringUtil.indexOf(tbNamePattern, "{");

            if (TStringUtil.startsWithIgnoreCase(tbNamePattern, virtualTbName)) {
                randomSuffixStartIndex = virtualTbName.length() + 1;
                if (leftCurlyBraceIndex < 0) {
                    // No table number placeholder
                    randomSuffixEndIndex = tbNamePattern.length();
                } else if (leftCurlyBraceIndex > virtualTbName.length()) {
                    // Table number placeholder exists
                    randomSuffixEndIndex = leftCurlyBraceIndex - 1;
                }
                if (randomSuffixEndIndex > (randomSuffixStartIndex + RANDOM_SUFFIX_LENGTH_OF_PHYSICAL_TABLE_NAME)) {
                    randomSuffixStartIndex = randomSuffixEndIndex - RANDOM_SUFFIX_LENGTH_OF_PHYSICAL_TABLE_NAME;
                }
            } else if (virtualTbName.length() > (MAX_TABLE_NAME_LENGTH_MYSQL_ALLOWS
                - RANDOM_SUFFIX_LENGTH_OF_PHYSICAL_TABLE_NAME - 1)) {
                // Logical table name has been truncated.
                String truncatedTableName;
                if (leftCurlyBraceIndex < 0) {
                    // No table number placeholder
                    truncatedTableName = TStringUtil.substring(tbNamePattern, 0,
                        tbNamePattern.length() - RANDOM_SUFFIX_LENGTH_OF_PHYSICAL_TABLE_NAME - 1);
                    if (TStringUtil.startsWithIgnoreCase(virtualTbName, truncatedTableName)) {
                        randomSuffixStartIndex = truncatedTableName.length() + 1;
                        randomSuffixEndIndex = tbNamePattern.length();
                    }
                } else {
                    // Table number placeholder exists
                    truncatedTableName = TStringUtil.substring(tbNamePattern, 0,
                        leftCurlyBraceIndex - 1 - RANDOM_SUFFIX_LENGTH_OF_PHYSICAL_TABLE_NAME - 1);
                    if (TStringUtil.startsWithIgnoreCase(virtualTbName, truncatedTableName)) {
                        randomSuffixStartIndex = truncatedTableName.length() + 1;
                        randomSuffixEndIndex = leftCurlyBraceIndex - 1;
                    }
                }
            }

            if (randomSuffixStartIndex > 0 && randomSuffixEndIndex > randomSuffixStartIndex) {
                String possibleRandomSuffix =
                    TStringUtil.substring(tbNamePattern, randomSuffixStartIndex, randomSuffixEndIndex);
                if (TStringUtil.isNotEmpty(possibleRandomSuffix)
                    && possibleRandomSuffix.length() == RANDOM_SUFFIX_LENGTH_OF_PHYSICAL_TABLE_NAME) {
                    randomSuffix = possibleRandomSuffix;
                }
            }
        }

        return randomSuffix;
    }

    public String extractTableNamePrefix() {
        String tableNamePrefix = null;

        if (TStringUtil.isNotEmpty(tbNamePattern)) {
            String possibleTableNamePrefix = null;

            int leftCurlyBraceIndex = TStringUtil.indexOf(tbNamePattern, "{");

            if (leftCurlyBraceIndex < 0) {
                // No table number placeholder
                possibleTableNamePrefix = TStringUtil.substring(tbNamePattern, 0);
            } else if (leftCurlyBraceIndex > 1) {
                // Table number placeholder exists
                possibleTableNamePrefix = TStringUtil.substring(tbNamePattern, 0, leftCurlyBraceIndex - 1);
            }

            if (TStringUtil.isNotEmpty(possibleTableNamePrefix)) {
                tableNamePrefix = possibleTableNamePrefix;
            }
        }

        return tableNamePrefix;
    }

    private void buildStaticTopology(Map<String, String> staticTopology) {
        Map<String, Set<String>> topologys = new HashMap<String, Set<String>>();
        for (Map.Entry<String, String> entry : staticTopology.entrySet()) {
            buildTopology(topologys, entry.getKey(), entry.getValue());
        }

        this.staticTopology = topologys;
    }

    private void buildExtTopology(Map<String, Set<String>> topologys) {
        if (extPartitions == null) {
            return;
        }
        if (topologys == null) {
            topologys = new HashMap<>();
        }
        for (int i = 0; i < extPartitions.size(); i++) {
            final MappingRule mappingRule = extPartitions.get(i);
            final String db = mappingRule.getDb();
            final String tb = mappingRule.getTb();
            final String dbKeyValue = mappingRule.getDbKeyValue();
            final String tbKeyValue = mappingRule.getTbKeyValue();
            if (!topologys.containsKey(db)) {
                Set<String> tables = new HashSet<String>();
                topologys.put(db, tables);
            }
            if (StringUtils.isEmpty(tbKeyValue)) {
                final Set<MappingRule> set = new HashSet<>();
                HashMap<String, Set<Object>> enumerates = new HashMap<>();
                final Rule<String> stringRule = dbShardRules.get(0);
                final String column = stringRule.getRuleColumns().keySet().iterator().next();
                Object realValue = dbKeyValue;
                if (stringRule.getExpression().toLowerCase().startsWith("uni_hash")) {
                    final RuleColumn ruleColumn = ((ExtPartitionGroovyRule) stringRule).getRuleColumns().get(column);
                    if (ruleColumn instanceof AdvancedParameter) {
                        final AtomIncreaseType atomicIncreateType = ((AdvancedParameter) ruleColumn).atomicIncreateType;
                        if (AtomIncreaseType.NUMBER == atomicIncreateType) {
                            try {
                                realValue = Long.valueOf(dbKeyValue);
                            } catch (NumberFormatException e) {
                                throw new IllegalArgumentException("Hot rule not support value for " + dbKeyValue);
                            }
                        }
                    }
                }
                final HashSet values = new HashSet();
                values.add(realValue);
                enumerates.put(column, values);
                for (Map<String, Object> sample : new Samples(enumerates)) { // 遍历笛卡尔抽样
                    String value = stringRule.eval(sample, outerContext, null);
                    final Set<String> strings = actualTopology.get(value);
                    topologys.get(db).addAll(strings);
                    mappingRule.setTbs(strings);
                }

            } else {
                topologys.get(db).add(tb);

            }
        }
    }

    private void buildExtMapping() {
        if (extPartitions == null) {
            return;
        }
        extDbKeyToMappingRules = new HashMap<>();
        extTbKeyToMappingRules = new HashMap<>();
        for (int i = 0; i < extPartitions.size(); i++) {
            final MappingRule mappingRule = extPartitions.get(i);
            final String db = mappingRule.getDb();
            final String tb = mappingRule.getTb();
            final String dbKeyValue = mappingRule.getDbKeyValue();
            final String tbKeyValue = mappingRule.getTbKeyValue();
            // set DB key to Mapping rule
            Set<MappingRule> mappingRules = extDbKeyToMappingRules.get(dbKeyValue);
            if (mappingRules == null) {
                mappingRules = new HashSet<>();
                extDbKeyToMappingRules.put(dbKeyValue, mappingRules);
            }
            mappingRules.add(mappingRule);
            // set TB key to Mapping rule
            if (StringUtils.isNotEmpty(tbKeyValue)) {
                final Set<MappingRule> set = new HashSet<>();
                set.add(mappingRule);
                extTbKeyToMappingRules.put(tbKeyValue, set);
            }
        }
    }

    private void buildTopology(Map<String, Set<String>> topologys, String entryKey, String entryValue) {
        List<String> keys = buildSlotStrings(entryKey);
        List<String> values = buildSlotStrings(entryValue);
        if (keys.size() > 1 && values.size() > 1) {
            throw new IllegalArgumentException("key/value size > 1 , keys : " + entryKey + " , values : " + entryValue);
        } else if (keys.size() > 1) {
            // 只分库不分表
            String value = tbNamePattern;
            if (values.size() == 1) {
                value = values.get(0);
            }
            for (String key : keys) {
                Set<String> tables = new HashSet<String>();
                tables.add(value);
                topologys.put(key, tables);
            }
        } else if (values.size() > 1) {
            // 只分表不分库TB
            String key = dbNamePattern;
            if (keys.size() == 1) {
                key = keys.get(0);
            }

            topologys.put(key, new HashSet<String>(values));
        } else if (keys.size() == 1 || values.size() == 1) {
            // 只处理单表
            String key = dbNamePattern;
            String value = tbNamePattern;
            if (keys.size() == 1) {
                key = keys.get(0);
            }

            if (values.size() == 1) {
                value = values.get(0);
            }
            Set<String> tables = new HashSet<String>();
            tables.add(value);
            topologys.put(key, tables);
        }
    }

    /**
     * 解析 user_0,user_[1-128],user_extra
     */
    private List<String> buildSlotStrings(String value) {
        List<String> values = new ArrayList<String>();
        String[] pieces = StringUtils.split(StringUtils.trim(value), ',');
        for (String piece : pieces) {
            // 首先匹配user_[0000-0128]_extra
            Matcher matcher = TOPOLOGY_PATTERN.matcher(piece);
            if (matcher.matches()) {
                String prefix = matcher.group(1);
                String startStr = matcher.group(3);
                String ednStr = matcher.group(4);
                int start = Integer.valueOf(startStr);
                int end = Integer.valueOf(ednStr);
                String postfix = matcher.group(5);
                for (int i = start; i <= end; i++) {
                    StringBuilder builder = new StringBuilder(value.length());
                    String str = String.valueOf(i);
                    // 处理0001类型
                    if (startStr.length() == ednStr.length() && startStr.startsWith("0")) {
                        str = StringUtils.leftPad(String.valueOf(i), startStr.length(), '0');
                    }

                    builder.append(prefix).append(str).append(postfix);
                    values.add(builder.toString());
                }
            } else {
                values.add(piece);
            }
        }

        return values;
    }

    public int getActualTbCount() {
        return actualTbCount;
    }

    public void setActualTbCount(int actualTbCount) {
        this.actualTbCount = actualTbCount;
    }

    public int getActualDbCount() {
        return actualDbCount;
    }

    public void setActualDbCount(int actualDbCount) {
        this.actualDbCount = actualDbCount;
    }

    public ShardFunctionMeta getDbShardFunctionMeta() {
        return dbShardFunctionMeta;
    }

    public void setDbShardFunctionMeta(ShardFunctionMeta dbShardFunctionMeta) {
        this.dbShardFunctionMeta = dbShardFunctionMeta;
    }

    public ShardFunctionMeta getTbShardFunctionMeta() {
        return tbShardFunctionMeta;
    }

    public void setTbShardFunctionMeta(ShardFunctionMeta tbShardFunctionMeta) {
        this.tbShardFunctionMeta = tbShardFunctionMeta;
    }

    public Map<String, Object> getDbShardFunctionMetaMap() {
        return dbShardFunctionMetaMap;
    }

    public void setDbShardFunctionMetaMap(Map<String, Object> dbShardFunctionMetaMap) {
        this.dbShardFunctionMetaMap = dbShardFunctionMetaMap;
    }

    public Map<String, Object> getTbShardFunctionMetaMap() {
        return tbShardFunctionMetaMap;
    }

    public void setTbShardFunctionMetaMap(Map<String, Object> tbShardFunctionMetaMap) {
        this.tbShardFunctionMetaMap = tbShardFunctionMetaMap;
    }

    public String[] getTbNamesOfExtraDb() {
        return tbNamesOfExtraDb;
    }

    public void setTbNamesOfExtraDb(String[] tbNamesOfExtraDb) {
        this.tbNamesOfExtraDb = tbNamesOfExtraDb;
    }

    public String[] getDbNamesOfExtraDb() {
        return dbNamesOfExtraDb;
    }

    public void setDbNamesOfExtraDb(String[] dbNamesOfExtraDb) {
        this.dbNamesOfExtraDb = dbNamesOfExtraDb;
    }

    public List<MappingRule> getExtPartitions() {
        return extPartitions;
    }

    public void setExtPartitions(List<MappingRule> extPartitions) {
        this.extPartitions = extPartitions;
        if (extPartitions != null && extPartitions.size() > 0) {
            Collections.sort(extPartitions);
        }
    }

    public Map<String, Set<MappingRule>> getExtDbKeyToMappingRules() {
        return extDbKeyToMappingRules;
    }

    public Map<String, Set<MappingRule>> getExtTbKeyToMappingRules() {
        return extTbKeyToMappingRules;
    }

    public int getRuleDbCount() {
        final Map<String, Set<String>> actualTopology = getActualTopology();
        Assert.assertTrue(actualTopology.size() >= ruleDbCount);
        return ruleDbCount;
    }

    public int getExtDbCount() {
        final Map<String, Set<String>> actualTopology = getActualTopology();
        Assert.assertTrue(actualTopology.size() >= ruleDbCount);
        return extDbCount;
    }

    public int getRuleTbCount() {
        final Map<String, Set<String>> actualTopology = getActualTopology();
        Assert.assertTrue(actualTopology.size() >= ruleDbCount);
        return ruleTbCount;
    }

    public int getExtTbCount() {
        final Map<String, Set<String>> actualTopology = getActualTopology();
        Assert.assertTrue(actualTopology.size() >= ruleDbCount);
        return extTbCount;
    }

    public boolean isRandomTableNamePatternEnabled() {
        return randomTableNamePatternEnabled;
    }

    public void setRandomTableNamePatternEnabled(boolean randomTableNamePatternEnabled) {
        this.randomTableNamePatternEnabled = randomTableNamePatternEnabled;
    }

    public String getExistingRandomSuffixForRecovery() {
        return existingRandomSuffixForRecovery;
    }

    public void setExistingRandomSuffixForRecovery(String existingRandomSuffixForRecovery) {
        this.existingRandomSuffixForRecovery = existingRandomSuffixForRecovery;
    }

    public String getTableNamePrefixForShadowTable() {
        return tableNamePrefixForShadowTable;
    }

    public void setTableNamePrefixForShadowTable(String tableNamePrefixForShadowTable) {
        this.tableNamePrefixForShadowTable = tableNamePrefixForShadowTable;
    }
}
