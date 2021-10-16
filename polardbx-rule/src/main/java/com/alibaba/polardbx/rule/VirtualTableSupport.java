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
import com.alibaba.polardbx.common.model.lifecycle.AbstractLifecycle;
import com.alibaba.polardbx.common.model.lifecycle.Lifecycle;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.rule.impl.DbVirtualNodeRule;
import com.alibaba.polardbx.rule.impl.GroovyRule;
import com.alibaba.polardbx.rule.impl.TableVirtualNodeRule;
import com.alibaba.polardbx.rule.virtualnode.DBTableMap;
import com.alibaba.polardbx.rule.virtualnode.TableSlotMap;
import com.alibaba.polardbx.rule.impl.ExtPartitionGroovyRule;
import com.alibaba.polardbx.rule.utils.SimpleNamedMessageFormat;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * 对原类做一些重构，避免污染主类的可阅读性<br/>
 * 1. 抽取showTopology <br/>
 * 2. 抽取一些不常用的配置以及一些遗留的配置
 *
 * @author jianghang 2013-11-4 下午8:04:59
 * @since 5.0.0
 */
public abstract class VirtualTableSupport extends AbstractLifecycle implements Lifecycle, VirtualTableRule {

    protected static final Logger logger = LoggerFactory.getLogger(TableRule.class);
    protected static final String tableNameSepInSpring = ",";
    protected static final int showColsPerRow = 5;
    protected static final String TOPOLOGY_PATTERN_FORMAT = "(.*)(\\[(\\d+)\\-(\\d+)\\])(.*)";       // 匹配offer[1-128]
    protected static final Pattern TOPOLOGY_PATTERN = Pattern.compile(TOPOLOGY_PATTERN_FORMAT);

    /** ============ 一些老的用法 ================= **/
    /**
     * 用来替换dbRules、tbRules中的占位符
     * 优先用dbRuleParames，tbRuleParames替换，其为空时再用ruleParames替换
     */
    protected String[] ruleParames;
    protected String[] dbRuleParames;
    protected String[] tbRuleParames;
    protected boolean lazyInit = false;

    /**
     * ============ 运行时变量 =================
     **/
    protected DBType dbType = DBType.MYSQL;                            // Oracle|MySql
    protected String virtualTbName;                                                     // 逻辑表名
    protected Map<String, String> customTopology;
    protected Map<String, String> extTopology;
    protected Map<String, Set<String>> actualTopology;
    protected Map<String, Set<String>> actualTopologyOfExtraDb;

    // ==================== 参数处理方法 ====================
    protected void replaceWithParam(Object[] rules, String[] params) {
        if (params == null || rules == null) {
            return;
        }

        for (int i = 0; i < rules.length; i++) {
            if (rules[i] instanceof String) {
                rules[i] = replaceWithParam((String) rules[i], params);
            }
        }
    }

    private String replaceWithParam(String template, String[] params) {
        if (params == null || template == null) {
            return template;
        }
        if (params.length != 0 && params[0].indexOf(":") != -1) {
            // 只要params的第一个参数中含有冒号，就认为是NamedParam
            return replaceWithNamedParam(template, params);
        }
        return new MessageFormat(template).format(params);
    }

    /**
     * <pre>
     * 存在类似的变量配置，shardColumn:#business_id#.longValue()
     * </pre>
     */
    private String replaceWithNamedParam(String template, String[] params) {
        Map<String, String> args = new HashMap<String, String>();
        for (String param : params) {
            int index = param.indexOf(":");
            if (index == -1) {
                throw new IllegalArgumentException("使用名字化的占位符替换失败！请检查配置。 params:" + Arrays.asList(params));
            }
            args.put(param.substring(0, index).trim(), param.substring(index + 1).trim());
        }
        return new SimpleNamedMessageFormat(template).format(args);
    }

    /**
     * @param rules 规则字符串配置
     * @param namePattern 根据isTableRule选择dbNamePattern or tbNamePattern
     * @param extraPackagesStr groovy的自定义package
     * @param dbTableMap db虚拟节点
     * @param tableSlotMap table虚拟节点
     * @param isTableRule 是否为table规则
     */
    protected List<Rule<String>> convertToRuleArray(String[] rules, String namePattern,
                                                    String extraPackagesStr, DBTableMap dbTableMap,
                                                    TableSlotMap tableSlotMap,
                                                    Map<String, Set<MappingRule>> extKeyToMappingRules,
                                                    boolean isTableRule) {
        List<Rule<String>> ruleList = new ArrayList<Rule<String>>(1);
        if (null == rules) {// 没有rule定义
            // 一致性hash配置
            // 按照现在需求不可能为tableRule
            if (tableSlotMap != null && dbTableMap != null && !isTableRule) {
                ruleList.add(new DbVirtualNodeRule(this, TStringUtil.EMPTY, dbTableMap, extraPackagesStr, lazyInit));
                return ruleList;
            } else {
                return null;
            }
        }

        for (String rule : rules) {
            if (TStringUtil.isNotEmpty(namePattern)) {
                // 存在dbNamePattern/tbNamePattern配置

                ruleList.add(new ExtPartitionGroovyRule(this, rule, namePattern, extraPackagesStr, extKeyToMappingRules,
                    lazyInit));

                // ruleList.add(new WrappedGroovyRule(rule, namePattern,
                // extraPackagesStr, lazyInit));

            } else {
                // table存在一致性hash时，必须要有rule ??
                if (tableSlotMap != null && dbTableMap != null && isTableRule) {
                    ruleList.add(new TableVirtualNodeRule(this, rule, tableSlotMap, extraPackagesStr, lazyInit));
                } else {
                    ruleList.add(new GroovyRule<String>(this, rule, extraPackagesStr, lazyInit));
                }
            }
        }

        return ruleList;
    }

    protected List<String> trimRuleString(List<String> ruleStrings) {
        List<String> result = new ArrayList<String>(ruleStrings.size());
        for (String ruleString : ruleStrings) {
            result.add(ruleString.trim());
        }
        return result;
    }

    // ==================== print topology =====================

    protected void showTopology(boolean showMap) {
        showTopology(this.actualTopology, showMap);
        if (this.actualTopologyOfExtraDb != null) {
            if (this.actualTopologyOfExtraDb != null && !this.actualTopologyOfExtraDb.isEmpty()) {
                showTopology(this.actualTopologyOfExtraDb, showMap);
            }
        }
    }

    protected void showTopology(Map<String, Set<String>> topologyObj, boolean showMap) {
        int crossIndex, endIndex, maxcolsPerRow = showColsPerRow, maxtbnlen = 1, maxdbnlen = 1;
        for (Map.Entry<String, Set<String>> e : topologyObj.entrySet()) {
            int colsPerRow = colsPerRow(e.getValue(), showColsPerRow);
            if (colsPerRow > maxcolsPerRow) {
                maxcolsPerRow = colsPerRow;
            }
            if (e.getKey().length() > maxdbnlen) {
                maxdbnlen = e.getKey().length(); // dbIndex最大长度
            }
            for (String tbn : e.getValue()) {
                if (tbn.length() > maxtbnlen) {
                    maxtbnlen = tbn.length(); // tableName最大长度
                }
            }
        }
        crossIndex = maxdbnlen + 1;
        endIndex = crossIndex + (maxtbnlen + 1) * maxcolsPerRow + 1;
        if (logger.isInfoEnabled()) {
            logger.info("Before topology of the virtual table " + this.virtualTbName);
        }
        StringBuilder sb = new StringBuilder("The topology of the virtual table " + this.virtualTbName);
        addLine(sb, crossIndex, endIndex);
        for (Map.Entry<String/* 库 */, Set<String/* 表 */>> e : topologyObj.entrySet()) {
            sb.append("\n|");
            sb.append(fillAfter(e.getKey(), maxdbnlen)).append("|");
            int i = 0, n = e.getValue().size();
            for (String tb : e.getValue()) {
                sb.append(fillAfter(tb, maxtbnlen)).append(",");
                i++;
                if (i % maxcolsPerRow == 0 && i < n) {
                    sb.append("|\n|").append(fillAfter(" ", maxdbnlen)).append("|");// 折行后把库列输出
                }
            }
            if (i % maxcolsPerRow != 0) {
                int taillen = (maxcolsPerRow - (i % maxcolsPerRow)) * (maxtbnlen + 1) + 1;
                sb.append(fillBefore("|", taillen));
            } else {
                sb.append("|");
            }

            addLine(sb, crossIndex, endIndex);
        }
        sb.append("\n");

        if (logger.isInfoEnabled()) {
            logger.info(sb.toString());
        }

        if (!showMap) {
            return;
        }
        sb = new StringBuilder("\nYou could add below segement as the actualTopology property to ");
        sb.append(this.virtualTbName + "'s VirtualTable bean in the rule file\n\n");
        sb.append("        <property name=\"actualTopology\">\n");
        sb.append("          <map>\n");
        for (Map.Entry<String/* 库 */, Set<String/* 表 */>> e : topologyObj.entrySet()) {
            sb.append("            <entry key=\"").append(e.getKey()).append("\" value=\"");
            for (String table : e.getValue()) {
                sb.append(table).append(tableNameSepInSpring);
            }
            if (sb.charAt(sb.length() - 1) == tableNameSepInSpring.charAt(0)) {
                sb.deleteCharAt(sb.length() - 1);
            }
            sb.append("\" />\n");
        }
        sb.append("          </map>\n");
        sb.append("        </property>\n");

        if (logger.isDebugEnabled()) {
            logger.debug(sb.toString());
        }
    }

    private int colsPerRow(Collection<String> c, int maxColPerRow) {
        int n = c.size();
        if (n <= maxColPerRow) {
            return n;
        }
        int maxfiti = maxColPerRow; // 不能整除的情况下，让最后一行空白最少的那个i
        int minblank = maxColPerRow; // 不能整除的情况下，最后一行的空白个数
        for (int i = maxColPerRow; i > 0; i--) {
            int mod = n % i;
            if (mod == 0) {
                if (n / i <= i) {
                    return i; // 行数不能大于列数
                } else {
                    break;
                }
            } else {
                if (i - mod < minblank) {
                    minblank = i - mod;
                    maxfiti = i;
                }
            }
        }

        return maxfiti;
    }

    private String fillAfter(String str, int len) {
        if (str.length() < len) {
            for (int i = 0, n = len - str.length(); i < n; i++) {
                str = str + " ";
            }
        }
        return str;
    }

    private String fillBefore(String str, int len) {
        if (str.length() < len) {
            for (int i = 0, n = len - str.length(); i < n; i++) {
                str = " " + str;
            }
        }
        return str;
    }

    private void addLine(StringBuilder sb, int crossIndex, int endIndex) {
        sb.append("\n+");
        for (int i = 1; i <= endIndex; i++) {
            if (i == crossIndex || i == endIndex) {
                sb.append("+");
            } else {
                sb.append("-");
            }
        }
    }

    // ================== setter / getter ====================

    public void setRuleParames(String ruleParames) {
        if (ruleParames.indexOf('|') != -1) {
            // 优先用|线分隔,因为有些规则表达式中会有逗号
            this.ruleParames = ruleParames.split("\\|");
        } else {
            this.ruleParames = ruleParames.split(",");
        }
    }

    public void setRuleParameArray(String[] ruleParames) {
        this.ruleParames = ruleParames;
    }

    public void setDbRuleParames(String dbRuleParames) {
        this.dbRuleParames = dbRuleParames.split(",");
    }

    public void setDbRuleParameArray(String[] dbRuleParames) {
        this.dbRuleParames = dbRuleParames;
    }

    public void setTbRuleParames(String tbRuleParames) {
        this.tbRuleParames = tbRuleParames.split(",");
    }

    public void setTbRuleParameArray(String[] tbRuleParames) {
        this.tbRuleParames = tbRuleParames;
    }

    @Override
    public DBType getDbType() {
        return dbType;
    }

    public void setDbType(DBType dbType) {
        this.dbType = dbType;
    }

    public String getVirtualTbName() {
        return virtualTbName;
    }

    public void setVirtualTbName(String virtualTbName) {
        this.virtualTbName = virtualTbName;
    }

    @Override
    public Map<String, Set<String>> getActualTopology() {
        return actualTopology;
    }

    // @Override
    // public Map<String, Set<String>> getActualTopology(Map<String, Object>
    // topologyParams) {
    // throw new NotSupportException();
    // }

    public boolean isLazyInit() {
        return lazyInit;
    }

    public void setLazyInit(boolean lazyInit) {
        this.lazyInit = lazyInit;
    }

    public Map<String, String> getCustomTopology() {
        return customTopology;
    }

    public void setCustomTopology(Map<String, String> customTopology) {
        this.customTopology = customTopology;
    }

    public void setStaticTopology(Map<String, String> customTopology) {
        // 兼容下5.1.16版本的方法名
        this.customTopology = customTopology;
    }

    /**
     * 判断是否为物理拓扑中的表
     */
    public boolean isActualTable(String actualTable) {
        if (actualTable == null) {
            return false;
        }

        for (Set<String> tables : this.getActualTopology().values()) {
            for (String table : tables) {
                if (table.equalsIgnoreCase(actualTable)) {
                    return true;
                }
            }
        }

        return false;
    }
}
