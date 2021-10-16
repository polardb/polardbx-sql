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
import com.alibaba.polardbx.rule.virtualnode.DBTableMap;
import com.alibaba.polardbx.rule.virtualnode.TableSlotMap;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * 对应月tddl中的一张逻辑表，每张逻辑表上存在db/tb的{@linkplain Rule}<br/>
 * 基于该静态Rule(利用枚举步长和次数)可简单推算出数据库拓扑结构
 *
 * @author linxuan
 */
public interface VirtualTableRule<D, T> {

    /**
     * 库规则链
     */
    List<Rule<String>> getDbShardRules();

    /**
     * 表规则链
     */
    List<Rule<String>> getTbShardRules();

    /**
     * 返回本规则实际对应的全部库表拓扑结构
     *
     * @return key:dbIndex; value:实际物理表名的集合
     */
    Map<String, Set<String>> getActualTopology();

    /**
     * 根据一些特定参数, 返回本规则实际对应的库表拓扑结构
     *
     * @return key:dbIndex; value:实际物理表名的集合
     */
    Map<String, Set<String>> getActualTopology(Map<String, Object> topologyParams);

    Object getOuterContext();

    public TableSlotMap getTableSlotMap();

    public DBTableMap getDbTableMap();

    // =========================================================================
    // 规则和其他属性的分割线
    // =========================================================================

    DBType getDbType();

    boolean isAllowFullTableScan();

    public String getTbNamePattern();

    public String getDbNamePattern();

    public String[] getDbRuleStrs();

    public String[] getTbRulesStrs();

    public boolean isBroadcast();

    public String getJoinGroup();

    // ==================== 获取rule的一些信息，比如分区字段 =================

    /**
     * 获取当前默认版本，逻辑表的分区字段，如果存在多个规则时，返回多个规则分区字段的总和
     */
    public List<String> getShardColumns();

    /**
     * 兼容精卫对拆分字段大写的需求
     */
    public List<String> getUpperShardColumns();
}
