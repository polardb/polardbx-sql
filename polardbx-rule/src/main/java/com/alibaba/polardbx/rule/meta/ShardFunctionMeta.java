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

package com.alibaba.polardbx.rule.meta;

import java.util.List;
import java.util.Map;

import com.alibaba.polardbx.rule.TableRule;

/**
 * @author chenghui.lch 2017年6月6日 下午1:54:06
 * @since 5.0.0
 */
public interface ShardFunctionMeta {

    public static final int SHARD_TYPE_DB = 0;

    public static final int SHARD_TYPE_TB = 1;

    public static final int SHARD_TYPE_DUAL = 999;

    public static final String SHARD_COLUMN_DUAL = "?";

    public static final String DB_SHARD_FUNC_KEY = "dbShardFunction";

    public static final String TB_SHARD_FUNC_KEY = "tbShardFunction";

    /**
     * 返回所使用的DRDS内部拆分函数的名字，用加以区分不同的拆分函数, 全局唯一, 通常是最终是写在规则里内部的groovy函数
     */
    public String getRuleShardFuncionName();

    /**
     * 返回所使用的DRDS内部拆分函数的详细细节，包括入参列表
     */
    public String getRuleShardFuncionDetails();

    /**
     * 返回用户ShowCreate时写入的拆分函数名字，不一定唯一
     */
    public String getCreateTableShardFuncionName();

    /**
     * 返回最终的保存在规则里的分库分表函数的字符串
     */
    public List<String> buildRuleShardFunctionStr();

    /**
     * 返回最终的保存在规则里的分库分表函数的字符串, 但是将列名统一替换为了'?',用于两个表的规则（或分库拆分方式与分表拆分方式）是否完全一致
     */
    public List<String> buildRuleShardFunctionStrIgnoreShardKey();

    /**
     * 返回ShowCreateTable 的 partition 字符串, 专用于ShowCreataTable语句
     */
    public String buildCreateTablePartitionStr();

    /**
     * 返回用户建表DDL的拆分函数及其参数，可用于对比分库的拆分方式与分表的拆分方式是否完全一致
     */
    public String buildCreateTablePartitionFunctionStr();

    /**
     * 设置函数的ShardFunMeta的相关参数
     *
     * <pre>
     *  注意，这里的 params 是指用户通过DDL向DRDS传入的参数，包括用户填写的拆分列列名
     * </pre>
     */
    public void initFunctionParams(List<String> params);

    /**
     * 初始化规则FuncionMeta，使用FuncionMeta需要进行init做些预备工作
     *
     * <pre>
     *
     * <pre/>
     */
    public void initShardFuncionMeta(TableRule rule);

    /**
     * 获取拆分函数所需要所有切分键列表
     */
    public List<String> getShardKeyList();

    /**
     * 获取分库数目
     */
    public Integer getDbCount();

    public void setDbCount(Integer dbCount);

    /**
     * 获取各个分库的物理分表数
     */
    public Integer getTbCountEachDb();

    public void setTbCountEachDb(Integer tbCountEachDb);

    /**
     * 获取切分类型，标识当前函数是用于分库还是分表，0:表示分库，1：表示分表
     */
    public Integer getShardType();

    public void setShardType(Integer shardType);

    /**
     * 获取物理分表的下标是否为全局唯一（各分库不重复）
     *
     * @return true：表示各分库的分表下标会重复；false: 表示各分库的分表下标不会重复
     */
    public Boolean getTbStandAlone();

    public void setTbStandAlone(Boolean tbStandAlone);

    /**
     * 设置参数对应的类型
     */
    public void setParamType(List<Class> paramTypes);

    /**
     * 检查insert时输入的相关参数
     */
    public void checkOnInsert(Map<String, Object> params);

    /**
     * 检测ShardFunMeta的内部参数合法性，通常用于建表时检测用户输入的参数是否合法
     */
    public void verifyFunMetaParams();

    /**
     * 检测当前的shardingMeta与目标的otherShardingMeta的拆分方式(或路由算法)是否完全一致（通常忽略列名或一些不影响会路由结果的其它参数）
     */
    public boolean isShardingEquals(ShardFunctionMeta otherShardingMeta);

    /**
     * 检测当前的shardingMeta与目标的otherShardingMeta的内容是否完全一致（不会忽略列名）
     */
    public boolean isRuleEquals(ShardFunctionMeta otherShardingMeta);

}
