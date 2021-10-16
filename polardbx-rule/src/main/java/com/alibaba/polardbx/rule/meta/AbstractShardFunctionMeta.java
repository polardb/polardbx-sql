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

import com.alibaba.polardbx.rule.TableRule;

/**
 * @author chenghui.lch 2017年7月12日 上午11:14:59
 * @since 5.0.0
 */
public abstract class AbstractShardFunctionMeta implements ShardFunctionMeta {

    protected static final String ruleShardFunctionFormat = "%s(%s)";

    /**
     * 所有的库分库数
     */
    protected Integer dbCount;

    /**
     * 平均每个分库的物理分表数目
     */
    protected Integer tbCountEachDb;

    /**
     * 标识函数用于切分DB还是切分TB
     */
    protected Integer shardType;

    /**
     * 标识物理表名是否为全局唯一, true: 表示全局不唯一；false: 表示全局唯一
     */
    protected Boolean tbStandAlone;

    @Override
    public void initShardFuncionMeta(TableRule rule) {
    }

    @Override
    public Integer getDbCount() {
        return dbCount;
    }

    @Override
    public void setDbCount(Integer dbCount) {
        this.dbCount = dbCount;
    }

    @Override
    public Integer getTbCountEachDb() {
        return tbCountEachDb;
    }

    @Override
    public void setTbCountEachDb(Integer tbCountEachDb) {
        this.tbCountEachDb = tbCountEachDb;
    }

    @Override
    public Integer getShardType() {
        return shardType;
    }

    @Override
    public void setShardType(Integer shardType) {
        this.shardType = shardType;
    }

    @Override
    public Boolean getTbStandAlone() {
        return tbStandAlone;
    }

    @Override
    public void setTbStandAlone(Boolean tbStandAlone) {
        this.tbStandAlone = tbStandAlone;
    }

    @Override
    public void verifyFunMetaParams() {
    }

    @Override
    public boolean isShardingEquals(ShardFunctionMeta otherShardingMeta) {
        return ruleShardingEqualsInner(otherShardingMeta, true);
    }

    @Override
    public boolean isRuleEquals(ShardFunctionMeta otherShardingMeta) {
        return ruleShardingEqualsInner(otherShardingMeta, false);
    }

    protected abstract boolean ruleShardingEqualsInner(ShardFunctionMeta otherShardFunMeta, boolean isIgnoredShardKey);
}
