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

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import com.alibaba.polardbx.common.utils.TStringUtil;

/**
 * 有符号二进制右移的函数描述
 *
 * @author chenghui.lch 2017年6月5日 下午1:40:04
 * @since 5.0.0
 */
public class BinRightShiftMeta extends AbstractShardFunctionMeta {

    protected static final String ruleShardFunctionFormat = "%s(%s)";

    /**
     * 拆分键的列名
     */
    protected String shardKeyName;

    /**
     * 二进制右移的位数
     */
    protected Integer shiftBitCount;

    protected List<Class> paramTypes;

    public BinRightShiftMeta() {
        shardKeyName = null;
        shiftBitCount = 0;
    }

    public Integer getShiftBitCount() {
        return shiftBitCount;
    }

    public void setShiftBitCount(Integer shiftBitCount) {
        this.shiftBitCount = shiftBitCount;
    }

    @Override
    public String getRuleShardFuncionName() {
        return "bin_right_shift";
    }

    @Override
    public String getCreateTableShardFuncionName() {

        return "RIGHT_SHIFT";
    }

    @Override
    public String getRuleShardFuncionDetails() {
        return "bin_right_shift(long shardKeyVal, int shiftBitCount, int dbCount, int tbCountEachDb,int shardType, boolean isTbStandAlone)";
    }

    @Override
    public List<String> buildRuleShardFunctionStrIgnoreShardKey() {
        return buildRuleShardFunctionStrInner(true);
    }

    @Override
    public List<String> buildRuleShardFunctionStr() {
        return buildRuleShardFunctionStrInner(false);
    }

    public List<String> buildRuleShardFunctionStrInner(boolean ignoredShardKey) {
        String ruleShardFuncionName = getRuleShardFuncionName();

        long enumStepLen = 1 << shiftBitCount;

        long enumTime = 0;
        if (shardType == SHARD_TYPE_DB) {
            enumTime = dbCount;
        } else {
            if (tbStandAlone) {
                enumTime = tbCountEachDb;
            } else {
                enumTime = dbCount * tbCountEachDb;
            }
        }

        Integer shardTypeVal = shardType;
        String shardKeyNameStr = shardKeyName.toUpperCase();
        if (ignoredShardKey) {
            shardKeyNameStr = SHARD_COLUMN_DUAL;
            shardTypeVal = SHARD_TYPE_DUAL;
        }
        String shardKeyStr = String.format("#%s, %s_number, %s#", shardKeyNameStr, enumStepLen, enumTime);
        String ruleShardFuncionParams = String.format("%s, %s, %s, %s, %s, %s",
            shardKeyStr,
            shiftBitCount,
            dbCount,
            tbCountEachDb,
            shardTypeVal,
            tbStandAlone);

        String finalRuleShardFunStr = String.format(ruleShardFunctionFormat,
            ruleShardFuncionName,
            ruleShardFuncionParams);

        List<String> rsList = new LinkedList<String>();
        rsList.add(finalRuleShardFunStr);
        return rsList;
    }

    @Override
    public String buildCreateTablePartitionFunctionStr() {

        String createTablePartitionFunctionName = getCreateTableShardFuncionName();

        String createTablePartitionFunStr = "";
        createTablePartitionFunStr = String.format("%s(`%s`, %s)",
            createTablePartitionFunctionName,
            shardKeyName,
            shiftBitCount);

        return createTablePartitionFunStr;
    }

    @Override
    public String buildCreateTablePartitionStr() {

        String createTablePartitionFunStr = buildCreateTablePartitionFunctionStr();

        String createTablePartitionStr = "";
        if (shardType == SHARD_TYPE_DB) {

            createTablePartitionStr = String.format("dbpartition by %s", createTablePartitionFunStr);
        } else if (shardType == SHARD_TYPE_TB) {
            createTablePartitionStr = String.format("tbpartition by %s", createTablePartitionFunStr);
            if (tbCountEachDb > 1) {
                createTablePartitionStr += String.format(" tbpartitions %s", tbCountEachDb);
            }
        }

        return createTablePartitionStr;
    }

    @Override
    public void setParamType(List<Class> paramTypes) {
        this.paramTypes = paramTypes;
    }

    @Override
    public void checkOnInsert(Map<String, Object> params) {
    }

    @Override
    public void initFunctionParams(List<String> params) {

        String shardKey = params.get(0);
        this.shardKeyName = shardKey;

        int shiftBitCount = Integer.valueOf(params.get(1));
        this.shiftBitCount = shiftBitCount;
    }

    public String getShardKeyName() {
        return shardKeyName;
    }

    public void setShardKeyName(String shardKeyName) {
        this.shardKeyName = shardKeyName;
    }

    @Override
    public List<String> getShardKeyList() {
        List<String> shardKeyList = new ArrayList<String>();
        shardKeyList.add(shardKeyName);
        return shardKeyList;
    }

    @Override
    protected boolean ruleShardingEqualsInner(ShardFunctionMeta otherShardFunMeta, boolean isIgnoredShardKey) {

        if (!(otherShardFunMeta instanceof BinRightShiftMeta)) {
            return false;
        }

        BinRightShiftMeta otherRightShfitHashMeta = (BinRightShiftMeta) otherShardFunMeta;
        if (this.tbStandAlone != otherRightShfitHashMeta.getTbStandAlone()) {
            return false;
        }

        if (this.dbCount != otherRightShfitHashMeta.getDbCount()) {
            return false;
        }

        if (this.tbCountEachDb != otherRightShfitHashMeta.getTbCountEachDb()) {
            return false;
        }

        if (this.shardType != otherRightShfitHashMeta.getShardType()) {
            return false;
        }

        if (this.shiftBitCount != otherRightShfitHashMeta.getShiftBitCount()) {
            return false;
        }

        if (!isIgnoredShardKey) {
            if (!TStringUtil.equalsIgnoreCase(this.shardKeyName, otherRightShfitHashMeta.getShardKeyName())) {
                return false;
            }
        }

        return true;

    }
}
