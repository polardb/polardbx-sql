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

import com.alibaba.polardbx.common.exception.TddlNestableRuntimeException;
import com.alibaba.polardbx.common.utils.TStringUtil;

/**
 * 字符串专用的HASH函数
 *
 * @author chenghui.lch 2017年6月5日 下午1:40:04
 * @since 5.0.0
 */
public class StringHashMeta extends AbstractShardFunctionMeta {

    protected static final String ruleShardFunctionFormat = "%s(%s)";

    public static Integer SUBSTR_TO_STR_TYPE_ID = 0;
    public static Integer SUBSTR_TO_INT_TYPE_ID = 1;

    public static Integer DEFAULT_RANDOM_SEED = -1;

    /**
     * 拆分键的列名
     */
    protected String shardKeyName;

    /**
     * 字符串的开始位置
     */
    protected Integer beginIndex;

    /**
     * 字符串的结束位置
     */
    protected Integer endIndex;

    /**
     * 类型ID, 表示对截取出来的子串，是当字符串进行HASH，还是当整数进行HASH
     */
    protected Integer typeId;

    /**
     * 字符串HASH函数的随机种子，默认是 31，但如果HASH不均衡，可以设为 31/131/13131/1313131/....
     * 当randomSeed为-1或<0时，会取默认种子31
     */
    protected Integer randomSeed;

    protected List<Class> paramTypes;

    public StringHashMeta() {
        shardKeyName = null;
        beginIndex = -1;
        endIndex = -1;
        typeId = 0;
        randomSeed = -1;
    }

    @Override
    public String getRuleShardFuncionName() {
        return "str_hash";
    }

    @Override
    public String getCreateTableShardFuncionName() {
        return "STR_HASH";
    }

    @Override
    public String getRuleShardFuncionDetails() {
        return "str_hash(long shardKeyVal,int beginIndex, int endIndex, int typeId, int seed, int dbCount, int tbCountEachDb,int shardType, boolean isTbStandAlone)";
    }

    @Override
    public List<String> buildRuleShardFunctionStrIgnoreShardKey() {
        return buildRuleShardFunctionStrInner(true);
    }

    @Override
    public List<String> buildRuleShardFunctionStr() {
        return buildRuleShardFunctionStrInner(false);
    }

    public List<String> buildRuleShardFunctionStrInner(boolean ignoredShardColumn) {
        String ruleShardFuncionName = getRuleShardFuncionName();

        long enumStepLen = 1;
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

        String shardKeyNameUpperCase = shardKeyName.toUpperCase();
        Integer shardTypeVal = shardType;
        if (ignoredShardColumn) {
            shardKeyNameUpperCase = SHARD_COLUMN_DUAL;
            shardTypeVal = SHARD_TYPE_DUAL;
        }

        String shardKeyStr = String.format("#%s, %s_strnum, %s#", shardKeyNameUpperCase, enumStepLen, enumTime);
        String ruleShardFuncionParams = String.format("%s, %s, %s, %s, %s, %s, %s, %s, %s",
            shardKeyStr,
            beginIndex,
            endIndex,
            typeId,
            randomSeed,
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
        String createTablePartitionFuncStr = "";
        createTablePartitionFuncStr = buildCreateTablePartitionStrWithParams(createTablePartitionFunctionName);
        return createTablePartitionFuncStr;
    }

    @Override
    public String buildCreateTablePartitionStr() {

        String createTablePartitionFuncStr = buildCreateTablePartitionFunctionStr();

        String createTablePartitionStr = "";
        String partitionType = "dbpartition";
        if (shardType == SHARD_TYPE_DB) {

            partitionType = "dbpartition";
            createTablePartitionStr = String.format("%s by %s", partitionType, createTablePartitionFuncStr);

        } else if (shardType == SHARD_TYPE_TB) {

            partitionType = "tbpartition";
            createTablePartitionStr = String.format("%s by %s", partitionType, createTablePartitionFuncStr);
            if (tbCountEachDb > 1) {
                createTablePartitionStr += String.format(" tbpartitions %s", tbCountEachDb);
            }

        }

        return createTablePartitionStr;
    }

    private String buildCreateTablePartitionStrWithParams(String createTablePartitionFunctionName) {
        String createTablePartitionStr;

        if (randomSeed > 0) {
            createTablePartitionStr = String.format("%s(`%s`, %s, %s, %s, %s)",
                createTablePartitionFunctionName,
                shardKeyName,
                beginIndex,
                endIndex,
                typeId,
                randomSeed);
            return createTablePartitionStr;
        } else if (typeId > 0) {
            createTablePartitionStr = String.format("%s(`%s`, %s, %s, %s)",
                createTablePartitionFunctionName,
                shardKeyName,
                beginIndex,
                endIndex,
                typeId);
        } else if (beginIndex > 0 || endIndex > 0) {
            createTablePartitionStr = String.format("%s(`%s`, %s, %s)",
                createTablePartitionFunctionName,
                shardKeyName,
                beginIndex,
                endIndex);
        } else {
            createTablePartitionStr = String.format("%s(`%s`)", createTablePartitionFunctionName, shardKeyName);
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

        int beginIndex = -1;
        int endIndex = -1;
        int typeId = StringHashMeta.SUBSTR_TO_STR_TYPE_ID;
        int seedVal = StringHashMeta.DEFAULT_RANDOM_SEED;

        if (params.size() >= 3) {
            beginIndex = Integer.valueOf(params.get(1));
            endIndex = Integer.valueOf(params.get(2));
        }

        if (params.size() >= 4) {
            typeId = Integer.valueOf(params.get(3));
        }

        if (params.size() >= 5) {
            seedVal = Integer.valueOf(params.get(4));
        }

        this.shardKeyName = shardKey;
        this.beginIndex = beginIndex;
        this.endIndex = endIndex;
        this.typeId = typeId;
        this.randomSeed = seedVal;
    }

    @Override
    public void verifyFunMetaParams() {

        if (beginIndex < -1 || endIndex < -1) {
            throw new TddlNestableRuntimeException("use invalid params of use str_hash function to create table");
        } else if (beginIndex > 0 && endIndex < 0) {

        } else if (endIndex > 0 && beginIndex < 0) {

        } else {
            if (endIndex < beginIndex) {
                throw new TddlNestableRuntimeException("use invalid params of use str_hash function to create table");
            }
        }

        if (typeId != SUBSTR_TO_STR_TYPE_ID && typeId != SUBSTR_TO_INT_TYPE_ID) {
            throw new TddlNestableRuntimeException("use invalid params of use str_hash function to create table");
        }

        if (randomSeed < 0 && randomSeed != DEFAULT_RANDOM_SEED) {
            throw new TddlNestableRuntimeException("use invalid params of use str_hash function to create table");
        }

        if (typeId == SUBSTR_TO_INT_TYPE_ID && randomSeed > 0) {
            throw new TddlNestableRuntimeException("use invalid params of use str_hash function to create table");
        }
    }

    @Override
    protected boolean ruleShardingEqualsInner(ShardFunctionMeta otherShardFunMeta, boolean isIgnoredShardKey) {

        if (!(otherShardFunMeta instanceof StringHashMeta)) {
            return false;
        }

        StringHashMeta otherStringHashMeta = (StringHashMeta) otherShardFunMeta;
        if (this.tbStandAlone != otherStringHashMeta.getTbStandAlone()) {
            return false;
        }

        if (this.dbCount != otherStringHashMeta.getDbCount()) {
            return false;
        }

        if (this.tbCountEachDb != otherStringHashMeta.getTbCountEachDb()) {
            return false;
        }

        if (this.shardType != otherStringHashMeta.getShardType()) {
            return false;
        }

        if (this.beginIndex != otherStringHashMeta.getBeginIndex()) {
            return false;
        }

        if (this.endIndex != otherStringHashMeta.getEndIndex()) {
            return false;
        }

        if (this.typeId != otherStringHashMeta.getTypeId()) {
            return false;
        }

        if (this.randomSeed != otherStringHashMeta.getRandomSeed()) {
            return false;
        }

        if (!isIgnoredShardKey) {
            if (!TStringUtil.equalsIgnoreCase(this.shardKeyName, otherStringHashMeta.getShardKeyName())) {
                return false;
            }
        }

        return true;
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

    public Integer getBeginIndex() {
        return beginIndex;
    }

    public void setBeginIndex(Integer beginIndex) {
        this.beginIndex = beginIndex;
    }

    public Integer getEndIndex() {
        return endIndex;
    }

    public void setEndIndex(Integer endIndex) {
        this.endIndex = endIndex;
    }

    public Integer getTypeId() {
        return typeId;
    }

    public void setTypeId(Integer typeId) {
        this.typeId = typeId;
    }

    public Integer getRandomSeed() {
        return randomSeed;
    }

    public void setRandomSeed(Integer randomSeed) {
        this.randomSeed = randomSeed;
    }

}
