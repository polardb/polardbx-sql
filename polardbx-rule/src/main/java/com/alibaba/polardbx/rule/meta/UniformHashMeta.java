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

import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.rule.Rule;
import com.alibaba.polardbx.rule.TableRule;
import com.alibaba.polardbx.rule.exception.TddlRuleException;
import com.alibaba.polardbx.rule.model.AdvancedParameter;
import com.alibaba.polardbx.rule.model.AdvancedParameter.AtomIncreaseType;

/**
 * 多列规则函数
 *
 * @author minggong 2017年9月21日 下午3:25:23
 * @since 5.1.25
 */
public class UniformHashMeta extends AbstractShardFunctionMeta {

    /**
     * 拆分键的列名
     */
    protected List<String> shardKeyNames = new LinkedList<String>();

    protected String shardKey;

    protected String isString;

    int eval(int i) {
        return i % 5;
    }

    @Override
    public String getRuleShardFuncionName() {
        return "uni_hash";
    }

    @Override
    public String getCreateTableShardFuncionName() {
        return "UNI_HASH";
    }

    @Override
    public String getRuleShardFuncionDetails() {
        return "uni_hash(column)";
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

        long enumStepLen = 1;

        long enumTime;
        if (tbStandAlone) {
            if (shardType == SHARD_TYPE_DB) {
                enumTime = dbCount;
            } else {
                enumTime = tbCountEachDb;
            }
        } else {
            enumTime = dbCount * tbCountEachDb;
        }

        List<String> rsList = new LinkedList<String>();

        String shardKeyNameStr = shardKey.toUpperCase();
        String shardKeyStr = null;
        Integer shardTypeVal = shardType;
        if (ignoredShardKey) {
            shardKeyNameStr = SHARD_COLUMN_DUAL;
            shardTypeVal = SHARD_TYPE_DUAL;
        }
        if ("true".equalsIgnoreCase(isString)) {
            shardKeyStr = String.format("#%s, %s, %s#", shardKeyNameStr, enumStepLen, enumTime);
        } else {
            shardKeyStr = String.format("#%s, %s_number, %s#", shardKeyNameStr, enumStepLen, enumTime);
        }

        String ruleShardFuncionParams = String.format("%s, %s, %s, %s, %s",
            shardKeyStr,
            dbCount,
            tbCountEachDb,
            shardTypeVal,
            tbStandAlone);

        String finalRuleShardFunStr = String.format(ruleShardFunctionFormat,
            ruleShardFuncionName,
            ruleShardFuncionParams);
        rsList.add(finalRuleShardFunStr);

        return rsList;
    }

    @Override
    public String buildCreateTablePartitionFunctionStr() {

        String createTablePartitionFunctionName = getCreateTableShardFuncionName();
        String createTablePartitionFuncStr = "";
        createTablePartitionFuncStr = String.format("%s(`%s`)", createTablePartitionFunctionName, shardKey);
        return createTablePartitionFuncStr;
    }

    @Override
    public String buildCreateTablePartitionStr() {

        String createTablePartitionFuncStr = buildCreateTablePartitionFunctionStr();

        String createTablePartitionStr = "";
        if (shardType == SHARD_TYPE_DB) {

            createTablePartitionStr = String.format("dbpartition by %s", createTablePartitionFuncStr);

        } else if (shardType == SHARD_TYPE_TB) {

            createTablePartitionStr = String.format("tbpartition by %s", createTablePartitionFuncStr);
            if (tbCountEachDb > 1) {
                createTablePartitionStr += String.format(" tbpartitions %s", tbCountEachDb);
            }

        }

        return createTablePartitionStr;
    }

    @Override
    public boolean ruleShardingEqualsInner(ShardFunctionMeta otherShardFunMeta, boolean isIgnoredShardKey) {

        if (!(otherShardFunMeta instanceof UniformHashMeta)) {
            return false;
        }

        UniformHashMeta otherUniHashMeta = (UniformHashMeta) otherShardFunMeta;
        if (this.tbStandAlone != otherUniHashMeta.getTbStandAlone()) {
            return false;
        }

        if (this.dbCount != otherUniHashMeta.getDbCount()) {
            return false;
        }

        if (this.tbCountEachDb != otherUniHashMeta.getTbCountEachDb()) {
            return false;
        }

        if (this.shardType != otherUniHashMeta.getShardType()) {
            return false;
        }

        if (!TStringUtil.equalsIgnoreCase(this.isString, otherUniHashMeta.getIsString())) {
            return false;
        }

        if (!isIgnoredShardKey) {
            if (!TStringUtil.equalsIgnoreCase(this.shardKey, otherUniHashMeta.getShardKey())) {
                return false;
            }
        }

        return true;
    }

    @Override
    public void setParamType(List<Class> paramTypes) {
        Class clz = paramTypes.get(0);
        if (clz == Number.class) {
            isString = "false";
        } else if (clz == String.class) {
            isString = "true";
        } else {
            throw new TddlRuleException("uni hash only support string or number type column");
        }
    }

    @Override
    public void checkOnInsert(Map<String, Object> params) {
    }

    @Override
    public void initFunctionParams(List<String> params) {

        if (params.size() < 1) {
            throw new TddlRuleException("range hash params error , current  :" + params);
        }
        shardKey = params.get(0);
        shardKeyNames.add(shardKey);
    }

    @Override
    public void initShardFuncionMeta(TableRule rule) {

        TableRule tableRule = rule;
        List<Rule<String>> ruleList = null;
        if (this.shardType == SHARD_TYPE_DB) {
            ruleList = tableRule.getDbShardRules();
        } else {
            ruleList = tableRule.getTbShardRules();
        }

        if (this.isString != null) {
            return;
        }

        if (ruleList != null) {
            int ruleSize = ruleList.size();
            for (int i = 0; i < ruleSize; ++i) {
                Rule<String> ruleObj = ruleList.get(i);

                Map<String, Rule.RuleColumn> rcMaps = ruleObj.getRuleColumns();
                for (Map.Entry<String, Rule.RuleColumn> ruleItem : rcMaps.entrySet()) {
                    if (ruleItem instanceof AdvancedParameter) {
                        AdvancedParameter apRuleItem = (AdvancedParameter) ruleItem;
                        if (apRuleItem.atomicIncreateType == AtomIncreaseType.STRING) {
                            this.isString = "true";
                        } else if (apRuleItem.atomicIncreateType == AtomIncreaseType.NUMBER
                            || apRuleItem.atomicIncreateType == AtomIncreaseType.NUMBER_ABS) {
                            this.isString = "false";
                        }
                    }
                }
            }
        }
    }

    @Override
    public List<String> getShardKeyList() {
        return shardKeyNames;
    }

    public static String getRuleShardFunctionFormat() {
        return ruleShardFunctionFormat;
    }

    public void setDbCount(Integer dbCount) {
        this.dbCount = dbCount;
    }

    public void setTbCountEachDb(Integer tbCountEachDb) {
        this.tbCountEachDb = tbCountEachDb;
    }

    public void setShardType(Integer shardType) {
        this.shardType = shardType;
    }

    public List<String> getShardKeyNames() {
        return shardKeyNames;
    }

    public void setShardKeyNames(List<String> shardKeyNames) {
        this.shardKeyNames = shardKeyNames;
    }

    public String getShardKey() {
        return shardKey;
    }

    public void setShardKey(String shardKey) {
        this.shardKey = shardKey;
    }

    public String getIsString() {
        return isString;
    }

    public void setIsString(String string) {
        isString = string;
    }

}
