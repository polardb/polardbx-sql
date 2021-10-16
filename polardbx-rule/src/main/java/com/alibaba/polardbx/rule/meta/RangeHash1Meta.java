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

import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.rule.TableRule;
import com.alibaba.polardbx.rule.Rule;
import com.alibaba.polardbx.rule.Rule.RuleColumn;
import com.alibaba.polardbx.rule.exception.TddlRuleException;
import com.alibaba.polardbx.rule.impl.groovy.GroovyStaticMethod;
import com.alibaba.polardbx.rule.model.AdvancedParameter;
import com.alibaba.polardbx.rule.model.AdvancedParameter.AtomIncreaseType;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * 多列规则函数
 *
 * @author jilong.ljl
 * @since 5.0.0
 */
public class RangeHash1Meta extends AbstractShardFunctionMeta {

    /**
     * 拆分键的列名
     */
    protected List<String> shardKeyNames = new LinkedList<String>();

    protected String isString;

    protected Integer rangeCount;

    protected List<Class> paramTypeList;

    protected String shardKeys;

    @Override
    public String getRuleShardFuncionName() {
        return "range_hash";
    }

    @Override
    public String getCreateTableShardFuncionName() {
        return "RANGE_HASH1";
    }

    @Override
    public String getRuleShardFuncionDetails() {

        return "range_hash1(column1, column2, rangeCount)";
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

        long enumTime = 0;
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

        String shardKeyStr;
        for (String shardKeyName : shardKeyNames) {

            Integer shardTypeVal = shardType;
            String shardKeyNameStr = shardKeyName.toUpperCase();
            if (ignoredShardKey) {
                shardKeyNameStr = SHARD_COLUMN_DUAL;
                shardTypeVal = SHARD_TYPE_DUAL;
            }

            if ("true".equalsIgnoreCase(isString)) {
                shardKeyStr = String.format("#%s, %s, %s#", shardKeyNameStr, enumStepLen, enumTime);
            } else {
                shardKeyStr = String.format("#%s, %s_number, %s#", shardKeyNameStr, enumStepLen, enumTime);
            }

            String ruleShardFuncionParams = String.format("%s, %s, %s, %s, %s, %s",
                shardKeyStr,
                rangeCount,
                dbCount,
                tbCountEachDb,
                shardTypeVal,
                tbStandAlone);

            String finalRuleShardFunStr = String.format(ruleShardFunctionFormat,
                ruleShardFuncionName,
                ruleShardFuncionParams);
            rsList.add(finalRuleShardFunStr);

        }

        return rsList;
    }

    @Override
    public String buildCreateTablePartitionFunctionStr() {

        String createTablePartitionFunctionName = getCreateTableShardFuncionName();

        String createTablePartitionFuncStr = "";
        createTablePartitionFuncStr = String.format("%s(`%s`, `%s`, %s)",
            createTablePartitionFunctionName,
            shardKeys.split(",")[0],
            shardKeys.split(",")[1],
            rangeCount);

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
    public void setParamType(List<Class> paramTypes) {
        paramTypeList = paramTypes;
        Class clz = paramTypes.get(0);
        if (clz == Number.class) {
            isString = "false";
        } else if (clz == String.class) {
            isString = "true";
        } else {
            throw new TddlRuleException("range hash only support string or number type column");
        }
    }

    @Override
    protected boolean ruleShardingEqualsInner(ShardFunctionMeta otherShardFunMeta, boolean isIgnoredShardKey) {

        if (!(otherShardFunMeta instanceof RangeHash1Meta)) {
            return false;
        }

        RangeHash1Meta otherRangeHashMeta = (RangeHash1Meta) otherShardFunMeta;
        if (this.tbStandAlone != otherRangeHashMeta.getTbStandAlone()) {
            return false;
        }

        if (this.dbCount != otherRangeHashMeta.getDbCount()) {
            return false;
        }

        if (this.tbCountEachDb != otherRangeHashMeta.getTbCountEachDb()) {
            return false;
        }

        if (this.shardType != otherRangeHashMeta.getShardType()) {
            return false;
        }

        if (this.rangeCount != otherRangeHashMeta.getRangeCount()) {
            return false;
        }

        if (!TStringUtil.equalsIgnoreCase(this.isString, otherRangeHashMeta.getIsString())) {
            return false;
        }

        if (!isIgnoredShardKey) {
            if (!TStringUtil.equalsIgnoreCase(this.shardKeys, otherRangeHashMeta.getShardKeys())) {
                return false;
            }
        }

        return true;
    }

    @Override
    public void checkOnInsert(Map<String, Object> params) {
        Long num = null;
        for (String paramName : shardKeys.split(",")) {
            String paramVal = TStringUtil.objToString(params.get(paramName.toLowerCase()));
            if (num == null) {
                num = GroovyStaticMethod.range_hash(paramVal, rangeCount, dbCount, tbCountEachDb, shardType, true);
            } else if (num != GroovyStaticMethod.range_hash(paramVal,
                rangeCount,
                dbCount,
                tbCountEachDb,
                shardType,
                true)) {
                throw new TddlRuleException("Range hash has mul rules, insert shard columns must be equal by rule.");
            }
        }
    }

    @Override
    public void initFunctionParams(List<String> params) {

        if (params.size() < 3) {
            throw new TddlRuleException("range hash params error , current  :" + params);
        }
        String colmun1 = params.get(0);
        String colmun2 = params.get(1);
        shardKeyNames.add(colmun1);
        shardKeyNames.add(colmun2);

        rangeCount = Integer.valueOf(params.get(2));
        shardKeys = colmun1 + "," + colmun2;
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

                Map<String, RuleColumn> rcMaps = ruleObj.getRuleColumns();
                for (Map.Entry<String, RuleColumn> ruleItem : rcMaps.entrySet()) {
                    if (ruleItem instanceof AdvancedParameter) {
                        AdvancedParameter apRuleItem = (AdvancedParameter) ruleItem;
                        if (apRuleItem.atomicIncreateType == AtomIncreaseType.STRING) {
                            this.isString = "true";
                        } else if (apRuleItem.atomicIncreateType == AtomIncreaseType.NUMBER
                            || apRuleItem.atomicIncreateType == AtomIncreaseType.NUMBER_ABS) {
                            this.isString = "false";
                        }
                        // 以找到的第1个为准，RANGE_HASH要求两个拆分列的类型必须一样（要么是str,要么是num）
                        break;
                    }
                }
            }
        }
    }

    @Override
    public List<String> getShardKeyList() {
        return shardKeyNames;
    }

    public Integer getRangeCount() {
        return rangeCount;
    }

    public void setRangeCount(Integer rangeCount) {
        this.rangeCount = rangeCount;
    }

    public List<Class> getParamTypeList() {
        return paramTypeList;
    }

    public void setParamTypeList(List<Class> paramTypeList) {
        this.paramTypeList = paramTypeList;
    }

    public String getShardKeys() {
        return shardKeys;
    }

    public void setShardKeys(String shardKeys) {
        this.shardKeys = shardKeys;
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

    public String getIsString() {
        return isString;
    }

    public void setIsString(String string) {
        isString = string;
    }

}
