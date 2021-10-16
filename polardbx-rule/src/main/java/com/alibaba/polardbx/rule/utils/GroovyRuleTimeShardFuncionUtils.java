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

package com.alibaba.polardbx.rule.utils;

import java.util.List;
import java.util.Map;

import com.alibaba.polardbx.rule.Rule;
import com.alibaba.polardbx.rule.TableRule;
import com.alibaba.polardbx.rule.ddl.PartitionByType;
import com.alibaba.polardbx.rule.ddl.PartitionByTypeUtils;
import com.alibaba.polardbx.rule.impl.WrappedGroovyRule;
import com.alibaba.polardbx.rule.model.AdvancedParameter;

/**
 * @author chenmo.cm
 * @date 2019/4/17 上午11:24
 */
public class GroovyRuleTimeShardFuncionUtils {


    protected static String DB_PARTITION_FORMAT = "dbpartition by %s(`%s`)";

    protected static String TB_PARTITION_FORMAT = "tbpartition by %s(`%s`) tbpartitions %s";

    /**
     * 检测下
     *
     * @param tableRule
     * @param dbOrTb
     * @return
     */
    public static boolean isGroovyRuleTimeShardFuncion(TableRule tableRule, String columnName, boolean dbOrTb ) {

        List<Rule> rules = tableRule.getDbShardRules();
        if (!dbOrTb) {
            rules = tableRule.getTbShardRules();
        }

        if (rules == null) {
            return false;
        }

        if (rules.size() == 0) {
            return false;
        }

        Object o = rules.get(0);
        if (!(o instanceof WrappedGroovyRule)) {
            throw new UnsupportedOperationException("Not supported 'if-else' or other rules ");
        }

        WrappedGroovyRule rule = (WrappedGroovyRule) o;
        Map<String, Rule.RuleColumn> ruleColumnMap = rule.getRuleColumns();

        if (ruleColumnMap.isEmpty() || ruleColumnMap.size() != 1) {
            throw new UnsupportedOperationException("Not supported form");
        }

        Rule.RuleColumn ruleColumn = ruleColumnMap.get(columnName);
        if (!(ruleColumn instanceof AdvancedParameter)) {
            return false;
        }

        AdvancedParameter.AtomIncreaseType type = ((AdvancedParameter) ruleColumn).atomicIncreateType;
        if (type.isTime() || type.isNoloopTime()) {
            return true;
        }

        return false;

    }

    /**
     *
     * @param tableRule
     * @param dbOrTb  db is true ,or tb is false
     * @return
     */
    public static String buildCreateTablePartitionStr(TableRule tableRule, long dbCount, long tbCountPerGroup, String columnName, boolean dbOrTb) {

        String partitionStr = "Unkown";

        List<Rule> rules = tableRule.getDbShardRules();
        if (!dbOrTb) {
            rules = tableRule.getTbShardRules();
        }

        Object o = rules.get(0);
        WrappedGroovyRule rule = (WrappedGroovyRule) o;
        Map<String, Rule.RuleColumn> ruleColumnMap = rule.getRuleColumns();
        Rule.RuleColumn ruleColumn = ruleColumnMap.get(columnName);

        String hashFuncType = ((AdvancedParameter)ruleColumn).getHashFuncType();
        PartitionByType partitionByType = PartitionByTypeUtils.getPartitionTypeByGroovyRuleTimeMethodName(hashFuncType);

        String ruleColumnName = ruleColumn.key;
        switch ( partitionByType ) {
            case DD:
            case MM:
            case WEEK:
            case MMDD:
            case YYYYDD:
            case YYYYMM:
            case YYYYWEEK:
            case YYYYDD_OPT:
            case YYYYMM_OPT:
            case YYYYWEEK_OPT:
                if (dbOrTb) {
                    // MMDD 函数不允许分表, 这里是为了兼容历史
                    partitionStr = String.format(DB_PARTITION_FORMAT, partitionByType.name(), ruleColumnName);
                } else {
                    partitionStr = String.format(TB_PARTITION_FORMAT, partitionByType.name(), ruleColumnName, tbCountPerGroup);
                }
                break;

        }

        return partitionStr;
    }
}
