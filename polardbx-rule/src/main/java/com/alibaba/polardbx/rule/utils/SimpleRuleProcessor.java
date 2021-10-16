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

import com.alibaba.polardbx.common.exception.TddlException;
import com.alibaba.polardbx.common.exception.TddlNestableRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.rule.TableRule;
import com.alibaba.polardbx.rule.model.TargetDB;
import com.alibaba.polardbx.rule.model.AdvancedParameter.AtomIncreaseType;
import org.apache.commons.lang.StringUtils;

import java.io.UnsupportedEncodingException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 特殊优化简单的规则，比如简单的数字和字符串hash转化为java直接调用，提升效率
 * <p>
 * <pre>
 * 匹配的简单规则：
 * 1. ((#{0},1,{1}#).longValue() % {2}).intdiv({3})
 * 2. (#{0},1,{1}#.hashCode().abs().longValue() % {2}).intdiv({3})
 * </pre>
 *
 * @author jianghang 2014年9月16日 下午5:23:07
 * @since 5.1.13
 */
public class SimpleRuleProcessor {

    private static final Logger logger = LoggerFactory
        .getLogger(SimpleRuleProcessor.class);
    private static final String TB_NUMBER_RULE_PATTERN_FORMAT =
        "^\\s*\\(?\\(?#([0-9a-zA-Z\\u4e00-\\u9fa5_-]+),1,(\\d+)#\\)?\\.longValue\\(\\)\\s*%\\s*(\\d+)\\)?\\s*$";
    private static final String DB_NUMBER_RULE_PATTERN_FORMAT =
        "^\\s*\\(\\(?#([0-9a-zA-Z\\u4e00-\\u9fa5_-]+),1,(\\d+)#\\)?\\.longValue\\(\\)\\s*%\\s*(\\d+)\\)\\.intdiv\\((\\d+)\\)\\s*$";
    private static final String TB_NUMBER_ABS_RULE_PATTERN_FORMAT =
        "^\\s*\\(?\\(?#([0-9a-zA-Z\\u4e00-\\u9fa5_-]+),1,(\\d+)#\\)?\\.longValue\\(\\)\\.abs\\(\\)\\s*%\\s*(\\d+)\\)?\\s*$";
    private static final String DB_NUMBER_ABS_RULE_PATTERN_FORMAT =
        "^\\s*\\(\\(?#([0-9a-zA-Z\\u4e00-\\u9fa5_-]+),1,(\\d+)#\\)?\\.longValue\\(\\)\\.abs\\(\\)\\s*%\\s*(\\d+)\\)\\.intdiv\\((\\d+)\\)\\s*$";
    private static final String TB_STRING_RULE_PATTERN_FORMAT =
        "^\\s*\\(?\\(?#([0-9a-zA-Z\\u4e00-\\u9fa5_-]+),1,(\\d+)#\\)?\\.hashCode\\(\\)\\.abs\\(\\)\\.longValue\\(\\)\\s*%\\s*(\\d+)\\)?\\s*$";
    private static final String DB_STRING_RULE_PATTERN_FORMAT =
        "^\\s*\\(\\(?#([0-9a-zA-Z\\u4e00-\\u9fa5_-]+),1,(\\d+)#\\)?\\.hashCode\\(\\)\\.abs\\(\\)\\.longValue\\(\\)\\s*%\\s*(\\d+)\\)\\.intdiv\\((\\d+)\\)\\s*$";

    /* 这个规则是create ddl根据mmdd_i/dd_i生成的 */
    private static final String DB_NUMBER_SPECIAL_DATE_RULE_PATTERN_FORMAT =
        "^\\s*\\([mM]{0,2}[dD]{2}_[iI]\\s*\\(?#([0-9a-zA-Z\\u4e00-\\u9fa5_-]+),1_date,(\\d+)#\\)?\\.longValue\\(\\)\\s*%\\s*(\\d+)\\)\\.intdiv\\((\\d+)\\)\\s*$";
    private static final String TB_NUMBER_SPECIAL_DATE_RULE_PATTERN_FORMAT =
        "^\\s*\\(?[mM]{0,2}[dD]{2}_[iI]\\s*\\(?#([0-9a-zA-Z\\u4e00-\\u9fa5_-]+),1_date,(\\d+)#\\)?\\.longValue\\(\\)\\s*%\\s*(\\d+)\\)?\\s*$";

    public static final Pattern TB_NUMBER_RULE_PATTERN = Pattern
        .compile(TB_NUMBER_RULE_PATTERN_FORMAT);
    public static final Pattern DB_NUMBER_RULE_PATTERN = Pattern
        .compile(DB_NUMBER_RULE_PATTERN_FORMAT);
    public static final Pattern TB_NUMBER_ABS_RULE_PATTERN = Pattern
        .compile(TB_NUMBER_ABS_RULE_PATTERN_FORMAT);
    public static final Pattern DB_NUMBER_ABS_RULE_PATTERN = Pattern
        .compile(DB_NUMBER_ABS_RULE_PATTERN_FORMAT);
    public static final Pattern TB_STRING_RULE_PATTERN = Pattern
        .compile(TB_STRING_RULE_PATTERN_FORMAT);
    public static final Pattern DB_STRING_RULE_PATTERN = Pattern
        .compile(DB_STRING_RULE_PATTERN_FORMAT);
    public static final Pattern DB_NUMBER_SPECIAL_DATE_RULE_PATTERN = Pattern
        .compile(DB_NUMBER_SPECIAL_DATE_RULE_PATTERN_FORMAT);
    public static final Pattern TB_NUMBER_SPECIAL_DATE_RULE_PATTERN = Pattern
        .compile(TB_NUMBER_SPECIAL_DATE_RULE_PATTERN_FORMAT);

    public static class SimpleRule {

        public int dbCount;
        public int tbCount;
        public int allCount;
        public String column;
        public AtomIncreaseType type;
    }

    public static TargetDB shard(TableRule table, Object value) {
        int tableCount = table.getTbCount();
        int tableIndex = -1;
        if (table.getPartitionType() == AtomIncreaseType.NUMBER) {
            if (value == null) {
                value = 0L;
            }

            if (value instanceof Number) {
            } else if (value instanceof String) {
                value = Long.valueOf(String.valueOf(value));
            } else {
                throw new IllegalArgumentException(value + "不是一个合法的数字类型");
            }

            tableIndex = (int) (((Number) value).longValue() % tableCount);
            if (tableIndex < 0) {
                throw new TddlNestableRuntimeException(new TddlException(ErrorCode.ERR_RULE_NO_ABS));
            }
        } else if (table.getPartitionType() == AtomIncreaseType.NUMBER_ABS) {
            if (value == null) {
                value = 0L;
            }

            if (value instanceof Number) {
            } else if (value instanceof String) {
                value = Long.valueOf(String.valueOf(value));
            } else {
                throw new IllegalArgumentException(value + "不是一个合法的数字类型");
            }

            tableIndex = (int) (RuleUtils.safeAbs(((Number) value).longValue() % tableCount));
        } else if (table.getPartitionType() == AtomIncreaseType.STRING) {

            if (value == null) {
                value = "0";
            }

            if (value instanceof String || value instanceof Number) {
                tableIndex = (RuleUtils.safeAbs(value.hashCode() % tableCount));
            } else {
                throw new IllegalArgumentException(value + "不是一个合法的字符串类型");
            }
        } else {
            throw new IllegalArgumentException("不是一个合法的简单规则类型");
        }

        TargetDB db = new TargetDB();
        db.setDbIndex(table.getDbNames()[tableIndex]);
        db.addOneTable(table.getTbNames()[tableIndex]);
        return db;
    }

    public static Pair<String, String> shardReturnPair(TableRule table, Object value, String encoding) {
        int tableCount = table.getTbCount();
        int tableIndex;
        AtomIncreaseType partitionType = table.getPartitionType();
        if (partitionType == AtomIncreaseType.NUMBER) {
            if (value == null) {
                value = 0L;
            }

            if (value instanceof Number) {
            } else if (value instanceof String) {
                value = Long.valueOf(String.valueOf(value));
            } else {
                throw new IllegalArgumentException(value + "不是一个合法的数字类型");
            }

            tableIndex = (int) (((Number) value).longValue() % tableCount);
            if (tableIndex < 0) {
                throw new TddlNestableRuntimeException(new TddlException(ErrorCode.ERR_RULE_NO_ABS));
            }
        } else if (partitionType == AtomIncreaseType.NUMBER_ABS) {
            if (value == null) {
                value = 0L;
            }

            if (value instanceof Number) {
            } else if (value instanceof String) {
                value = Long.valueOf(String.valueOf(value));
            } else {
                throw new IllegalArgumentException(value + "不是一个合法的数字类型");
            }

            tableIndex = (int) (RuleUtils.safeAbs(((Number) value).longValue() % tableCount));
        } else if (partitionType == AtomIncreaseType.STRING) {

            if (value == null) {
                value = "0";
            }

            if (value instanceof String || value instanceof Number) {
                tableIndex = (RuleUtils.safeAbs(value.hashCode() % tableCount));
            } else if (value instanceof byte[]) {
                try {
                    String strVal = null;
                    if (encoding != null) {
                        strVal = new String((byte[]) value, encoding);
                    } else {
                        strVal = new String((byte[]) value);
                    }
                    tableIndex = (RuleUtils.safeAbs(strVal.hashCode() % tableCount));
                } catch (UnsupportedEncodingException ex) {
                    throw new IllegalArgumentException(value + "不是一个合法的字符串byte数组");
                }
            } else {
                throw new IllegalArgumentException(value + "不是一个合法的字符串类型");
            }
        } else {
            throw new IllegalArgumentException("不是一个合法的简单规则类型");
        }

        return new Pair<>(table.getDbNames()[tableIndex], table.getTbNames()[tableIndex]);
    }

    public static void process(TableRule table) {

        // 把普通规则判断成简单规则，导致表数量计算错误
        if (table.getStaticTopology() != null && table.getActualTopology().size() > 0) {
            return;
        }

        if (table.getDbRuleStrs() == null) {
            if (ConfigDataMode.isFastMock()) {
                table.setDbCount(1);
                table.setTbCount(1);
            } else {
                table.setDbCount(0);
            }
            return;
        }

        if (table.getDbRuleStrs().length != 1) {
            return;
        }

        String expr = table.getDbRuleStrs()[0];
        SimpleRule dbRule = parseDB(expr);
        if (dbRule == null) {
            return;
        }

        if (table.getTbRulesStrs() != null && table.getTbRulesStrs().length != 1) {
            return;
        }

        if (table.getTbRulesStrs() != null) {
            // 如果存在表规则，验证一下
            expr = table.getTbRulesStrs()[0];
            SimpleRule tbRule = parseTB(expr);
            if (tbRule == null) {
                return;
            }

            if (dbRule.type != tbRule.type) {
                return;
            }

            if (dbRule.tbCount != tbRule.tbCount) {
                return;
            }
        }
        table.setSimple(true);
        int tbCount = dbRule.tbCount;
        int dbCount = dbRule.dbCount;
        String[] tableNames = new String[tbCount];
        String[] dbNames = new String[tbCount];

        String[] tableNamesOfExtraDb = new String[tbCount];
        String[] dbNamesOfExtraDb = new String[tbCount];

        String tbPlaceHolder = TStringUtil.getBetween(table.getTbNamePattern(), "{", "}");
        String dbPlaceHolder = TStringUtil.getBetween(table.getDbNamePattern(), "{", "}");

        for (int i = 0; i < tbCount; i++) {
            if (StringUtils.isEmpty(tbPlaceHolder)) {
                tableNames[i] = table.getTbNamePattern();
            } else {
                String tbIndex = TStringUtil.placeHolder(tbPlaceHolder.length(), i);
                tableNames[i] = TStringUtil.replace(table.getTbNamePattern(), "{" + tbPlaceHolder + "}", tbIndex);
            }

            if (StringUtils.isEmpty(dbPlaceHolder)) {
                dbNames[i] = table.getDbNamePattern();
            } else {
                String dbIndex = TStringUtil.placeHolder(dbPlaceHolder.length(), i / dbCount);
                dbNames[i] = TStringUtil.replace(table.getDbNamePattern(), "{" + dbPlaceHolder + "}", dbIndex);
            }

        }

        table.setDbCount(dbCount);
        table.setTbCount(tbCount);
        table.setTbNames(tableNames);
        table.setDbNames(dbNames);
        table.setPartitionType(dbRule.type);

    }

    public static SimpleRule parseDB(String expression) {
        if (StringUtils.isEmpty(expression)) {
            return null;
        }

        try {
            Matcher matcher = DB_NUMBER_RULE_PATTERN.matcher(expression);
            if (matcher.matches()) {
                SimpleRule rule = buildRule(matcher);
                if (rule != null) {
                    rule.type = AtomIncreaseType.NUMBER;
                }
                return rule;
            }

            matcher = DB_NUMBER_ABS_RULE_PATTERN.matcher(expression);
            if (matcher.matches()) {
                SimpleRule rule = buildRule(matcher);
                if (rule != null) {
                    rule.type = AtomIncreaseType.NUMBER_ABS;
                }
                return rule;
            }

            matcher = DB_STRING_RULE_PATTERN.matcher(expression);
            if (matcher.matches()) {
                SimpleRule rule = buildRule(matcher);
                if (rule != null) {
                    rule.type = AtomIncreaseType.STRING;
                }
                return rule;
            }
        } catch (Throwable e) {
            logger.info("parse error", e);
        }

        return null;
    }

    public static SimpleRule parseTB(String expression) {
        if (StringUtils.isEmpty(expression)) {
            return null;
        }

        try {
            Matcher matcher = TB_NUMBER_RULE_PATTERN.matcher(expression);
            if (matcher.matches()) {
                SimpleRule rule = buildRule(matcher);
                if (rule != null) {
                    rule.type = AtomIncreaseType.NUMBER;
                }
                return rule;
            }

            matcher = TB_NUMBER_ABS_RULE_PATTERN.matcher(expression);
            if (matcher.matches()) {
                SimpleRule rule = buildRule(matcher);
                if (rule != null) {
                    rule.type = AtomIncreaseType.NUMBER_ABS;
                }
                return rule;
            }

            matcher = TB_STRING_RULE_PATTERN.matcher(expression);
            if (matcher.matches()) {
                SimpleRule rule = buildRule(matcher);
                if (rule != null) {
                    rule.type = AtomIncreaseType.STRING;
                }
                return rule;
            }
        } catch (Throwable e) {
            logger.info("parse error", e);
        }

        return null;
    }

    private static SimpleRule buildRule(Matcher matcher) {
        SimpleRule rule = new SimpleRule();
        String column = matcher.group(1);
        String allCount = matcher.group(2);
        String tbCount = matcher.group(3);
        rule.tbCount = Integer.valueOf(tbCount);
        rule.column = column;
        rule.allCount = Integer.valueOf(allCount);
        if (matcher.groupCount() >= 4) {
            String dbCount = matcher.group(4);
            rule.dbCount = Integer.valueOf(dbCount);
        } else {
            rule.dbCount = 1;
        }

        if (rule.tbCount != rule.allCount) {
            return null;
        }
        return rule;
    }

    /**
     * 使用正则表达式判断规则中是否含有dd_i和mmdd_i两个函数，为了解决这两个特殊时间函数 枚举不到31天和闰年366天的情况
     * <p>
     * df                           @param                                                                                                                       `1expression
     */
    public static boolean isSpecialDateMethod(String expression) {
        if (!TStringUtil.isEmpty(expression)) {
            Matcher matcher = DB_NUMBER_SPECIAL_DATE_RULE_PATTERN.matcher(expression);
            if (matcher.find()) {
                return true;
            }

            matcher = TB_NUMBER_SPECIAL_DATE_RULE_PATTERN.matcher(expression);
            if (matcher.find()) {
                return true;
            }
        }
        return false;
    }

}
