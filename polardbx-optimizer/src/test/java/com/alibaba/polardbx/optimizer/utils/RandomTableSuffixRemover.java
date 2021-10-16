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

package com.alibaba.polardbx.optimizer.utils;

import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.index.CandidateIndex;
import com.alibaba.polardbx.rule.TableRule;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.alibaba.polardbx.common.ddl.newengine.DdlConstants.RANDOM_SUFFIX_LENGTH_OF_PHYSICAL_TABLE_NAME;

/**
 * @author Eric Fu
 */
public class RandomTableSuffixRemover {

    private final String appName;

    public RandomTableSuffixRemover(String appName) {
        this.appName = appName;
    }

    private static final String HINT_PREFIX = "/*+TDDL";
    private static final String HINT_REAL_TABLE = "real_table=";
    private static final String HINT_REALTABS = "'realtabs'";

    private static final String TABLES_PREFIX = "tables=\"";
    private static final String PARAMS_PREFIX = "params=\"";
    private static final String COMMON_PATTERN = "([\\s\\S]+?)";
    private static final String COMMON_SUFFIX = "\"";
    private static final Pattern TABLES_PATTERN = Pattern.compile(TABLES_PREFIX + COMMON_PATTERN + COMMON_SUFFIX);
    private static final Pattern PARAMS_PATTERN = Pattern.compile(PARAMS_PREFIX + COMMON_PATTERN + COMMON_SUFFIX);

    private static final String REAL_TABLE_PREFIX = "real_table=\\(\"";
    private static final String REAL_TABLE_SUFFIX = "\"\\)";
    private static final Pattern REAL_TABLE_PATTERN = Pattern.compile(REAL_TABLE_PREFIX + COMMON_PATTERN
        + REAL_TABLE_SUFFIX);
    private static final String REALTABS_PREFIX = "'realtabs':\\[";
    private static final String REALTABS_SUFFIX = "\\]";
    private static final Pattern REALTABS_PATTERN = Pattern.compile(REALTABS_PREFIX + COMMON_PATTERN
        + REALTABS_SUFFIX);

    private static final String SEPARATOR_DOT = ".";
    private static final String SEPARATOR_COMMA = ",";
    private static final String SEPARATOR_UNDERLINE = "_";
    private static final String LEFT_SQUARE_BRACKET = "[";
    private static final String RIGHT_SQUARE_BRACKET = "]";
    private static final String WHITE_SPACE = " ";
    private static final String BACKTICKS = "`";
    private static final String SINGLE_QUOTATION = "'";

    public String replaceRealPhysicalTableNames(String targetSql, String targetPlan) {
        Set<String> realTables = new HashSet<>();
        if (TStringUtil.containsIgnoreCase(targetSql, HINT_PREFIX)
            && (TStringUtil.containsIgnoreCase(targetSql, HINT_REAL_TABLE) || TStringUtil.containsIgnoreCase(targetSql,
            HINT_REALTABS))) {
            collectRealTablesFromHint(targetSql, realTables);
        }
        String tablesDone =
            replaceRealPhysicalTableNames(targetPlan, TABLES_PATTERN, TABLES_PREFIX, COMMON_SUFFIX, realTables);
        return replaceRealPhysicalTableNames(tablesDone, PARAMS_PATTERN, PARAMS_PREFIX, COMMON_SUFFIX, realTables);
    }

    private void collectRealTablesFromHint(String targetSql, Set<String> realTables) {
        collectRealTablesFromHint(targetSql, REAL_TABLE_PATTERN, realTables);
        collectRealTablesFromHint(targetSql, REALTABS_PATTERN, realTables);
    }

    private void collectRealTablesFromHint(String targetSql, Pattern pattern, Set<String> realTables) {
        try {
            Matcher matcher = pattern.matcher(targetSql);
            if (!matcher.find()) {
                return;
            }
            do {
                String matched = matcher.group(1);
                if (TStringUtil.contains(matched, SEPARATOR_COMMA)) {
                    String[] tempRealTables = matched.split(SEPARATOR_COMMA);
                    for (String tempRealTable : tempRealTables) {
                        realTables.add(extractTableName(tempRealTable));
                    }
                } else {
                    realTables.add(matched);
                }
            } while (matcher.find());
        } catch (Throwable t) {
            // Ignored.
        }
    }

    private String extractTableName(String origTableName) {
        String newTableName = TStringUtil.trim(origTableName);

        if (TStringUtil.startsWith(newTableName, SINGLE_QUOTATION)
            && TStringUtil.endsWith(newTableName, SINGLE_QUOTATION)) {
            return TStringUtil.substring(newTableName, 1, newTableName.length() - 1);
        }

        return newTableName;
    }

    private String replaceRealPhysicalTableNames(String targetPlan, Pattern pattern, String prefix, String suffix,
                                                 Set<String> realTables) {
        try {
            Matcher matcher = pattern.matcher(targetPlan);
            if (!matcher.find()) {
                return targetPlan;
            }
            StringBuffer sb = new StringBuffer();
            // The plan may contain multiple tables, such as join, so we have to
            // process them one by one.
            do {
                String matched = matcher.group(1);
                String replaced = processMatched(matched, realTables);
                String replacement = prefix + replaced + suffix;
                matcher.appendReplacement(sb, replacement);
            } while (matcher.find());
            // Complete this round of replacement.
            return matcher.appendTail(sb).toString();
        } catch (Throwable t) {
            return targetPlan;
        }
    }

    private String processMatched(String matched, Set<String> realTables) {
        String matchedPrefix;
        String tableNamePart;

        int dotIndex = TStringUtil.indexOf(matched, SEPARATOR_DOT);
        if (dotIndex >= 0) {
            matchedPrefix = TStringUtil.substring(matched, 0, dotIndex + 1);
            tableNamePart = TStringUtil.substring(matched, dotIndex + 1);
        } else {
            matchedPrefix = "";
            tableNamePart = matched;
        }

        if (TStringUtil.isEmpty(tableNamePart)) {
            return matched;
        }

        String newTableNamePart;
        boolean wrappedBySquareBrackets = false;

        if (TStringUtil.startsWith(tableNamePart, LEFT_SQUARE_BRACKET)
            && TStringUtil.endsWith(tableNamePart, RIGHT_SQUARE_BRACKET)) {
            wrappedBySquareBrackets = true;
            tableNamePart = TStringUtil.substring(tableNamePart, 1, tableNamePart.length() - 1);
        }

        StringBuilder sb = new StringBuilder();
        List<String> tableNames = splitByComma(tableNamePart);
        for (String tableName : tableNames) {
            String newTableName = processTableName(tableName, realTables);
            sb.append(SEPARATOR_COMMA).append(newTableName);
        }
        newTableNamePart = sb.deleteCharAt(0).toString();

        if (wrappedBySquareBrackets) {
            newTableNamePart = LEFT_SQUARE_BRACKET + newTableNamePart + RIGHT_SQUARE_BRACKET;
        }

        return matchedPrefix + newTableNamePart;
    }

    private List<String> splitByComma(String tableNamePart) {
        List<String> tableNames = new ArrayList<>();

        int leftBracketIndex = TStringUtil.indexOf(tableNamePart, LEFT_SQUARE_BRACKET);
        int rightBracketIndex = TStringUtil.indexOf(tableNamePart, RIGHT_SQUARE_BRACKET);

        if (leftBracketIndex < 0 || rightBracketIndex < 0 || leftBracketIndex > rightBracketIndex) {
            // No square bracket pair at all, so split and return right now
            if (TStringUtil.contains(tableNamePart, SEPARATOR_COMMA)) {
                String[] tableNameArray = TStringUtil.split(tableNamePart, SEPARATOR_COMMA);
                tableNames.addAll(Arrays.asList(tableNameArray));
            } else {
                tableNames.add(tableNamePart);
            }
            return tableNames;
        }

        int commaIndex = TStringUtil.indexOf(tableNamePart, SEPARATOR_COMMA);

        while (commaIndex > leftBracketIndex && commaIndex < rightBracketIndex) {
            // Don't split if the comma is within square brackets.
            commaIndex = TStringUtil.indexOf(tableNamePart, SEPARATOR_COMMA, commaIndex + 1);
        }

        if (commaIndex < 0) {
            tableNames.add(tableNamePart);
            return tableNames;
        }

        String singleTableName = TStringUtil.substring(tableNamePart, 0, commaIndex);
        tableNames.add(singleTableName);

        tableNamePart = TStringUtil.substring(tableNamePart, commaIndex + 1, tableNamePart.length());

        List<String> leftTableNames = splitByComma(tableNamePart);
        tableNames.addAll(leftTableNames);

        return tableNames;
    }

    private String processTableName(String tableName, Set<String> realTables) {
        boolean startWithBlank = false;
        boolean endWithBlank = false;
        boolean wrappedByBackticks = false;
        boolean containNumericSuffix;
        boolean needReplace = true;
        String logicalTableName;
        String randomSuffix;
        String tableNameSuffix;

        if (TStringUtil.contains(tableName, LEFT_SQUARE_BRACKET)
            && TStringUtil.contains(tableName, RIGHT_SQUARE_BRACKET)) {
            return tableName;
        }

        if (TStringUtil.startsWith(tableName, WHITE_SPACE)) {
            startWithBlank = true;
            if (TStringUtil.endsWith(tableName, WHITE_SPACE)) {
                endWithBlank = true;
            }
            tableName = TStringUtil.trim(tableName);
        }

        if (TStringUtil.startsWith(tableName, BACKTICKS) && TStringUtil.endsWith(tableName, BACKTICKS)) {
            wrappedByBackticks = true;
            tableName = TStringUtil.substring(tableName, 1, tableName.length() - 1);
        }

        String newTableName = tableName;

        for (String realTable : realTables) {
            if (TStringUtil.equalsIgnoreCase(TStringUtil.trim(tableName), realTable)) {
                needReplace = false;
            }
        }

        if (needReplace) {
            // Not sure if the table name contains a numeric suffix, so we
            // have to try to determine it via rule.
            TableRule tableRule = OptimizerContext.getContext(appName).getRuleManager().getTableRule(tableName);

            if (tableRule != null) {
                // It's a logical table name.
                containNumericSuffix = false;
            } else {
                if (tableName.indexOf(CandidateIndex.WHAT_IF_INDEX_INFIX) != -1) {
                    containNumericSuffix = false;
                } else {
                    // It's a table name with numeric suffix.
                    containNumericSuffix = true;
                }
            }

            if (containNumericSuffix) {
                int endIndex = TStringUtil.lastIndexOf(tableName, SEPARATOR_UNDERLINE);
                if (endIndex >= 0) {
                    // The table name contains numeric suffix
                    logicalTableName = TStringUtil.substring(tableName, 0, endIndex);
                    tableNameSuffix = TStringUtil.substring(tableName, endIndex);
                } else {
                    // It's probably not a table name.
                    containNumericSuffix = false;
                    logicalTableName = tableName;
                    tableNameSuffix = "";
                }
            } else {
                logicalTableName = tableName;
                tableNameSuffix = "";
            }

            tableRule = OptimizerContext.getContext(appName).getRuleManager().getTableRule(logicalTableName);

            if (tableRule != null) {
                String tableNamePattern = tableRule.getTbNamePattern();

                int startIndex = logicalTableName.length() + 1;
                if (containNumericSuffix) {
                    int endIndex = TStringUtil.lastIndexOf(tableNamePattern, SEPARATOR_UNDERLINE);
                    randomSuffix = TStringUtil.substring(tableNamePattern, startIndex, endIndex);
                } else {
                    randomSuffix = TStringUtil.substring(tableNamePattern, startIndex);
                }

                if (TStringUtil.isEmpty(randomSuffix)
                    || randomSuffix.length() != RANDOM_SUFFIX_LENGTH_OF_PHYSICAL_TABLE_NAME) {
                    newTableName = tableName;
                } else {
                    newTableName = logicalTableName + SEPARATOR_UNDERLINE + randomSuffix + tableNameSuffix;
                }
            } else {
                newTableName = logicalTableName;
            }
        }

        if (wrappedByBackticks) {
            newTableName = BACKTICKS + newTableName + BACKTICKS;
        }

        if (startWithBlank) {
            newTableName = WHITE_SPACE + newTableName;
        }

        if (endWithBlank) {
            newTableName = newTableName + WHITE_SPACE;
        }

        return newTableName;
    }
}
