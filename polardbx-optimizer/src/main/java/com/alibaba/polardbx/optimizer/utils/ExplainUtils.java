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

import com.alibaba.polardbx.common.jdbc.RawString;
import com.alibaba.polardbx.common.model.sqljep.Comparative;
import com.alibaba.polardbx.common.model.sqljep.ComparativeBaseList;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.optimizer.core.rel.PhyTableOperation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.Util;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ExplainUtils {

    public final static Pattern GROUP_INDEX_PATTERN = Pattern.compile(".*(_(\\d+)_|_(\\d+$))");

    public static String compressName(Collection<String> inputs) {
        if (null == inputs || inputs.size() <= 0) {
            return "";
        }

        if (inputs.size() == 1) {
            return inputs.iterator().next();
        }

        List<Integer> indexList = new LinkedList<>();
        Map<Integer, String> indexValueStrMap = new LinkedHashMap<>();
        String firstPrefix = null;
        String prefix = "";
        for (final String input : inputs) {
            prefix = input;
            try {
                Matcher m = GROUP_INDEX_PATTERN.matcher(input);

                String indexPart = null;
                if (m.find()) {
                    indexPart = Util.replace(m.group(1), "_", "");
                    prefix = input.substring(0, m.start(1));
                }
                if (firstPrefix == null) {
                    firstPrefix = prefix;
                }
                if (!firstPrefix.equals(prefix)) {
                    throw new Exception("not same");
                }
                Integer indexValue = Integer.valueOf(indexPart);
                indexList.add(indexValue);
                indexValueStrMap.put(indexValue, indexPart);
            } catch (Exception e) {
                return org.apache.commons.lang3.StringUtils.join(inputs, ",");
            }
        } // end of for

        Collections.sort(indexList);

        StringBuilder result = new StringBuilder();

        if (indexList.size() > 1) {
            result.append("[");
        } else if (indexList.size() == 1) {
            result.append(prefix);
        }

        int lastSuffixValue = -1;
        int consecutiveCount = 0;
        for (int index = 0; index < indexList.size(); index++) {
            if (lastSuffixValue >= 0) {
                int suffix = indexList.get(index);
                if (suffix - lastSuffixValue == 1) {
                    consecutiveCount++;
                    lastSuffixValue = suffix;
                } else {
                    // 发现新节点不连续了
                    if (consecutiveCount == 1) {
                        result.append(",").append(indexValueStrMap.get(lastSuffixValue));
                    } else if (consecutiveCount > 1) {
                        result.append("-").append(indexValueStrMap.get(lastSuffixValue));
                    }

                    consecutiveCount = 0;

                    // 上一个压缩段完结, 开个新的
                    result.append(",");
                    result.append(indexValueStrMap.get(suffix));
                    lastSuffixValue = suffix;
                }
            } else {
                // 第一次
                result.append(indexValueStrMap.get(indexList.get(index)));
                lastSuffixValue = indexList.get(index);
            }
        }

        // 有剩余节点
        if (consecutiveCount == 1) {
            result.append(",").append(indexValueStrMap.get(lastSuffixValue));
        } else if (consecutiveCount > 1) {
            result.append("-").append(indexValueStrMap.get(lastSuffixValue));
        }

        if (indexList.size() > 1) {
            result.append("]");
        }

        return result.toString();
    }

    public static String toString(RexNode rex, RelNode rel) {

        StringBuilder res = new StringBuilder();

        return res.toString();

    }

    public static String getPhyTableString(List<String> tableNames, Map<String, List<List<String>>> targetTable) {
        final Map<String, Set<String>> map = new LinkedHashMap<>();
        final Set<String> groups = targetTable.keySet();

        for (String table : tableNames) {
            map.put(table, new HashSet<String>());
        }

        for (Entry<String, List<List<String>>> entry : targetTable.entrySet()) {
            for (List<String> phyTableNames : entry.getValue()) {
                for (int i = 0; i < phyTableNames.size(); i++) {
                    if (i >= tableNames.size()) {
                        break;
                    }
                    map.get(tableNames.get(i)).add(phyTableNames.get(i));
                }
            }
        }

        return compressPhyTableString(map, groups);
    }

    public static String getPhyTableString(List<String> tableNames, List<RelNode> phyTables) {
        Map<String, Set<String>> map = new LinkedHashMap<>();
        Set<String> groups = new HashSet<>();

        for (String table : tableNames) {
            map.put(table, new HashSet<String>());
        }

        for (RelNode item : phyTables) {
            List<List<String>> phyTableNames = null;
            if (item instanceof PhyTableOperation) {
                PhyTableOperation phyTable = (PhyTableOperation) item;
                groups.add(phyTable.getDbIndex());
                phyTableNames = phyTable.getTableNames();
            } else {
                continue;
            }

            if (null == phyTableNames) {
                continue;
            }

            for (List<String> tables : phyTableNames) {
                if (tables.size() != tableNames.size()) {
                    continue;
                }
                for (int tableIndex = 0; tableIndex < tableNames.size(); tableIndex++) {
                    String physicalTableName = tables.get(tableIndex);
                    map.get(tableNames.get(tableIndex)).add(physicalTableName);
                } // end of for
            } // end of for
        } // end of for

        return compressPhyTableString(map, groups);
    }

    public static String compressPhyTableString(Map<String, Set<String>> map, Set<String> groups) {
        String groupName = compressName(groups);
        List<String> tableNamesCompressed = new ArrayList<>(map.size());
        for (String tableName : map.keySet()) {
            String tableNameCompressed = compressName(map.get(tableName));
            if (TStringUtil.isEmpty(tableNameCompressed)
                || TStringUtil.equalsIgnoreCase(tableName, tableNameCompressed)) {
                tableNamesCompressed.add(tableName);
            } else {
                if (tableNameCompressed.charAt(0) != '[') {
                    tableNamesCompressed.add(tableNameCompressed);
                } else {
                    tableNamesCompressed.add(tableName + "_" + tableNameCompressed);
                }
            }
        }

        String tableNameCompressedJoind = TStringUtil.join(tableNamesCompressed, ",");

        if (TStringUtil.isEmpty(tableNameCompressedJoind) && TStringUtil.isEmpty(groupName)) {
            return null;
        }
        if (TStringUtil.isEmpty(groupName)) {
            return tableNameCompressedJoind;
        }
        if (TStringUtil.isEmpty(tableNameCompressedJoind)) {
            return groupName;
        } else {
            return groupName + "." + tableNameCompressedJoind;
        }
    }

    public static String comparativeToString(String column, Comparative comparative, String outterOp) {
        StringBuilder result = new StringBuilder();

        if (comparative instanceof ComparativeBaseList) {
            final ComparativeBaseList comparativeList = (ComparativeBaseList) comparative;
            final String op = comparativeList.getRelation();

            final StringBuilder operandsBuilder = new StringBuilder();
            final boolean addBrackets = TStringUtil.isNotBlank(outterOp) && (!TStringUtil.equals(op, outterOp));

            if (addBrackets) {
                operandsBuilder.append("(");
            }

            for (Comparative operand : comparativeList.getList()) {
                if (operandsBuilder.length() > 1) {
                    operandsBuilder.append(" ").append(op).append(" ");
                }

                operandsBuilder.append(comparativeToString(column, operand, op));
            } // end of for

            if (addBrackets) {
                operandsBuilder.append(")");
            }

            result.append(operandsBuilder.toString());
        } else {
            result.append(column)
                .append(" ")
                .append(Comparative.getComparisonName(comparative.getComparison()))
                .append(" ");
            if (comparative.getValue() instanceof RawString) {
                result.append(((RawString) comparative.getValue()).display());
            } else {
                result.append(comparative.getValue());
            }
        }

        return result.toString();
    }

    public static String getTimeStamp(Date date) {
        SimpleDateFormat sdfDate = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");//dd/MM/yyyy
        String strDate = sdfDate.format(date);
        return strDate;
    }
}
