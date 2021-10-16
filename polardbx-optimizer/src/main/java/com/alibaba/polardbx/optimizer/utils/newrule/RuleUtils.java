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

package com.alibaba.polardbx.optimizer.utils.newrule;

import com.alibaba.polardbx.common.eagleeye.EagleeyeHelper;
import com.alibaba.polardbx.common.model.Group;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.optimizer.exception.OptimizerException;
import com.alibaba.polardbx.rule.TableRule;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.RandomStringUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.alibaba.polardbx.common.ddl.newengine.DdlConstants.MAX_TABLE_NAME_LENGTH_MYSQL_ALLOWS;
import static com.alibaba.polardbx.common.ddl.newengine.DdlConstants.RANDOM_SUFFIX_LENGTH_OF_PHYSICAL_TABLE_NAME;

/**
 * Created by simiao on 14-12-3.
 */
public class RuleUtils {

    /**
     * 根据现有group key名称 与 物理表名称 返回对应的pattern 比如<br/>
     * ["21212121112_GXC_123_0001_RDS","21212121112_GXC_123_0002_RDS"] ->
     * "21212121112_GXC_123_{0000}_RDS"<br/>
     * ["21212121112_GXC_123_0001","21212121112_GXC_123_0002"] ->
     * "21212121112_GXC_123_{0000}"<br/>
     * ["001_GXC_123","002_GXC_123"] -> "{000}_GXC_123"<br/>
     * ["tb_09_13","tb_09_12","tb_10_13"] -> "tb_{00}_{00}"<br/>
     *
     * @return 正常返回对应的pattern, 没找到则return null;
     */
    public static String findDbNameOrTableNamePattern(List<String> names) {
        /* 使用prectrl版本的 */
        if (names == null || names.size() == 0) {
            throw new IllegalArgumentException("db names or table names can not be empty.");
        }

        if (names.size() == 1) {
            return names.get(0);
        }

        String first = names.get(0);
        // 要求每个name的 长度一样，否则返回null
        for (String name : names) {
            if (name.length() != first.length()) {
                return null;
            }
        }

        Pattern pattern = Pattern.compile("\\d+");
        List<Matcher> matchers = new ArrayList<Matcher>(names.size());
        for (String name : names) {
            matchers.add(pattern.matcher(name));
        }

        /* 先循环找到可变数字部分在原始字符串的start,end位置 */
        List<int[]> patternPartPositions = new ArrayList<int[]>();
        while (true) {
            boolean stop = false;
            for (Matcher matcher : matchers) {
                boolean find = matcher.find();
                if (!find) {
                    stop = true;
                    break;
                }
            }

            if (stop) {
                break;
            }

            int prevStart = -1;
            int prevEnd = -1;

            Set<String> diffParts = new HashSet<String>();
            for (Matcher matcher : matchers) {
                if (prevStart == -1 && prevEnd == -1) {
                    prevStart = matcher.start();
                    prevEnd = matcher.end();
                    diffParts.add(matcher.group());
                    continue;
                }

                if (matcher.start() == prevStart && matcher.end() == prevEnd) {
                    diffParts.add(matcher.group());
                    // 如果对应位置的数字部分有不一样则认为该部分需要替换为{0..}，记录该位置start,end
                    if (diffParts.size() > 1) {
                        patternPartPositions.add(new int[] {prevStart, prevEnd});
                        break;
                    }
                } else {
                    // 在每个name中找到对应的数字部分，如果位置不一致则认为没有对应的patter 返回null
                    return null;
                }
            }
        }

        if (patternPartPositions.size() == 0) {
            return null;
        }

        StringBuilder resultBuilder = new StringBuilder();
        // 根据第一个name，与要替换的start,end 创建最终pattern字符串
        for (int i = 0; i < first.length(); i++) {
            int[] replacePosition = null;
            for (int[] position : patternPartPositions) {
                if (i >= position[0] && i < position[1]) {
                    replacePosition = position;
                }
            }

            if (replacePosition != null) {
                patternPartPositions.remove(replacePosition);
                resultBuilder.append('{');
                for (int j = replacePosition[0]; j < replacePosition[1]; j++) {
                    resultBuilder.append('0');
                }
                resultBuilder.append('}');
                i += replacePosition[1] - replacePosition[0] - 1;
            } else {
                resultBuilder.append(first.charAt(i));
            }
        }

        return resultBuilder.toString();
    }

    public static String listToString(List<String> groupList) {
        StringBuffer sb = new StringBuffer();
        boolean first = true;
        for (String group : groupList) {
            if (first) {
                first = false;
            } else {
                sb.append(", ");
            }
            sb.append(group);
        }
        return sb.toString();

    }

    public static int calculateDigit(int num) {
        int maxdigit = 0;
        for (; num > 0; ) {
            maxdigit++;
            num /= 10;
        }

        return maxdigit;
    }

    public static List<String> findDefaultGroupList(String[] contentArray, String defaultDb) {
        List<String> groups = new ArrayList<>();

        // 公有云包括一个默认库组（其中一个为默认库）和一个热点库组
        // 默认库组命名规则：AAA_BBB_{0000}, AAA_BBB_{0001}
        // 热点库组命名规则：AAA_BBB_{0000}_HOT, AAA_BBB_{0001}_HOT
        for (String group : contentArray) {
            if (defaultDb.length() == group.length() && !group.endsWith("_HOT")) {
                groups.add(group);
            }
        }

        return groups;
    }

    public static List<String> findMaxMatchList(String[] contentArray) {
        List<List<String>> groupLists = groupMatchList(contentArray);

        /**
         * 不需要返回数目大于等于group_count的最小的第一个，没有就返回最大的，
         * 只返回最大的就够了，正常情况下只会有一个真正的分库组和一个default库
         */
        int maxIndex = 0;
        for (int i = 0; i < groupLists.size(); i++) {
            List<String> groupList = groupLists.get(i);

            if (groupList.size() > groupLists.get(maxIndex).size()) {
                maxIndex = i;
            }
        }

        return groupLists.get(maxIndex);

    }

    public static List<List<String>> groupMatchList(String[] contentArray) {
        List<List<String>> res = new ArrayList<List<String>>();
        List<String> section = new ArrayList<String>();
        int sectionStart = 0;
        int i;
        for (i = 0; i < contentArray.length; i++) {
            if (getSimilarityRatio(contentArray[sectionStart], contentArray[i]) > 0.8f
                && contentArray[sectionStart].charAt(0) == contentArray[i].charAt(0)) {
                /* 这里第一行也包含了，相似并且第一个字符也要相等 */
                section.add(contentArray[i]);
            } else {
                /* 新的section开始了 */
                res.add(section);
                section = new ArrayList<String>();

                section.add(contentArray[i]);
                sectionStart = i;
            }
        }
        if (i == contentArray.length) {
            /* 最后一个section必须要最后添加 */
            res.add(section);
        }

        return res;
    }

    /**
     * first 一定在contentArray中
     */
    public static List<String> findMatchList(String first, String[] contentArray) {
        List<String> res = new ArrayList<String>();

        for (int i = 0; i < contentArray.length; i++) {
            if (first.equals(contentArray[i]) && res.size() == 0) {
                /* 第一个相等的 */
                res.add(contentArray[i]);
                continue;
            }

            if (res.size() > 0) {
                if (getSimilarityRatio(first, contentArray[i]) > 0.8f && first.charAt(0) == contentArray[i].charAt(0)) {
                    /* 相似并且第一个字符也要相等 */
                    res.add(contentArray[i]);
                } else {
                    break;
                }
            }
        }

        return res;
    }

    public static List<String> filterSpecialGroup(List<String> dbList, String defaultDb) {
        List<String> res = new ArrayList<String>();

        for (String group : dbList) {
            if (!group.equalsIgnoreCase("DUAL_GROUP")) {
                if (!group.equalsIgnoreCase(defaultDb) || ConfigDataMode.isMock()) {
                    res.add(group);
                }
            }
        }

        return res;
    }

    public static String[] asStringArray(List<String> strList) {
        String[] res = new String[strList.size()];
        for (int i = 0; i < res.length; i++) {
            res[i] = strList.get(i);
        }
        return res;
    }

    private static int min(int one, int two, int three) {
        return (one = one < two ? one : two) < three ? one : three;
    }

    public static float getSimilarityRatio(String str, String target) {
        return 1 - (float) calculateStringDistance(str, target) / Math.max(str.length(), target.length());
    }

    private static int calculateStringDistance(String str, String target) {
        int d[][]; // 矩阵
        int n = str.length();
        int m = target.length();
        int i; // 遍历str的
        int j; // 遍历target的
        char ch1; // str的
        char ch2; // target的
        int temp; // 记录相同字符,在某个矩阵位置值的增量,不是0就是1
        if (n == 0) {
            return m;
        }
        if (m == 0) {
            return n;
        }
        d = new int[n + 1][m + 1];
        for (i = 0; i <= n; i++) { // 初始化第一列
            d[i][0] = i;
        }

        for (j = 0; j <= m; j++) { // 初始化第一行
            d[0][j] = j;
        }

        for (i = 1; i <= n; i++) { // 遍历str
            ch1 = str.charAt(i - 1);
            // 去匹配target
            for (j = 1; j <= m; j++) {
                ch2 = target.charAt(j - 1);
                if (ch1 == ch2 || ch1 == ch2 + 32 || ch1 + 32 == ch2) {
                    temp = 0;
                } else {
                    temp = 1;
                }
                // 左边+1,上边+1, 左上角+temp取最小
                d[i][j] = min(d[i - 1][j] + 1, d[i][j - 1] + 1, d[i - 1][j - 1] + temp);
            }
        }
        return d[n][m];
    }

    /**
     * @return upper-case, empty is possible
     */
    public static String aliasUnescapeUppercase(String alias, boolean toUppercase) {
        if (alias == null || alias.length() <= 0) {
            return alias;
        }
        switch (alias.charAt(0)) {
        case '`':
            return unescapeName(alias, toUppercase);
        case '\'':
            return TStringUtil.getUnescapedString(alias.substring(1, alias.length() - 1), toUppercase);
        default:
            if (toUppercase) {
                return alias.toUpperCase();
            } else {
                return alias;
            }
        }
    }

    public static String unescapeName(String name, boolean toUppercase) {
        if (name == null || name.length() <= 0) {
            return name;
        }
        if (name.charAt(0) != '`') {
            return toUppercase ? name.toUpperCase() : name;
        }
        if (name.charAt(name.length() - 1) != '`') {
            throw new IllegalArgumentException("id start with a '`' must end with a '`', id: " + name);
        }
        StringBuilder sb = new StringBuilder(name.length() - 2);
        final int endIndex = name.length() - 1;
        boolean hold = false;
        for (int i = 1; i < endIndex; ++i) {
            char c = name.charAt(i);
            if (c == '`' && !hold) {
                hold = true;
                continue;
            }
            hold = false;
            if (toUppercase && c >= 'a' && c <= 'z') {
                c -= 32;
            }
            sb.append(c);
        }
        return sb.toString();
    }

    public static boolean checkIfGroupMatch(Set<String> selGroupList, Set<String> genGroupList) {
        /* 真实的group一定能包含所有推演的group才可以 */
        return selGroupList.containsAll(genGroupList);
    }

    public static List<String> groupToStringList(List<Group> dbList) {
        List<String> groupList = new ArrayList<String>();
        for (Group group : dbList) {
            groupList.add(group.getName());
        }

        return groupList;
    }

    /**
     * 检查创建的结果是否所有分表名全局唯一
     */
    public static boolean checkIfTableNameUnique(Map<String, Set<String>> topology) {
        Set<String> processedTables = new HashSet<String>();

        for (Set<String> tablesInGroup : topology.values()) {
            for (String table : tablesInGroup) {
                if (processedTables.contains(table)) {
                    return false;
                }
                processedTables.add(table);
            }
        }
        return true;
    }

    /**
     * 目前检查是否生成的分表数与客户期望的分表数相同，如果不同则报错 因为目前对于 db指定 week(gmt_create) tb指定
     * week(gmt_create)不论客户指定多大的tbpartitions数目
     * 因为一维参数的值域只可能出现1~7所以生成的物理表总数也是1~7而不是更大，因为更大也没有意义不会被访问到
     * 目前因为没有需求所以暂时先不允许这种分法
     */
    public static boolean checkIfTableNumberOk(Map<String, Set<String>> topology, int groups, int table_per_group) {
        int genTables = 0;

        for (Set<String> tablesInGroup : topology.values()) {
            genTables += tablesInGroup.size();
        }

        return (genTables == (groups * table_per_group));
    }

    public static String genTBPartitionDefinition(String tableName, int group_count, int table_count_on_each_group) {
        // /* 如果指定一个表的时候，逻辑表即物理表名 */
        // if (table_count_on_each_group == 1) {
        // return tableName;
        // }

        int maxdigit = RuleUtils.calculateDigit(group_count * table_count_on_each_group);

        StringBuffer sb = new StringBuffer();

        sb.append(tableName);
        sb.append("_{");
        for (int i = 0; i < maxdigit; i++) {
            sb.append('0');
        }
        sb.append("}");

        return sb.toString();
    }

    public static String genStandAloneTBPartitionDefinition(String tableName, int table_count_on_each_group) {
        /* 如果指定一个表的时候，逻辑表即物理表名 */
        if (table_count_on_each_group == 1) {
            return tableName;
        }

        int maxdigit = RuleUtils.calculateDigit(table_count_on_each_group);

        StringBuffer sb = new StringBuffer();

        sb.append(tableName);
        sb.append("_{");
        for (int i = 0; i < maxdigit; i++) {
            sb.append('0');
        }
        sb.append("}");

        return sb.toString();
    }

    /**
     * 根据已经存在的group列表要反推到dbNamePattern，顺序是
     * <p>
     * <pre>
     * 1. 取得List<Group>
     * 2. 过滤掉DUAL_GROUP
     * 3. 排序
     * 4. 根据未排序前的第一个group找在排序中的位置并找到与之匹配的最大相似的group
     * 5. 从头和从尾找到他们的不同处
     * 6. 结合group数目将不同部分转成{0...}的形式
     * </pre>
     */
    public static String genDBPatitionDefinition(int groupCount, int perTable, List<Group> dbList, String defaultDb,
                                                 List<String> genGroupList) {

        if (groupCount == 1) {
            /**
             * ---- comment below is deprecated.----------
             * 只有当需要的库和表数都是1才用defaultdb，否则对于单独defaultdb情况下就无法扩展了
             * 如果这种情况下defaultdb也属于分库也是可以的 ---- comment above is deprecated.
             * --------- 只要是groupCount=1 都认为是单库
             */
            genGroupList.add(defaultDb);
            return defaultDb;
        }
        /* 过滤掉DUAL_GROUP */
        List<String> groupList = RuleUtils.groupToStringList(dbList);
        groupList = RuleUtils.filterSpecialGroup(groupList, defaultDb);
        String[] groupArray = RuleUtils.asStringArray(groupList);

        /* 排序 */
        Arrays.sort(groupArray);

        if (StringUtils.isNotBlank(defaultDb)) {
            groupList = RuleUtils.findDefaultGroupList(groupArray, defaultDb);
        } else {
            /* 由于一般只有一个分库组，所以找最大相似数目的组列表 */
            groupList = RuleUtils.findMaxMatchList(groupArray);
        }

        if (groupCount == 0) {
            /**
             * 如果group_count为-1，表示没有指定dbpartitions，这样直接使用自动获取的
             * 并且返回的groupList的大小一定是推荐的group数目
             */
            groupCount = groupList.size();
        }

        /* 真正的group数一定要>=需求的group数，并且一定总表数一定是当前分库的倍数 */

        /* 这里判断是否需求的已经超过存在的group数量 */
        checkGroupCount(groupCount, perTable, groupList);

        List<String> remainGroups = new ArrayList<String>();
        for (int i = 0; i < groupCount; i++) {
            remainGroups.add(groupList.get(i));
        }
        groupList = remainGroups;
        // logger.info("group count is " + groupList.size());

        /* 将选取的groupList拷贝到外面做检查 */
        genGroupList.addAll(groupList);

        return RuleUtils.findDbNameOrTableNamePattern(groupList);
    }

    public static void checkGroupCount(int groupCount, int perTable, List<String> groupList) {
        if (groupList == null || groupList.size() == 0) {
            throw new IllegalArgumentException("no real groups for required group count:" + groupCount);
        } else if (groupList.size() < groupCount) {
            /* 需求的组一定不能超过物理存在的组数目 */
            throw new IllegalArgumentException("Required group count:" + groupCount + " exceed physical group count:"
                + groupList.size());
        } else if ((groupCount * perTable) % groupList.size() != 0) {
            /* 需求的总表数目一定要是真正物理存在组数目的倍数 */
            throw new IllegalArgumentException("Required total table count:" + groupCount * perTable
                + " must be multiple of physical group count:" + groupList.size());
        } else if ((groupCount * perTable) < groupList.size()) {
            /**
             * 需求的总表数一定药大于等于实际物理group数目，一定要能布满所有group， 这种情况下可以帮助客户平均分布所有的表，TODO:
             * 不过为了简单还是先不允许
             */
            throw new IllegalArgumentException("Required total table count:" + groupCount * perTable
                + " must at least equals to physical group count:" + groupList.size());
        }

        /**
         * 这里已经满足:
         *
         * <pre>
         * 1) 真实group数 >= 需求的group数
         * 2) 需求的总表数能够整除真实的group数
         * 3) 需求的总表数一定 >= 真实的group数 特别的,如果对于一开始需要的分库数小于所有的物理库数的情况下，需要尽量扩展到所有的 分库上
         * </pre>
         */
        if (groupCount < groupList.size()) {
            // groupCount.setValue(groupList.size());
            // perTable.setValue((groupNum*perTableNum)/groupList.size());
            throw new IllegalArgumentException("Required group count:" + groupCount
                + " less than physical group count:" + groupList.size()
                + ", please adjust parameters to dbpartitions:" + groupList.size()
                + " tbpartitions:" + ((groupCount * perTable) / groupList.size()));
        }
    }

    public static String genTableNameWithRandomSuffix(TableRule tableRule, String tableName) {
        return genTableNameWithRandomSuffix(tableRule, tableName, 1);
    }

    public static String genTableNameWithRandomSuffix(TableRule tableRule, String tableName, int tableCountPerGroup) {
        return genTableNameWithRandomSuffix(tableRule, tableName, 1, tableCountPerGroup);
    }

    public static String genTableNameWithRandomSuffix(TableRule tableRule, String tableName, int groupCount,
                                                      int tableCountPerGroup) {
        if (TStringUtil.isNotEmpty(tableRule.getTableNamePrefixForShadowTable())) {
            return EagleeyeHelper.TEST_TABLE_PREFIX + tableRule.getTableNamePrefixForShadowTable();
        }

        int lengthPlaceHolder = 0;
        int numExtraUnderScores = 1;

        if (tableCountPerGroup > 1) {
            lengthPlaceHolder = RuleUtils.calculateDigit(groupCount * tableCountPerGroup);
            numExtraUnderScores++;
        }

        int maxAllowedLengthOfTableNamePrefix = MAX_TABLE_NAME_LENGTH_MYSQL_ALLOWS
            - RANDOM_SUFFIX_LENGTH_OF_PHYSICAL_TABLE_NAME - numExtraUnderScores
            - lengthPlaceHolder;
        if (maxAllowedLengthOfTableNamePrefix < 0) {
            throw new OptimizerException("Unexpected large length of table name place holder: " + lengthPlaceHolder);
        }

        String randomSuffix;
        String existingRandomSuffix = tableRule.getExistingRandomSuffixForRecovery();

        if (TStringUtil.isNotEmpty(existingRandomSuffix)) {
            randomSuffix = existingRandomSuffix;
        } else {
            randomSuffix = RandomStringUtils.randomAlphanumeric(RANDOM_SUFFIX_LENGTH_OF_PHYSICAL_TABLE_NAME);
        }

        if (tableName.length() > maxAllowedLengthOfTableNamePrefix) {
            tableName = TStringUtil.substring(tableName, 0, maxAllowedLengthOfTableNamePrefix);
        }

        return tableName + "_" + randomSuffix;
    }

}
