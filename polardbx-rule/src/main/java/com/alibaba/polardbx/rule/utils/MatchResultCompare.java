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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.alibaba.polardbx.rule.model.MatcherResult;
import com.alibaba.polardbx.rule.model.TargetDB;

/**
 * 有些乱七八糟的oriDb/oriTable比较，我也没想明白具体的应用场景<br/>
 * 猜测应该和动态迁移有关，比如指定某个库/表完成了迁移，就单独开放这个表写入权限
 * 
 * @author <a href="junyu@taobao.com">junyu</a>
 * @author <a href="jianghang.loujh@taobao.com">jianghang</a>
 * @version 1.0
 * @since 1.6
 * @date 2011-4-22 12:49:53
 */
public class MatchResultCompare {

    /**
     * 新旧matcherResult对比
     * 
     * @param resultNew
     * @param resultOld
     * @return 如果库表完全相同返回true,否则返回false
     */
    public static boolean matchResultCompare(MatcherResult resultNew, MatcherResult resultOld) {
        return matchResultCompare(resultNew, resultOld, null, null);
    }

    /**
     * 新旧MatchResult对比,如果不同,对比当前库表是否和新规则计算结果一致
     * 
     * @param resultNew
     * @param resultOld
     * @param oriDb
     * @param oriTable
     * @return
     */
    public static boolean matchResultCompare(MatcherResult resultNew, MatcherResult resultOld, String oriDb,
                                             String oriTable) {
        List<TargetDB> targetNew = resultNew.getCalculationResult();
        List<TargetDB> targetOld = resultOld.getCalculationResult();
        return innerCompare(targetNew, targetOld, oriDb, oriTable);
    }

    /**
     * 确定取数据的库表是否在规则计算的结果之内.
     * 
     * @param resultNew
     * @param oriDb
     * @param oriTable
     * @return false 不存在, true 表示规则计算结果完全包含取数据的库表
     */
    public static boolean oriDbTabCompareWithMatchResult(MatcherResult resultNew, String oriDb, String oriTable) {
        List<TargetDB> targetNew = resultNew.getCalculationResult();
        return oriDbTabCompareWithTargetDb(targetNew, oriDb, oriTable);
    }

    /**
     * 新旧TargetDB对比
     * 
     * @param targetNew
     * @param targetOld
     * @return 如果库表完全相同返回true,否则返回false
     */
    public static boolean targetDbCompare(List<TargetDB> targetNew, List<TargetDB> targetOld) {
        return targetDbCompare(targetNew, targetOld, null, null);
    }

    /**
     * 新旧TargetDB对比,如果不同,对比当前库表是否和新规则计算结果一致
     * 
     * @param targetNew
     * @param targetOld
     * @param oriDb
     * @param oriTable
     * @return
     */
    public static boolean targetDbCompare(List<TargetDB> targetNew, List<TargetDB> targetOld, String oriDb,
                                          String oriTable) {
        return innerCompare(targetNew, targetOld, oriDb, oriTable);
    }

    /**
     * 确定取数据的库表是否在规则计算的结果之内.
     * 
     * @param resultNew
     * @param oriDb
     * @param oriTable
     * @return false 不存在, true 表示规则计算结果完全包含取数据的库表
     */
    public static boolean oriDbTabCompareWithTargetDb(List<TargetDB> targetNew, String oriDb, String oriTable) {
        Map<String, Map<String, String>> dbTabMap = getTargetMap(targetNew);
        Map<String, String> tables = dbTabMap.get(oriDb);
        if (tables == null) {
            return false;
        } else if (tables.get(oriTable) == null) {
            return false;
        } else {
            return true;
        }
    }

    // ========================== helper method =====================

    /**
     * 对比下新旧库
     */
    private static boolean innerCompare(List<TargetDB> targetNew, List<TargetDB> targetOld, String oriDb,
                                        String oriTable) {
        Map<String, Map<String, String>> newOne = getTargetMap(targetNew);
        Map<String, Map<String, String>> oldOne = getTargetMap(targetOld);
        boolean dbDiff = false;
        boolean tbDiff = false;
        // 正向比较
        for (Map.Entry<String, Map<String, String>> entry : newOne.entrySet()) {
            Map<String, String> oldTables = oldOne.get(entry.getKey());
            if (oldTables != null) {
                // 正向比较表
                for (Map.Entry<String, String> newTbEntry : entry.getValue().entrySet()) {
                    String tb = oldTables.get(newTbEntry.getKey());
                    if (tb == null) {
                        tbDiff = true;
                    }
                }
                // 反向比较表
                for (Map.Entry<String, String> oldTbEntry : oldTables.entrySet()) {
                    String tb = entry.getValue().get(oldTbEntry.getKey());
                    if (tb == null) {
                        tbDiff = true;
                    }
                }
            } else {
                dbDiff = true;
            }
        }

        // 反向只检测库计算结果是否相同,因为表计算结果比较正向就可以得到
        for (Map.Entry<String, Map<String, String>> entry : oldOne.entrySet()) {
            Map<String, String> newTables = newOne.get(entry.getKey());
            if (newTables == null) {
                dbDiff = true;
            }
        }

        return compareResultAnalyse(newOne, oriDb, oriTable, dbDiff, tbDiff);
    }

    /**
     * 判断oriDb+oriTable是否出现在目标中
     */
    private static boolean compareResultAnalyse(Map<String, Map<String, String>> newResult, String oriDb,
                                                String oriTable, boolean dbDiff, boolean tbDiff) {
        // 如果表计算结果相同,库计算结果不同
        if (dbDiff && oriDb != null) {
            Map<String, String> tables = newResult.get(oriDb);
            if (tables != null) {
                // 如果本库就是目标库
                return compareResutlAnalyseTable(tables, oriTable, tbDiff);
            } else {
                // 如果本库不是目标库,那么说明需要迁移.
                return false;
            }
        } else if (dbDiff && oriDb == null) {
            return false;
        } else {
            // 如果库结果计算也相同,看表计算结果
            return compareResutlAnalyseTable(newResult.get(oriDb), oriTable, tbDiff);
        }
    }

    /**
     * 判断oriTable是否出现在目标表中
     */
    private static boolean compareResutlAnalyseTable(Map<String, String> tables, String oriTable, boolean tbDiff) {
        if (tbDiff && oriTable != null) {
            if (tables.get(oriTable) != null) {
                return true;
            } else {
                return false;
            }
        } else if (tbDiff && oriTable == null) {
            return false;
        } else {
            return true;
        }
    }

    /**
     * 将数据转化为map结构
     * 
     * @param targetDbs
     * @return
     */
    private static Map<String, Map<String, String>> getTargetMap(List<TargetDB> targetDbs) {
        Map<String, Map<String, String>> reMap = new HashMap<String, Map<String, String>>();
        for (TargetDB db : targetDbs) {
            Map<String, String> tableMap = new HashMap<String, String>();
            Set<String> tables = db.getTableNames();
            for (String table : tables) {
                tableMap.put(table, table);
            }
            reMap.put(db.getDbIndex(), tableMap);
        }

        return reMap;
    }
}
