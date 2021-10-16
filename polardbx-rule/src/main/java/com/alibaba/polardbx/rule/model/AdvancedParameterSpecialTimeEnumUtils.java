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

package com.alibaba.polardbx.rule.model;

import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.alibaba.polardbx.rule.enumerator.utils.CrossMonthDate;
import com.alibaba.polardbx.rule.enumerator.utils.CrossYearDate;
import com.alibaba.polardbx.rule.impl.groovy.GroovyStaticMethod;
import com.alibaba.polardbx.rule.model.AdvancedParameter.AtomIncreaseType;
import com.alibaba.polardbx.rule.utils.GroovyRuleConstant;

/**
 * @author chenmo.cm
 * @date 2019/5/9 上午11:25
 */
public class AdvancedParameterSpecialTimeEnumUtils {

    /**
     * 计算两个枚举值集合点的交集
     *
     * <prep>
     *
     *  1. 对于字符串、数字的枚举值集合，求枚举值集合的交集是以枚举值的交集取其本身的hash值作为求交的依据；
     *  2. 日期类枚举值集合，求枚举值集合的交集需要以时间对象实际参与路由计算的部分来计算它本身的hash值，并作为求交的依据；
     *     例如， 对于 YYYYMM: 2019-4-21 与 2019-4-23 ，其参与路由计算的是YYYY与MM，所以应取2019-4作为求交的hash值，
     *           所以，2019-4-21 与 2019-4-23是有交集的
     *           
     *           对于 MM: 3-1 、4-20 ，它们只有MM参与路由计算，所以应该是取3月与4月进行求交，显然 3月与4月没有交集
     *           
     *           对于YYYYDD: 2019-4-21 与 2019-4-22 由于其日期有参与路由计算，所以需要使用 YYYY MM DD 3者进行求Hash值，并计算交集
     *
     * </prep>
     *
     * @param ad
     * @param enumPointSet1
     * @param enumPointSet2
     * @return
     */
    public static Set<Object> computeEnumPointIntersection( AdvancedParameter ad, Set<Object> enumPointSet1, Set<Object> enumPointSet2  ) {

        /**
         * 目前暂时只支持 YYYYMM/YYYYMM_OPT/MM, 后边迭代再加上YYYYDD/YYYYWEEK等
         */

        String hashFuncType = ad.hashFuncType;
        Set<Object> intersectionSet = new HashSet<Object>();
        if (hashFuncType.equals(GroovyRuleConstant.YYYY_MM_I_METHOD)
            || hashFuncType.equals(GroovyRuleConstant.YYYY_MM_I_OPT_METHOD)
            || hashFuncType.equals(GroovyRuleConstant.MM_I_METHOD) ) {

            Map<Long, Object> routeHashValPointListMap = new HashMap<Long, Object>();
            for ( Object point : enumPointSet1 ) {
                Long hashVal = getHashValForTimeEnumPoint(ad, (Date)point);
                if (!routeHashValPointListMap.containsKey(hashVal)) {
                    routeHashValPointListMap.put(hashVal, point);
                }
            }
            for ( Object point : enumPointSet2 ) {
                Long hashVal = getHashValForTimeEnumPoint(ad, (Date)point);
                if (!routeHashValPointListMap.containsKey(hashVal)) {
                    routeHashValPointListMap.put(hashVal, point);
                } else {
                    intersectionSet.add(point);
                }
            }
        } else {

            // 原来的逻辑
            intersectionSet.addAll(enumPointSet1);
            intersectionSet.retainAll(enumPointSet2);
        }

        return intersectionSet;
    }

    /**
     * 根据不同的时间拆分函数类型，获取不同的时间拆分函数的hash值
     * @param ad
     * @param datePoint
     * @return
     */
    protected static  Long getHashValForTimeEnumPoint( AdvancedParameter ad, Date datePoint  ) {
        long hashVal = -1;

        String hashFuncType = ad.hashFuncType;
        if (hashFuncType.equals(GroovyRuleConstant.YYYY_MM_I_METHOD) || hashFuncType.equals(GroovyRuleConstant.YYYY_MM_I_OPT_METHOD) ) {
            hashVal =  GroovyStaticMethod.yyyymm_i(datePoint);
        } else if (hashFuncType.equals(GroovyRuleConstant.MM_I_METHOD)) {
            hashVal =  GroovyStaticMethod.mm_i(datePoint);
        } else {
            datePoint.hashCode();
        }
        return hashVal;
    }

    /**
     *
     *
     * 问题背景：
     * 自然年有大小年之分，自然月也有大小月之分 对于需要按天（或按周）进行分片枚举时，
     * 枚举的时间点会因大小年或大小月产生不连接的函数，导致分片缺失,例如：
     * 		DD 是直接取日期的天数作为hash值，
     *       假如逻辑表 按 DD(#1_date,31#) % 31 分表不分库（共31张分表），
     *       现在查询： '2018-02-1' <= t <'2018-04-1' 之间的数据，
     *       由于2月份只有28天（假设是平年），所以只需要遍历 第1~28张 分表，
     *       而3月份有31天，则需要 遍历 第1~31张 分表.
     *       但是，从 2018-02-1 开始按天枚举，枚举31次后，得到的结果为
     *       2.1 ~ 2.28  （前28次枚举）
     *       3.1 ~ 3.3   （后3次枚举，共31天）
     *       然后，这次枚举涉及的天数是是 1~28 （因为后3次的枚举出现了重复），
     *       导致3月份的数据会扫描不全，产生分片遗漏。
     *
     *  该函数的作用： 对于时间枚举点，判断一下这个时间点的下一个时间枚举点是否将要跨库年跨月，
     *      如果是，则它这个时间点检测一下是否还有遗漏的分片还没有扫描，如果有的话，就通过人工添加的
     *      特殊时间枚举点（CrossYearDate或CrossMonthDate）补上。这些特殊时间枚举点能路由到那些缺失的
     *      分片，从而实现跨年或跨月的范围查询的分片补全。
     *
     * @param ad
     * @param enumPoint 时间枚举点
     * @param allEnumResult
     * @param enumDirection  enumDirection: 0--时间从小到大；1--时间从大到小
     */
    public static void processSpecialTimeEnumPoint( AdvancedParameter ad, Date enumPoint,  Set<Object> allEnumResult, int enumDirection ) {

        if (ad.atomicIncreateType == AtomIncreaseType.WEEK || ad.atomicIncreateType == AtomIncreaseType.WEEK_ABS ) {

            // 获取hash函数类型
            String hashFuncType = ad.hashFuncType;

            if (hashFuncType.equalsIgnoreCase(GroovyRuleConstant.YYYY_WEEK_I_METHOD)) {
                processSpecialTimeEnumPointForYYYYWEEK(ad, enumPoint, allEnumResult, enumDirection, false);
            } else if (hashFuncType.equalsIgnoreCase(GroovyRuleConstant.YYYY_WEEK_I_OPT_METHOD)) {
                processSpecialTimeEnumPointForYYYYWEEK(ad, enumPoint, allEnumResult, enumDirection, true);

            }

        } else if (ad.atomicIncreateType == AtomIncreaseType.DATE || ad.atomicIncreateType == AtomIncreaseType.DATE_ABS) {

            // 获取hash函数类型
            String hashFuncType = ad.hashFuncType;

            if (hashFuncType.equalsIgnoreCase(GroovyRuleConstant.YYYY_DD_I_METHOD)) {

                // 获取表的总举次数
                int cumulativeTimes = ad.cumulativeTimes;

                // 获取下一个枚举点
                Calendar cal = Calendar.getInstance();
                cal.setTime(enumPoint);
                int yearOfCurPoint = cal.get(Calendar.YEAR);

                if (enumDirection == 0) {
                    cal.add(Calendar.DATE, 1);
                } else {
                    cal.add(Calendar.DATE, -1);
                }
                Date nextEnumPoint = cal.getTime();
                int yearOfNextPoint = cal.get(Calendar.YEAR);

                boolean isNextPointWillCrossYear = checkIfNextPointWillCrossYear(enumDirection, yearOfCurPoint,
                    yearOfNextPoint);

                if (!isNextPointWillCrossYear) {
                    // 当前日期不是一年中的最后一天
                    return;
                }

                // 来到这里，当前日期不是一年中的最后一天

                // 获取当前枚举时间点所对应的分表下标
                long idxOfCurPoint = GroovyStaticMethod.yyyydd_i(enumPoint) % cumulativeTimes;
                // 获取当前枚举时间点所对应的分表下标
                long idxOfNextPoint = GroovyStaticMethod.yyyydd_i(nextEnumPoint) % cumulativeTimes;

                eumPointForSpecialDate(enumPoint, allEnumResult, cumulativeTimes, idxOfCurPoint, idxOfNextPoint, false, enumDirection);

            } else if (hashFuncType.equalsIgnoreCase(GroovyRuleConstant.YYYY_DD_I_OPT_METHOD)) {


                // 获取表的总举次数
                int cumulativeTimes = ad.cumulativeTimes;

                // 获取下一个枚举点
                Calendar cal = Calendar.getInstance();
                cal.setTime(enumPoint);
                int yearOfCurPoint = cal.get(Calendar.YEAR);

                if (enumDirection == 0) {
                    cal.add(Calendar.DATE, 1);
                } else {
                    cal.add(Calendar.DATE, -1);
                }
                Date nextEnumPoint = cal.getTime();
                int yearOfNextPoint = cal.get(Calendar.YEAR);

                boolean isNextPointWillCrossYear = checkIfNextPointWillCrossYear(enumDirection, yearOfCurPoint,
                    yearOfNextPoint);

                if (!isNextPointWillCrossYear) {
                    // 当前日期不是一年中的最后一天
                    return;
                }

                // 来到这里，当前日期不是一年中的最后一天

                /**PRP
                 * 注意，由于YYYY_DD_I_OPT 将不再支持用于作为分库函数，所以这里直接按分表的计算方式进行补缺失的分片枚举值
                 */
                // 获取当前枚举时间点所对应的分表下标
                long idxOfCurPoint = GroovyStaticMethod.yyyydd_i_opt(enumPoint, cumulativeTimes, 0, 0);

                // 获取当前枚举时间点所对应的分表下标
                long idxOfNextPoint = GroovyStaticMethod.yyyydd_i_opt(nextEnumPoint, cumulativeTimes, 0, 0);

                eumPointForSpecialDate(enumPoint, allEnumResult, cumulativeTimes, idxOfCurPoint, idxOfNextPoint, false, enumDirection);

            } else if (hashFuncType.equalsIgnoreCase(GroovyRuleConstant.MM_DD_I_METHOD)) {


                // 获取表的总举次数
                int cumulativeTimes = ad.cumulativeTimes;

                // 获取下一个枚举点
                Calendar cal = Calendar.getInstance();
                cal.setTime(enumPoint);
                int yearOfCurPoint = cal.get(Calendar.YEAR);

                if (enumDirection == 0) {
                    cal.add(Calendar.DATE, 1);
                } else {
                    cal.add(Calendar.DATE, -1);
                }
                Date nextEnumPoint = cal.getTime();
                int yearOfNextPoint = cal.get(Calendar.YEAR);

                boolean isNextPointWillCrossYear = checkIfNextPointWillCrossYear(enumDirection, yearOfCurPoint,
                    yearOfNextPoint);

                if (!isNextPointWillCrossYear) {
                    // 当前日期不是一年中的最后一天
                    return;
                }

                // 来到这里，当前日期不是一年中的最后一天

                // 获取当前枚举时间点所对应的分表下标
                long idxOfCurPoint = GroovyStaticMethod.mmdd_i(enumPoint) % cumulativeTimes;

                // 获取当前枚举时间点所对应的分表下标
                long idxOfNextPoint = GroovyStaticMethod.mmdd_i(nextEnumPoint) % cumulativeTimes;

                eumPointForSpecialDate(enumPoint, allEnumResult, cumulativeTimes, idxOfCurPoint, idxOfNextPoint, false, enumDirection);

            }  else if (hashFuncType.equalsIgnoreCase(GroovyRuleConstant.DD_I_METHOD)) {

                // 获取表的总举次数
                int cumulativeTimes = ad.cumulativeTimes;

                // 获取下一个枚举点
                Calendar cal = Calendar.getInstance();
                cal.setTime(enumPoint);
                int monthOfCurPoint = cal.get(Calendar.MONTH);

                if (enumDirection == 0) {
                    cal.add(Calendar.DATE, 1);
                } else {
                    cal.add(Calendar.DATE, -1);
                }

                Date nextEnumPoint = cal.getTime();
                int monthOfNextPoint = cal.get(Calendar.MONTH);


                boolean isNextPointWillCrossMonth = checkIfNextPointWillCrossYear(enumDirection, monthOfCurPoint,
                    monthOfNextPoint);

                if (!isNextPointWillCrossMonth) {
                    // 当前日期不是一月中的最后一天
                    return;
                }

                // 来到这里，当前日期不是一个月中的最后一天

                // 获取当前枚举时间点所对应的分表下标
                long idxOfCurPoint = GroovyStaticMethod.dd_i(enumPoint) % cumulativeTimes;

                // 获取当前枚举时间点所对应的分表下标
                long idxOfNextPoint = GroovyStaticMethod.dd_i(nextEnumPoint) % cumulativeTimes;

                eumPointForSpecialDate(enumPoint, allEnumResult, cumulativeTimes, idxOfCurPoint, idxOfNextPoint, true, enumDirection);

            }

        }

    }

    private static boolean checkIfNextPointWillCrossYear(int enumDirection, int yearOfCurPoint, int yearOfNextPoint) {
        boolean isNextPointWillCrossYear = false;
        if (enumDirection == 0) {
            if (yearOfNextPoint > yearOfCurPoint) {
                isNextPointWillCrossYear = true;
            }
        } else {
            if (yearOfNextPoint < yearOfCurPoint) {
                isNextPointWillCrossYear = true;
            }
        }
        return isNextPointWillCrossYear;
    }

    private static void processSpecialTimeEnumPointForYYYYWEEK(AdvancedParameter ad, Date enumPoint,
                                                               Set<Object> allEnumResult, int enumDirection, boolean isYyyyweekOpt) {


        // 获取表的总举次数
        int cumulativeTimes = ad.cumulativeTimes;

        // 获取下一个枚举点
        Calendar cal = Calendar.getInstance();
        cal.setTime(enumPoint);

        // 获取一下当前时间的所属周的年份
        int weekIdxOfCurPoint = cal.get(Calendar.WEEK_OF_YEAR);

        if (enumDirection == 0 ) {
            // 日期向后推移7天
            cal.add(Calendar.WEEK_OF_YEAR, 1);
        } else {
            // 日期向前推前7天
            cal.add(Calendar.WEEK_OF_YEAR, -1);
        }

        Date nextEnumPoint = cal.getTime();
        int weekIdxOfNextPoint = cal.get(Calendar.WEEK_OF_YEAR);

        boolean isCurDatePointInMagicPoint = false;
        boolean isNextDatePointInMagicPoint = false;
        Date magicPointOfCurPoint = checkAndGetEnumDateInMagicPointsIfNeedForYYYYWEEK(enumPoint);
        if (magicPointOfCurPoint != null) {
            isCurDatePointInMagicPoint = true;
        }
        Date magicPointOfNextPoint = checkAndGetEnumDateInMagicPointsIfNeedForYYYYWEEK(nextEnumPoint);
        if (magicPointOfNextPoint != null) {
            isNextDatePointInMagicPoint = true;
        }

        boolean isNextDatePointWillCrossYear = false;
        if (enumDirection == 0) {
            if (weekIdxOfCurPoint > weekIdxOfNextPoint) {
                isNextDatePointWillCrossYear = true;
            }
        } else {
            if (weekIdxOfCurPoint < weekIdxOfNextPoint) {
                isNextDatePointWillCrossYear = true;
            }
        }

        if (!isNextDatePointWillCrossYear) {

            if (isCurDatePointInMagicPoint) {
                allEnumResult.add(magicPointOfCurPoint);
            }

            return;

        }

        Date newEnumPoint = enumPoint;
        if (isCurDatePointInMagicPoint) {
            newEnumPoint = magicPointOfCurPoint;
        }

        Date newNextEnumPoint = nextEnumPoint;
        if (isNextDatePointInMagicPoint) {
            newNextEnumPoint = magicPointOfNextPoint;
        }

        // 获取当前枚举时间点所对应的分表下标

        long idxOfCurPoint = -1;
        long idxOfNextPoint = -1;
        if (!isYyyyweekOpt) {
            idxOfCurPoint = GroovyStaticMethod.yyyyweek_i(newEnumPoint) % cumulativeTimes;

            // 获取当前枚举时间点所对应的分表下标
            idxOfNextPoint = GroovyStaticMethod.yyyyweek_i(newNextEnumPoint) % cumulativeTimes;
        } else {

            /**
             * YYYYWEEK_OPT 将不再支持用于作为分库函数
             */

            // Date date, int allTbCount, int allDbCount, int allInstCount
            idxOfCurPoint = GroovyStaticMethod.yyyyweek_i_opt(newEnumPoint, cumulativeTimes, 0, 0) % cumulativeTimes;

            // 获取当前枚举时间点所对应的分表下标
            idxOfNextPoint = GroovyStaticMethod.yyyyweek_i_opt(newNextEnumPoint, cumulativeTimes, 0, 0) % cumulativeTimes;
        }


        long tmpNextIdx = idxOfCurPoint;
        long tmpTargetIdx = idxOfNextPoint;

        long offset = 0;
        long realOffSetVal = 0;

        while (true ) {

            if (enumDirection == 0) {
                long newTmpNextIdx = tmpNextIdx + 1;
                tmpNextIdx = newTmpNextIdx % cumulativeTimes;
            } else {
                long newTmpNextIdx = tmpNextIdx - 1;
                if (newTmpNextIdx < 0) {
                    newTmpNextIdx = cumulativeTimes - 1;
                }
                tmpNextIdx = newTmpNextIdx % cumulativeTimes;
            }

            if (tmpNextIdx == tmpTargetIdx) {
                break;
            } else {

                // 检测到漏表
                if (enumDirection == 0) {
                    ++offset;
                } else {
                    --offset;
                }


                CrossYearDate crossYearDate = null;
                Date enumPointToBeAdd = newEnumPoint;
                if (enumPointToBeAdd instanceof CrossYearDate) {
                    long offSetOfNewEnumPoint = ((CrossYearDate)enumPointToBeAdd).getOffset();
                    crossYearDate = new CrossYearDate(enumPointToBeAdd.getTime());
                    realOffSetVal = offset + offSetOfNewEnumPoint;
                    crossYearDate.setOffset(realOffSetVal);
                } else {
                    crossYearDate = new CrossYearDate(enumPointToBeAdd.getTime());
                    realOffSetVal = offset;
                    crossYearDate.setOffset(realOffSetVal);
                }

                allEnumResult.add(crossYearDate);
            }
        }
    }

    protected static void eumPointForSpecialDate(Date enumPoint, Set<Object> allEnumResult, int cumulativeTimes,
                                                 long idxOfCurPoint, long idxOfNextPoint, boolean isCrossMonth, int enumDirection) {
        long tmpNextIdx = idxOfCurPoint;
        long offset = 0;
        while (true ) {

            long tmpNewNextIdx = -1;
            if (enumDirection == 0) {
                tmpNewNextIdx = tmpNextIdx + 1;
            } else {
                tmpNewNextIdx = tmpNextIdx - 1;
                if (tmpNewNextIdx < 0) {
                    tmpNewNextIdx = cumulativeTimes - 1;
                }
            }

            tmpNextIdx = tmpNewNextIdx % cumulativeTimes;
            if (tmpNextIdx == idxOfNextPoint) {
                break;
            } else {

                // 检测到漏表
                if (enumDirection == 0) {
                    ++offset;
                } else {
                    --offset;
                }

                if (isCrossMonth) {
                    CrossMonthDate crossMonthDate = new CrossMonthDate(enumPoint.getTime());
                    crossMonthDate.setOffset(offset);
                    allEnumResult.add(crossMonthDate);
                } else {
                    CrossYearDate crossYearDate = new CrossYearDate(enumPoint.getTime());
                    crossYearDate.setOffset(offset);
                    allEnumResult.add(crossYearDate);
                }


            }
        }
    }

    /**
     * 该函数的作用： 检测一个时间点（按week进行枚举），是否为奇异点
     *  如果是，返回奇异点对应的使用正确年份的修正后的时间点；
     *  如果否，返回NULL
     *
     * 奇异点的定义：
     *  YYYYWEEK / YYYYWEEK_OPT 的路由函数实现上有个BUG：
     *      对于跨年周中日期的下一周对应时间点，
     *      即下图中 大括号 {}之间的日期 ( 2019.1.5 ~ 2019.1.11 )
     *      这段时间日期因为  YYYYWEEK / YYYYWEEK_OPT 实现的BUG，在计算
     *      其日期对应的hash值时，会错误地使用了去年（即用了2018，而没有用2019年）的年份。
     *
     * 这个BUG不会影响用户的点查点写，但会对 YYYYWEEK / YYYYWEEK_OPT 的日期枚举产生不连续，因此也会产生分片缺失。
     * 这部分的时间点，定义为奇异点。
     *
     *  年份                               2018 |  2019
     *  日期                ...   28 29  30  31 |  1    2    3    4     5   6   7   8   9   10  11  ...
     *  星期：     <7 1 2 > 3 4 5 6 （7   1  2)  | 3    4    5    6   { 7   1   2   3   4   5   6} (7 1 2) 3 4 5 6 7
     *                     -----------------
     *   {}之间的日期就是奇异日期点，要找出来
     *
     * 奇异点也会导致分片缺失，因此针对这类时间单独识别出来并补充上正确的时间点。
     *
     * @param enumPoint
     * @return
     */
    public static CrossYearDate checkAndGetEnumDateInMagicPointsIfNeedForYYYYWEEK(Date enumPoint ) {


        boolean isCurDatePointInMagicPoint = false;


        Calendar calOfCurEnumPoint = Calendar.getInstance();
        calOfCurEnumPoint.setTime(enumPoint);

        int yearOfCurPoint = calOfCurEnumPoint.get(Calendar.YEAR);
        int weekIdxOfCurPoint = calOfCurEnumPoint.get(Calendar.WEEK_OF_YEAR);


        // 计算一下当前日期的7天前的日期
        Calendar prevPoint = (Calendar) calOfCurEnumPoint.clone();
        prevPoint.add(Calendar.WEEK_OF_YEAR, -1);
        int yearOfPrevPoint = prevPoint.get(Calendar.YEAR);
        int weekIdxOfPrevPoint = prevPoint.get(Calendar.WEEK_OF_YEAR);

        if (  ( yearOfPrevPoint < yearOfCurPoint ) && ( weekIdxOfPrevPoint < weekIdxOfCurPoint ) ) {

            // 找到奇异日期点
            isCurDatePointInMagicPoint = true;

        }

        if (!isCurDatePointInMagicPoint) {
            return null;
        }


        // 获取当前枚举时间点所对应的分表下标
        CrossYearDate crossYearDate = new CrossYearDate(enumPoint.getTime());
        crossYearDate.setOffset(54);
        return crossYearDate;

    }

}
