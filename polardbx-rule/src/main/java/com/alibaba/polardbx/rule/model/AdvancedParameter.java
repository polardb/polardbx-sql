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

import com.alibaba.polardbx.common.utils.time.old.DateUtils;
import com.alibaba.polardbx.rule.utils.GroovyRuleConstant;
import com.taobao.tddl.common.utils.TddlToStringStyle;
import com.alibaba.polardbx.rule.MappingRule;
import com.alibaba.polardbx.rule.Rule;
import com.alibaba.polardbx.rule.TableRule;
import com.alibaba.polardbx.rule.impl.GroovyRule;
import com.alibaba.polardbx.rule.meta.StringNum;
import com.alibaba.polardbx.rule.utils.AdvancedParameterParser;
import com.alibaba.polardbx.rule.utils.NoloopCoverRuleProcessor;
import com.alibaba.polardbx.rule.VirtualTableSupport;
import com.alibaba.polardbx.rule.utils.GroovyRuleShardFuncFinder;
import org.apache.commons.lang.builder.ToStringBuilder;

import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/**
 * 通过{@linkplain AdvancedParameterParser}.getAdvancedParamByParamTokenNew()进行构建
 */
public class AdvancedParameter extends Rule.RuleColumn {

    private static long CONST_WEEK = 3600 * 1000 * 24 * 7;
    /**
     * 自增，给枚举器用的
     */
    public final Comparable<?> atomicIncreateValue;
    /**
     * 叠加次数，给枚举器用的
     */
    public Integer cumulativeTimes;

    /**
     * 分库分表规则中定义的原始的枚举次数
     */
    public Integer cumulativeTimesInRule;
    /**
     * 决定当前参数是否允许范围查询如>= <= ...
     */
    public final boolean needMergeValueInCloseInterval;
    /**
     * 自增的类型，包括
     */
    public AtomIncreaseType atomicIncreateType;
    /**
     * 起始与结束值对象列表，通过"|"分割
     */
    public final Range[] rangeArray;
    /**
     * 当前Rule包含需要特殊处理枚举的时间函数如data_i和mmdd_i
     */
    private boolean isSpecialDateMethod = false;

    /**
     * 当前AdvancedParameter所属的表达式规则
     */
    protected Rule exprRule = null;

    /**
     * 当前AdvancedParameter所属的tableRule所对应的hashFunc类型
     * <pre>
     *
     * <pre/>
     */
    protected String hashFuncType;

    public AdvancedParameter(String key, Comparable<?> atomicIncreateValue, Integer cumulativeTimes, boolean needAppear,
                             AtomIncreaseType atomicIncreateType, Range[] rangeObjectArray,
                             Rule exprRuleOfAdvancedParameter) {
        super(key, needAppear);

        this.atomicIncreateValue = atomicIncreateValue;
        this.atomicIncreateType = atomicIncreateType;
        this.cumulativeTimes = cumulativeTimes;
        this.cumulativeTimesInRule = cumulativeTimes;
        this.rangeArray = rangeObjectArray;

        if (atomicIncreateValue != null) {
            this.needMergeValueInCloseInterval = true;
        } else {
            this.needMergeValueInCloseInterval = false;
        }

        this.exprRule = exprRuleOfAdvancedParameter;
        initAdvancedParameterContextByExprRule(this.exprRule);

    }

    protected void initAdvancedParameterContextByExprRule(Rule exprRule) {
        if (exprRule instanceof GroovyRule) {
            GroovyRule groovyRule = (GroovyRule) exprRule;

            VirtualTableSupport virRule = groovyRule.getTableRule();
            TableRule tableRule = null;
            if (virRule instanceof TableRule) {
                tableRule = (TableRule) virRule;

                if ((tableRule.getDbShardFunctionMeta() == null && tableRule.getTbShardFunctionMeta() == null)) {
                    String expr = groovyRule.getExpression().toLowerCase();
                    hashFuncType = "unknown";
                    for (int i = 0; i < GroovyRuleShardFuncFinder.groovyDateMethodFunctionList.size(); ++i) {
                        String groovyFunName = GroovyRuleShardFuncFinder.groovyDateMethodFunctionList.get(i);
                        if (expr.contains(groovyFunName)) {
                            hashFuncType = groovyFunName;
                            if (groovyFunName.equals(GroovyRuleConstant.YYYY_WEEK_I_METHOD) || groovyFunName.equals(
                                GroovyRuleConstant.YYYY_WEEK_I_OPT_METHOD)) {
                                /**
                                 * YYYYWEEK change to use the day as min enum unit, so total enum time use 366
                                 */
                                this.atomicIncreateType = AtomIncreaseType.DATE;
                                int oldCumulativeTimes = this.cumulativeTimes;
                                this.cumulativeTimes = Integer.valueOf((oldCumulativeTimes / 53) + 1) * 366;
                                if (this.rangeArray != null && this.rangeArray.length > 0) {
                                    Range rng = this.rangeArray[0];
                                    this.rangeArray[0] = new Range(rng.start, cumulativeTimes);
                                }

                            }
                            break;
                        }
                    }
                } else {
                    hashFuncType = "unknown";
                }
            } else {
                hashFuncType = "unknown";
            }

        }
    }

    public Set<Object> enumerateRangeWithHot(TableRule tableRule, Map<String, Set<MappingRule>> extMappingRules) {
        Set<Object> sets = enumerateRange(tableRule);
        if (extMappingRules != null) {
            for (Iterator<String> iterator = extMappingRules.keySet().iterator(); iterator.hasNext(); ) {
                String next = iterator.next();
                final Set<MappingRule> mappingRules = extMappingRules.get(next);
                sets.addAll(mappingRules);
            }
        }
        return sets;
    }

    /**
     * TDDL初始化的自枚举
     * <p>
     * <pre>
     *      TDDL初始化需要进行规则自枚举，自枚举的一大特别是无需关注起始值可自定义起始值
     *
     *      TDDL的规则枚举按类型可以分为3大类型：整数、时间与字符串
     *      TDDL的规则枚举定义可以分为没带初始值的枚枚举与没带初始化的区间枚举
     *
     *          (col?表示为col可选列，若col是可先，则选择rule时，sql可以不包含该列。到时对该列值域做遍历)
     *
     *
     *
     *      =======以下的规则枚举都是合法表达式=======
     *      整数类型：
     *
     *          #id,4# --> 默认步长1，默认类型是整型, 默认起始值为0，枚举区间[0,4)，枚举4次，枚举结果： 0,1,2,3,4
     *          #id,1_number,4# -->  步长1，类型是整型, 默认起始值为0，枚举区间[0,4]，枚举4次，枚举结果： 0,1,2,3,4
     *          #id,1,4# -->  步长1，默认类型是整型, 默认起始值为0，枚举区间[0,4],枚举4次，枚举结果： 0,1,2,3,4
     *          #id,4,4# -->  步长4，默认类型是整型, 默认起始值为4，枚举区间[0,4],枚举4次，枚举结果： 0,1,2,3,4,4(+1*4),8(+2*4),12(+3*4),16(+4*4)
     *          #id,4,-4# --> 步长4，默认类型是整型, 默认起始值为-4，枚举区间[-4,0],枚举4次，枚举结果： -4,-3,-2,-1, 0,-16(-4*4),-12(-3*4),-8(-2*4),-4(-1*4),0(0*4)
     *
     *          (如果范围有多段("|"分割)，那么规则的总枚举次数[至少枚举多少次可以知道完成全表扫描]以第一段的跨度为标准)
     *          #id,2_number,4|6|8_10# 步长2, 类型是整型, 分3个区间[0~4]、6与[8~10]进行枚举,总枚举为4-0
     *                                      ，枚举结果： 0,1,2,3,4, 2,4,6,8
     *                                                  6,
     *                                                8,9,10 , 16,18,20
     *          #id,4_number,-1_4m#  --> 步长4, 类型是整型, 分1个区间[-1,4000000]进行枚举,枚举4000002次
     *
     *      时间类型：
     *          #gmt_create,2_month,-2_2#
     *              默认步长2个月，默认类型是时间, 默认起始值为0，枚举区间[-2,2]，枚举5次，枚举结果：
     *
     *                  now - 2month,
     *                  now - 1month,
     *                  now,
     *                  now + 1month,
     *                  now + 2month,
     *
     *                  now - 4month,
     *                  now - 2month,
     *                  now,
     *                  now + 2month,
     *                  now + 4month
     *
     *          #gmt_create,1_week,4#
     *          #gmt_create,1_date,-4_4#
     *          #gmt_create,1_hour,12#
     *
     *      字符串类型：
     *          #name,2_string,a_z#
     *              步长2个char，类型是str, 默认起始值为a，枚举区间[a,z]，枚举26次，
     *              枚举结果：
     *                  a,b,c,d,...,z,
     *
     *          #name,1_string,12#
     *              步长1个char，类型是str, 默认起始值为0，枚举区间[0,12]，枚举12次，
     *              枚举结果（ascii）：
     *                  0,1,2,3,...,12
     * </pre>
     */
    public Set<Object> enumerateRange(TableRule tableRule) {
        Set<Object> values = new HashSet<Object>();
        if (atomicIncreateType.isTime()) {
            /**
             * 因为当前时间会导致枚举取样有时不能达到31天或者闰年不能到366天，所以这里设置特殊的年份做起点， 使用1908-1-1作为起点
             * 而且因为有用户直接依赖当前的时间进行枚举，所以不能对所有情形都使用这个固定日期枚举
             * 如yymmdd(#gmt,1_year,-3_3#)
             */
            Calendar c = Calendar.getInstance();

            if (isSpecialDateMethod) {
                /**
                 * 这里对于ddlCreate调用产生的规则都认为需要枚举时间到最大31天和 366天闰年上限
                 */
                @SuppressWarnings("deprecation")
                Date specialDate = new Date(1908 - 1900, 1 - 1, 1);
                c.setTime(specialDate);
            } else if (atomicIncreateType == AtomIncreaseType.DATE) {
                Date specialDate = new Date(1928 - 1900, 1 - 1, 1);
                c.setTime(specialDate);
            }
            int startYear = 0;
            int endYear = 0;
            Object evalTime;
            for (Range ro : rangeArray) {
                for (int i = ro.start; i <= ro.end; i++) {
                    evalTime = evalTime(c, i);

                    if (i == ro.start && evalTime instanceof Date) {
                        Calendar calendar = Calendar.getInstance();
                        calendar.setTime((Date) evalTime);
                        startYear = calendar.get(Calendar.YEAR);
                    }
                    if (i == ro.end && evalTime instanceof Date) {
                        Calendar calendar = Calendar.getInstance();
                        calendar.setTime((Date) evalTime);
                        endYear = calendar.get(Calendar.YEAR);
                    }

                    values.add(evalTime);

                    if (evalTime instanceof Date) {
                        processSpecialTimeEnumPoint(this, (Date) evalTime, values);
                    }
                }

                // 特别处理下，如果补偿不为1的情况
                if (atomicIncreateValue instanceof Number && ((Number) atomicIncreateValue).longValue() > 1) {
                    long incV = ((Number) atomicIncreateValue).longValue();
                    for (int i = ro.start; i <= ro.end; i++) {
                        values.add(evalTime(c, (int) (i * incV)));
                    }
                }

                //if (atomicIncreateValue instanceof DateEnumerationParameter
                //    && ((DateEnumerationParameter) atomicIncreateValue).atomicIncreatementNumber == 1) {
                //    // Noted that the first day of year is 1, thus we will miss
                //    // day 0 in hashcode calc for every
                //    // When enumeration step is 1, do compensation for every
                //    // year that has been enumerated.
                //    for (int i = 1; i <= endYear - startYear; i++) {
                //        values.add(new CrossYearDateCompensation(startYear + i));
                //    }
                //}
            }
        } else if (atomicIncreateType.isNoloopTime()) {
            /**
             * NoloopTime
             * 枚举出的表名即为日期本身,如按年月拆分,表名后缀即为table_201704,按年日拆分表名后缀为table_20170401
             */
            for (Range ro : rangeArray) {
                int interval = 0;
                Date start = DateUtils.str_to_time(String.valueOf(ro.start));
                Date end = DateUtils.str_to_time(String.valueOf(ro.end));
                Calendar calStart = new GregorianCalendar();
                Calendar calEnd = new GregorianCalendar();
                calStart.setTime(start);
                calEnd.setTime(end);
                if (start.after(end)) {
                    throw new IllegalArgumentException("start time must less than end time");
                }
                switch (atomicIncreateType) {
                case MONTH_ABS:
                    interval = (calEnd.get(Calendar.YEAR) - calStart.get(Calendar.YEAR)) * 12
                        + calEnd.get(Calendar.MONTH) - calStart.get(Calendar.MONTH) + 1;
                    break;
                case DATE_ABS:
                    do {
                        ++interval;
                        calStart.add(Calendar.DATE, 1);
                    } while (!calStart.after(calEnd));
                    calStart.setTime(start); // calculate dates, reset the
                    // start calendar
                    break;
                case WEEK_ABS:
                    // 计算起至时间的相隔周数,严格计算每个日期所在的物理周
                    int sunday = 0;
                    int w = 0;
                    while (!calStart.after(calEnd)) {
                        w = calStart.get(Calendar.DAY_OF_WEEK);
                        if (w == Calendar.SUNDAY) {
                            ++sunday;
                        }
                        calStart.set(Calendar.DATE, calStart.get(Calendar.DATE) + 1);
                    }
                    interval = w != Calendar.SUNDAY ? ++sunday : sunday;
                    calStart.setTime(start); // calculate dates, reset the
                    // start calendar
                    break;
                }
                if (tableRule != null) {
                    tableRule.setEnd(ro.end);
                    tableRule.setStart(ro.start);
                    tableRule.setPartitionType(atomicIncreateType);
                    tableRule.setCoverRuleProcessor(new NoloopCoverRuleProcessor());
                }
                for (int i = 0; i < interval; i++) {
                    values.add(evalTime(calStart, i));
                }
            }
        } else if (AtomIncreaseType.STRING.equals(atomicIncreateType)) {
            // 以前string类型，常见的写法会是 #name,1,1024#.toString().hashcode() % 1024
            // 问题就处在toString()方法，如果是只添加Integer类型的数字，toString之后的hashcode就不是联系了.
            // 要么去掉.toString(),要么就是改为1_string类型
            for (Range ro : rangeArray) {
                for (int i = ro.start; i <= ro.end; i++) {
                    // Character.MAX_VALUE , 16位,最大值为65536，一般很少有这么多分表数
                    // 如果真出现了>65536，那范围枚举的值可能就不准了
                    values.add(String.valueOf((char) i));
                }
            }
        } else if (AtomIncreaseType.STRNUM.equals(atomicIncreateType)) {
            // 以前string类型，常见的写法会是 #name,1,1024#.toString().hashcode() % 1024
            // 问题就处在toString()方法，如果是只添加Integer类型的数字，toString之后的hashcode就不是联系了.
            // 要么去掉.toString(),要么就是改为1_string类型
            for (Range ro : rangeArray) {
                for (int i = ro.start; i <= ro.end; i++) {
                    // Character.MAX_VALUE , 16位,最大值为65536，一般很少有这么多分表数
                    // 如果真出现了>65536，那范围枚举的值可能就不准了
                    values.add(new StringNum(Integer.valueOf((char) i)));
                }
            }
        } else {
            for (Range ro : rangeArray) {
                for (int i = ro.start; i <= ro.end; i++) {
                    values.add(i);
                }

                // 特别处理下，如果补偿不为1的情况
                if (atomicIncreateValue instanceof Number && ((Number) atomicIncreateValue).longValue() > 1) {
                    long incV = ((Number) atomicIncreateValue).longValue();
                    for (int i = ro.start; i <= ro.end; i++) {
                        values.add(i * incV);
                    }
                }
            }
        }
        return values;
    }

    /**
     * 枚举所有值
     */
    public Set<Object> enumerateRange(Object basepoint) {
        if (basepoint instanceof Number) {
            return enumerateRange(((Number) basepoint).intValue());
        } else if (basepoint instanceof String) {
            /**
             * 目前该方法只是在初始化拓扑的时候会用到，如果出现string，
             * 一定是配置了1_string，就会拿string值的第一个char进行处理
             */
            return enumerateRange(((String) basepoint).charAt(0));
        } else if (basepoint instanceof StringNum) {
            // 目前该方法只是在初始化拓扑的时候会用到，如果出现string，一定是配置了1_string，拿第一个char进行处理
            return enumerateRange(((StringNum) basepoint).number);
        } else if (basepoint instanceof Calendar) {
            return enumerateRange((Calendar) basepoint);
        } else if (basepoint instanceof Date) {
            // add by junyu,因为后面evalTime的时候把结果返回了Date类型，所以这边也要增加这个逻辑
            Calendar cal = Calendar.getInstance();
            cal.setTime((Date) basepoint);
            return enumerateRange(cal);
        } else {
            throw new IllegalArgumentException(basepoint + " applies on atomicIncreateType: " + atomicIncreateType);
        }
    }

    /**
     * 枚举所有值
     */
    public Set<Object> enumerateRange(int basepoint) {
        Set<Object> values = new HashSet<Object>();
        if (AtomIncreaseType.NUMBER.equals(atomicIncreateType)) {
            // 特别处理下，如果补偿不为1的情况
            int start = basepoint;

            long incV = 1;
            if (atomicIncreateValue instanceof Number && ((Number) atomicIncreateValue).longValue() > 1) {
                incV = ((Number) atomicIncreateValue).longValue();
            } else if (atomicIncreateValue instanceof Integer) {
                incV = (Integer) atomicIncreateValue;
            } else if (atomicIncreateValue instanceof Long) {
                incV = (Long) atomicIncreateValue;
            }

            for (int i = 0; i <= this.cumulativeTimes; i++) {
                values.add(start);
                start += incV;
            }
        } else if (AtomIncreaseType.STRING.equals(atomicIncreateType)) {
            int start = basepoint;
            int end = start + this.cumulativeTimes;
            for (int i = start; i <= end; i++) {
                values.add(String.valueOf((char) i));
            }
        } else if (AtomIncreaseType.STRNUM.equals(atomicIncreateType)) {
            int start = basepoint;
            int end = start + this.cumulativeTimes;
            for (int i = start; i <= end; i++) {
                values.add(new StringNum(Integer.valueOf((char) i)));
            }
        } else {
            throw new IllegalArgumentException("Number applies on atomicIncreateType: " + atomicIncreateType);
        }
        return values;
    }

    /**
     * 枚举所有值
     */
    public Set<Object> enumerateRange(Calendar basepoint) {
        Set<Object> values = new HashSet<Object>();
        if (atomicIncreateType.isTime()) {
            for (int i = 0; i < this.cumulativeTimes; i++) {

                Object enumPoint = evalTime(basepoint, i);
                values.add(enumPoint);
                if (enumPoint instanceof Date) {
                    processSpecialTimeEnumPoint(this, (Date) enumPoint, values);
                }
            }
        } else if (atomicIncreateType.isNoloopTime()) {
            return values;
        } else {
            throw new IllegalArgumentException("Calendar applies on atomicIncreateType: " + atomicIncreateType);
        }
        return values;
    }

    private Object evalTime(Calendar base, int i) {
        Calendar c = (Calendar) base.clone();
        if (AtomIncreaseType.YEAR.equals(atomicIncreateType)) {
            c.add(Calendar.YEAR, i);
        } else if (AtomIncreaseType.MONTH.equals(atomicIncreateType)
            || AtomIncreaseType.MONTH_ABS.equals(atomicIncreateType)) {
            c.add(Calendar.MONTH, i);
        } else if (AtomIncreaseType.WEEK.equals(atomicIncreateType)
            || AtomIncreaseType.WEEK_ABS.equals(atomicIncreateType)) {
            c.add(Calendar.WEEK_OF_YEAR, i);
        } else if (AtomIncreaseType.DATE.equals(atomicIncreateType)
            || AtomIncreaseType.DATE_ABS.equals(atomicIncreateType)) {
            c.add(Calendar.DATE, i);
        } else if (AtomIncreaseType.HOUR.equals(atomicIncreateType)) {
            c.add(Calendar.HOUR_OF_DAY, i);
        } else {
            throw new IllegalArgumentException("atomicIncreateType:" + atomicIncreateType);
        }
        // return c;
        // modify by junyu,与sql参数保持一致类型
        return c.getTime();
    }

    public boolean isSpecialDateMethod() {
        return isSpecialDateMethod;
    }

    public void setIsSpecialDateMethod(boolean isSpecialDateMethod) {
        this.isSpecialDateMethod = isSpecialDateMethod;
    }

    public String toString() {
        return ToStringBuilder.reflectionToString(this, TddlToStringStyle.DEFAULT_STYLE);
    }

    public Integer getCumulativeTimesInRule() {
        return cumulativeTimesInRule;
    }

    public void setCumulativeTimesInRule(Integer cumulativeTimesInRule) {
        this.cumulativeTimesInRule = cumulativeTimesInRule;
    }

    /**
     * 参数自增类型，现在支持4种(#2011-12-5,modify by junyu,add HOUR type)
     */
    public static enum AtomIncreaseType {
        HOUR, DATE, WEEK, MONTH, YEAR, NUMBER, STRING, STRNUM, NUMBER_ABS, DATE_ABS,
        WEEK_ABS, MONTH_ABS;

        public static Set<AtomIncreaseType> ATOM_INCR_TYPE_FOR_TIME = new HashSet<AtomIncreaseType>();

        static {
            ATOM_INCR_TYPE_FOR_TIME.add(HOUR);
            ATOM_INCR_TYPE_FOR_TIME.add(DATE);
            ATOM_INCR_TYPE_FOR_TIME.add(WEEK);
            ATOM_INCR_TYPE_FOR_TIME.add(MONTH);
            ATOM_INCR_TYPE_FOR_TIME.add(YEAR);
            ATOM_INCR_TYPE_FOR_TIME.add(DATE_ABS);
            ATOM_INCR_TYPE_FOR_TIME.add(WEEK_ABS);
            ATOM_INCR_TYPE_FOR_TIME.add(MONTH_ABS);
        }

        public static Set<String> ATOM_ENUM_TYPE_STR_NAME_FOR_TIME = new HashSet<>();

        static {
            ATOM_ENUM_TYPE_STR_NAME_FOR_TIME.add(HOUR.toString().toUpperCase());
            ATOM_ENUM_TYPE_STR_NAME_FOR_TIME.add(DATE.toString().toUpperCase());
            ATOM_ENUM_TYPE_STR_NAME_FOR_TIME.add(WEEK.toString().toUpperCase());
            ATOM_ENUM_TYPE_STR_NAME_FOR_TIME.add(MONTH.toString().toUpperCase());
            ATOM_ENUM_TYPE_STR_NAME_FOR_TIME.add(YEAR.toString().toUpperCase());
            ATOM_ENUM_TYPE_STR_NAME_FOR_TIME.add(DATE_ABS.toString().toUpperCase());
            ATOM_ENUM_TYPE_STR_NAME_FOR_TIME.add(WEEK_ABS.toString().toUpperCase());
            ATOM_ENUM_TYPE_STR_NAME_FOR_TIME.add(MONTH_ABS.toString().toUpperCase());
        }

        public static Set<AtomIncreaseType> ATOM_INCR_TYPE_FOR_NOLOOP_TIME = new HashSet<AtomIncreaseType>();

        static {
            ATOM_INCR_TYPE_FOR_NOLOOP_TIME.add(DATE_ABS);
            ATOM_INCR_TYPE_FOR_NOLOOP_TIME.add(WEEK_ABS);
            ATOM_INCR_TYPE_FOR_NOLOOP_TIME.add(MONTH_ABS);
        }

        /**
         * 这个方法有点坑, 如果时间枚举类型放在NUMBER之后，就错了
         */
        public boolean isTime() {
            //return this.ordinal() < NUMBER_ABS.ordinal();
            return ATOM_INCR_TYPE_FOR_TIME.contains(this);
        }

        public boolean isNoloopTime() {
            //return this.ordinal() > NUMBER_ABS.ordinal();
            return ATOM_INCR_TYPE_FOR_NOLOOP_TIME.contains(this);
        }
    }

    public static class Range {

        public final Integer start; // 起始值
        public final Integer end;   // 结束值

        public Range(Integer start, Integer end) {
            this.start = start;
            this.end = end;
        }
    }

    public String getHashFuncType() {
        return hashFuncType;
    }

    /**
     * 为处理一年中最后一天或一年中最后一周或一个月中最后一天的特殊时间点，补充遗漏的枚举值，以补充缺失分片
     *
     * @param enumDirection enumDirection: 0--时间从小到大；1--时间从大到小
     */
    public static void processSpecialTimeEnumPoint(AdvancedParameter ad, Date enumPoint, Set<Object> allEnumResult) {
        AdvancedParameterSpecialTimeEnumUtils.processSpecialTimeEnumPoint(ad, enumPoint, allEnumResult, 0);
        return;
    }

    /**
     * 为处理一年中最后一天或一年中最后一周或一个月中最后一天的特殊时间点，补充遗漏的枚举值，以补充缺失分片，
     * 指定枚举方向
     *
     * @param enumDirection enumDirection: 0--时间从小到大；1--时间从大到小
     */
    public static void processSpecialTimeEnumPoint(AdvancedParameter ad, Date enumPoint, Set<Object> allEnumResult,
                                                   int enumDirection) {
        AdvancedParameterSpecialTimeEnumUtils.processSpecialTimeEnumPoint(ad, enumPoint, allEnumResult, enumDirection);
    }
}
