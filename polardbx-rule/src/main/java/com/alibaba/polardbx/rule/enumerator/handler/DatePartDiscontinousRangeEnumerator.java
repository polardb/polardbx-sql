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

package com.alibaba.polardbx.rule.enumerator.handler;

import java.util.Calendar;
import java.util.Date;
import java.util.Set;

import com.alibaba.polardbx.common.model.sqljep.Comparative;
import com.alibaba.polardbx.rule.model.AdvancedParameter;
import com.alibaba.polardbx.rule.model.DateEnumerationParameter;

/**
 * 基于时间类型的枚举器
 *
 * @author jianghang 2013-10-29 下午5:23:01
 * @since 5.0.0
 */
public class DatePartDiscontinousRangeEnumerator extends PartDiscontinousRangeEnumerator {

    private static final long LIMIT_UNIT_OF_DATE = 1L;
    private static final int DEFAULT_DATE_ATOMIC_VALUE = 1;
    private final static ThreadLocal threaLocal = new ThreadLocal();

    @Override
    protected Comparative changeGreater2GreaterOrEq(Comparative from) {
        if (from.getComparison() == Comparative.GreaterThan) {
            Date gtDate = (Date) from.getValue();
            long gtOrEqDate = gtDate.getTime() + LIMIT_UNIT_OF_DATE;
            return new Comparative(Comparative.GreaterThanOrEqual, new Date(gtOrEqDate));

        }
        return from;
    }

    @Override
    protected Comparative changeLess2LessOrEq(Comparative to) {
        if (to.getComparison() == Comparative.LessThan) {
            Date less = (Date) to.getValue();
            long lessOrEqDate = less.getTime() - LIMIT_UNIT_OF_DATE;
            return new Comparative(Comparative.LessThanOrEqual, new Date(lessOrEqDate));
        }
        return to;
    }

    @SuppressWarnings("rawtypes")
    @Override
    protected Comparable getOneStep(Comparable source, Comparable atomIncVal) {
        DateEnumerationParameter dateEnumerationParameter = getDateEnumerationParameter(atomIncVal);
        Calendar cal = getCalendar((Date) source);
        cal.add(dateEnumerationParameter.calendarFieldType, dateEnumerationParameter.atomicIncreatementNumber);
        return cal.getTime();
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Override
    protected boolean inputCloseRangeGreaterThanMaxFieldOfDifination(Comparable from, Comparable to,
                                                                     Integer cumulativeTimes,
                                                                     Comparable<?> atomIncrValue) {
        if (cumulativeTimes == null) {
            return false;
        }
        Calendar cal = getCalendar((Date) from);
        int rangeSet = cumulativeTimes;
        if (atomIncrValue instanceof Integer) {
            // 兼容老实现，对应#gmt,1,7#这种
            cal.add(Calendar.DATE, rangeSet * (Integer) atomIncrValue);
        } else if (atomIncrValue instanceof DateEnumerationParameter) {
            // 对应#gmt,1_month,12这样的情况
            DateEnumerationParameter dep = (DateEnumerationParameter) atomIncrValue;
            cal.add(dep.calendarFieldType, rangeSet * dep.atomicIncreatementNumber);
        } else if (atomIncrValue == null) {
            // 兼容老实现，对应没有#gmt#.这种普通的情况，
            cal.add(Calendar.DATE, rangeSet);
        } else {
            throwNotSupportIllegalArgumentException(atomIncrValue);
        }

        if (to.compareTo(cal.getTime()) >= 0) {
            return true;
        }
        return false;
    }

    private void throwNotSupportIllegalArgumentException(Object arg) {
        throw new IllegalArgumentException("不能识别的类型:" + arg + " .type is" + arg.getClass());
    }

    public void processAllPassableFields(Comparative begin, Set<Object> retValue, Integer cumulativeTimes,
                                         Comparable<?> atomicIncreationValue) {
        DateEnumerationParameter dateEnumerationParameter = getDateEnumerationParameter(atomicIncreationValue);
        // 把> < 替换为>= <=
        begin = changeGreater2GreaterOrEq(begin);
        begin = changeLess2LessOrEq(begin);

        Calendar cal = getCalendar((Date) begin.getValue());
        int comparasion = begin.getComparison();
        AdvancedParameter ap = (AdvancedParameter) this.ruleColumn;
        if (comparasion == Comparative.GreaterThanOrEqual) {
            // 添加当前时间，因为当前时间内已经是大于等于了。

            Date beginTime = cal.getTime();
            retValue.add(beginTime);
            AdvancedParameter.processSpecialTimeEnumPoint(ap, beginTime, retValue, 0);

            // 减少一次迭代，因为已经加了当前时间
            for (int i = 0; i < cumulativeTimes - 1; i++) {
                cal.add(dateEnumerationParameter.calendarFieldType, dateEnumerationParameter.atomicIncreatementNumber);
                Date newTimePoint = cal.getTime();
                retValue.add(newTimePoint);
                AdvancedParameter.processSpecialTimeEnumPoint(ap, newTimePoint, retValue, 0);

            }
        } else if (comparasion == Comparative.LessThanOrEqual) {

            // 添加当前时间，因为当前时间内已经是小于等于了。

            Date beginTime = cal.getTime();
            retValue.add(beginTime);
            AdvancedParameter.processSpecialTimeEnumPoint(ap, beginTime, retValue, 1);

            for (int i = 0; i < cumulativeTimes - 1; i++) {

                cal.add(dateEnumerationParameter.calendarFieldType, -1
                    * dateEnumerationParameter.atomicIncreatementNumber);

                Date newTimePoint = cal.getTime();
                retValue.add(newTimePoint);
                AdvancedParameter.processSpecialTimeEnumPoint(ap, newTimePoint, retValue, 1);
            }
        }
    }

    /**
     * 识别data自增参数
     */
    protected DateEnumerationParameter getDateEnumerationParameter(Comparable<?> comparable) {
        DateEnumerationParameter dateEnumerationParameter = null;
        if (comparable == null) {
            dateEnumerationParameter = new DateEnumerationParameter(DEFAULT_DATE_ATOMIC_VALUE);
        } else if (comparable instanceof Integer) {
            dateEnumerationParameter = new DateEnumerationParameter((Integer) comparable);
        } else if (comparable instanceof DateEnumerationParameter) {
            dateEnumerationParameter = (DateEnumerationParameter) comparable;
        } else {
            throwNotSupportIllegalArgumentException(comparable);
        }
        return dateEnumerationParameter;
    }

    private Calendar getCalendar(Date date2BeSet) {
        // 优化重用一下Calendar
        Calendar cal = (Calendar) threaLocal.get();
        if (cal == null) {
            cal = Calendar.getInstance();
            threaLocal.set(cal);
        }
        cal.setTime(date2BeSet);
        return cal;
    }
}
