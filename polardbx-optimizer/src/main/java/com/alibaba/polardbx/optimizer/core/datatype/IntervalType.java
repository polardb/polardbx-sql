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

package com.alibaba.polardbx.optimizer.core.datatype;

import com.alibaba.polardbx.common.utils.time.old.DateUtils;
import com.taobao.tddl.common.utils.TddlToStringStyle;
import org.apache.commons.lang.builder.ToStringBuilder;

import java.time.LocalDateTime;
import java.util.Calendar;

/**
 * mysql interval时间类型
 *
 * @author jianghang 2014-4-16 下午1:34:00
 * @since 5.0.7
 */
public class IntervalType extends TimestampType {

    private int year = 0;
    private boolean isYearSet = false;
    private int month = 0;
    private boolean isMonthSet = false;
    private int day = 0;
    private boolean isDaySet = false;
    private int hour = 0;
    private boolean isHourSet = false;
    private int minute = 0;
    private boolean isMinuteSet = false;
    private int second = 0;
    private boolean isSecondSet = false;
    private int microsecond = 0;
    private boolean isMicrosecondSet = false;
    private int milliSeconds = 0;
    private boolean isMilliSecondSet = false;
    private int factor = 1;

    public void process(Calendar cal, int factor) {
        factor = this.factor * factor;
        if (isYearSet) {
            cal.add(Calendar.YEAR, year * factor);
        }
        if (isMonthSet) {
            cal.add(Calendar.MONTH, month * factor);
        }
        if (isDaySet) {
            cal.add(Calendar.DAY_OF_YEAR, day * factor);
        }
        if (isHourSet) {
            cal.add(Calendar.HOUR_OF_DAY, hour * factor);
        }
        if (isMinuteSet) {
            cal.add(Calendar.MINUTE, minute * factor);
        }
        if (isSecondSet) {
            cal.add(Calendar.SECOND, second * factor);
        }
        if (isMilliSecondSet) {
            cal.add(Calendar.MILLISECOND, milliSeconds * factor);
        }
        // 微秒的支持会有问题，目前只能到毫秒单位
        if (isMicrosecondSet) {
            cal.add(Calendar.MILLISECOND, (microsecond / 1000) * factor);
        }
    }

    public LocalDateTime process(LocalDateTime time, int factor) {
        factor = this.factor * factor;
        if (isYearSet) {
            time = time.plusYears(year * factor);
        }
        if (isMonthSet) {
            time = time.plusMonths(month * factor);
        }
        if (isDaySet) {
            time = time.plusDays(day * factor);
        }
        if (isHourSet) {
            time = time.plusHours(hour * factor);
        }
        if (isMinuteSet) {
            time = time.plusMinutes(minute * factor);
        }
        if (isSecondSet) {
            time = time.plusSeconds(second * factor);
        }
        if (isMilliSecondSet) {
            time = time.plusNanos(milliSeconds * factor * 1000000);
        }
        if (isMicrosecondSet) {
            time = time.plusNanos(microsecond * factor * 1000);
        }
        return time;
    }

    public java.sql.Time process(java.sql.Time time, int factor) {
        factor = this.factor * factor;
        long ms = microsecond;
        while (ms > 999999) {
            ms = ms / 10L;
        }
        int maxHour = (day * 24 + hour);
        boolean outRange = maxHour > DateUtils.MAX_HOUR;
        if (outRange) {
            maxHour = DateUtils.MAX_HOUR;
            second = 59;
            minute = 59;
            microsecond = 0;
            ms = 0;
        }
        long diff = factor * (maxHour * 60 * 60 * 1000L + minute * 60 * 1000L + second * 1000L + ms / 1000L);

        time.setTime(time.getTime() + diff);
        return time;
    }

    public int getYear() {
        return year;
    }

    public void setYear(int year) {
        this.year = year;
        this.isYearSet = true;
    }

    public int getMonth() {
        return month;
    }

    public void setMonth(int month) {
        this.month = month;
        this.isMonthSet = true;
    }

    public int getDay() {
        return day;
    }

    public void setDay(int day) {
        this.day = day;
        this.isDaySet = true;
    }

    public int getHour() {
        return hour;
    }

    public void setHour(int hour) {
        this.hour = hour;
        this.isHourSet = true;
    }

    public int getMinute() {
        return minute;
    }

    public void setMinute(int minute) {
        this.minute = minute;
        this.isMinuteSet = true;
    }

    public int getSecond() {
        return second;
    }

    public void setSecond(int second) {
        this.second = second;
        this.isSecondSet = true;
    }

    public int getMicrosecond() {
        return microsecond;
    }

    public void setMicrosecond(int microsecond) {
        this.microsecond = microsecond;
        this.isMicrosecondSet = true;
    }

    public void setMilliSecond(int milliSeconds) {
        this.milliSeconds = milliSeconds;
        this.isMilliSecondSet = true;
    }

    public void setFactor(int factor) {
        this.factor = factor;
    }

    @Override
    public String toString() {
        return ToStringBuilder.reflectionToString(this, TddlToStringStyle.DEFAULT_STYLE);
    }

    public boolean isFineGrainedTimeUintSet() {
        return isMilliSecondSet || isHourSet || isMinuteSet || isSecondSet || isMicrosecondSet;
    }

}
