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

package com.alibaba.polardbx.common.utils.time.calculator;

public class MySQLInterval {
    private long year;
    private long month;
    private long day;
    private long hour;
    private long minute;
    private long second;
    private long secondPart;
    private boolean neg;
    private boolean isZero;

    public MySQLInterval(long year, long month, long day, long hour, long minute, long second, long secondPart,
                         boolean neg) {
        this.year = year;
        this.month = month;
        this.day = day;
        this.hour = hour;
        this.minute = minute;
        this.second = second;
        this.secondPart = secondPart;
        this.neg = neg;
    }

    public MySQLInterval() {
        this(0, 0, 0, 0, 0, 0, 0, false);
    }

    public long getYear() {
        return year;
    }

    public void setYear(long year) {
        this.year = year;
    }

    public long getMonth() {
        return month;
    }

    public void setMonth(long month) {
        this.month = month;
    }

    public long getDay() {
        return day;
    }

    public void setDay(long day) {
        this.day = day;
    }

    public long getHour() {
        return hour;
    }

    public void setHour(long hour) {
        this.hour = hour;
    }

    public long getMinute() {
        return minute;
    }

    public void setMinute(long minute) {
        this.minute = minute;
    }

    public long getSecond() {
        return second;
    }

    public void setSecond(long second) {
        this.second = second;
    }

    public long getSecondPart() {
        return secondPart;
    }

    public void setSecondPart(long secondPart) {
        this.secondPart = secondPart;
    }

    public boolean isNeg() {
        return neg;
    }

    public void setNeg(boolean neg) {
        this.neg = neg;
    }

    public boolean isZero() {
        return isZero;
    }

    public void setZero(boolean zero) {
        isZero = zero;
    }
}
