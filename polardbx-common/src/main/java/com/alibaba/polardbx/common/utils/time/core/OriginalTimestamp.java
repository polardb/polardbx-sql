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

package com.alibaba.polardbx.common.utils.time.core;

import java.io.Serializable;
import java.sql.Timestamp;
import java.util.Date;

public class OriginalTimestamp extends Timestamp implements OriginalTemporalValue, Serializable {
    private final MysqlDateTime mysqlDateTime;

    public OriginalTimestamp(MysqlDateTime t) {
        super(UNSET_VALUE);

        this.mysqlDateTime = t.clone();
    }

    @Override
    public MysqlDateTime getMysqlDateTime() {
        return mysqlDateTime;
    }

    @Override
    public void initTime() {
        if (super.getTime() == UNSET_VALUE) {

            long tsAsMillis = mysqlDateTime.toEpochMillsForDatetime();
            super.setTime(tsAsMillis);
            super.setNanos((int) mysqlDateTime.getSecondPart());
        }
    }

    @Override
    public int getNanos() {
        return (int) mysqlDateTime.getSecondPart();
    }

    @Override
    public String toString() {
        return mysqlDateTime.toDatetimeString(-1);
    }

    @Override
    public long getTime() {
        initTime();
        return super.getTime();
    }

    @Override
    public Object clone() {
        initTime();
        return super.clone();
    }

    @Override
    public boolean before(Date when) {
        initTime();
        return super.before(when);
    }

    @Override
    public boolean after(Date when) {
        initTime();
        return super.after(when);
    }

    @Override
    public boolean equals(Timestamp ts) {
        initTime();
        return super.equals(ts);
    }

    @Override
    public boolean equals(Object ts) {
        initTime();
        return super.equals(ts);
    }

    @Override
    public int compareTo(Timestamp ts) {
        initTime();
        return super.compareTo(ts);
    }

    @Override
    public int compareTo(Date o) {
        initTime();
        return super.compareTo(o);
    }

    @Override
    public int getYear() {
        initTime();
        return super.getYear();
    }

    @Override
    public void setYear(int year) {
        initTime();
        super.setYear(year);
    }

    @Override
    public int getMonth() {
        initTime();
        return super.getMonth();
    }

    @Override
    public void setMonth(int month) {
        initTime();
        super.setMonth(month);
    }

    @Override
    public int getDate() {
        initTime();
        return super.getDate();
    }

    @Override
    public void setDate(int date) {
        initTime();
        super.setDate(date);
    }

    @Override
    public int getDay() {
        initTime();
        return super.getDay();
    }

    @Override
    public int getHours() {
        initTime();
        return super.getHours();
    }

    @Override
    public void setHours(int hours) {
        initTime();
        super.setHours(hours);
    }

    @Override
    public int getMinutes() {
        initTime();
        return super.getMinutes();
    }

    @Override
    public void setMinutes(int minutes) {
        initTime();
        super.setMinutes(minutes);
    }

    @Override
    public int getSeconds() {
        initTime();
        return super.getSeconds();
    }

    @Override
    public void setSeconds(int seconds) {
        initTime();
        super.setSeconds(seconds);
    }

    @Override
    public String toLocaleString() {
        initTime();
        return super.toLocaleString();
    }

    @Override
    public String toGMTString() {
        initTime();
        return super.toGMTString();
    }

    @Override
    public int getTimezoneOffset() {
        initTime();
        return super.getTimezoneOffset();
    }
}
