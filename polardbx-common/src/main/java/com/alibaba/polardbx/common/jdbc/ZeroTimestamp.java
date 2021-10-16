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

package com.alibaba.polardbx.common.jdbc;

import java.sql.Timestamp;
import java.util.Date;

public class ZeroTimestamp extends Timestamp {
    public static ZeroTimestamp instance = new ZeroTimestamp();
    private static final long serialVersionUID = 1L;

    private ZeroTimestamp() {
        super(0, 0, 0, 0, 0, 0, 0);
    }

    @Override
    public void setTime(long time) {
        throw new RuntimeException("Value '0000-00-00 00:00:00' can not be represented as java.sql.Timestamp");
    }

    @Override
    public long getTime() {
        return 0L;
    }

    @Override
    public String toString() {
        return "0000-00-00 00:00:00";
    }

    @Override
    public int getNanos() {
        return 0;
    }

    @Override
    public void setNanos(int n) {
        throw new RuntimeException("Value '0000-00-00 00:00:00' can not be represented as java.sql.Timestamp");
    }

    @Override
    public boolean equals(Timestamp ts) {
        return this == ts;
    }

    @Override
    public boolean equals(Object ts) {
        return this == ts;
    }

    @Override
    public boolean before(Timestamp ts) {
        throw new RuntimeException("Value '0000-00-00 00:00:00' can not be represented as java.sql.Timestamp");
    }

    @Override
    public boolean after(Timestamp ts) {
        throw new RuntimeException("Value '0000-00-00 00:00:00' can not be represented as java.sql.Timestamp");
    }

    @Override
    public int compareTo(Timestamp ts) {
        return this == ts ? 0 : -1;
    }

    @Override
    public int compareTo(Date o) {
        return this == o ? 0 : -1;
    }
}

