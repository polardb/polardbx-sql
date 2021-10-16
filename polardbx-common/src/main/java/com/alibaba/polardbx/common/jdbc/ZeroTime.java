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

import java.sql.Time;
import java.util.Date;

public class ZeroTime extends Time {
    public static ZeroTime instance = new ZeroTime();
    private static final long serialVersionUID = 1L;

    private ZeroTime() {
        super(0L);
    }

    @Override
    public void setTime(long time) {
        throw new RuntimeException("Value '00:00:00' can not be represented as java.sql.Time");
    }

    @Override
    public long getTime() {
        return 0L;
    }

    @Override
    public String toString() {
        return "00:00:00";
    }

    @Override
    public boolean equals(Object ts) {
        return this == ts;
    }

    @Override
    public boolean before(Date ts) {
        throw new RuntimeException("Value '00:00:00' can not be represented as java.sql.Time");
    }

    @Override
    public boolean after(Date ts) {
        throw new RuntimeException("Value '00:00:00' can not be represented as java.sql.Time");
    }

    @Override
    public int compareTo(Date ts) {
        return this == ts ? 0 : -1;
    }
}

