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

import java.util.Date;

public class InvalidDate extends ZeroDate {
    public static InvalidDate instance = new InvalidDate();
    private static final long serialVersionUID = 1L;

    private InvalidDate() {
    }

    @Override
    public void setTime(long time) {
        throw new RuntimeException("Value 'InvalidDate' can not be represented as java.sql.Date");
    }

    @Override
    public String toString() {
        return "InvalidDate";
    }

    @Override
    public boolean before(Date ts) {
        throw new RuntimeException("Value 'InvalidDate' can not be represented as java.sql.Date");
    }

    @Override
    public boolean after(Date ts) {
        throw new RuntimeException("Value 'InvalidDate' can not be represented as java.sql.Date");
    }
}
