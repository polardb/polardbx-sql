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

package org.apache.calcite.adapter.java;

/**
 * Created by chuanqin on 18/1/18.
 */
public class Unsigned extends Number {
    long value;

    public Unsigned(long value) {
        this.value = value;
    }

    @Override public int intValue() {
        return (int) value;
    }

    @Override public long longValue() {
        return value;
    }

    @Override public float floatValue() {
        return value;
    }

    @Override public double doubleValue() {
        return value;
    }
}
