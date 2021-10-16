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

package com.alibaba.polardbx.optimizer.core.expression.bean;

public class NullValue implements Comparable {

    private static NullValue instance = new NullValue();
    private String str = "NULL";

    public static NullValue getNullValue() {
        return instance;
    }

    private NullValue() {
    }

    public int compareTo(Object o) {
        if (o == this) {
            return 0;
        }
        if (o instanceof NullValue) {
            return 0;
        }
        return -1;

    }

    public String toString() {
        return str;
    }

}
