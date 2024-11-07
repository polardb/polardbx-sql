/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the License);
 * you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.polardbx.gms.metadb.table;

public enum ColumnarPartitionStatus {
    ABSENT(0),
    PUBLIC(1);

    private final int value;

    ColumnarPartitionStatus(int value) {
        this.value = value;
    }

    public int getValue() {
        return this.value;
    }

    public static ColumnarPartitionStatus convert(int value) {
        switch (value) {
        case 0:
            return ABSENT;
        case 1:
            return PUBLIC;
        default:
            return null;
        }
    }
}
