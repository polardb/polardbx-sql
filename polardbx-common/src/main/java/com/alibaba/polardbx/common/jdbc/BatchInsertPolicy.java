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

public enum BatchInsertPolicy {
    NONE {
        @Override
        public String getName() {
            return "NONE";
        }
    },

    SPLIT {
        @Override
        public String getName() {
            return "SPLIT";
        }
    };

    public String getName() {
        return "";
    }

    public static BatchInsertPolicy getPolicyByName(String name) {
        for (BatchInsertPolicy policy : BatchInsertPolicy.values()) {
            if (policy.getName().equalsIgnoreCase(name)) {
                return policy;
            }
        }
        return null;
    }

    public static String getVariableName() {
        return "BATCH_INSERT_POLICY";
    }
}
