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

package com.alibaba.polardbx.optimizer.biv;

public class MockDataKey {
    private ConsistencySequence consistencySequence;
    private String sql;

    public MockDataKey(ConsistencySequence consistencySequence, String sql) {
        this.consistencySequence = consistencySequence;
        this.sql = sql;
    }

    public ConsistencySequence getConsistencySequence() {
        return consistencySequence;
    }

    public String getSql() {
        return sql;
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof MockDataKey)) {
            return false;
        }
        return consistencySequence.equals(((MockDataKey) obj).getConsistencySequence()) && sql
            .equals(((MockDataKey) obj).getSql());
    }
}
