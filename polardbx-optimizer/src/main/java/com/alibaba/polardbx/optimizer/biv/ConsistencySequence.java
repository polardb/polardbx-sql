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

import com.google.common.collect.Lists;

import java.util.List;

public class ConsistencySequence {
    private List<String> sqlSeq = Lists.newArrayList();
    public static final ConsistencySequence EMPTY = new ConsistencySequence();

    public ConsistencySequence() {
    }

    public ConsistencySequence(List<String> sqlSeq) {
        this.sqlSeq.addAll(sqlSeq);
    }

    public List<String> getSqlSeq() {
        return sqlSeq;
    }

    public void setSqlSeq(List<String> sqlSeq) {
        this.sqlSeq = sqlSeq;
    }

    public void addUp(ConsistencySequence consistencySequence) {
        if (consistencySequence == null) {
            return;
        }
        sqlSeq.addAll(consistencySequence.getSqlSeq());
    }

    public void addSql(String sql) {
        sqlSeq.add(sql);
    }

    public ConsistencySequence merge(ConsistencySequence consistencySequence) {
        if (consistencySequence == null) {
            return this;
        }
        List<String> newSqlSeq = Lists.newArrayList();
        newSqlSeq.addAll(sqlSeq);
        newSqlSeq.addAll(consistencySequence.getSqlSeq());
        return new ConsistencySequence(newSqlSeq);
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof ConsistencySequence)) {
            return false;
        }
        if (sqlSeq.size() != ((ConsistencySequence) obj).sqlSeq.size()) {
            return false;
        }
        int num = sqlSeq.size();
        for (int i = 0; i < num; i++) {
            if (!sqlSeq.get(i).equals(((ConsistencySequence) obj).getSqlSeq().get(i))) {
                return false;
            }
        }
        return true;
    }
}
