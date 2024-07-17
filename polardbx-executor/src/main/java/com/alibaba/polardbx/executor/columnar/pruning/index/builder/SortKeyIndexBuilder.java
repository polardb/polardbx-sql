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

package com.alibaba.polardbx.executor.columnar.pruning.index.builder;

import com.alibaba.polardbx.executor.columnar.pruning.index.SortKeyIndex;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.google.common.collect.Lists;
import org.apache.orc.OrcProto;

import java.util.Iterator;
import java.util.List;

/**
 * @author fangwu
 */
public class SortKeyIndexBuilder {
    private int colId;
    private DataType dt;
    private List<Long> dataEntry = Lists.newArrayList();

    public void appendDataEntry(OrcProto.IntegerStatistics integerStatistics) {
        dataEntry.add(integerStatistics.getMinimum());
        dataEntry.add(integerStatistics.getMaximum());
    }

    public void appendDataEntry(long min, long max) {
        dataEntry.add(min);
        dataEntry.add(max);
    }

    public SortKeyIndex build() {
        long[] data = new long[dataEntry.size()];
        Iterator<Long> it = dataEntry.iterator();
        int cur = 0;
        while (it.hasNext()) {
            Long l = it.next();
            data[cur++] = l;
        }
        return SortKeyIndex.build(colId, data, dt);
    }

    public void setDt(DataType dt) {
        this.dt = dt;
    }

    public void setColId(int colId) {
        this.colId = colId;
    }
}
