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

package com.alibaba.polardbx.executor.operator.util;

import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.executor.utils.ExecUtils;
import com.alibaba.polardbx.executor.utils.OrderByOption;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.row.Row;

import java.util.Comparator;
import java.util.List;

public class ChunkWithPositionComparator {

    private final Comparator<Row> rowComparator;

    public ChunkWithPositionComparator(List<OrderByOption> orderBys, List<DataType> columnMetas) {
        this.rowComparator = ExecUtils.getComparator(orderBys, columnMetas);
    }

    public int compareTo(Chunk left, int leftPosition, Chunk right, int rightPosition) {
        return rowComparator.compare(left.rowAt(leftPosition), right.rowAt(rightPosition));
    }

    public int compareTo(Row row, Chunk right, int rightPosition) {
        return rowComparator.compare(row, right.rowAt(rightPosition));
    }
}