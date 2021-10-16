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

package com.alibaba.polardbx.executor.cursor.impl;

import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableList;
import com.alibaba.polardbx.executor.cursor.AbstractCursor;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.mpp.operator.WorkProcessor;
import com.alibaba.polardbx.executor.utils.ExecUtils;
import com.alibaba.polardbx.executor.utils.OrderByOption;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.row.Row;

import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.alibaba.polardbx.executor.mpp.operator.WorkProcessor.mergeSorted;

/**
 * Merge Sort Cursor
 */
public class MergeSortCursor extends AbstractCursor {

    private final List<Cursor> cursors;
    private final List<OrderByOption> orderBys;
    private final long limit;
    private long fetched;
    private long skipped;

    private Iterator<Optional<Row>> sortedRows;

    public MergeSortCursor(ExecutionContext context, List<Cursor> cursors,
                           List<OrderByOption> orderBys, long offset, long limit) {
        super(ExecUtils.isOperatorMetricEnabled(context));
        this.cursors = cursors;
        this.orderBys = orderBys;
        this.limit = limit;
        this.skipped = offset;
        this.fetched = limit;
        this.returnColumns = cursors.get(0).getReturnColumns();
    }

    @Override
    public void doInit() {
        if (limit > 0) {
            if (inited) {
                return;
            }
            inited = true;

            List<WorkProcessor<Row>> sortedChunks = cursors.stream().map(
                input -> WorkProcessor.fromIterator(new ResultIterator(input))).collect(toImmutableList());

            List<WorkProcessor<Row>> sortedStreams = ImmutableList.<WorkProcessor<Row>>builder()
                .addAll(sortedChunks)
                .build();

            final Comparator<Row> rowComparator = ExecUtils.getComparator(orderBys,
                getReturnColumns().stream().map(columnMeta -> columnMeta.getDataType()).collect(Collectors.toList()));
            this.sortedRows = mergeSorted(sortedStreams, rowComparator).yieldingIterator();
        }
    }

    @Override
    public Row doNext() {
        if (fetched <= 0) {
            return null;
        }
        while (true) {
            if (!sortedRows.hasNext()) {
                return null;
            }
            Optional<Row> row = this.sortedRows.next();
            if (skipped > 0) {
                skipped--;
            } else {
                fetched--;
                if (!row.isPresent()) {
                    return null;
                } else {
                    Row ret = row.get();
                    return ret;
                }
            }
        }
    }

    @Override
    public List<Throwable> doClose(List<Throwable> exs) {
        for (Cursor c : this.cursors) {
            if (c != null) {
                exs = c.close(exs);
            }
        }
        return exs;
    }

    private class ResultIterator extends AbstractIterator<Row> {
        Cursor input;

        public ResultIterator(Cursor input) {
            this.input = input;
        }

        @Override
        protected Row computeNext() {
            Row ret = input.next();
            if (ret == null) {
                endOfData();
            }
            return ret;
        }
    }
}
