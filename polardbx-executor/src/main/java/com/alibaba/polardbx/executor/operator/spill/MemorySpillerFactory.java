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

package com.alibaba.polardbx.executor.operator.spill;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.spill.SpillMonitor;
import org.apache.calcite.sql.OutFileParams;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.util.concurrent.Futures.immediateFuture;

@VisibleForTesting
public class MemorySpillerFactory implements SpillerFactory {

    private long spillsCount;

    @Override
    public Spiller create(List<DataType> types, SpillMonitor spillMonitor, OutFileParams outFileParams) {
        return new Spiller() {
            private final List<Iterable<Chunk>> spills = new ArrayList<>();

            @Override
            public ListenableFuture<?> spill(Iterator<Chunk> pageIterator, boolean append) {
                spillsCount++;
                spills.add(ImmutableList.copyOf(pageIterator));
                return immediateFuture(null);
            }

            @Override
            public List<Iterator<Chunk>> getSpills() {
                return spills.stream()
                    .map(Iterable::iterator)
                    .collect(toImmutableList());
            }

            @Override
            public void close() {
                spills.clear();
            }

        };
    }

    public long getSpillsCount() {
        return spillsCount;
    }

    @Override
    public SingleStreamSpillerFactory getStreamSpillerFactory() {
        return null;
    }
}
