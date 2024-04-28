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

import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.spill.QuerySpillSpaceMonitor;
import com.alibaba.polardbx.optimizer.spill.SpillMonitor;
import com.google.common.io.Closer;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.calcite.sql.OutFileParams;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

public class GenericSpiller implements Spiller {

    private List<DataType> types;
    private SingleStreamSpillerFactory singleStreamSpillerFactory;
    private final Closer closer = Closer.create();
    private ListenableFuture<?> previousSpill = Futures.immediateFuture(null);
    private final List<SingleStreamSpiller> singleStreamSpillers = new ArrayList<>();
    private final SpillMonitor spillMonitor;
    private final OutFileParams outFileParams;

    public GenericSpiller(List<DataType> types, SingleStreamSpillerFactory singleStreamSpillerFactory,
                          SpillMonitor spillMonitor, OutFileParams outFileParams) {
        this.types = requireNonNull(types, "types can not be null");
        this.singleStreamSpillerFactory =
            requireNonNull(singleStreamSpillerFactory, "singleStreamSpillerFactory can not be null");
        this.spillMonitor = spillMonitor;
        this.outFileParams = outFileParams;
    }

    @Override
    public ListenableFuture<?> spill(Iterator<Chunk> chunkIterator, boolean append) {
        checkNoSpillInProgress();
        SingleStreamSpiller singleStreamSpiller;
        if (append && singleStreamSpillers.size() > 0) {
            singleStreamSpiller = singleStreamSpillers.get(0);
        } else {
            singleStreamSpiller = singleStreamSpillerFactory.create(
                spillMonitor.tag(), types, spillMonitor.newLocalSpillMonitor(), outFileParams);
            singleStreamSpillers.add(singleStreamSpiller);
            closer.register(singleStreamSpiller);
            if (spillMonitor instanceof QuerySpillSpaceMonitor) {
                ((QuerySpillSpaceMonitor) spillMonitor).register(singleStreamSpiller);
            }
        }
        previousSpill = singleStreamSpiller.spill(chunkIterator);
        return previousSpill;
    }

    private void checkNoSpillInProgress() {
        checkState(previousSpill.isDone(), "previous spill still in progress");
    }

    @Override
    public List<Iterator<Chunk>> getSpills() {
        checkNoSpillInProgress();
        return singleStreamSpillers.stream()
            .map(SingleStreamSpiller::getSpilledChunks)
            .collect(toList());
    }

    @Override
    public List<Iterator<Chunk>> getSpills(long maxChunkNum) {
        return singleStreamSpillers.stream()
            .map(spiller -> spiller.getSpilledChunks(maxChunkNum))
            .collect(toList());
    }

    @Override
    public void flush() {
        singleStreamSpillers.forEach(SingleStreamSpiller::flush);
    }

    @Override
    public void close() {
        try {
            closer.close();
        } catch (IOException e) {
            throw new RuntimeException("could not close some single stream spillers", e);
        }
    }
}
