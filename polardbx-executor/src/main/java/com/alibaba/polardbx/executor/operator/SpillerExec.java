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

package com.alibaba.polardbx.executor.operator;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.executor.chunk.ChunkBuilder;
import com.alibaba.polardbx.executor.operator.spill.SingleStreamSpiller;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;

import java.util.Iterator;
import java.util.List;

public class SpillerExec implements Executor {
    List<DataType> dataTypes;
    SingleStreamSpiller spiller;
    ChunkBufferFromRow willSpillBuffer;

    Iterator<Chunk> unspilledChunks;
    private ListenableFuture<?> spillBlocked = Futures.immediateFuture(null);
    private ListenableFuture<?> produceBlocked = ProducerExecutor.NOT_BLOCKED;

    public SpillerExec(SingleStreamSpiller spiller, ChunkBuilder chunkbuilder) {
        this.dataTypes = chunkbuilder.getTypes();
        this.spiller = spiller;
        this.willSpillBuffer = new ChunkBufferFromRow(chunkbuilder);
    }

    public void addRowToSpill(Chunk srcChunk, int srcPosition) {
        willSpillBuffer.addRow(srcChunk, srcPosition);
    }

    public void spillRows(boolean force) {
        willSpillBuffer.flushToBuffer(force);
        if (willSpillBuffer.hasNextChunk()) {
            spillBlocked = spiller.spill(willSpillBuffer.getChunksAndDeleteAfterRead());
        }
    }

    public void spillChunk(Chunk chunk) {
        spillBlocked = spiller.spill(chunk);
    }

    public void spillChunksIterator(Iterator<Chunk> chunks) {
        spillBlocked = spiller.spill(chunks);
    }

    public ListenableFuture<?> spillIsBlocked() {
        return spillBlocked;
    }

    public void buildUnspill() {
        willSpillBuffer.flushToBuffer(true);
        unspilledChunks = spiller.getSpilledChunks();
    }

    @Override
    public void open() {

    }

    @Override
    public Chunk nextChunk() {
        if (willSpillBuffer.hasNextChunk()) {
            return willSpillBuffer.nextChunk();
        }
        if (unspilledChunks.hasNext()) {
            //TODO: async unspill
            return unspilledChunks.next();
        }
        return null;
    }

    @Override
    public void close() {

    }

    @Override
    public boolean produceIsFinished() {
        return !willSpillBuffer.hasNextChunk() && !unspilledChunks.hasNext();
    }

    @Override
    public ListenableFuture<?> produceIsBlocked() {
        //always immediateFuture current implement
        return produceBlocked;
    }

    @Override
    public List<DataType> getDataTypes() {
        return dataTypes;
    }

    @Override
    public List<Executor> getInputs() {
        throw new UnsupportedOperationException(getClass().getName());
    }

}
