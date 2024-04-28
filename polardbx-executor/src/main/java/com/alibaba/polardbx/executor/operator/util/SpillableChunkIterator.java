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
import com.alibaba.polardbx.executor.operator.spill.Spiller;
import com.google.common.util.concurrent.ListenableFuture;

import java.util.Iterator;

public class SpillableChunkIterator implements Iterator<Chunk> {

    private Iterator<Chunk> iterator;

    public SpillableChunkIterator(Iterator<Chunk> iterator) {
        this.iterator = iterator;
    }

    @Override
    public boolean hasNext() {
        return iterator.hasNext();
    }

    @Override
    public Chunk next() {
        return iterator.next();
    }

    public ListenableFuture<?> spill(Spiller spiller) {
        return spiller.spill(iterator, false);
    }

    public void setIterator(Iterator<Chunk> iterator) {
        this.iterator = iterator;
    }
}
