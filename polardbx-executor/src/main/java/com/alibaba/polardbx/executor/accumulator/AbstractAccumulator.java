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

package com.alibaba.polardbx.executor.accumulator;

import com.alibaba.polardbx.executor.chunk.Block;
import com.alibaba.polardbx.executor.chunk.Chunk;

/**
 * Base class for Accumulator.
 * <p>
 * Override one of the three <code>accumulate</code> method to make it work
 *
 * @author Eric Fu
 */
abstract class AbstractAccumulator implements Accumulator {

    @Override
    public final void accumulate(int groupId, Chunk inputChunk, int position) {
        final int inputSize = getInputTypes().length;
        if (inputSize == 0) {
            accumulate(groupId);
        } else if (inputSize == 1) {
            accumulate(groupId, inputChunk.getBlock(0), position);
        } else {
            throw new UnsupportedOperationException(getClass().getName() + " has multiple arguments");
        }
    }

    /**
     * accumulate method with no arguments e.g. COUNT(*)
     */
    void accumulate(int groupId) {
        throw new AssertionError("not implemented");
    }

    /**
     * accumulate method with one arguments e.g. SUM(x)
     *
     * @param position value position in block
     */
    void accumulate(int groupId, Block block, int position) {
        throw new AssertionError("not implemented");
    }
}
