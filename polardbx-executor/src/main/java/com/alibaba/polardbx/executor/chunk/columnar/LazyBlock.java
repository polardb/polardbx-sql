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

package com.alibaba.polardbx.executor.chunk.columnar;

import com.alibaba.polardbx.executor.chunk.Block;
import com.alibaba.polardbx.executor.chunk.RandomAccessBlock;

/**
 * The data block with delay materialization.
 */
public interface LazyBlock extends RandomAccessBlock, Block {
    /**
     * Get internal arrow-vector.
     */
    Block getLoaded();

    /**
     * Load arrow-block data lazily.
     */
    void load();

    /**
     * whether the array is already materialized.
     */
    boolean hasVector();

    BlockLoader getLoader();

    /**
     * Release the reference of column reader used by this lazy block.
     */
    void releaseRef();
}
