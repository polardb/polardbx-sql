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
import com.alibaba.polardbx.executor.operator.scan.CacheReader;
import com.alibaba.polardbx.executor.operator.scan.ColumnReader;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;

import java.io.IOException;

/**
 * A block-level loader of one column in one row group.
 * Several block-level loader will share the column reader spanning multiple row groups.
 */
public interface BlockLoader {
    /**
     * Trigger the processing of loading.
     */
    Block load(DataType dataType, int[] selection, int selSize) throws IOException;

    /**
     * Get the column reader inside this block loader.
     * Several block-level loader will share the column reader spanning multiple row groups.
     *
     * @return Column reader.
     */
    ColumnReader getColumnReader();

    CacheReader<Block> getCacheReader();

    int startPosition();

    int positionCount();
}
