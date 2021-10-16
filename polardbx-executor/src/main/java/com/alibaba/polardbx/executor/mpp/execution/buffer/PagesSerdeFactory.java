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

package com.alibaba.polardbx.executor.mpp.execution.buffer;

import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import io.airlift.compress.lz4.Lz4Compressor;
import io.airlift.compress.lz4.Lz4Decompressor;

import java.util.List;
import java.util.Optional;

public class PagesSerdeFactory {

    private final boolean compressionEnabled;

    public PagesSerdeFactory(boolean compressionEnabled) {
        this.compressionEnabled = compressionEnabled;
    }

    public PagesSerde createPagesSerde(List<DataType> types) {
        if (compressionEnabled) {
            return new PagesSerde(Optional.of(new Lz4Compressor()), Optional.of(new Lz4Decompressor()), types);
        } else {
            return new PagesSerde(Optional.empty(), Optional.empty(), types);
        }
    }
}
