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

package com.alibaba.polardbx.executor.columnar;

import com.alibaba.polardbx.common.Engine;
import lombok.Data;
import org.roaringbitmap.RoaringBitmap;

import java.io.Closeable;
import java.io.IOException;

/**
 * Read and parse .del file in given storage engine into memory as bitmaps.
 */
public interface DeletionFileReader extends Closeable {
    /**
     * It means read fully when length = EOF (end of file)
     */
    int EOF = -1;

    /**
     * The format of deletion entry unit:
     * 1. generated timestamp
     * 2. file identifier of .csv or .orc
     * 3. memory structure of deletion masks in format of bitmap.
     */
    @Data
    class DeletionEntry {
        private final long tso;
        private final int fileId;
        private final RoaringBitmap bitmap;

        public DeletionEntry(long tso, int fileId, RoaringBitmap bitmap) {
            this.tso = tso;
            this.fileId = fileId;
            this.bitmap = bitmap;
        }
    }

    /**
     * Open the .del file resource.
     *
     * @param engine storage engine of .del file.
     * @param delFileName file name of .del without uri prefix like 'oss://dir/'
     * @param offset file offset to read from.
     * @param length file length to read.
     * @throws IOException throw exception when IO failure
     */
    void open(Engine engine, String delFileName, int offset, int length) throws IOException;

    /**
     * Fetch and parse the next deletion entry unit.
     */
    DeletionEntry next();

    int position();
}
