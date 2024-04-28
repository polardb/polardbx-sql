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
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.gms.engine.FileSystemUtils;
import org.roaringbitmap.RoaringBitmap;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.text.MessageFormat;

/**
 * Simple implementation of .del file reader.
 * It will load all .del file bytes into memory, and parse bytes to DeletionEntry unit byte-by-byte.
 */
public class SimpleDeletionFileReader implements DeletionFileReader {
    private static final Logger LOGGER = LoggerFactory.getLogger("oss");
    private ByteBuffer byteBuffer;
    private int offset;

    @Override
    public void open(Engine engine, String delFileName, int offset, int length) throws IOException {
        // synchronous reading (it may cause OOM)
        byte[] buffer;

        if (!FileSystemUtils.fileExists(delFileName, engine, true)) {
            buffer = new byte[0];
            LOGGER.warn(
                MessageFormat.format("{0} in Engine:{1} is not exists with offset:{2} and length:{3}", delFileName,
                    engine, offset, length));
        } else if (offset == 0 && length == EOF) {
            // read fully
            buffer = FileSystemUtils.readFullyFile(delFileName, engine, true);
        } else {
            // read from offset
            buffer = new byte[length];
            FileSystemUtils.readFile(delFileName, offset, length, buffer, engine, true);
        }
        this.byteBuffer = ByteBuffer.wrap(buffer);
        this.offset = offset;
    }

    @Override
    public DeletionEntry next() {
        // We suppose that the data in byte buffer is complete serialized bitmap list.
        if (byteBuffer.hasRemaining()) {
            final int sizeInBytes = byteBuffer.getInt();
            final int fileId = byteBuffer.getInt();
            final long tso = byteBuffer.getLong();
            RoaringBitmap bitmap = new RoaringBitmap();
            try {
                bitmap.deserialize(byteBuffer);
                byteBuffer.position(byteBuffer.position()
                    + sizeInBytes - (Integer.BYTES + Long.BYTES));
            } catch (IOException e) {
                LOGGER.error(MessageFormat.format(
                    "current bitmap information: sizeInBytes = {0}, fileId = {1}, tso = {2}, dataLen = {3}",
                    sizeInBytes, fileId, tso, byteBuffer.remaining()), e);
                throw GeneralUtil.nestedException(e);
            }

            return new DeletionEntry(tso, fileId, bitmap);
        }
        return null;
    }

    @Override
    public int position() {
        return offset + byteBuffer.position();
    }

    @Override
    public void close() {
        if (byteBuffer != null) {
            this.byteBuffer.clear();
        }
    }
}
