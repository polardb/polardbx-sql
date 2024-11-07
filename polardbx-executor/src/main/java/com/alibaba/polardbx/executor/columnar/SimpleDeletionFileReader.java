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
import org.apache.hadoop.fs.FSDataInputStream;
import org.roaringbitmap.RoaringBitmap;

import java.io.IOException;
import java.text.MessageFormat;

/**
 * Simple implementation of .del file reader.
 * It will load all .del file bytes into memory, and parse bytes to DeletionEntry unit byte-by-byte.
 */
public class SimpleDeletionFileReader implements DeletionFileReader {
    private static final Logger LOGGER = LoggerFactory.getLogger("oss");
    private int offset;
    private int length;
    private int pos = 0;
    private FSDataInputStream inputStream;

    @Override
    public void open(Engine engine, String delFileName, int offset, int length) throws IOException {

        // TODO(siyun): turn into streaming read
        if (!FileSystemUtils.fileExists(delFileName, engine, true)) {
            this.inputStream = null;
            LOGGER.warn(
                MessageFormat.format("{0} in Engine:{1} is not exists with offset:{2} and length:{3}", delFileName,
                    engine, offset, length));
        }
        this.inputStream = FileSystemUtils.openStreamFileWithBuffer(delFileName, engine, true);
        if (offset > 0) {
            inputStream.seek(offset);
        }

        this.offset = offset;
        this.length = length == EOF ? Integer.MAX_VALUE : length;
    }

    @Override
    public DeletionEntry next() {
        if (inputStream == null) {
            return null;
        }

        // We suppose that the data in byte buffer is complete serialized bitmap list.
        try {
            if (inputStream.getPos() < offset + length && inputStream.available() > 0) {
                final int sizeInBytes = inputStream.readInt();
                final int fileId = inputStream.readInt();
                final long tso = inputStream.readLong();
                RoaringBitmap bitmap = new RoaringBitmap();
                try {
                    bitmap.deserialize(inputStream);
                } catch (IOException e) {
                    LOGGER.error(MessageFormat.format(
                        "current bitmap information: sizeInBytes = {0}, fileId = {1}, tso = {2}, position = {3}",
                        sizeInBytes, fileId, tso, inputStream.getPos()), e);
                    throw GeneralUtil.nestedException(e);
                }

                pos = (int) inputStream.getPos();
                return new DeletionEntry(tso, fileId, bitmap);
            }
        } catch (IOException e) {
            LOGGER.error(MessageFormat.format("current readBytesCount = {0}", pos), e);
        }
        return null;
    }

    @Override
    public int position() {
        return pos;
    }

    @Override
    public void close() {
        if (inputStream != null) {
            try {
                inputStream.close();
            } catch (Throwable t) {
                // ignore
            }
        }
    }
}
