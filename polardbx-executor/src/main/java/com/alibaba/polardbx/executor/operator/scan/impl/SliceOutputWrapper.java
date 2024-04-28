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

package com.alibaba.polardbx.executor.operator.scan.impl;

import com.google.common.base.Preconditions;
import io.airlift.slice.SliceOutput;
import org.apache.orc.customized.ORCDataOutput;

import java.nio.ByteBuffer;

/**
 * A ORCDataOutput wrapped with SliceOutput object.
 * It will automatically maintain the current offset of bytes.
 */
public class SliceOutputWrapper implements ORCDataOutput {
    private final SliceOutput sliceOutput;

    public SliceOutputWrapper(SliceOutput sliceOutput) {
        this.sliceOutput = sliceOutput;
    }

    public void read(ByteBuffer buffer, int bytesToRead) {
        Preconditions.checkArgument(bytesToRead <= buffer.remaining());

        // must be an instance of HeapByteBuffer
        Preconditions.checkArgument(buffer.array() != null);

        // NOTE:
        // HeapByteBuffer.get(byte[] dst, int offset, int length)
        // ix(position()) = position + offset
        sliceOutput.write(buffer.array(), buffer.arrayOffset() + buffer.position(), bytesToRead);
        buffer.position(buffer.position() + bytesToRead);
    }
}
