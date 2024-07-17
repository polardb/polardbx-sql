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

package org.apache.orc.customized;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;

import java.nio.ByteBuffer;

public class DefaultORCMemoryAllocator implements ORCMemoryAllocator {
    private BufferAllocator allocator = new RootAllocator();

    @Override
    public ByteBuffer allocateOnHeap(int bufferSize) {
        return allocateOnHeap(bufferSize, null);
    }

    @Override
    public ByteBuffer allocateOnHeap(int bufferSize, ORCProfile profile) {
        ByteBuffer result = ByteBuffer.allocate(bufferSize);

        if (profile != null) {
            profile.update(bufferSize);
        }

        return result;
    }

    @Override
    public ByteBuffer allocateOffHeap(int bufferSize) {
        return ByteBuffer.allocateDirect(bufferSize);
    }

    @Override
    public Recyclable<ByteBuffer> pooledDirect(int bufferSize) {
        ArrowBuf arrowBuf = allocator.buffer(bufferSize);
        return new Recyclable<ByteBuffer>() {
            @Override
            public ByteBuffer get() {
                return arrowBuf.nioBuffer();
            }

            @Override
            public void recycle() {
                arrowBuf.close();
            }
        };
    }
}
