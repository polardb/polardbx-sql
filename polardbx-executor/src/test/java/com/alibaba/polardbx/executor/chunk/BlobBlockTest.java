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

package com.alibaba.polardbx.executor.chunk;

import com.alibaba.druid.mock.MockBlob;
import com.alibaba.polardbx.common.memory.MemoryCountable;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.SliceOutput;
import org.junit.Assert;
import org.junit.Test;

import java.sql.Blob;

public class BlobBlockTest extends BaseBlockTest {

    @Test
    public void testMockBlob() {
        final Blob[] values = new Blob[] {
            new MockBlob(), new MockBlob()
        };

        BlobBlockBuilder blockBuilder = new BlobBlockBuilder(5);
        for (Blob value : values) {
            if (value != null) {
                blockBuilder.writeBlob(value);
            } else {
                blockBuilder.appendNull();
            }
        }
        blockBuilder.build();
    }

    @Test
    public void testBlobEncoding() {
        // new 4 blob objects
        final Blob[] blobs = new Blob[] {
            new com.alibaba.polardbx.optimizer.core.datatype.Blob(new byte[] {0x7E, 0x7F}),
            new com.alibaba.polardbx.optimizer.core.datatype.Blob(new byte[] {0x1, 0x2, 0x3}),
            new com.alibaba.polardbx.optimizer.core.datatype.Blob(new byte[] {0x4, 0x5, 0x6, 0x7}),
            new com.alibaba.polardbx.optimizer.core.datatype.Blob(new byte[] {0x8, 0x9, 0xA, 0xB, 0xC})
        };

        // get blob block
        BlobBlockBuilder blobBlockBuilder = new BlobBlockBuilder(1024);
        for (Blob blob : blobs) {
            blobBlockBuilder.appendNull();
            blobBlockBuilder.writeBlob(blob);
        }
        MemoryCountable.checkDeviation(blobBlockBuilder, .05d, true);
        Block blobBlock = blobBlockBuilder.build();
        MemoryCountable.checkDeviation(blobBlock, .05d, true);

        // serialize
        BlobBlockEncoding encoding = new BlobBlockEncoding();
        SliceOutput output = new DynamicSliceOutput(8);
        encoding.writeBlock(output, blobBlock);

        // deserialize
        Block blobBlockShuffled = encoding.readBlock(output.slice().getInput());
        MemoryCountable.checkDeviation(blobBlockShuffled, .05d, true);

        // check
        for (int i = 0; i < blobBlock.getPositionCount(); i++) {
            Assert.assertTrue(blobBlock.equals(i, blobBlockShuffled, i));
        }
    }

    @Test
    public void testCopy() {
        final Blob[] blobs = new Blob[] {
            new com.alibaba.polardbx.optimizer.core.datatype.Blob(new byte[] {0x7E, 0x7F}),
            new com.alibaba.polardbx.optimizer.core.datatype.Blob(new byte[] {0x1, 0x2, 0x3}),
            new com.alibaba.polardbx.optimizer.core.datatype.Blob(new byte[] {0x4, 0x5, 0x6, 0x7}),
            new com.alibaba.polardbx.optimizer.core.datatype.Blob(new byte[] {0x8, 0x9, 0xA, 0xB, 0xC})
        };

        BlobBlockBuilder blobBlockBuilder = new BlobBlockBuilder(1);
        for (Blob blob : blobs) {
            blobBlockBuilder.appendNull();
            blobBlockBuilder.writeBlob(blob);
        }
        BlobBlock blobBlock = (BlobBlock) blobBlockBuilder.build();
        MemoryCountable.checkDeviation(blobBlock, .05d, true);

        BlobBlock newBlock = BlobBlock.from(blobBlock, blobBlock.getPositionCount(), null);
        MemoryCountable.checkDeviation(newBlock, .05d, true);
        for (int i = 0; i < blobBlock.getPositionCount(); i++) {
            if (blobBlock.isNull(i)) {
                Assert.assertTrue(newBlock.isNull(i));
                continue;
            }
            Assert.assertEquals(blobBlock.getBlob(i), newBlock.getBlob(i));
            Assert.assertEquals(blobBlock.hashCode(i), newBlock.hashCode(i));
            Assert.assertEquals(blobBlock.hashCodeUseXxhash(i), newBlock.hashCodeUseXxhash(i));
            Assert.assertEquals(blobBlock.checksum(i), newBlock.checksum(i));
        }

        int[] sel = new int[] {0, 1, 2, 3, 6, 7};
        BlobBlock newBlock2 = BlobBlock.from(blobBlock, sel.length, sel);
        MemoryCountable.checkDeviation(newBlock2, .05d, true);
        for (int i = 0; i < sel.length; i++) {
            int j = sel[i];
            if (blobBlock.isNull(j)) {
                Assert.assertTrue(newBlock2.isNull(i));
                continue;
            }
            Assert.assertEquals(blobBlock.getBlob(j), newBlock2.getBlob(i));
        }
    }
}
