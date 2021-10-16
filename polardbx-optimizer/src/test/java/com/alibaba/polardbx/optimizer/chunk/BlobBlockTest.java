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

package com.alibaba.polardbx.optimizer.chunk;

import com.alibaba.druid.mock.MockBlob;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.SliceOutput;
import org.junit.Assert;
import org.junit.Test;

import java.sql.Blob;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

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
        Block blobBlock = blobBlockBuilder.build();

        // serialize
        BlobBlockEncoding encoding = new BlobBlockEncoding();
        SliceOutput output = new DynamicSliceOutput(8);
        encoding.writeBlock(output, blobBlock);

        // deserialize
        Block blobBlockShuffled = encoding.readBlock(output.slice().getInput());

        // check
        for (int i = 0; i != blobBlock.getPositionCount(); i++) {
            assertTrue(blobBlock.equals(i, blobBlockShuffled, i));
        }
    }
}
