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

import io.airlift.slice.Slice;
import io.airlift.slice.SliceOutput;
import io.airlift.slice.Slices;
import org.junit.Assert;
import org.junit.Test;

public class SliceWriteBytesTest {

    @Test
    public void writeLongerBytesShouldThrowEx() {
        Slice slice = Slices.allocate(1000);
        SliceOutput sliceOutput = slice.getOutput();
        boolean fail = false;
        try {
            sliceOutput.writeBytes(new byte[1024]);
        } catch (IndexOutOfBoundsException ex) {
            fail = true;
        }
        Assert.assertTrue(fail);
    }

    @Test
    public void wirteLessBytesShouldSuccess() {
        Slice slice = Slices.allocate(1000);
        SliceOutput sliceOutput = slice.getOutput();
        boolean fail = false;
        try {
            sliceOutput.appendInt(1);
            sliceOutput.writeBytes(new byte[16]);
            sliceOutput.writeBytes(new byte[512]);
            sliceOutput.appendByte(0);
        } catch (IndexOutOfBoundsException ex) {
            fail = true;
        }
        Assert.assertFalse(fail);
    }
}
