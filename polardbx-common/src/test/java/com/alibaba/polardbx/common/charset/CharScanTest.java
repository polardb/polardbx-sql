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

package com.alibaba.polardbx.common.charset;

import com.alibaba.polardbx.common.charset.CharsetFactory;
import com.alibaba.polardbx.common.charset.CharsetHandler;
import io.airlift.slice.SliceInput;
import io.airlift.slice.Slices;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;

public class CharScanTest {
    private static final String[] STRINGS = {"e", "n", "g", "l", "i", "s", "h", "😁", "中", "文", "乀", "艽"};
    private static final String TEST_STR = "english😁中文乀艽";
    private static final Integer[] SIZES = Arrays
        .stream(STRINGS)
        .map(s -> s.getBytes().length)
        .toArray(Integer[]::new);

    @Test
    public void testBytes() {
        CharsetHandler utf8Handler = CharsetFactory.DEFAULT_CHARSET_HANDLER;
        String str = TEST_STR;
        byte[] bytes = str.getBytes();
        int pos = 0;
        int offset = 0;
        int charLen = 0;
        while (charLen > 0 && offset < bytes.length) {
            // check each length of char
            charLen = utf8Handler.nextCharLen(bytes, offset, bytes.length - offset);
            offset += charLen;

            // compare to java encoded bytes
            Assert.assertEquals(SIZES[pos++].intValue(), charLen);
        }
    }

    @Test
    public void testSlice() {
        CharsetHandler utf8Handler = CharsetFactory.DEFAULT_CHARSET_HANDLER;
        String str = TEST_STR;
        byte[] bytes = str.getBytes();
        int pos = 0;
        int charLen = 0;
        SliceInput sliceInput = Slices.wrappedBuffer(bytes).getInput();
        while (charLen > 0 && sliceInput.isReadable()) {
            // check each length of char
            charLen = utf8Handler.nextCharLen(sliceInput);

            // compare to java encoded bytes
            Assert.assertEquals(SIZES[pos++].intValue(), charLen);
        }
    }


}
