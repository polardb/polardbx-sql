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

import com.alibaba.polardbx.common.utils.time.parser.StringNumericParser;
import com.alibaba.polardbx.common.collation.CollationHandler;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceInput;

import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;

public interface CharsetHandler {

    int INVALID_CODE = -1;

    CharsetName getName();

    Charset getCharset();

    CollationHandler getCollationHandler();

    Slice encodeWithReplace(String str);

    default Slice encode(String unicodeChars) throws CharacterCodingException {
        return encodeWithReplace(unicodeChars);
    }

    byte[] getBytes(String str);

    Slice encodeFromUtf8(Slice utf8str);

    String decode(Slice slice);

    String decode(byte[] bytes);

    String decode(byte[] bytes, int index, int len);

    String decode(Slice slice, int index, int len);

    default Slice toLowerCase(Slice slice) {
        return null;
    }

    default Slice toUpperCase(Slice slice) {
        return null;
    }

    default int nextChar(SliceInput sliceInput) {
        return 0;
    }

    default int nextCharLen(SliceInput sliceInput) {return 0;}

    default int nextCharLen(byte[] bytes, int offset, int length) {return 0;}

    int maxLenOfMultiBytes();

    int minLenOfMultiBytes();

    default void parseToLong(byte[] numericAsBytes, int begin, int end, long[] results) {
        long[] res = StringNumericParser.parseString(numericAsBytes, begin, end);
        System.arraycopy(res, 0, results, 0, 3);
    }

    void parseToLongWithRound(byte[] numericAsBytes, int begin, int end, long[] results, boolean isUnsigned);

    int parseFromLong(long toParse, boolean isUnsigned, byte[] result, int len);
}
