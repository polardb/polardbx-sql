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

package com.alibaba.polardbx.common.collation;

import com.alibaba.polardbx.common.charset.CharsetName;
import com.alibaba.polardbx.common.charset.CollationName;
import com.alibaba.polardbx.common.charset.SortKey;
import com.alibaba.polardbx.common.charset.CharsetFactory;
import com.alibaba.polardbx.common.charset.CharsetHandler;
import io.airlift.slice.Slice;

import java.util.Optional;

public interface CollationHandler {

    boolean DIFF_IF_ONLY_END_SPACE_DIFFERENCE = false;

    long INIT_HASH_VALUE_1 = 1l;

    long INIT_HASH_VALUE_2 = 4l;

    int INVALID_CODE = -1;

    CollationName getName();

    CharsetName getCharsetName();

    default boolean isCaseSensitive() {
        return getName().isCaseSensitive();
    }

    CharsetHandler getCharsetHandler();

    default int compare(String utf16Str1, String utf16Str2) {
        if (utf16Str1 == null) {
            return -1;
        } else if (utf16Str2 == null) {
            return 1;
        }
        return isCaseSensitive() ? utf16Str1.compareTo(utf16Str2) : utf16Str1.compareToIgnoreCase(utf16Str2);
    }

    default int compare(char[] utf16Char1, char[] utf16Char2) {
        return compare(new String(utf16Char1), new String(utf16Char2));
    }

    default int compare(Slice binaryStr1, Slice binaryStr2) {
        String utf16Str1 = CharsetFactory.INSTANCE.DEFAULT_CHARSET_HANDLER.decode(binaryStr1);
        String utf16Str2 = CharsetFactory.INSTANCE.DEFAULT_CHARSET_HANDLER.decode(binaryStr2);
        return compare(utf16Str1, utf16Str2);
    }

    default int compareSp(Slice binaryStr1, Slice binaryStr2) {
        return compare(binaryStr1, binaryStr2);
    }

    default SortKey getSortKey(Slice str, int maxLength) {
        return null;
    }

    default int instr(Slice source, Slice target) {
        return 0;
    }

    default boolean wildCompare(Slice slice, Slice wildCard) {
        return false;
    }

    default int hashcode(Slice str) {
        return Optional.ofNullable(str)
            .map(CharsetFactory.INSTANCE.DEFAULT_CHARSET_HANDLER::decode)
            .map(String::hashCode)
            .orElse(0);
    }

    default void hashcode(byte[] bytes, int end, long[] numbers) {

        hashcode(bytes, 0, end, numbers);
    }

    default void hashcode(byte[] bytes, int begin, int end, long[] numbers) {

    }

    default byte padChar() {
        return 0x20;
    }
}
