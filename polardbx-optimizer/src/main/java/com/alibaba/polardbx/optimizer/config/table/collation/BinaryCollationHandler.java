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

package com.alibaba.polardbx.optimizer.config.table.collation;

import com.alibaba.polardbx.common.charset.CharsetName;
import com.alibaba.polardbx.common.charset.CollationName;
import com.alibaba.polardbx.common.charset.SortKey;
import com.alibaba.polardbx.optimizer.config.table.charset.CharsetHandler;
import com.google.common.base.Preconditions;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceInput;

public class BinaryCollationHandler extends AbstractCollationHandler {
    public BinaryCollationHandler(CharsetHandler charsetHandler) {
        super(charsetHandler);
    }

    @Override
    public CollationName getName() {
        return CollationName.BINARY;
    }

    @Override
    public CharsetName getCharsetName() {
        return CharsetName.BINARY;
    }

    @Override
    public boolean isCaseSensitive() {
        return true;
    }

    @Override
    public CharsetHandler getCharsetHandler() {
        return charsetHandler;
    }

    @Override
    public int compare(Slice str1, Slice str2) {
        SliceInput leftInput = str1.getInput();
        SliceInput rightInput = str2.getInput();
        while (leftInput.isReadable() && rightInput.isReadable()) {
            byte b1 = leftInput.readByte();
            byte b2 = rightInput.readByte();
            if (b1 != b2) {
                return Byte.toUnsignedInt(b1) - Byte.toUnsignedInt(b2);
            }
        }
        return leftInput.available() - rightInput.available();
    }

    @Override
    public int compareSp(Slice str1, Slice str2) {
        return compare(str1, str2);
    }

    @Override
    public SortKey getSortKey(Slice str, int maxLength) {
        byte[] keys = str.getBytes();
        return new SortKey(CharsetName.BINARY, CollationName.BINARY, str.getInput(), keys);
    }

    @Override
    public int instr(Slice source, Slice target) {
        return instrForSingleByte(source, target, b -> Byte.toUnsignedInt(b));
    }

    @Override
    public int hashcode(Slice binaryStr) {
        long tmp1 = INIT_HASH_VALUE_1;
        long tmp2 = INIT_HASH_VALUE_2;
        SliceInput sliceInput = binaryStr.getInput();

        while (sliceInput.isReadable()) {
            byte b = sliceInput.readByte();
            tmp1 ^= (((tmp1 & 0x3F) + tmp2) * Byte.toUnsignedInt(b)) + (tmp1 << 8);
            tmp2 += 3;
        }
        return (int) tmp1;
    }

    @Override
    public void hashcode(byte[] bytes, int begin, int end, long[] numbers) {
        Preconditions.checkArgument(numbers.length == 2);
        long tmp1 = numbers[0];
        long tmp2 = numbers[1];

        for (int i = begin; i < end; i++) {
            byte b = bytes[i];
            tmp1 ^= (((tmp1 & 63) + tmp2) * ((int) b) & 0xff) + (tmp1 << 8);
            tmp2 += 3;
        }
        numbers[0] = tmp1;
        numbers[1] = tmp2;
    }

    @Override
    public byte padChar() {
        return 0;
    }
}
