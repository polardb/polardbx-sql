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

package com.alibaba.polardbx.optimizer.core.field;

import com.alibaba.polardbx.common.charset.CharsetFactory;
import com.alibaba.polardbx.common.charset.CharsetHandler;
import com.alibaba.polardbx.common.charset.CharsetName;
import com.alibaba.polardbx.common.charset.SortKey;
import com.alibaba.polardbx.common.collation.CollationHandler;
import com.alibaba.polardbx.common.type.MySQLStandardFieldType;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

/**
 * MySQL varchar(n) data type.
 * n 0~255: 1 leading-byte for data length.
 * n 256~65535: 2 leading-byte for data length.
 */
public class VarcharField extends CharField {
    private static final int UNSET_CHAR_LENGTH = -1;
    private int varLength;

    public VarcharField(DataType<?> fieldType) {
        super(fieldType);
        varLength = UNSET_CHAR_LENGTH;
    }

    @Override
    public void reset() {
        super.reset();
        varLength = UNSET_CHAR_LENGTH;
    }

    @Override
    public void hash(long[] numbers) {
        long nr1 = numbers[0];
        long nr2 = numbers[1];
        if (isNull()) {
            nr1 ^= (nr1 << 1) | 1;
            numbers[0] = nr1;
        } else {
            int length = (isNull || varLength == UNSET_CHAR_LENGTH) ? 0 : varLength;
            int startPos = startPos();
            CollationHandler collationHandler = getCollationHandler();
            collationHandler.hashcode(packedBinary, startPos, startPos + length, numbers);
        }
    }

    @Override
    public Slice stringValue(SessionProperties sessionProperties) {
        // cut from start pos.
        int start = startPos();
        Slice original = Slices.wrappedBuffer(packedBinary, start, varLength);
        if (!fieldType.isUtf8Encoding()) {
            // convert to utf-8
            CharsetHandler charsetHandler = getCharsetHandler();
            String unicodeStr = charsetHandler.decode(original);
            return CharsetFactory.DEFAULT_CHARSET_HANDLER.encodeWithReplace(unicodeStr);
        } else {
            return Slices.copyOf(original);
        }
    }

    @Override
    protected void postHandle(int lengthOfCopy) {
        // Set the length info at the first position.
        int lengthBytes = startPos();
        if (lengthBytes == 1) {
            packedBinary[0] = (byte) lengthOfCopy;
        } else {
            packedBinary[0] = (byte) (lengthOfCopy >> 8);
            packedBinary[1] = (byte) lengthOfCopy;
        }
        varLength = lengthOfCopy;
    }

    @Override
    protected boolean countSpace() {
        return true;
    }

    @Override
    protected int startPos() {
        return fieldType.length() > 255 ? 2 : 1;
    }

    @Override
    public MySQLStandardFieldType standardFieldType() {
        return MySQLStandardFieldType.MYSQL_TYPE_VAR_STRING;
    }

    public int getVarLength() {
        return varLength;
    }

    @Override
    public void makeSortKey(byte[] result, int len) {
        if (getCharsetHandler().getName() == CharsetName.BINARY) {
            // Store length last in high-byte order to sort longer strings first
            if (startPos() == 1) {
                result[len - 1] = (byte) varLength;
            } else {
                result[len - 2] = (byte) (varLength >> 8);
                result[len - 1] = (byte) varLength;
            }
            len -= startPos();
        }

        // get slice to cal sort key.
        Slice original = Slices.wrappedBuffer(packedBinary, startPos(), varLength);

        CollationHandler collationHandler = getCollationHandler();
        SortKey sortKey = collationHandler.getSortKey(original, len);

        // copy from sort key bytes
        System.arraycopy(sortKey.keys, 0, result, 0, Math.min(len, sortKey.keys.length));
    }
}
