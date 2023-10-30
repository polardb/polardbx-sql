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

package com.alibaba.polardbx.optimizer.config.table.charset;

import com.alibaba.polardbx.common.charset.CharsetName;
import com.alibaba.polardbx.common.charset.CollationName;
import com.alibaba.polardbx.optimizer.config.table.collation.Utf8mb40900AiCiCollationHandler;
import com.alibaba.polardbx.optimizer.config.table.collation.Utf8mb4BinCollationHandler;
import com.alibaba.polardbx.optimizer.config.table.collation.Utf8mb4GeneralCiCollationHandler;
import com.alibaba.polardbx.optimizer.config.table.collation.Utf8mb4Unicode520CiCollationHandler;
import com.alibaba.polardbx.optimizer.config.table.collation.Utf8mb4UnicodeCiCollationHandler;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceInput;

public class Utf8mb4CharsetHandler extends AbstractCharsetHandler {
    Utf8mb4CharsetHandler(CollationName checkedCollationName) {
        super(CharsetName.UTF8MB4.toJavaCharset(), checkedCollationName);
        switch (checkedCollationName) {
        case UTF8MB4_GENERAL_CI:
            this.collationHandler = new Utf8mb4GeneralCiCollationHandler(this);
            break;
        case UTF8MB4_BIN:
            this.collationHandler = new Utf8mb4BinCollationHandler(this);
            break;
        case UTF8MB4_UNICODE_CI:
            this.collationHandler = new Utf8mb4UnicodeCiCollationHandler(this);
            break;
        case UTF8MB4_0900_AI_CI:
            this.collationHandler = new Utf8mb40900AiCiCollationHandler(this);
            break;
        case UTF8MB4_UNICODE_520_CI:
            this.collationHandler = new Utf8mb4Unicode520CiCollationHandler(this);
            break;
        default:
            throw new UnsupportedOperationException();
        }
    }

    @Override
    public CharsetName getName() {
        return CharsetName.UTF8MB4;
    }

    @Override
    public Slice encodeFromUtf8(Slice utf8str) {
        return utf8str;
    }

    @Override
    public int maxLenOfMultiBytes() {
        return 4;
    }

    @Override
    public int minLenOfMultiBytes() {
        return 1;
    }

    @Override
    public int nextChar(SliceInput buff) {
        return nextCharUtf8(buff);
    }

    @Override
    public int nextCharLen(SliceInput sliceInput) {
        return nextCharLenUtf8(sliceInput);
    }

    @Override
    public int nextCharLen(byte[] bytes, int offset, int length) {
        return nextCharLenUtf8(bytes, offset, length);
    }
}
