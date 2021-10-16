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
import com.alibaba.polardbx.common.charset.CharsetHandler;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceInput;

import static com.alibaba.polardbx.common.collation.AbstractCollationHandler.codepointOfUTF8;

public class Utf8mb4UnicodeCiCollationHandler extends AbstractUCACollationHandler {

    public Utf8mb4UnicodeCiCollationHandler(CharsetHandler charsetHandler) {
        super(charsetHandler);
    }

    @Override
    int getCodePoint(SliceInput sliceInput) {
        return codepointOfUTF8(sliceInput);
    }

    @Override
    public CollationName getName() {
        return CollationName.UTF8MB4_UNICODE_CI;
    }

    @Override
    public CharsetName getCharsetName() {
        return CharsetName.UTF8MB4;
    }

    @Override
    public int instr(Slice source, Slice target) {
        return instrForMultiBytes(source, target,
            sliceInput -> codepointOfUTF8(sliceInput));
    }
}
