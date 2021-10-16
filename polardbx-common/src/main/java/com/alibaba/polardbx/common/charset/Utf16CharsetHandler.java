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

import com.alibaba.polardbx.common.collation.Utf16BinCollationHandler;
import com.alibaba.polardbx.common.collation.Utf16GeneralCiCollationHandler;
import com.alibaba.polardbx.common.collation.Utf16UnicodeCiCollationHandler;

public class Utf16CharsetHandler extends AbstractCharsetHandler {
    Utf16CharsetHandler(CollationName checkedCollationName) {
        super(CharsetName.UTF16.toJavaCharset(), checkedCollationName);
        switch (checkedCollationName) {
        case UTF16_GENERAL_CI:
            this.collationHandler = new Utf16GeneralCiCollationHandler(this);
            break;
        case UTF16_BIN:
            this.collationHandler = new Utf16BinCollationHandler(this);
            break;
        case UTF16_UNICODE_CI:
            this.collationHandler = new Utf16UnicodeCiCollationHandler(this);
            break;
        default:
            throw new UnsupportedOperationException();
        }
    }

    @Override
    public CharsetName getName() {
        return CharsetName.UTF16;
    }

    @Override
    public int maxLenOfMultiBytes() {
        return 4;
    }

    @Override
    public int minLenOfMultiBytes() {
        return 2;
    }
}
