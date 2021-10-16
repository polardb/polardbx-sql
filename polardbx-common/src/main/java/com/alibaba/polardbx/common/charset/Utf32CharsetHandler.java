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

import com.alibaba.polardbx.common.collation.Utf32BinCollationHandler;
import com.alibaba.polardbx.common.collation.Utf32UnicodeCiCollationHandler;
import com.alibaba.polardbx.common.collation.Utf32GeneralCiCollationHandler;

public class Utf32CharsetHandler extends AbstractCharsetHandler {
    Utf32CharsetHandler(CollationName checkedCollationName) {
        super(CharsetName.UTF32.toJavaCharset(), checkedCollationName);
        switch (checkedCollationName) {
        case UTF32_GENERAL_CI:
            this.collationHandler = new Utf32GeneralCiCollationHandler(this);
            break;
        case UTF32_BIN:
            this.collationHandler = new Utf32BinCollationHandler(this);
            break;
        case UTF32_UNICODE_CI:
            this.collationHandler = new Utf32UnicodeCiCollationHandler(this);
            break;
        default:
            throw new UnsupportedOperationException();
        }
    }

    @Override
    public CharsetName getName() {
        return CharsetName.UTF32;
    }

    @Override
    public int maxLenOfMultiBytes() {
        return 4;
    }

    @Override
    public int minLenOfMultiBytes() {
        return 4;
    }
}
