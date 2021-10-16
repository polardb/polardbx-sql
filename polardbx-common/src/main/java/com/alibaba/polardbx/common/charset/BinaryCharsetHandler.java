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

import com.alibaba.polardbx.common.collation.BinaryCollationHandler;

public class BinaryCharsetHandler extends AbstractCharsetHandler {
    BinaryCharsetHandler(CollationName checkedCollationName) {
        super(CharsetName.BINARY.toJavaCharset(), checkedCollationName);
        switch (checkedCollationName) {
        case BINARY:
            this.collationHandler = new BinaryCollationHandler(this);
            break;
        default:
            throw new UnsupportedOperationException();
        }
    }

    @Override
    public CharsetName getName() {
        return CharsetName.BINARY;
    }

    @Override
    public int maxLenOfMultiBytes() {
        return 1;
    }

    @Override
    public int minLenOfMultiBytes() {
        return 1;
    }

}
