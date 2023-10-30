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
import com.alibaba.polardbx.optimizer.config.table.collation.Big5BinCollationHandler;
import com.alibaba.polardbx.optimizer.config.table.collation.Big5ChineseCiCollationHandler;

public class Big5CharsetHandler extends AbstractCharsetHandler {
    Big5CharsetHandler(CollationName checkedCollationName) {
        super(CharsetName.BIG5.toJavaCharset(), checkedCollationName);
        switch (checkedCollationName) {
        case BIG5_BIN:
            this.collationHandler = new Big5BinCollationHandler(this);
            break;
        case BIG5_CHINESE_CI:
            this.collationHandler = new Big5ChineseCiCollationHandler(this);
            break;
        default:
            throw new UnsupportedOperationException();
        }
    }

    @Override
    public CharsetName getName() {
        return CharsetName.BIG5;
    }

    @Override
    public int maxLenOfMultiBytes() {
        return 2;
    }

    @Override
    public int minLenOfMultiBytes() {
        return 1;
    }
}
