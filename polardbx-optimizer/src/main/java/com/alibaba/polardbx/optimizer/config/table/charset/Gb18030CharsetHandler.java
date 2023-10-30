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
import com.alibaba.polardbx.optimizer.config.table.collation.Gb18030BinCollationHandler;
import com.alibaba.polardbx.optimizer.config.table.collation.Gb18030ChineseCiCollationHandler;
import com.alibaba.polardbx.optimizer.config.table.collation.Gb18030Unicode520CiCollationHandler;

public class Gb18030CharsetHandler extends AbstractCharsetHandler {
    Gb18030CharsetHandler(CollationName checkedCollationName) {
        super(CharsetName.GB18030.toJavaCharset(), checkedCollationName);
        switch (checkedCollationName) {
        case GB18030_CHINESE_CI:
            this.collationHandler = new Gb18030ChineseCiCollationHandler(this);
            break;
        case GB18030_BIN:
            this.collationHandler = new Gb18030BinCollationHandler(this);
            break;
        case GB18030_UNICODE_520_CI:
            this.collationHandler = new Gb18030Unicode520CiCollationHandler(this);
            break;
        default:
            throw new UnsupportedOperationException();
        }
    }

    @Override
    public CharsetName getName() {
        return CharsetName.GB18030;
    }

    @Override
    public int maxLenOfMultiBytes() {
        return 4;
    }

    @Override
    public int minLenOfMultiBytes() {
        return 1;
    }
}
