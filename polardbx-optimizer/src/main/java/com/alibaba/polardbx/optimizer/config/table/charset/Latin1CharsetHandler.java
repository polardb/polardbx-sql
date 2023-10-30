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
import com.alibaba.polardbx.optimizer.config.table.collation.Latin1BinCollationHandler;
import com.alibaba.polardbx.optimizer.config.table.collation.Latin1German2CollationHandler;
import com.alibaba.polardbx.optimizer.config.table.collation.SimpleCollationHandler;

import static com.alibaba.polardbx.common.charset.CollationName.LATIN1_DANISH_CI;
import static com.alibaba.polardbx.common.charset.CollationName.LATIN1_GENERAL_CI;
import static com.alibaba.polardbx.common.charset.CollationName.LATIN1_GENERAL_CS;
import static com.alibaba.polardbx.common.charset.CollationName.LATIN1_GERMAN1_CI;
import static com.alibaba.polardbx.common.charset.CollationName.LATIN1_SPANISH_CI;
import static com.alibaba.polardbx.common.charset.CollationName.LATIN1_SWEDISH_CI;

public class Latin1CharsetHandler extends AbstractCharsetHandler {

    Latin1CharsetHandler(CollationName checkedCollationName) {
        super(CharsetName.LATIN1.toJavaCharset(), checkedCollationName);
        switch (checkedCollationName) {
        case LATIN1_SWEDISH_CI:
            this.collationHandler = new SimpleCollationHandler(this, LATIN1_SWEDISH_CI);
            break;
        case LATIN1_DANISH_CI:
            this.collationHandler = new SimpleCollationHandler(this, LATIN1_DANISH_CI);
            break;
        case LATIN1_GENERAL_CI:
            this.collationHandler = new SimpleCollationHandler(this, LATIN1_GENERAL_CI);
            break;
        case LATIN1_GENERAL_CS:
            this.collationHandler = new SimpleCollationHandler(this, LATIN1_GENERAL_CS);
            break;
        case LATIN1_GERMAN1_CI:
            this.collationHandler = new SimpleCollationHandler(this, LATIN1_GERMAN1_CI);
            break;
        case LATIN1_SPANISH_CI:
            this.collationHandler = new SimpleCollationHandler(this, LATIN1_SPANISH_CI);
            break;
        case LATIN1_BIN:
            this.collationHandler = new Latin1BinCollationHandler(this);
            break;
        case LATIN1_GERMAN2_CI:
            this.collationHandler = new Latin1German2CollationHandler(this);
            break;
        default:
            throw new UnsupportedOperationException();
        }
    }

    @Override
    public CharsetName getName() {
        return CharsetName.LATIN1;
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
