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

package com.alibaba.polardbx.executor.archive.columns;

import com.alibaba.polardbx.common.utils.binlog.JsonConversion;
import com.alibaba.polardbx.common.utils.binlog.LogBuffer;
import com.alibaba.polardbx.executor.chunk.BlockBuilder;
import com.alibaba.polardbx.executor.columnar.CSVRow;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;

import java.nio.charset.Charset;

public class JsonColumnProvider extends StringColumnProvider {
    @Override
    public void parseRow(BlockBuilder blockBuilder, CSVRow row, int columnId, DataType dataType) {
        if (row.isNullAt(columnId)) {
            blockBuilder.appendNull();
            return;
        }

        byte[] bytes = row.getBytes(columnId);
        String charsetName = dataType.getCharsetName().getJavaCharset();

        blockBuilder.writeString(ColumnProvider.convertToString(bytes, charsetName));
    }
}
