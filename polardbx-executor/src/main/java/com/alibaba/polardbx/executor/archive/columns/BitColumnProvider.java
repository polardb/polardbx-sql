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

import com.alibaba.polardbx.common.CrcAccumulator;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.executor.Xprotocol.XRowSet;
import com.alibaba.polardbx.executor.chunk.BlockBuilder;
import com.alibaba.polardbx.executor.columnar.CSVRow;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.row.Row;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;

import java.nio.ByteBuffer;
import java.time.ZoneId;
import java.util.Optional;

public class BitColumnProvider extends IntegerColumnProvider {

    @Override
    public void putRow(ColumnVector columnVector, int rowNumber, Row row, int columnId, DataType dataType,
                       ZoneId timezone, Optional<CrcAccumulator> accumulator) {
        if (row instanceof XRowSet) {
            try {
                ((XRowSet) row).fastParseToColumnVector(columnId, ColumnProviders.UTF_8, columnVector, rowNumber,
                    accumulator);
            } catch (Exception e) {
                throw GeneralUtil.nestedException(e);
            }
        } else {
            byte[] bytes = row.getBytes(columnId);
            if (bytes == null) {
                columnVector.isNull[rowNumber] = true;
                columnVector.noNulls = false;
                ((LongColumnVector) columnVector).vector[rowNumber] = 0;
                accumulator.ifPresent(CrcAccumulator::appendNull);
            } else {
                ((LongColumnVector) columnVector).vector[rowNumber] = bytes[0];

                accumulator.ifPresent(a -> a.appendHash(Byte.hashCode(bytes[0])));
            }
        }
    }

    @Override
    public void parseRow(BlockBuilder blockBuilder, CSVRow row, int columnId, DataType dataType) {
        if (row.isNullAt(columnId)) {
            blockBuilder.appendNull();
        } else {
            ByteBuffer bytes = row.getBytes(columnId);
            int intVal = ColumnProvider.intFromByte(bytes);
            blockBuilder.writeInt(intVal);
        }
    }
}
