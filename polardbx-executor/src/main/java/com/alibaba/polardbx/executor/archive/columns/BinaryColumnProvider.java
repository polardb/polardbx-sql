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
import com.alibaba.polardbx.common.orc.OrcBloomFilter;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.executor.Xprotocol.XRowSet;
import com.alibaba.polardbx.executor.chunk.BlockBuilder;
import com.alibaba.polardbx.executor.columnar.CSVRow;
import com.alibaba.polardbx.optimizer.core.datatype.BinaryType;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.field.SessionProperties;
import com.alibaba.polardbx.optimizer.core.row.Row;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.orc.TypeDescription;

import java.nio.ByteBuffer;
import java.time.ZoneId;
import java.util.Optional;

public class BinaryColumnProvider implements ColumnProvider<String> {
    @Override
    public TypeDescription orcType() {
        return TypeDescription.createVarchar();
    }

    @Override
    public void transform(ColumnVector vector, BlockBuilder blockBuilder, int startIndex, int endIndex,
                          SessionProperties sessionProperties) {
        BytesColumnVector bytesColumnVector = (BytesColumnVector) vector;
        for (int i = startIndex; i < endIndex; i++) {
            int idx = i;
            if (vector.isRepeating) {
                idx = 0;
            }
            if (vector.isNull[idx]) {
                blockBuilder.appendNull();
            } else {
                blockBuilder.writeByteArray(
                    bytesColumnVector.vector[idx],
                    bytesColumnVector.start[idx],
                    bytesColumnVector.length[idx]
                );
            }
        }
    }

    @Override
    public void transform(ColumnVector vector, BlockBuilder blockBuilder, int[] selection, int selSize,
                          SessionProperties sessionProperties) {
        BytesColumnVector bytesColumnVector = (BytesColumnVector) vector;
        for (int i = 0; i < selSize; i++) {
            int j = selection[i];
            int idx = j;
            if (vector.isRepeating) {
                idx = 0;
            }
            if (vector.isNull[idx]) {
                blockBuilder.appendNull();
            } else {
                blockBuilder.writeByteArray(
                    bytesColumnVector.vector[idx],
                    bytesColumnVector.start[idx],
                    bytesColumnVector.length[idx]
                );
            }
        }
    }

    @Override
    public void putBloomFilter(ColumnVector vector, OrcBloomFilter bf, int startIndex, int endIndex) {
        BytesColumnVector vec = (BytesColumnVector) vector;
        for (int i = startIndex; i < endIndex; i++) {
            int idx = i;
            if (vector.isRepeating) {
                idx = 0;
            }
            if (vector.isNull[idx]) {
                bf.add(null);
            } else {
                bf.addBytes(vec.vector[idx], vec.start[idx], vec.length[idx]);
            }
        }
    }

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
                ((BytesColumnVector) columnVector).setRef(rowNumber, ColumnProviders.EMPTY_BYTES, 0, 0);

                accumulator.ifPresent(CrcAccumulator::appendNull);
            } else {
                ((BytesColumnVector) columnVector).setVal(rowNumber, bytes);

                accumulator.ifPresent(a -> a.appendBytes(bytes, 0, bytes.length));
            }
        }
    }

    @Override
    public void parseRow(BlockBuilder blockBuilder, CSVRow row, int columnId, DataType dataType) {
        if (row.isNullAt(columnId)) {
            blockBuilder.appendNull();
            return;
        }

        ByteBuffer bytes = row.getBytes(columnId);
        BinaryType binaryType = (BinaryType) dataType;

        if (binaryType.isFixedLength() && bytes.remaining() != binaryType.length()) {
            // TODO(siyun): avoid array copy once
            byte[] paddingBytes = ColumnProvider.convertToPaddingBytes(bytes, binaryType);
            blockBuilder.writeByteArray(paddingBytes);
        } else {
            blockBuilder.writeByteArray(bytes.array(), bytes.position(), bytes.remaining());
        }
    }
}
