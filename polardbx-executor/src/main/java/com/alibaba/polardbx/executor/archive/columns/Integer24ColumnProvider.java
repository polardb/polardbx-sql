package com.alibaba.polardbx.executor.archive.columns;

import com.alibaba.polardbx.executor.chunk.BlockBuilder;
import com.alibaba.polardbx.executor.columnar.CSVRow;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;

public class Integer24ColumnProvider extends IntegerColumnProvider {

    private static final int INT_24_SIGNED_BIT = 0x00800000;
    private static final int INT_24_SIGNED_PAD = 0xFF000000;

    @Override
    public void parseRow(BlockBuilder blockBuilder, CSVRow row, int columnId, DataType dataType) {
        if (row.isNullAt(columnId)) {
            blockBuilder.appendNull();
        } else {
            byte[] bytes = row.getBytes(columnId);
            int intVal = ColumnProvider.intFromByte(bytes, bytes.length);
            if ((intVal & INT_24_SIGNED_BIT) > 0) {
                // For signed negative int_24, fill leading ones
                intVal |= INT_24_SIGNED_PAD;
            }
            blockBuilder.writeInt(intVal);
        }
    }
}
