package com.alibaba.polardbx.executor.archive.columns;

import com.alibaba.polardbx.executor.chunk.BlockBuilder;
import com.alibaba.polardbx.executor.columnar.CSVRow;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.EnumType;

public class EnumColumnProvider extends StringColumnProvider {
    @Override
    public void parseRow(BlockBuilder blockBuilder, CSVRow row, int columnId, DataType dataType) {
        if (row.isNullAt(columnId)) {
            blockBuilder.appendNull();
            return;
        }

        byte[] bytes = row.getBytes(columnId);
        int intVal = ColumnProvider.intFromByte(bytes, bytes.length);
        blockBuilder.writeString(((EnumType) dataType).convertTo(intVal));
    }
}
