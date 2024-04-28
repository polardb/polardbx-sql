package com.alibaba.polardbx.executor.archive.columns;

import com.alibaba.polardbx.executor.chunk.BlockBuilder;
import com.alibaba.polardbx.executor.columnar.CSVRow;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;

public class YearColumnProvider extends LongColumnProvider {
    @Override
    public void parseRow(BlockBuilder blockBuilder, CSVRow row, int columnId, DataType dataType) {
        if (row.isNullAt(columnId)) {
            blockBuilder.appendNull();
        } else {
            byte[] bytes = row.getBytes(columnId);
            long longVal = ColumnProvider.longFromByte(bytes, bytes.length);
            blockBuilder.writeLong(longVal == 0 ? 0 : longVal + 1900);
        }
    }
}
