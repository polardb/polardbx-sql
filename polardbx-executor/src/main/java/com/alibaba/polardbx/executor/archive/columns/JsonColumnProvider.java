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
