package com.alibaba.polardbx.executor.columnar;

import com.alibaba.polardbx.common.Engine;
import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

/**
 * Read .csv in given storage engine into memory as list of chunks.
 */
public interface CSVFileReader extends Closeable {
    /**
     * It means read fully when length = EOF (end of file)
     */
    int EOF = -1;

    /**
     * Open the .csv file resource.
     *
     * @param context execution context.
     * @param columnMetas the column meta of .csv file, including the implicit columns.
     * @param chunkLimit maximum chunk size to fetch at a time.
     * @param engine storage engine of .csv file
     * @param csvFileName csv file name (without uri prefix like oss://dir/)
     * @param offset file offset to read from
     * @param length file length to read
     * @throws IOException throw exception when IO failure
     */
    void open(ExecutionContext context,
              List<ColumnMeta> columnMetas,
              int chunkLimit,
              Engine engine,
              String csvFileName,
              int offset,
              int length) throws IOException;

    /**
     * Fetch the next chunk under the size limit.
     *
     * @return executor chunk
     */
    Chunk next();

    Chunk nextUntilPosition(long pos);

}
