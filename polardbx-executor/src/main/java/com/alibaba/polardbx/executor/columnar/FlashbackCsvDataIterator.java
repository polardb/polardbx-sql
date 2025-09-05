package com.alibaba.polardbx.executor.columnar;

import com.alibaba.polardbx.common.Engine;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.executor.gms.FileVersionStorage;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;

import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

public class FlashbackCsvDataIterator implements Iterator<Chunk> {
    private final AutoFlashbackCSVFileReader csvFileReader;
    private final String csvFileName;
    private final long tso;
    private Chunk nextChunk = null;
    private boolean endOfData = false;

    public FlashbackCsvDataIterator(AutoFlashbackCSVFileReader csvFileReader,
                                    String csvFileName, long tso, int maxLength, List<ColumnMeta> columnMetas,
                                    Engine engine) {
        this.csvFileReader = csvFileReader;
        this.csvFileName = csvFileName;
        this.tso = tso;
        try {
            this.csvFileReader.open(new ExecutionContext(), columnMetas, FileVersionStorage.CSV_CHUNK_LIMIT, engine,
                csvFileName, tso, maxLength);
        } catch (Throwable t) {
            throw new TddlRuntimeException(ErrorCode.ERR_LOAD_CSV_FILE, t,
                String.format("Failed to open csv file reader, file name: %s, tso: %d", csvFileName, tso));
        }
    }

    @Override
    public boolean hasNext() {
        if (endOfData) {
            return false;
        }
        if (nextChunk == null) {
            fetchNextChunk();
        }
        return nextChunk != null;
    }

    @Override
    public Chunk next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        Chunk currentChunk = nextChunk;
        fetchNextChunk(); // Pre-fetch the next chunk
        return currentChunk;
    }

    private void fetchNextChunk() {
        try {
            nextChunk = csvFileReader.next();
            if (nextChunk == null) {
                endOfData = true;
                csvFileReader.close(); // Close the reader as there's no more data to read
            }
        } catch (Throwable t) {
            endOfData = true;
            closeReader();
            throw new TddlRuntimeException(ErrorCode.ERR_LOAD_CSV_FILE, t,
                String.format("Failed during reading csv file, file name: %s, tso: %d", csvFileName, tso));
        }
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException("Remove operation is not supported by this iterator");
    }

    public void closeReader() {
        try {
            csvFileReader.close();
        } catch (Throwable t) {
            // ignored
        }
    }

    @Override
    protected void finalize() throws Throwable {
        closeReader(); // Ensure the reader is closed when the iterator is garbage-collected
        super.finalize();
    }
}
