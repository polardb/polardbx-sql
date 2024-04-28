package com.alibaba.polardbx.executor.columnar;

import com.alibaba.polardbx.common.Engine;
import lombok.Data;
import org.roaringbitmap.RoaringBitmap;

import java.io.Closeable;
import java.io.IOException;

/**
 * Read and parse .del file in given storage engine into memory as bitmaps.
 */
public interface DeletionFileReader extends Closeable {
    /**
     * It means read fully when length = EOF (end of file)
     */
    int EOF = -1;

    /**
     * The format of deletion entry unit:
     * 1. generated timestamp
     * 2. file identifier of .csv or .orc
     * 3. memory structure of deletion masks in format of bitmap.
     */
    @Data
    class DeletionEntry {
        private final long tso;
        private final int fileId;
        private final RoaringBitmap bitmap;

        public DeletionEntry(long tso, int fileId, RoaringBitmap bitmap) {
            this.tso = tso;
            this.fileId = fileId;
            this.bitmap = bitmap;
        }
    }

    /**
     * Open the .del file resource.
     *
     * @param engine storage engine of .del file.
     * @param delFileName file name of .del without uri prefix like 'oss://dir/'
     * @param offset file offset to read from.
     * @param length file length to read.
     * @throws IOException throw exception when IO failure
     */
    void open(Engine engine, String delFileName, int offset, int length) throws IOException;

    /**
     * Fetch and parse the next deletion entry unit.
     */
    DeletionEntry next();

    int position();
}
