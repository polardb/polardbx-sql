package com.alibaba.polardbx.executor.operator.scan.impl;

import com.alibaba.polardbx.executor.operator.scan.CsvScanTestBase;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.Field;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import org.junit.Test;
import org.roaringbitmap.RoaringBitmap;

import java.util.Collections;
import java.util.HashMap;

import static org.junit.Assert.*;

public class FlashbackScanPreProcessorTest extends CsvScanTestBase {

    @Test
    public void testGenerateDeletionInColumnarMode() {
        RoaringBitmap actualBitmap = flashbackScanPreProcessor.generateDeletion(DATA_FILE_PATH);

        assertEquals("Bitmaps should match in columnar mode", DELETE_COUNT, actualBitmap.getCardinality());
    }

    @Test
    public void testGenerateDeletionInArchiveMode() {
        // Setting conditions to simulate archive mode: tso and allDelPositions are null
        flashbackScanPreProcessor = new FlashbackScanPreProcessor(configuration,
            fileSystem, SCHEMA_NAME, LOGICAL_TABLE_NAME, true, true,
            Collections.singletonList(new ColumnMeta("t1", "pk", "pk", new Field(DataTypes.LongType))),
            Collections.emptyList(), new HashMap<>(), 0.5, 0.5, columnarManager, null,
            Collections.singletonList(1L), null);

        RoaringBitmap actualBitmap = flashbackScanPreProcessor.generateDeletion(DATA_FILE_PATH);

        // In archive mode, the bitmap should be empty
        assertEquals("Bitmap should be empty in archive mode", 0, actualBitmap.getCardinality());
    }
}