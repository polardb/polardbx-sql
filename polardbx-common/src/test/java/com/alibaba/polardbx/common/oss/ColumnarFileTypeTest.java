package com.alibaba.polardbx.common.oss;

import org.junit.Assert;
import org.junit.Test;

public class ColumnarFileTypeTest {
    @Test
    public void test() {
        ColumnarFileType result;
        result =  ColumnarFileType.of("ORC");
        Assert.assertEquals(ColumnarFileType.ORC, result);
        Assert.assertFalse(result.isDeltaFile());

        result =  ColumnarFileType.of("CSV");
        Assert.assertEquals(ColumnarFileType.CSV, result);
        Assert.assertTrue(result.isDeltaFile());

        result =  ColumnarFileType.of("DEL");
        Assert.assertEquals(ColumnarFileType.DEL, result);
        Assert.assertTrue(result.isDeltaFile());

        result =  ColumnarFileType.of("SET");
        Assert.assertEquals(ColumnarFileType.SET, result);
        result =  ColumnarFileType.of("PK_IDX_LOG");
        Assert.assertEquals(ColumnarFileType.PK_IDX_LOG, result);
        result =  ColumnarFileType.of("PK_IDX_LOG_META");
        Assert.assertEquals(ColumnarFileType.PK_IDX_LOG_META, result);
        result =  ColumnarFileType.of("PK_IDX_SNAPSHOT");
        Assert.assertEquals(ColumnarFileType.PK_IDX_SNAPSHOT, result);
        result =  ColumnarFileType.of("PK_IDX_LOCK");
        Assert.assertEquals(ColumnarFileType.PK_IDX_LOCK, result);
        result =  ColumnarFileType.of("PK_IDX_SST");
        Assert.assertEquals(ColumnarFileType.PK_IDX_SST, result);
        result =  ColumnarFileType.of("PK_IDX_BF");
        Assert.assertEquals(ColumnarFileType.PK_IDX_BF, result);

        result =  ColumnarFileType.of("orc");
        Assert.assertEquals(ColumnarFileType.ORC, result);
        result =  ColumnarFileType.of("csv");
        Assert.assertEquals(ColumnarFileType.CSV, result);
        result =  ColumnarFileType.of("del");
        Assert.assertEquals(ColumnarFileType.DEL, result);
        result =  ColumnarFileType.of("set");
        Assert.assertEquals(ColumnarFileType.SET, result);
        result =  ColumnarFileType.of("pk_idx_log");
        Assert.assertEquals(ColumnarFileType.PK_IDX_LOG, result);
        result =  ColumnarFileType.of("pk_idx_log_meta");
        Assert.assertEquals(ColumnarFileType.PK_IDX_LOG_META, result);
        result =  ColumnarFileType.of("pk_idx_snapshot");
        Assert.assertEquals(ColumnarFileType.PK_IDX_SNAPSHOT, result);
        result =  ColumnarFileType.of("pk_idx_lock");
        Assert.assertEquals(ColumnarFileType.PK_IDX_LOCK, result);
        result =  ColumnarFileType.of("pk_idx_sst");
        Assert.assertEquals(ColumnarFileType.PK_IDX_SST, result);
        result =  ColumnarFileType.of("pk_idx_lock");
        Assert.assertEquals(ColumnarFileType.PK_IDX_LOCK, result);
    }
}