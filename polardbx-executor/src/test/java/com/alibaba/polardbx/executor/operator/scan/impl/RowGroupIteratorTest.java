package com.alibaba.polardbx.executor.operator.scan.impl;

import com.alibaba.polardbx.executor.operator.scan.ColumnReader;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DecimalType;
import org.apache.orc.OrcProto;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

public class RowGroupIteratorTest {

    @Test
    public void testDecimalReader() {
        DataType dec64Type = new DecimalType(16, 4);
        ExecutionContext context = new ExecutionContext();
        context.getParamManager().getProps().put("ENABLE_COLUMNAR_DECIMAL64", "true");
        OrcProto.ColumnEncoding directEncoding = mockDirectEncoding();

        ColumnReader decimal64Reader = RowGroupIteratorImpl.getDecimalReader(true, context, dec64Type, 0, false, null,
            null, null, 1000, directEncoding, false);
        Assert.assertTrue(decimal64Reader instanceof LongColumnReader);

        ColumnReader decimalReader = RowGroupIteratorImpl.getDecimalReader(false, context, dec64Type, 0, false, null,
            null, null, 1000, directEncoding, false);
        Assert.assertTrue(decimalReader instanceof DecimalColumnReader);

        context.getParamManager().getProps().put("ENABLE_COLUMNAR_DECIMAL64", "false");
        ColumnReader decimal64ToDecReader =
            RowGroupIteratorImpl.getDecimalReader(true, context, dec64Type, 0, false, null,
                null, null, 1000, directEncoding, false);
        Assert.assertTrue(decimal64ToDecReader instanceof Decimal64ToDecimalColumnReader);

        DataType decType = new DecimalType(19, 4);
        ColumnReader decimalReader2 = RowGroupIteratorImpl.getDecimalReader(true, context, decType, 0, false, null,
            null, null, 1000, directEncoding, false);
        Assert.assertTrue(decimalReader2 instanceof DecimalColumnReader);

        OrcProto.ColumnEncoding dictEncoding = mockDictEncoding();
        ColumnReader dictDecimalReader = RowGroupIteratorImpl.getDecimalReader(true, context, decType, 0, false, null,
            null, null, 1000, dictEncoding, false);
        Assert.assertTrue(dictDecimalReader instanceof DictionaryDecimalColumnReader);
    }

    private OrcProto.ColumnEncoding mockDictEncoding() {
        OrcProto.ColumnEncoding encoding = Mockito.mock(OrcProto.ColumnEncoding.class);
        Mockito.when(encoding.getKind()).thenReturn(OrcProto.ColumnEncoding.Kind.DICTIONARY);
        return encoding;
    }

    private OrcProto.ColumnEncoding mockDirectEncoding() {
        OrcProto.ColumnEncoding encoding = Mockito.mock(OrcProto.ColumnEncoding.class);
        Mockito.when(encoding.getKind()).thenReturn(OrcProto.ColumnEncoding.Kind.DIRECT);
        return encoding;
    }
}
