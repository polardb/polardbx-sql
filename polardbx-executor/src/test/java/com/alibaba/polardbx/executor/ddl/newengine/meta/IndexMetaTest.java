package com.alibaba.polardbx.executor.ddl.newengine.meta;

import com.alibaba.polardbx.executor.gsi.GsiUtils;
import com.alibaba.polardbx.gms.metadb.table.IndexStatus;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.GsiMetaManager;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import org.apache.calcite.sql.SqlAlterTable;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.mockito.Mockito.when;

public class IndexMetaTest {

    @Test
    public void testBuildIndexMetaByAddColumns() {
        TableMeta tableMeta = Mockito.mock(TableMeta.class);
        ColumnMeta columnMetaA = Mockito.mock(ColumnMeta.class);
        ColumnMeta columnMetaB = Mockito.mock(ColumnMeta.class);

        when(columnMetaA.getName()).thenReturn("a");
        when(columnMetaB.getName()).thenReturn("b");
        when(columnMetaA.isNullable()).thenReturn(true);
        when(columnMetaB.isNullable()).thenReturn(false);

        List<ColumnMeta> columnMetas = new ArrayList<>();
        columnMetas.add(columnMetaA);
        columnMetas.add(columnMetaB);

        List<String> columnNames = new ArrayList<>();
        columnNames.add("a");
        columnNames.add("b");

        when(tableMeta.getPhysicalColumns()).thenReturn(columnMetas);

        List<GsiMetaManager.IndexRecord> records =
            GsiUtils.buildIndexMetaByAddColumns(tableMeta, columnNames, "wumu", "t1", "gsi1", 1,
                IndexStatus.ABSENT);

        Assert.assertEquals(records.size(), 2);
        Assert.assertNull(records.get(0).getIndexType());
        Assert.assertEquals("", records.get(0).getIndexComment());
        Assert.assertNull(records.get(1).getIndexType());
        Assert.assertEquals("", records.get(1).getIndexComment());
    }

    @Test
    public void testBuildIndexMetaByAddColumns2() {
        List<GsiMetaManager.IndexRecord> indexRecords = new ArrayList<>();
        SqlAlterTable alterTable = Mockito.mock(SqlAlterTable.class);
        when(alterTable.getAlters()).thenReturn(new ArrayList<>());

        Map<SqlAlterTable.ColumnOpt, List<String>> map = new HashMap<>();
        List<String> columnNames = new ArrayList<>();
        columnNames.add("x");
        columnNames.add("y");
        map.put(SqlAlterTable.ColumnOpt.ADD, columnNames);
        when(alterTable.getColumnOpts()).thenReturn(map);

        GsiUtils.buildIndexMetaByAddColumns(indexRecords, alterTable, "wumu", "t1", "gsi1", 1, IndexStatus.ABSENT);

        Assert.assertEquals(indexRecords.size(), 2);
        Assert.assertNull(indexRecords.get(0).getIndexType());
        Assert.assertEquals("", indexRecords.get(0).getIndexComment());
        Assert.assertNull(indexRecords.get(1).getIndexType());
        Assert.assertEquals("", indexRecords.get(1).getIndexComment());
    }
}
