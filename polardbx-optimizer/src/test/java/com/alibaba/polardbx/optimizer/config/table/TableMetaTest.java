package com.alibaba.polardbx.optimizer.config.table;

import com.alibaba.polardbx.gms.metadb.table.IndexStatus;
import com.alibaba.polardbx.gms.metadb.table.IndexVisibility;
import com.alibaba.polardbx.gms.metadb.table.LackLocalIndexStatus;
import com.alibaba.polardbx.gms.metadb.table.TableStatus;
import com.clearspring.analytics.util.Lists;
import com.google.common.collect.ImmutableList;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * @author fangwu
 */
public class TableMetaTest {
    /**
     * Tests that findGlobalSecondaryIndexByName returns null when GSI is empty.
     */
    @Test
    public void testFindGlobalSecondaryIndexByNameWithEmptyGsi() {
        TableMeta tableMeta = mock(TableMeta.class);

        when(tableMeta.findGlobalSecondaryIndexByName("indexName")).thenCallRealMethod();

        assertNull(tableMeta.findGlobalSecondaryIndexByName("indexName"));
    }

    /**
     * Tests that findGlobalSecondaryIndexByName returns null when there's no matching index.
     */
    @Test
    public void testFindGlobalSecondaryIndexByNameWithNonMatchingIndex()
        throws NoSuchFieldException, IllegalAccessException {
        TableMeta tableMeta = createMockTableMeta();

        assertNull(tableMeta.findGlobalSecondaryIndexByName("nonExistingIndex"));
    }

    /**
     * Tests that findGlobalSecondaryIndexByName returns the correct GsiIndexMetaBean when a match exists.
     */
    @Test
    public void testFindGlobalSecondaryIndexByNameWithMatchingIndex()
        throws NoSuchFieldException, IllegalAccessException {
        TableMeta tableMeta = createMockTableMeta();
        Field field = TableMeta.class.getDeclaredField("gsiPublished");
        field.setAccessible(true);
        GsiMetaManager.GsiIndexMetaBean expectedGsiMeta =
            ((Map<String, GsiMetaManager.GsiIndexMetaBean>) field.get(tableMeta)).get("MATCHING_INDEX");

        assertEquals(expectedGsiMeta, tableMeta.findGlobalSecondaryIndexByName("MATCHING_INDEX"));
    }

    /**
     * Tests that findGlobalSecondaryIndexByName with index name with space
     */
    @Test
    public void testFindGlobalSecondaryIndexByNameWithMatchingIndexNameWithSpace()
        throws NoSuchFieldException, IllegalAccessException {
        TableMeta tableMeta = createMockTableMeta();
        Field field = TableMeta.class.getDeclaredField("gsiPublished");
        field.setAccessible(true);
        GsiMetaManager.GsiIndexMetaBean expectedGsiMeta =
            ((Map<String, GsiMetaManager.GsiIndexMetaBean>) field.get(tableMeta)).get("MATCHING INDEX ");

        assertEquals(expectedGsiMeta, tableMeta.findGlobalSecondaryIndexByName("MATCHING INDEX "));
        assertNotEquals(expectedGsiMeta, tableMeta.findGlobalSecondaryIndexByName("MATCHING INDEX"));
    }

    /**
     * Tests that findGlobalSecondaryIndexByName with index name of cci
     */
    @Test
    public void testFindCCIByName()
        throws NoSuchFieldException, IllegalAccessException {
        TableMeta tableMeta = createMockTableMeta();
        Field field = TableMeta.class.getDeclaredField("columnarIndexPublished");
        field.setAccessible(true);
        GsiMetaManager.GsiIndexMetaBean cciMeta =
            ((Map<String, GsiMetaManager.GsiIndexMetaBean>) field.get(tableMeta)).get("CCI");
        assertEquals(cciMeta, tableMeta.findGlobalSecondaryIndexByName("CCI"));
        assertNotEquals(cciMeta, tableMeta.findGlobalSecondaryIndexByName("MATCHING_INDEX"));
    }

    /**
     * Input is an empty string, expecting null as a result.
     */
    @Test
    public void testFindLocalIndexByNameWithEmptyString() {
        TableMeta tableMeta = mock(TableMeta.class);
        List<IndexMeta> allIndexes = new ArrayList<>();

        when(tableMeta.getAllIndexes()).thenReturn(allIndexes);

        assertNull(tableMeta.findLocalIndexByName(""));
    }

    /**
     * Input is the name of a primary key index, expecting the primary key index.
     */
    @Test
    public void testFindLocalIndexByNameWithPrimaryKeyIndexName() {
        TableMeta tableMeta = mock(TableMeta.class);
        IndexMeta primaryKeyIndex = mock(IndexMeta.class);

        when(primaryKeyIndex.isPrimaryKeyIndex()).thenReturn(true);
        when(tableMeta.getPrimaryIndex()).thenReturn(primaryKeyIndex);
        when(tableMeta.getAllIndexes()).thenReturn(ImmutableList.of(primaryKeyIndex));
        when(tableMeta.findLocalIndexByName("PRIMARY")).thenCallRealMethod();

        assertEquals(primaryKeyIndex, tableMeta.findLocalIndexByName("PRIMARY"));
    }

    /**
     * Input matches a non-primary key index, expecting that index.
     */
    @Test
    public void testFindLocalIndexByNameWithMatchingNonPrimaryKeyIndex() {
        TableMeta tableMeta = mock(TableMeta.class);
        List<IndexMeta> allIndexes = new ArrayList<>();
        IndexMeta nonPrimaryKeyIndex = mock(IndexMeta.class);
        allIndexes.add(nonPrimaryKeyIndex);

        when(nonPrimaryKeyIndex.isPrimaryKeyIndex()).thenReturn(false);
        when(nonPrimaryKeyIndex.getPhysicalIndexName()).thenReturn("matching_index");
        when(tableMeta.getAllIndexes()).thenReturn(allIndexes);
        when(tableMeta.findLocalIndexByName("matching_index")).thenCallRealMethod();

        assertEquals(nonPrimaryKeyIndex, tableMeta.findLocalIndexByName("matching_index"));
    }

    /**
     * Input is a non-existing index name, expecting null as a result.
     */
    @Test
    public void testFindLocalIndexByNameWithNonExistingIndexName() {
        TableMeta tableMeta = mock(TableMeta.class);
        List<IndexMeta> allIndexes = new ArrayList<>();

        when(tableMeta.getAllIndexes()).thenReturn(allIndexes);
        when(tableMeta.findLocalIndexByName("non_existent")).thenCallRealMethod();

        assertNull(tableMeta.findLocalIndexByName("non_existent"));
    }

    /**
     * Helper method to create a mocked TableMeta instance with sample GSI data.
     *
     * @return Mocked TableMeta instance
     */
    private TableMeta createMockTableMeta() throws NoSuchFieldException, IllegalAccessException {
        Map<String, GsiMetaManager.GsiIndexMetaBean> gsiPublished = new HashMap<>();
        Map<String, GsiMetaManager.GsiIndexMetaBean> columnarIndexPublished = new HashMap<>();
        GsiMetaManager.GsiIndexMetaBean gsiMeta1 =
            new GsiMetaManager.GsiIndexMetaBean(
                "testIndex",
                "testSchema",
                "testTable",
                false,
                "index_schema",
                "matching_Index",
                Lists.newArrayList(),
                Lists.newArrayList(),
                "indexType",
                "",
                "",
                null,
                "indexTableName",
                IndexStatus.PUBLIC,
                0,
                false,
                false,
                 IndexVisibility.VISIBLE, LackLocalIndexStatus.NO_LACKIING);
        gsiPublished.put("MATCHING_INDEX", gsiMeta1);

        GsiMetaManager.GsiIndexMetaBean cciMeta =
            new GsiMetaManager.GsiIndexMetaBean(
                "testIndex",
                "testSchema",
                "testTable",
                false,
                "index_schema",
                "cci",
                Lists.newArrayList(),
                Lists.newArrayList(),
                "indexType",
                "",
                "",
                null,
                "indexTableName",
                IndexStatus.PUBLIC,
                0,
                true,
                true,
                 IndexVisibility.VISIBLE, LackLocalIndexStatus.NO_LACKIING);
        columnarIndexPublished.put("CCI", cciMeta);

        GsiMetaManager.GsiIndexMetaBean gsiMeta2 = new GsiMetaManager.GsiIndexMetaBean(
            "testIndex",
            "testSchema",
            "testTable",
            false,
            "index_schema",
            "non_Matching_Index",
            Lists.newArrayList(),
            Lists.newArrayList(),
            "indexType",
            "",
            "",
            null,
            "indexTableName",
            IndexStatus.PUBLIC,
            0,
            false,
            false,
             IndexVisibility.VISIBLE, LackLocalIndexStatus.NO_LACKIING);
        gsiPublished.put("NON_MATCHING_INDEX", gsiMeta2);

        GsiMetaManager.GsiIndexMetaBean gsiMeta3 =
            new GsiMetaManager.GsiIndexMetaBean(
                "testIndex",
                "testSchema",
                "testTable",
                false,
                "index_schema",
                "matching Index ",
                Lists.newArrayList(),
                Lists.newArrayList(),
                "indexType",
                "",
                "",
                null,
                "indexTableName",
                IndexStatus.PUBLIC,
                0,
                false,
                false,
                 IndexVisibility.VISIBLE, LackLocalIndexStatus.NO_LACKIING);
        gsiPublished.put("MATCHING INDEX ", gsiMeta3);

        TableMeta tableMeta = new TableMeta(
            "testSchema",
            "testTable",
            Lists.newArrayList(),
            null,
            Lists.newArrayList(),
            true,
            TableStatus.PUBLIC,
            0L,
            0);

        Field field = TableMeta.class.getDeclaredField("gsiPublished");
        field.setAccessible(true);
        field.set(tableMeta, gsiPublished);

        field = TableMeta.class.getDeclaredField("columnarIndexPublished");
        field.setAccessible(true);
        field.set(tableMeta, columnarIndexPublished);

        return tableMeta;
    }
}
