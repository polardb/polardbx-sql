package com.alibaba.polardbx.optimizer.core.planner;

import com.alibaba.polardbx.common.exception.TddlNestableRuntimeException;
import com.alibaba.polardbx.gms.metadb.table.TablesExtAccessor;
import com.alibaba.polardbx.gms.metadb.table.TablesExtRecord;
import com.alibaba.polardbx.gms.partition.TablePartitionAccessor;
import com.alibaba.polardbx.gms.partition.TablePartitionRecord;
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.optimizer.config.table.GsiMetaManager;
import com.alibaba.polardbx.optimizer.core.planner.rule.util.CBOUtil;
import org.junit.Assert;
import org.junit.Before;
import org.junit.After;
import org.junit.Test;
import org.mockito.MockedStatic;

import java.sql.Connection;
import java.util.Collections;
import java.util.List;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

public class IsCciTest {

    private MockedStatic<MetaDbUtil> metaDbUtilMockedStatic;

    private TablePartitionAccessor tablePartitionAccessorMock;

    private TablesExtAccessor tablesExtAccessorMock;

    @Before
    public void setUp() {
        metaDbUtilMockedStatic = mockStatic(MetaDbUtil.class);
        tablePartitionAccessorMock = mock(TablePartitionAccessor.class);
        tablesExtAccessorMock = mock(TablesExtAccessor.class);
    }

    @After
    public void tearDown() {
        metaDbUtilMockedStatic.close();
    }

    @Test
    public void testIsCciWithNewPartitionDb() throws Exception {
        try (MockedStatic<DbInfoManager> dbInfoManagerMockedStatic = mockStatic(DbInfoManager.class)) {
            DbInfoManager dbInfoManagerMock = mock(DbInfoManager.class);
            dbInfoManagerMockedStatic.when(DbInfoManager::getInstance).thenReturn(dbInfoManagerMock);
            when(dbInfoManagerMock.isNewPartitionDb(anyString())).thenReturn(true);

            Connection connectionMock = mock(Connection.class);
            metaDbUtilMockedStatic.when(MetaDbUtil::getConnection).thenReturn(connectionMock);

            List<TablePartitionRecord> tablePartitionRecordsMock =
                Collections.singletonList(new TablePartitionRecord());
            when(tablePartitionAccessorMock.getTablePartitionsByDbNameTbName(anyString(), anyString(), eq(false)))
                .thenReturn(tablePartitionRecordsMock);
            tablePartitionRecordsMock.get(0).tblType = 7;

            boolean result =
                CBOUtil.isCci("schemaName", "tableName", tablePartitionAccessorMock, tablesExtAccessorMock);

            Assert.assertTrue(result);
        }
    }

    @Test
    public void testIsCciWithOldPartitionDb() throws Exception {
        try (MockedStatic<DbInfoManager> dbInfoManagerMockedStatic = mockStatic(DbInfoManager.class)) {
            DbInfoManager dbInfoManagerMock = mock(DbInfoManager.class);
            dbInfoManagerMockedStatic.when(DbInfoManager::getInstance).thenReturn(dbInfoManagerMock);
            when(dbInfoManagerMock.isNewPartitionDb(anyString())).thenReturn(false);

            Connection connectionMock = mock(Connection.class);
            metaDbUtilMockedStatic.when(MetaDbUtil::getConnection).thenReturn(connectionMock);

            TablesExtRecord tablesExtRecordsMock = new TablesExtRecord();
            when(tablesExtAccessorMock.query(anyString(), anyString(), eq(false)))
                .thenReturn(tablesExtRecordsMock);
            tablesExtRecordsMock.tableType = GsiMetaManager.TableType.COLUMNAR.getValue();

            boolean result =
                CBOUtil.isCci("schemaName", "tableName", tablePartitionAccessorMock, tablesExtAccessorMock);

            Assert.assertTrue(result);
        }
    }

    @Test
    public void testIsCciWhenExceptionThrown() {
        metaDbUtilMockedStatic.when(() -> MetaDbUtil.getConnection())
            .thenThrow(new TddlNestableRuntimeException("Failed to get connection"));
        try {
            CBOUtil.isCci("schemaName", "tableName", tablePartitionAccessorMock, tablesExtAccessorMock);
            Assert.fail("Expected TddlNestableRuntimeException to be thrown");
        } catch (TddlNestableRuntimeException e) {
            Assert.assertEquals("check table is cci failed!", e.getMessage());
        } catch (Exception e) {
            Assert.fail("Unexpected exception type: " + e.getClass().getSimpleName());
        }
    }

    @Test
    public void testIsCciWithBlankSchemaName() {
        Assert.assertFalse(CBOUtil.isCci("", "tableName", tablePartitionAccessorMock, tablesExtAccessorMock));
    }

    @Test
    public void testIsCciWithBlankTableName() {
        Assert.assertFalse(CBOUtil.isCci("schemaName", "", tablePartitionAccessorMock, tablesExtAccessorMock));
    }

    @Test
    public void testIsCciWithBothBlank() {
        Assert.assertFalse(CBOUtil.isCci("", "", tablePartitionAccessorMock, tablesExtAccessorMock));
    }
}
