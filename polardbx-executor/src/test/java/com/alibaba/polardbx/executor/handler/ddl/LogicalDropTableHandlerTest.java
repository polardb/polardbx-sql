package com.alibaba.polardbx.executor.handler.ddl;

import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.properties.ParamManager;
import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.executor.utils.DdlUtils;
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.optimizer.archive.CheckOSSArchiveUtil;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalDropTable;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.gsi.TruncateTableWithGsiPreparedData;
import com.alibaba.polardbx.optimizer.ttl.TtlUtil;
import org.junit.Test;
import org.mockito.MockedStatic;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

public class LogicalDropTableHandlerTest {

    @Test
    public void testDropCrossDbWithRecycleBin() {
        String schema1 = "test_schema1";
        String schema2 = "test_schema2";
        String table = "test_table";
        LogicalDropTableHandler handler = new LogicalDropTableHandler(null);

        ExecutionContext ec = new ExecutionContext(schema2);
        LogicalDropTable logicalDropTable = mock(LogicalDropTable.class);
        when(logicalDropTable.getSchemaName()).thenReturn(schema1);
        when(logicalDropTable.getTableName()).thenReturn(table);
        ParamManager pm = mock(ParamManager.class);
        ec.setParamManager(pm);
        when(pm.getBoolean(eq(ConnectionParams.ENABLE_RECYCLEBIN))).thenReturn(true);
        when(pm.getBoolean(eq(ConnectionParams.PURGE_FILE_STORAGE_TABLE))).thenReturn(false);

        try (MockedStatic<TtlUtil> mockedStaticTtlUtil = mockStatic(TtlUtil.class)) {
            mockedStaticTtlUtil.when(
                () -> TtlUtil.checkIfDropArcTblViewOfTtlTableWithCci(eq(schema1), eq(table), eq(ec))).thenReturn(false);
            mockedStaticTtlUtil.when(() -> TtlUtil.checkIfAllowedDropTableOperation(eq(schema1), eq(table), eq(ec)))
                .thenReturn(true);

            handler.buildDdlJob(logicalDropTable, ec);

            Assert.fail();
        } catch (Throwable t) {
            t.printStackTrace();
            Assert.assertTrue(t.getMessage().contains(
                "ERR-CODE: [TDDL-4630][ERR_RECYCLEBIN_EXECUTE] drop table across db is not supported in recycle bin;"
                    + "use the same db or use hint /*TDDL:ENABLE_RECYCLEBIN=false*/ "
                    + "to disable recycle bin"));
        }
    }

    @Test
    public void testDropGSITableWithRecycleBin() {
        String schema = "test_schema";
        String table = "test_table";
        LogicalDropTableHandler handler = new LogicalDropTableHandler(null);

        ExecutionContext ec = new ExecutionContext(schema);
        LogicalDropTable logicalDropTable = mock(LogicalDropTable.class);
        when(logicalDropTable.getSchemaName()).thenReturn(schema);
        when(logicalDropTable.getTableName()).thenReturn(table);
        ParamManager pm = mock(ParamManager.class);
        ec.setParamManager(pm);
        when(pm.getBoolean(eq(ConnectionParams.ENABLE_RECYCLEBIN))).thenReturn(true);

        DbInfoManager dbInfoManager = mock(DbInfoManager.class);
        TruncateTableWithGsiPreparedData data = mock(TruncateTableWithGsiPreparedData.class);
        when(data.getSchemaName()).thenReturn(schema);
        when(logicalDropTable.isPurge()).thenReturn(false);

        // shard table & gsi

        try (MockedStatic<TtlUtil> mockedStaticTtlUtil = mockStatic(TtlUtil.class);
            MockedStatic<DdlUtils> mockedStaticDdlUtils = mockStatic(DdlUtils.class);) {
            mockedStaticTtlUtil.when(
                () -> TtlUtil.checkIfDropArcTblViewOfTtlTableWithCci(eq(schema), eq(table), eq(ec))).thenReturn(false);
            mockedStaticTtlUtil.when(() -> TtlUtil.checkIfAllowedDropTableOperation(eq(schema), eq(table), eq(ec)))
                .thenReturn(true);

            when(dbInfoManager.isNewPartitionDb(eq(schema))).thenReturn(false);
            when(logicalDropTable.isWithGsi()).thenReturn(true);

            handler.buildDdlJob(logicalDropTable, ec);
            Assert.fail();
        } catch (Throwable t) {
            t.printStackTrace();
            Assert.assertTrue(t.getMessage().contains(
                "ERR-CODE: [TDDL-4630][ERR_RECYCLEBIN_EXECUTE] "
                    + "drop table with gsi is not supported in recycle bin;"
                    + "use hint /*TDDL:ENABLE_RECYCLEBIN=false*/ "
                    + "to disable recycle bin"));
        }

        // shard table and non-gsi and recycle bin check fail to support
        try (MockedStatic<DbInfoManager> mockedStaticDbInfoManager = mockStatic(DbInfoManager.class);
            MockedStatic<LogicalCommonDdlHandler> mockedStaticLogicalCommonDdlHandler = mockStatic(
                LogicalCommonDdlHandler.class);
            MockedStatic<TtlUtil> mockedStaticTtlUtil = mockStatic(TtlUtil.class);
            MockedStatic<DdlUtils> mockedStaticDdlUtils = mockStatic(DdlUtils.class);) {
            mockedStaticDbInfoManager.when(DbInfoManager::getInstance).thenReturn(dbInfoManager);
            mockedStaticLogicalCommonDdlHandler.when(
                () -> LogicalCommonDdlHandler.isAvailableForRecycleBin(eq(table), eq(ec))).thenReturn(false);
            mockedStaticTtlUtil.when(
                () -> TtlUtil.checkIfDropArcTblViewOfTtlTableWithCci(eq(schema), eq(table), eq(ec))).thenReturn(false);
            mockedStaticTtlUtil.when(() -> TtlUtil.checkIfAllowedDropTableOperation(eq(schema), eq(table), eq(ec)))
                .thenReturn(true);

            when(dbInfoManager.isNewPartitionDb(eq(schema))).thenReturn(false);
            when(logicalDropTable.isWithGsi()).thenReturn(true);

            handler.buildDdlJob(logicalDropTable, ec);
            Assert.fail();
        } catch (Throwable t) {
            System.out.println(t.getMessage());
            Assert.assertTrue(t.getMessage().contains(
                "ERR-CODE: [TDDL-4630][ERR_RECYCLEBIN_EXECUTE] drop table with gsi is not supported in recycle bin;"
                    + "use hint /*TDDL:ENABLE_RECYCLEBIN=false*/ "
                    + "to disable recycle bin "));
        }

        // auto table and gsi
        try (MockedStatic<DbInfoManager> mockedStaticDbInfoManager = mockStatic(DbInfoManager.class);
            MockedStatic<LogicalCommonDdlHandler> mockedStaticLogicalCommonDdlHandler = mockStatic(
                LogicalCommonDdlHandler.class);
            MockedStatic<CheckOSSArchiveUtil> mockedStaticCheckOSSArchiveUtil = mockStatic(CheckOSSArchiveUtil.class);
            MockedStatic<TtlUtil> mockedStaticTtlUtil = mockStatic(TtlUtil.class);
            MockedStatic<DdlUtils> mockedStaticDdlUtils = mockStatic(DdlUtils.class);) {
            mockedStaticDbInfoManager.when(DbInfoManager::getInstance).thenReturn(dbInfoManager);
            mockedStaticLogicalCommonDdlHandler.when(
                () -> LogicalCommonDdlHandler.isAvailableForRecycleBin(eq(table), eq(ec))).thenReturn(false);
            mockedStaticTtlUtil.when(
                () -> TtlUtil.checkIfDropArcTblViewOfTtlTableWithCci(eq(schema), eq(table), eq(ec))).thenReturn(false);
            mockedStaticTtlUtil.when(() -> TtlUtil.checkIfAllowedDropTableOperation(eq(schema), eq(table), eq(ec)))
                .thenReturn(true);

            when(dbInfoManager.isNewPartitionDb(eq(schema))).thenReturn(true);
            when(logicalDropTable.isWithGsi()).thenReturn(true);

            handler.buildDdlJob(logicalDropTable, ec);
            Assert.fail();
        } catch (Throwable t) {
            t.printStackTrace();
            Assert.assertTrue(t.getMessage().contains(
                "ERR-CODE: [TDDL-4630][ERR_RECYCLEBIN_EXECUTE] drop table with gsi is not supported in recycle bin;"
                    + "use hint /*TDDL:ENABLE_RECYCLEBIN=false*/ "
                    + "to disable recycle bin "));
        }

        // auto table and non-gsi and recycle bin check fail to support
        try (MockedStatic<DbInfoManager> mockedStaticDbInfoManager = mockStatic(DbInfoManager.class);
            MockedStatic<LogicalCommonDdlHandler> mockedStaticLogicalCommonDdlHandler = mockStatic(
                LogicalCommonDdlHandler.class);
            MockedStatic<CheckOSSArchiveUtil> mockedStaticCheckOSSArchiveUtil = mockStatic(CheckOSSArchiveUtil.class);
            MockedStatic<TtlUtil> mockedStaticTtlUtil = mockStatic(TtlUtil.class);
            MockedStatic<DdlUtils> mockedStaticDdlUtils = mockStatic(DdlUtils.class);) {
            mockedStaticDbInfoManager.when(DbInfoManager::getInstance).thenReturn(dbInfoManager);
            mockedStaticLogicalCommonDdlHandler.when(
                () -> LogicalCommonDdlHandler.isAvailableForRecycleBin(eq(table), eq(ec))).thenReturn(false);
            mockedStaticTtlUtil.when(
                () -> TtlUtil.checkIfDropArcTblViewOfTtlTableWithCci(eq(schema), eq(table), eq(ec))).thenReturn(false);
            mockedStaticTtlUtil.when(() -> TtlUtil.checkIfAllowedDropTableOperation(eq(schema), eq(table), eq(ec)))
                .thenReturn(true);

            when(dbInfoManager.isNewPartitionDb(eq(schema))).thenReturn(true);
            when(logicalDropTable.isWithGsi()).thenReturn(true);

            handler.buildDdlJob(logicalDropTable, ec);

            Assert.fail();
        } catch (Throwable t) {
            System.out.println(t.getMessage());
            Assert.assertTrue(t.getMessage().contains(
                "ERR-CODE: [TDDL-4630][ERR_RECYCLEBIN_EXECUTE] "
                    + "drop table with gsi is not supported in recycle bin;"
                    + "use hint /*TDDL:ENABLE_RECYCLEBIN=false*/ "
                    + "to disable recycle bin "));
        }
    }
}
