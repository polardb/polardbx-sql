package com.alibaba.polardbx.executor.handler.ddl;

import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.properties.ParamManager;
import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.executor.ddl.job.builder.DdlPhyPlanBuilder;
import com.alibaba.polardbx.executor.ddl.job.converter.PhysicalPlanData;
import com.alibaba.polardbx.executor.ddl.newengine.job.ExecutableDdlJob;
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalCreateTable;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalTruncateTable;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.CreateTablePreparedData;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.gsi.TruncateTableWithGsiPreparedData;
import com.alibaba.polardbx.optimizer.partition.common.PartitionTableType;
import org.junit.Test;
import org.mockito.MockedStatic;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

public class LogicalTruncateTableHandlerTest {

    @Test
    public void testGetExecutableDdlJob() {
        String schema = "test_schema";
        LogicalTruncateTableHandler handler = new LogicalTruncateTableHandler(null);

        LogicalCreateTable logicalCreateTable = mock(LogicalCreateTable.class);
        ExecutionContext ec = new ExecutionContext(schema);

        CreateTablePreparedData createTablePreparedData = mock(CreateTablePreparedData.class);

        DdlPhyPlanBuilder builder = mock(DdlPhyPlanBuilder.class);
        PhysicalPlanData physicalPlanData = mock(PhysicalPlanData.class);
        try {
            when(logicalCreateTable.isReImportTable()).thenReturn(true);
            handler.getExecutableDdlJob(logicalCreateTable, ec, null, builder, createTablePreparedData,
                physicalPlanData);
        } catch (Throwable t) {
            // ignore
        }
        try {
            when(logicalCreateTable.isReImportTable()).thenReturn(false);
            handler.getExecutableDdlJob(logicalCreateTable, ec, null, builder, createTablePreparedData,
                physicalPlanData);
        } catch (Throwable t) {
            // ignore
        }
    }

    @Test
    public void testGetType() {
        LogicalCreateTable logicalCreateTable = mock(LogicalCreateTable.class);
        when(logicalCreateTable.isPartitionTable()).thenReturn(true);

        PartitionTableType type = LogicalTruncateTableHandler.getType(logicalCreateTable);

        assert type == PartitionTableType.PARTITION_TABLE;

        when(logicalCreateTable.isPartitionTable()).thenReturn(false);
        when(logicalCreateTable.isBroadCastTable()).thenReturn(true);

        type = LogicalTruncateTableHandler.getType(logicalCreateTable);

        assert type == PartitionTableType.BROADCAST_TABLE;

        when(logicalCreateTable.isBroadCastTable()).thenReturn(false);

        type = LogicalTruncateTableHandler.getType(logicalCreateTable);

        assert type == PartitionTableType.SINGLE_TABLE;
    }

    @Test
    public void testCrossDbTruncateWithRecycleBin() {
        String schema1 = "test_schema1";
        String schema2 = "test_schema2";
        String table = "test_table";
        LogicalTruncateTableHandler handler = mock(LogicalTruncateTableHandler.class);
        when(handler.buildDdlJob(any(), any())).thenCallRealMethod();
        when(handler.buildTruncateTableJob(any(), any())).thenReturn(mock(ExecutableDdlJob.class));

        ExecutionContext ec = new ExecutionContext(schema1);
        LogicalTruncateTable logicalTruncateTable = mock(LogicalTruncateTable.class);
        TruncateTableWithGsiPreparedData data = mock(TruncateTableWithGsiPreparedData.class);
        when(logicalTruncateTable.getSchemaName()).thenReturn(schema2);
        when(logicalTruncateTable.getTableName()).thenReturn(table);
        when(logicalTruncateTable.getTruncateTableWithGsiPreparedData()).thenReturn(data);
        when(data.getSchemaName()).thenReturn(schema1);
        ParamManager pm = mock(ParamManager.class);
        ec.setParamManager(pm);
        when(pm.getBoolean(eq(ConnectionParams.ENABLE_RECYCLEBIN))).thenReturn(true);

        DbInfoManager dbInfoManager = mock(DbInfoManager.class);
        when(dbInfoManager.isNewPartitionDb(anyString())).thenReturn(false);
        try (MockedStatic<DbInfoManager> mockedStaticDbInfoManager = mockStatic(DbInfoManager.class)) {
            mockedStaticDbInfoManager.when(DbInfoManager::getInstance).thenReturn(dbInfoManager);

            handler.buildDdlJob(logicalTruncateTable, ec);
            Assert.fail();
        } catch (Throwable t) {
            Assert.assertTrue(t.getMessage().contains("truncate table across db is not supported in recycle bin;"
                + "use the same db or use hint /*TDDL:ENABLE_RECYCLEBIN=false*/ " + "to disable recycle bin"));
        }
    }

    @Test
    public void testTruncateGSITableWithRecycleBin() {
        String schema = "test_schema";
        String table = "test_table";
        LogicalTruncateTableHandler handler = new LogicalTruncateTableHandler(null);

        ExecutionContext ec = new ExecutionContext(schema);
        LogicalTruncateTable logicalTruncateTable = mock(LogicalTruncateTable.class);
        when(logicalTruncateTable.getSchemaName()).thenReturn(schema);
        when(logicalTruncateTable.getTableName()).thenReturn(table);
        ParamManager pm = mock(ParamManager.class);
        ec.setParamManager(pm);
        when(pm.getBoolean(eq(ConnectionParams.ENABLE_RECYCLEBIN))).thenReturn(true);

        DbInfoManager dbInfoManager = mock(DbInfoManager.class);
        TruncateTableWithGsiPreparedData data = mock(TruncateTableWithGsiPreparedData.class);
        when(logicalTruncateTable.getTruncateTableWithGsiPreparedData()).thenReturn(data);
        when(data.getSchemaName()).thenReturn(schema);
        when(logicalTruncateTable.isPurge()).thenReturn(false);

        // shard table & gsi
        try (MockedStatic<DbInfoManager> mockedStaticDbInfoManager = mockStatic(DbInfoManager.class)) {
            mockedStaticDbInfoManager.when(DbInfoManager::getInstance).thenReturn(dbInfoManager);

            when(dbInfoManager.isNewPartitionDb(eq(schema))).thenReturn(false);
            when(logicalTruncateTable.isWithGsi()).thenReturn(true);

            handler.buildDdlJob(logicalTruncateTable, ec);
            Assert.fail();
        } catch (Throwable t) {
            Assert.assertTrue(t.getMessage().contains(
                "ERR-CODE: [TDDL-4630][ERR_RECYCLEBIN_EXECUTE] truncate table with gsi is not supported in recycle bin;"
                    + "use hint /*TDDL:ENABLE_RECYCLEBIN=false*/ " + "to disable recycle bin "));
        }

        // shard table and non-gsi and recycle bin check fail to support
        try (MockedStatic<DbInfoManager> mockedStaticDbInfoManager = mockStatic(DbInfoManager.class);
            MockedStatic<LogicalCommonDdlHandler> mockedStaticLogicalCommonDdlHandler = mockStatic(
                LogicalCommonDdlHandler.class)) {
            mockedStaticDbInfoManager.when(DbInfoManager::getInstance).thenReturn(dbInfoManager);
            mockedStaticLogicalCommonDdlHandler.when(
                () -> LogicalCommonDdlHandler.isAvailableForRecycleBin(eq(table), eq(ec))).thenReturn(false);

            when(dbInfoManager.isNewPartitionDb(eq(schema))).thenReturn(false);
            when(logicalTruncateTable.isWithGsi()).thenReturn(true);

            handler.buildDdlJob(logicalTruncateTable, ec);
            Assert.fail();
        } catch (Throwable t) {
            System.out.println(t.getMessage());
            Assert.assertTrue(t.getMessage().contains(
                "ERR-CODE: [TDDL-4630][ERR_RECYCLEBIN_EXECUTE] truncate table with gsi is not supported in recycle bin;"
                    + "use hint /*TDDL:ENABLE_RECYCLEBIN=false*/ " + "to disable recycle bin "));
        }

        // auto table and gsi
        try (MockedStatic<DbInfoManager> mockedStaticDbInfoManager = mockStatic(DbInfoManager.class)) {
            mockedStaticDbInfoManager.when(DbInfoManager::getInstance).thenReturn(dbInfoManager);

            when(dbInfoManager.isNewPartitionDb(eq(schema))).thenReturn(true);
            when(logicalTruncateTable.isWithGsi()).thenReturn(true);

            handler.buildDdlJob(logicalTruncateTable, ec);
            Assert.fail();
        } catch (Throwable t) {
            Assert.assertTrue(t.getMessage().contains(
                "ERR-CODE: [TDDL-4630][ERR_RECYCLEBIN_EXECUTE] truncate table with gsi is not supported in recycle bin;"
                    + "use hint /*TDDL:ENABLE_RECYCLEBIN=false*/ " + "to disable recycle bin "));
        }

        // auto table and non-gsi and recycle bin check fail to support
        try (MockedStatic<DbInfoManager> mockedStaticDbInfoManager = mockStatic(DbInfoManager.class);
            MockedStatic<LogicalCommonDdlHandler> mockedStaticLogicalCommonDdlHandler = mockStatic(
                LogicalCommonDdlHandler.class)) {
            mockedStaticDbInfoManager.when(DbInfoManager::getInstance).thenReturn(dbInfoManager);
            mockedStaticLogicalCommonDdlHandler.when(
                () -> LogicalCommonDdlHandler.isAvailableForRecycleBin(eq(table), eq(ec))).thenReturn(false);

            when(dbInfoManager.isNewPartitionDb(eq(schema))).thenReturn(true);
            when(logicalTruncateTable.isWithGsi()).thenReturn(true);

            handler.buildDdlJob(logicalTruncateTable, ec);

            Assert.fail();
        } catch (Throwable t) {
            System.out.println(t.getMessage());
            Assert.assertTrue(t.getMessage().contains(
                "ERR-CODE: [TDDL-4630][ERR_RECYCLEBIN_EXECUTE] truncate table with gsi is not supported in recycle bin;"
                    + "use hint /*TDDL:ENABLE_RECYCLEBIN=false*/ " + "to disable recycle bin "));
        }
    }
}
