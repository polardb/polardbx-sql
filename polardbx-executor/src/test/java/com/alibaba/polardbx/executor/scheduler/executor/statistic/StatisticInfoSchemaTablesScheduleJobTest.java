package com.alibaba.polardbx.executor.scheduler.executor.statistic;

import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.properties.ConnectionProperties;
import com.alibaba.polardbx.executor.common.ExecutorContext;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.scheduler.ScheduledJobsManager;
import com.alibaba.polardbx.executor.scheduler.executor.ScheduleJobStarter;
import com.alibaba.polardbx.executor.spi.ITopologyExecutor;
import com.alibaba.polardbx.gms.config.impl.InstConfUtil;
import com.alibaba.polardbx.gms.config.impl.MetaDbInstConfigManager;
import com.alibaba.polardbx.gms.scheduler.ExecutableScheduledJob;
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.SchemaManager;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.rpc.compatible.ArrayResultSet;
import com.google.common.collect.ImmutableList;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.MockedStatic;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

public class StatisticInfoSchemaTablesScheduleJobTest {

    @Test
    public void testExecuteInterruptedTestTrue() {
        Properties properties = new Properties();
        properties.setProperty(ConnectionProperties.ALERT_STATISTIC_INTERRUPT, "true");
        try (MockedStatic<MetaDbInstConfigManager> mockedStaticMetaDbInstConfigManager =
            mockStatic(MetaDbInstConfigManager.class)) {
            MetaDbInstConfigManager metaDbInstConfigManager = mock(MetaDbInstConfigManager.class);
            when(metaDbInstConfigManager.getCnVariableConfigMap()).thenReturn(properties);
            mockedStaticMetaDbInstConfigManager.when(MetaDbInstConfigManager::getInstance)
                .thenReturn(metaDbInstConfigManager);
            try (MockedStatic<ScheduleJobStarter> mockedStaticScheduleJobStarter =
                mockStatic(ScheduleJobStarter.class);
                MockedStatic<ScheduledJobsManager> mockedStaticScheduledJobsManager =
                    mockStatic(ScheduledJobsManager.class);) {
                ExecutableScheduledJob executableScheduledJob = new ExecutableScheduledJob();
                executableScheduledJob.setScheduleId(1);
                executableScheduledJob.setFireTime(System.currentTimeMillis());
                executableScheduledJob.setTimeZone("SYSTEM");
                Assert.assertFalse(new StatisticInfoSchemaTablesScheduleJob(executableScheduledJob).execute());
            }
        }
    }

    @Test
    public void testDisableStatisticBackground() {
        Properties properties = new Properties();
        properties.setProperty(ConnectionProperties.ALERT_STATISTIC_INTERRUPT, "false");
        properties.setProperty(ConnectionProperties.ENABLE_INFO_SCHEMA_TABLES_STAT_COLLECTION, "false");
        try (MockedStatic<MetaDbInstConfigManager> mockedStaticMetaDbInstConfigManager =
            mockStatic(MetaDbInstConfigManager.class)) {
            MetaDbInstConfigManager metaDbInstConfigManager = mock(MetaDbInstConfigManager.class);
            when(metaDbInstConfigManager.getCnVariableConfigMap()).thenReturn(properties);
            mockedStaticMetaDbInstConfigManager.when(MetaDbInstConfigManager::getInstance)
                .thenReturn(metaDbInstConfigManager);
            try (MockedStatic<ScheduleJobStarter> mockedStaticScheduleJobStarter =
                mockStatic(ScheduleJobStarter.class);
                MockedStatic<ScheduledJobsManager> mockedStaticScheduledJobsManager =
                    mockStatic(ScheduledJobsManager.class);) {
                AtomicReference<String> remark = new AtomicReference<>();
                mockedStaticScheduledJobsManager.when(() -> ScheduledJobsManager.casStateWithFinishTime(
                        anyLong(), anyLong(), any(), any(), anyLong(), anyString()))
                    .then(invocation -> {
                        remark.set(invocation.getArgument(5));
                        return true;
                    });

                mockedStaticScheduledJobsManager.when(() -> ScheduledJobsManager.casStateWithStartTime(
                        anyLong(), anyLong(), any(), any(), anyLong()))
                    .thenReturn(true);

                ExecutableScheduledJob executableScheduledJob = new ExecutableScheduledJob();
                executableScheduledJob.setScheduleId(1);
                executableScheduledJob.setFireTime(System.currentTimeMillis());
                executableScheduledJob.setTimeZone("SYSTEM");
                Assert.assertTrue(new StatisticInfoSchemaTablesScheduleJob(executableScheduledJob).execute());
                Assert.assertTrue(remark.get().equalsIgnoreCase(
                    "statistic background collection task (info_schema.tables) disabled."));
            }
        }
    }

    @Test
    public void testNotInMaintenanceWindow() {
        Properties properties = new Properties();
        properties.setProperty(ConnectionProperties.ALERT_STATISTIC_INTERRUPT, "false");
        properties.setProperty(ConnectionProperties.ENABLE_INFO_SCHEMA_TABLES_STAT_COLLECTION, "true");
        try (MockedStatic<MetaDbInstConfigManager> mockedStaticMetaDbInstConfigManager =
            mockStatic(MetaDbInstConfigManager.class)) {
            MetaDbInstConfigManager metaDbInstConfigManager = mock(MetaDbInstConfigManager.class);
            when(metaDbInstConfigManager.getCnVariableConfigMap()).thenReturn(properties);
            mockedStaticMetaDbInstConfigManager.when(MetaDbInstConfigManager::getInstance)
                .thenReturn(metaDbInstConfigManager);
            try (MockedStatic<ScheduleJobStarter> mockedStaticScheduleJobStarter =
                mockStatic(ScheduleJobStarter.class);
                MockedStatic<ScheduledJobsManager> mockedStaticScheduledJobsManager =
                    mockStatic(ScheduledJobsManager.class);
                MockedStatic<InstConfUtil> mockedStaticInstConfUtil =
                    mockStatic(InstConfUtil.class);) {
                AtomicReference<String> remark = new AtomicReference<>();
                mockedStaticScheduledJobsManager.when(() -> ScheduledJobsManager.casStateWithFinishTime(
                        anyLong(), anyLong(), any(), any(), anyLong(), anyString()))
                    .then(invocation -> {
                        remark.set(invocation.getArgument(5));
                        return true;
                    });

                mockedStaticScheduledJobsManager.when(() -> ScheduledJobsManager.casStateWithStartTime(
                        anyLong(), anyLong(), any(), any(), anyLong()))
                    .thenReturn(true);

                mockedStaticInstConfUtil.when(InstConfUtil::isInMaintenanceTimeWindow).thenReturn(false);
                mockedStaticInstConfUtil.when(() -> InstConfUtil.getBool(
                        eq(ConnectionParams.ENABLE_INFO_SCHEMA_TABLES_STAT_COLLECTION)))
                    .thenReturn(true);
                ExecutableScheduledJob executableScheduledJob = new ExecutableScheduledJob();
                executableScheduledJob.setScheduleId(1);
                executableScheduledJob.setFireTime(System.currentTimeMillis());
                executableScheduledJob.setTimeZone("SYSTEM");
                Assert.assertTrue(new StatisticInfoSchemaTablesScheduleJob(executableScheduledJob).execute());
                Assert.assertTrue(remark.get().equalsIgnoreCase(
                    "statistic background collection task (info_schema.tables) not in maintenance window."));
            }
        }
    }

    @Test
    public void testCasFail() {
        Properties properties = new Properties();
        properties.setProperty(ConnectionProperties.ALERT_STATISTIC_INTERRUPT, "false");
        properties.setProperty(ConnectionProperties.ENABLE_INFO_SCHEMA_TABLES_STAT_COLLECTION, "false");
        try (MockedStatic<MetaDbInstConfigManager> mockedStaticMetaDbInstConfigManager =
            mockStatic(MetaDbInstConfigManager.class)) {
            MetaDbInstConfigManager metaDbInstConfigManager = mock(MetaDbInstConfigManager.class);
            when(metaDbInstConfigManager.getCnVariableConfigMap()).thenReturn(properties);
            mockedStaticMetaDbInstConfigManager.when(MetaDbInstConfigManager::getInstance)
                .thenReturn(metaDbInstConfigManager);
            try (MockedStatic<ScheduleJobStarter> mockedStaticScheduleJobStarter =
                mockStatic(ScheduleJobStarter.class);
                MockedStatic<ScheduledJobsManager> mockedStaticScheduledJobsManager =
                    mockStatic(ScheduledJobsManager.class);) {
                mockedStaticScheduledJobsManager.when(() -> ScheduledJobsManager.casStateWithStartTime(
                        anyLong(), anyLong(), any(), any(), anyLong()))
                    .thenReturn(false);

                ExecutableScheduledJob executableScheduledJob = new ExecutableScheduledJob();
                executableScheduledJob.setScheduleId(1);
                executableScheduledJob.setFireTime(System.currentTimeMillis());
                executableScheduledJob.setTimeZone("SYSTEM");
                Assert.assertFalse(new StatisticInfoSchemaTablesScheduleJob(executableScheduledJob).execute());
            }
        }
    }

    @Test
    public void testEmptySchemas() {
        Properties properties = new Properties();
        properties.setProperty(ConnectionProperties.ALERT_STATISTIC_INTERRUPT, "false");
        properties.setProperty(ConnectionProperties.ENABLE_INFO_SCHEMA_TABLES_STAT_COLLECTION, "true");
        try (MockedStatic<MetaDbInstConfigManager> mockedStaticMetaDbInstConfigManager =
            mockStatic(MetaDbInstConfigManager.class)) {
            MetaDbInstConfigManager metaDbInstConfigManager = mock(MetaDbInstConfigManager.class);
            when(metaDbInstConfigManager.getCnVariableConfigMap()).thenReturn(properties);
            mockedStaticMetaDbInstConfigManager.when(MetaDbInstConfigManager::getInstance)
                .thenReturn(metaDbInstConfigManager);
            try (MockedStatic<ScheduleJobStarter> mockedStaticScheduleJobStarter =
                mockStatic(ScheduleJobStarter.class);
                MockedStatic<ScheduledJobsManager> mockedStaticScheduledJobsManager =
                    mockStatic(ScheduledJobsManager.class);
                MockedStatic<DbInfoManager> mockedStaticDbInfoManager =
                    mockStatic(DbInfoManager.class);
                MockedStatic<InstConfUtil> mockedStaticInstConfUtil =
                    mockStatic(InstConfUtil.class);) {
                AtomicReference<String> remark = new AtomicReference<>();
                mockedStaticScheduledJobsManager.when(() -> ScheduledJobsManager.casStateWithFinishTime(
                        anyLong(), anyLong(), any(), any(), anyLong(), anyString()))
                    .then(invocation -> {
                        remark.set(invocation.getArgument(5));
                        return true;
                    });
                mockedStaticInstConfUtil.when(InstConfUtil::isInMaintenanceTimeWindow).thenReturn(true);
                mockedStaticInstConfUtil.when(() -> InstConfUtil.getBool(
                        eq(ConnectionParams.ENABLE_INFO_SCHEMA_TABLES_STAT_COLLECTION)))
                    .thenReturn(true);

                mockedStaticScheduledJobsManager.when(() -> ScheduledJobsManager.casStateWithStartTime(
                        anyLong(), anyLong(), any(), any(), anyLong()))
                    .thenReturn(true);

                DbInfoManager dbInfoManager = mock(DbInfoManager.class);
                mockedStaticDbInfoManager.when(DbInfoManager::getInstance).thenReturn(dbInfoManager);
                when(dbInfoManager.getDbList()).thenReturn(new ArrayList<>());

                ExecutableScheduledJob executableScheduledJob = new ExecutableScheduledJob();
                executableScheduledJob.setScheduleId(1);
                executableScheduledJob.setFireTime(System.currentTimeMillis());
                executableScheduledJob.setTimeZone("SYSTEM");
                Assert.assertTrue(new StatisticInfoSchemaTablesScheduleJob(executableScheduledJob).execute());
                Assert.assertTrue(remark.get().equalsIgnoreCase(""));
            }
        }
    }

    @Ignore
    public void test() throws SQLException {
        Properties properties = new Properties();
        properties.setProperty(ConnectionProperties.ALERT_STATISTIC_INTERRUPT, "false");
        properties.setProperty(ConnectionProperties.ENABLE_INFO_SCHEMA_TABLES_STAT_COLLECTION, "true");
        try (MockedStatic<MetaDbInstConfigManager> mockedStaticMetaDbInstConfigManager =
            mockStatic(MetaDbInstConfigManager.class)) {
            MetaDbInstConfigManager metaDbInstConfigManager = mock(MetaDbInstConfigManager.class);
            when(metaDbInstConfigManager.getCnVariableConfigMap()).thenReturn(properties);
            mockedStaticMetaDbInstConfigManager.when(MetaDbInstConfigManager::getInstance)
                .thenReturn(metaDbInstConfigManager);
            try (MockedStatic<ScheduleJobStarter> mockedStaticScheduleJobStarter =
                mockStatic(ScheduleJobStarter.class);
                MockedStatic<ScheduledJobsManager> mockedStaticScheduledJobsManager =
                    mockStatic(ScheduledJobsManager.class);
                MockedStatic<DbInfoManager> mockedStaticDbInfoManager =
                    mockStatic(DbInfoManager.class);
                MockedStatic<InstConfUtil> mockedStaticInstConfUtil =
                    mockStatic(InstConfUtil.class);
                MockedStatic<MetaDbUtil> mockedStaticMetaDbUtil =
                    mockStatic(MetaDbUtil.class);
            ) {
                AtomicReference<String> remark = new AtomicReference<>();
                mockedStaticScheduledJobsManager.when(() -> ScheduledJobsManager.casStateWithFinishTime(
                        anyLong(), anyLong(), any(), any(), anyLong(), anyString()))
                    .then(invocation -> {
                        remark.set(invocation.getArgument(5));
                        return true;
                    });
                mockedStaticInstConfUtil.when(InstConfUtil::isInMaintenanceTimeWindow).thenReturn(true);
                mockedStaticInstConfUtil.when(() -> InstConfUtil.getBool(
                        eq(ConnectionParams.ENABLE_INFO_SCHEMA_TABLES_STAT_COLLECTION)))
                    .thenReturn(true);

                mockedStaticScheduledJobsManager.when(() -> ScheduledJobsManager.casStateWithStartTime(
                        anyLong(), anyLong(), any(), any(), anyLong()))
                    .thenReturn(true);

                DbInfoManager dbInfoManager = mock(DbInfoManager.class);
                mockedStaticDbInfoManager.when(DbInfoManager::getInstance).thenReturn(dbInfoManager);
                List<String> schemas = new ArrayList<>();
                String db1 = "db1";
                schemas.add(db1);
                schemas.add("polardbx");
                schemas.add("notActive");
                when(dbInfoManager.getDbList()).thenReturn(schemas);

                OptimizerContext db1OptimizerContext = new OptimizerContext(db1);
                OptimizerContext.loadContext(db1OptimizerContext);
                SchemaManager schemaManager = mock(SchemaManager.class);
                db1OptimizerContext.setSchemaManager(schemaManager);
                TableMeta tableMeta1 = mock(TableMeta.class);
                TableMeta tableMeta2 = mock(TableMeta.class);
                List<TableMeta> tableMetas = ImmutableList.of(tableMeta1, tableMeta2);
                when(schemaManager.getAllUserTables()).thenReturn(tableMetas);
                // tb1 was long time not updated.
                String tb1 = "tb1";
                // tb2 was just updated
                String tb2 = "tb2";
                when(tableMeta1.getTableName()).thenReturn(tb1);
                when(tableMeta2.getTableName()).thenReturn(tb2);
                when(schemaManager.getTable(tb1)).thenReturn(tableMeta1);
                when(schemaManager.getTable(tb2)).thenReturn(tableMeta2);

                OptimizerContext polardbxOptimizerContext = new OptimizerContext("polardbx");
                OptimizerContext.loadContext(polardbxOptimizerContext);

                Connection connection = mock(Connection.class);
                mockedStaticMetaDbUtil.when(MetaDbUtil::getConnection).thenReturn(connection);
                PreparedStatement preparedStatement = mock(PreparedStatement.class);
                when(connection.prepareStatement(anyString())).thenReturn(preparedStatement);

                ArrayResultSet arrayResultSet = new ArrayResultSet();
                arrayResultSet.getColumnName().add("table_name");
                arrayResultSet.getRows().add(new Object[] {tb1});
                when(preparedStatement.executeQuery()).thenReturn(arrayResultSet);

                ITopologyExecutor topologyExecutor = mock(ITopologyExecutor.class);
                ExecutorContext executorContext = mock(ExecutorContext.class);
                when(executorContext.getTopologyExecutor()).thenReturn(topologyExecutor);
                ExecutorContext.setContext(db1, executorContext);
                Cursor cursor = mock(Cursor.class);
                when(topologyExecutor.execByExecPlanNode(any(), any())).thenReturn(cursor);

                ExecutableScheduledJob executableScheduledJob = new ExecutableScheduledJob();
                executableScheduledJob.setScheduleId(1);
                executableScheduledJob.setFireTime(System.currentTimeMillis());
                executableScheduledJob.setTimeZone("SYSTEM");
                Assert.assertTrue(new StatisticInfoSchemaTablesScheduleJob(executableScheduledJob).execute());
                Assert.assertTrue(remark.get().equalsIgnoreCase(""));
            }
        }
    }

}
