package com.alibaba.polardbx.executor.scheduler.executor.statistic;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.executor.gms.util.StatisticSubProcessUtils;
import com.alibaba.polardbx.executor.scheduler.ScheduledJobsManager;
import com.alibaba.polardbx.executor.scheduler.executor.ScheduleJobStarter;
import com.alibaba.polardbx.executor.statistic.entity.PolarDbXSystemTableNDVSketchStatistic;
import com.alibaba.polardbx.gms.config.impl.InstConfUtil;
import com.alibaba.polardbx.gms.config.impl.MetaDbInstConfigManager;
import com.alibaba.polardbx.gms.module.Module;
import com.alibaba.polardbx.gms.module.ModuleLogInfo;
import com.alibaba.polardbx.gms.module.StatisticModuleLogUtil;
import com.alibaba.polardbx.gms.scheduler.ExecutableScheduledJob;
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.SchemaManager;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.config.table.statistic.StatisticManager;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.MockitoAnnotations;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.alibaba.polardbx.gms.module.LogPattern.NOT_ENABLED;
import static com.alibaba.polardbx.gms.module.LogPattern.STATE_CHANGE_FAIL;
import static com.alibaba.polardbx.optimizer.config.table.statistic.StatisticUtils.DEFAULT_SAMPLE_SIZE;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * statistic sketch ndv job
 *
 * @author fangwu
 */
public class StatisticHllScheduledJobTest {

    @InjectMocks
    private StatisticHllScheduledJob statisticHllScheduledJob;

    @Mock
    private PolarDbXSystemTableNDVSketchStatistic polarDbXSystemTableNDVSketchStatistic;

    @Mock
    private OptimizerContext optimizerContext;

    @Before
    public void setUp() {
        MockitoAnnotations.initMocks(this);
    }

    /**
     * 测试用例1: 当传入的schemas为空时，不删除任何数据。
     */
    @Test
    public void testRemoveNotExistSchemasEmptySchemas() {
        when(polarDbXSystemTableNDVSketchStatistic.loadAllSchemaAndTableName()).thenReturn(new HashMap<>());

        List<String> schemas = Collections.emptyList();
        StatisticHllScheduledJob.removeNotExistSchemas();

        verify(polarDbXSystemTableNDVSketchStatistic, times(0)).deleteBySchemaName(anyString());
        verify(polarDbXSystemTableNDVSketchStatistic, times(0)).deleteByTableName(anyString(), anyString());
    }

    /**
     * 测试用例2: 当当前schema不存在于传入的schemas列表中时，应该删除该schema的所有统计数据。
     */
    @Test
    public void testRemoveNotExistSchemasSchemaNotInList() {
        Map<String, Set<String>> curSchemaMap = new HashMap<>();
        curSchemaMap.put("test_schema", Collections.singleton("table1"));

        when(polarDbXSystemTableNDVSketchStatistic.loadAllSchemaAndTableName()).thenReturn(curSchemaMap);

        List<String> schemas = Collections.singletonList("other_schema");

        try (
            MockedStatic<PolarDbXSystemTableNDVSketchStatistic> polarDbXSystemTableNDVSketchStatisticMockedStatic = mockStatic(
                PolarDbXSystemTableNDVSketchStatistic.class)) {
            polarDbXSystemTableNDVSketchStatisticMockedStatic.when(PolarDbXSystemTableNDVSketchStatistic::getInstance)
                .thenReturn(polarDbXSystemTableNDVSketchStatistic);

            StatisticHllScheduledJob.removeNotExistSchemas();

            verify(polarDbXSystemTableNDVSketchStatistic, times(1)).deleteBySchemaName(eq("test_schema"));
            verify(polarDbXSystemTableNDVSketchStatistic, times(0)).deleteByTableName(anyString(), anyString());
        }
    }

    /**
     * 测试用例3: 当当前schema存在于传入的schemas列表中，但其下的表不存在于数据库中时，应该删除该表的统计数据。
     */
    @Test
    public void testRemoveNotExistSchemasTableDoesNotExist() {
        Map<String, Set<String>> curSchemaMap = new HashMap<>();
        curSchemaMap.put("test_schema", Collections.singleton("table1"));
        curSchemaMap.put("test_schema1", Collections.singleton("table1"));

        SchemaManager schemaManager = mock(SchemaManager.class);
        when(polarDbXSystemTableNDVSketchStatistic.loadAllSchemaAndTableName()).thenReturn(curSchemaMap);
        when(optimizerContext.getLatestSchemaManager()).thenReturn(schemaManager);
        when(schemaManager.getTableWithNull(anyString())).thenReturn(null);

        try (
            MockedStatic<PolarDbXSystemTableNDVSketchStatistic> polarDbXSystemTableNDVSketchStatisticMockedStatic = mockStatic(
                PolarDbXSystemTableNDVSketchStatistic.class);
            MockedStatic<OptimizerContext> optimizerContextMockedStatic = mockStatic(OptimizerContext.class)) {
            polarDbXSystemTableNDVSketchStatisticMockedStatic.when(PolarDbXSystemTableNDVSketchStatistic::getInstance)
                .thenReturn(polarDbXSystemTableNDVSketchStatistic);
            optimizerContextMockedStatic.when(() -> OptimizerContext.getContext(eq("test_schema")))
                .thenReturn(optimizerContext);
            optimizerContextMockedStatic.when(() -> OptimizerContext.getContext(eq("test_schema1")))
                .thenReturn(null);

            StatisticHllScheduledJob.removeNotExistSchemas();

            verify(polarDbXSystemTableNDVSketchStatistic, times(1)).deleteByTableName(eq("test_schema"), eq("table1"));

        }
    }

    @Test
    public void testDoExecute() {
        String schema = "test_schema";
        MetaDbInstConfigManager.setConfigFromMetaDb(false);
        final ModuleLogInfo moduleLogInfo = mock(ModuleLogInfo.class);
        ExecutableScheduledJob executableScheduledJob = mock(ExecutableScheduledJob.class);
        try (MockedStatic<InstConfUtil> instConfUtilMockedStatic = mockStatic(InstConfUtil.class);
            MockedStatic<StatisticModuleLogUtil> statisticModuleLogUtilMockedStatic = mockStatic(
                StatisticModuleLogUtil.class);
            MockedStatic<ScheduleJobStarter> scheduleJobStarterMockedStatic = mockStatic(ScheduleJobStarter.class);
            MockedStatic<ModuleLogInfo> moduleLogInfoMockedStatic = mockStatic(ModuleLogInfo.class)
        ) {
            instConfUtilMockedStatic.when(() -> InstConfUtil.getBool(ConnectionParams.ALERT_STATISTIC_INTERRUPT))
                .thenReturn(true);
            instConfUtilMockedStatic.when(() -> InstConfUtil.getInt(any())).thenReturn(1);
            instConfUtilMockedStatic.when(() -> InstConfUtil.getLong(any())).thenReturn(1L);

            StatisticHllScheduledJob job = new StatisticHllScheduledJob(executableScheduledJob);
            job.setFromScheduleJob(false);
            try {
                job.doExecute();
                Assert.fail("should throw TddlRuntimeException");
            } catch (TddlRuntimeException e) {
                e.printStackTrace();
                assert e.getErrorCode() == ErrorCode.ERR_STATISTIC_JOB_INTERRUPTED.getCode();
            }
            instConfUtilMockedStatic.when(() -> InstConfUtil.getBool(ConnectionParams.ALERT_STATISTIC_INTERRUPT))
                .thenReturn(false);
            instConfUtilMockedStatic.when(
                    () -> InstConfUtil.getBool(ConnectionParams.ENABLE_BACKGROUND_STATISTIC_COLLECTION))
                .thenReturn(false);
            job.setFromScheduleJob(true);

            boolean rs = job.doExecute();

            statisticModuleLogUtilMockedStatic.verify(() -> StatisticModuleLogUtil.logNormal(eq(NOT_ENABLED), any()),
                times(1));
            assert rs;

            try (MockedStatic<ScheduledJobsManager> scheduledJobsManagerMockedStatic = mockStatic(
                ScheduledJobsManager.class);
                MockedStatic<DbInfoManager> dbInfoManagerMockedStatic = mockStatic(DbInfoManager.class);
                MockedStatic<OptimizerContext> optimizerContextMockedStatic = mockStatic(OptimizerContext.class);
                MockedStatic<StatisticSubProcessUtils> statisticSubProcessUtilsMockedStatic = mockStatic(
                    StatisticSubProcessUtils.class);
                MockedStatic<StatisticManager> statisticManagerMockedStatic = mockStatic(StatisticManager.class);
            ) {
                instConfUtilMockedStatic.when(
                        () -> InstConfUtil.getBool(ConnectionParams.ENABLE_BACKGROUND_STATISTIC_COLLECTION))
                    .thenReturn(true);
                job.setFromScheduleJob(true);
                scheduledJobsManagerMockedStatic.when(
                        () -> ScheduledJobsManager.casStateWithStartTime(anyLong(), anyLong(), any(), any(), anyLong()))
                    .thenReturn(false);
                moduleLogInfoMockedStatic.when(ModuleLogInfo::getInstance).thenReturn(moduleLogInfo);

                rs = job.doExecute();

                verify(moduleLogInfo, times(1)).logRecord(eq(Module.SCHEDULE_JOB), eq(STATE_CHANGE_FAIL), any(), any());
                assert !rs;

                job.setFromScheduleJob(false);
                List<String> schemas = Lists.newArrayList();
                schemas.add(schema);
                DbInfoManager dbInfoManager = mock(DbInfoManager.class);
                when(dbInfoManager.getDbList()).thenReturn(schemas);
                dbInfoManagerMockedStatic.when(DbInfoManager::getInstance).thenReturn(dbInfoManager);
                optimizerContextMockedStatic.when(OptimizerContext::getActiveSchemaNames)
                    .thenReturn(Sets.newHashSet(schemas));
                optimizerContextMockedStatic.when(() -> OptimizerContext.getContext(schema))
                    .thenReturn(optimizerContext);
                SchemaManager schemaManager = mock(SchemaManager.class);
                when(optimizerContext.getLatestSchemaManager()).thenReturn(schemaManager);
                String table1 = "table1";
                String table2 = "table2";
                TableMeta tableMeta1 = mock(TableMeta.class);
                TableMeta tableMeta2 = mock(TableMeta.class);
                when(tableMeta1.getTableName()).thenReturn(table1);
                when(tableMeta2.getTableName()).thenReturn(table2);
                List<TableMeta> tbls = Lists.newArrayList();
                tbls.add(tableMeta1);
                tbls.add(tableMeta2);
                when(schemaManager.getAllUserTables()).thenReturn(tbls);
                when(schemaManager.getTableWithNull(anyString())).thenReturn(tableMeta1);

                StatisticManager statisticManager = mock(StatisticManager.class);
                statisticManagerMockedStatic.when(StatisticManager::getInstance).thenReturn(statisticManager);

                StatisticManager.CacheLine cacheLine1 = mock(StatisticManager.CacheLine.class);
                StatisticManager.CacheLine cacheLine2 = mock(StatisticManager.CacheLine.class);

                when(statisticManager.getCacheLine(schema, table1)).thenReturn(cacheLine1);
                when(statisticManager.getCacheLine(schema, table2)).thenReturn(cacheLine2);

                when(cacheLine1.getRowCount()).thenReturn(DEFAULT_SAMPLE_SIZE + 1L);
                when(cacheLine2.getRowCount()).thenReturn(DEFAULT_SAMPLE_SIZE - 1L);
                statisticSubProcessUtilsMockedStatic.clearInvocations();

                job.doExecute();

                statisticSubProcessUtilsMockedStatic.verify(
                    () -> StatisticSubProcessUtils.sketchTableDdl(eq(schema), eq(table1), eq(false), any()),
                    times(1));

                statisticSubProcessUtilsMockedStatic.when(
                        () -> StatisticSubProcessUtils.sketchTableDdl(anyString(), eq(table1), anyBoolean(), any()))
                    .thenThrow(new RuntimeException("ut test"));
                try {
                    job.doExecute();
                    Assert.fail("try test should throw RuntimeException");
                } catch (Exception e) {
                    e.printStackTrace();
                    assert e.getMessage().contains("failed to finish sample job");
                }

                String table3 = "table3";
                when(tableMeta1.getTableName()).thenReturn(table3);
                when(statisticManager.hasNdvSketch(anyString(), anyString())).thenReturn(true);
                tbls.remove(tableMeta2);
                when(statisticManager.getCacheLine(schema, table3)).thenReturn(cacheLine2);

                rs = job.doExecute();

                assert !rs;
            }

        }
    }
}
