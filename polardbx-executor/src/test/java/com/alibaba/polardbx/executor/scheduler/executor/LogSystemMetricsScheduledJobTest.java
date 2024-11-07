package com.alibaba.polardbx.executor.scheduler.executor;

import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.executor.scheduler.ScheduledJobsManager;
import com.alibaba.polardbx.executor.sync.MetricSyncAction;
import com.alibaba.polardbx.gms.config.impl.InstConfUtil;
import com.alibaba.polardbx.gms.config.impl.MetaDbInstConfigManager;
import com.alibaba.polardbx.gms.scheduler.ExecutableScheduledJob;
import com.alibaba.polardbx.gms.sync.GmsSyncManagerHelper;
import com.alibaba.polardbx.gms.sync.SyncScope;
import com.google.common.collect.Lists;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;

import static com.alibaba.polardbx.common.scheduler.FiredScheduledJobState.FAILED;
import static com.alibaba.polardbx.common.scheduler.FiredScheduledJobState.QUEUED;
import static com.alibaba.polardbx.common.scheduler.FiredScheduledJobState.RUNNING;
import static com.alibaba.polardbx.common.utils.Assert.assertTrue;
import static org.junit.Assert.assertFalse;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;

/**
 * @author fangwu
 */
public class LogSystemMetricsScheduledJobTest {
    private static long scheduleId = 1L;
    private static long fireTime = 2L;
    private static long startTime = 3L;

    private static LogSystemMetricsScheduledJob job;
    private static MockedStatic<ScheduledJobsManager> mockedScheduledJobsManager;
    private static MockedStatic<ScheduleJobStarter> mockedScheduleJobStarter;
    private static MockedStatic<GmsSyncManagerHelper> mockedGmsSyncManagerHelper;
    private static MockedStatic<InstConfUtil> mockedInst;
    private static MockedStatic<ConfigDataMode> mockedConfigDataMode;

    @BeforeClass
    public static void setUp() {
        MetaDbInstConfigManager.setConfigFromMetaDb(false);
        Mockito.clearAllCaches();

        ExecutableScheduledJob executableScheduledJob = new ExecutableScheduledJob();
        executableScheduledJob.setScheduleId(scheduleId);
        executableScheduledJob.setFireTime(fireTime);
        executableScheduledJob.setStartTime(startTime);

        job = new LogSystemMetricsScheduledJob(executableScheduledJob);
        mockedInst = Mockito.mockStatic(InstConfUtil.class);
        mockedInst.when(() -> InstConfUtil.getInt(any())).thenReturn(5);
        mockedInst.when(() -> InstConfUtil.getLong(any())).thenReturn(5L);
        mockedScheduleJobStarter = Mockito.mockStatic(ScheduleJobStarter.class);
        mockedScheduleJobStarter.when(ScheduleJobStarter::launchAll).thenAnswer((Answer<Void>) invocation -> null);
        mockedScheduledJobsManager = Mockito.mockStatic(ScheduledJobsManager.class);
        mockedGmsSyncManagerHelper = Mockito.mockStatic(GmsSyncManagerHelper.class);
        mockedConfigDataMode = Mockito.mockStatic(ConfigDataMode.class);
        mockedConfigDataMode.when(ConfigDataMode::isFastMock).thenReturn(true);
    }

    @AfterClass
    public static void tearDown() {
        mockedScheduledJobsManager.close();
        mockedGmsSyncManagerHelper.close();
        mockedScheduleJobStarter.close();
        mockedConfigDataMode.close();
        mockedInst.close();
        MetaDbInstConfigManager.setConfigFromMetaDb(true);
        Mockito.clearAllCaches();
    }

    @Test
    public void testMarkRunningWhenQueuedShouldReturnTrue() {
        // Arrange
        mockedScheduledJobsManager.clearInvocations();
        mockedScheduledJobsManager.when(() ->
                ScheduledJobsManager.casStateWithStartTime(eq(scheduleId), eq(fireTime), eq(QUEUED), eq(RUNNING),
                    anyLong()))
            .thenReturn(true);
        // Act
        boolean result = job.markRunning(scheduleId, fireTime, startTime);
        // Assert
        assertTrue(result);
        mockedScheduledJobsManager.verify(() ->
            ScheduledJobsManager.casStateWithStartTime(eq(scheduleId), eq(fireTime), eq(QUEUED), eq(RUNNING),
                anyLong()));
    }

    @Test
    public void testMarkRunningWhenNotQueuedShouldReturnFalse() {
        // Arrange
        mockedScheduledJobsManager.clearInvocations();
        mockedScheduledJobsManager.when(() ->
                ScheduledJobsManager.casStateWithStartTime(eq(scheduleId), eq(fireTime), eq(QUEUED), eq(RUNNING),
                    anyLong()))
            .thenReturn(false);

        // Act
        boolean result = job.markRunning(scheduleId, fireTime, startTime);

        // Assert
        assertFalse(result);
        mockedScheduledJobsManager.verify(() ->
            ScheduledJobsManager.casStateWithStartTime(eq(scheduleId), eq(fireTime), eq(QUEUED), eq(RUNNING),
                anyLong()));
    }

    /**
     * check job interrupted when ENABLE_LOG_SYSTEM_METRICS is false
     */
    @Test
    public void testExecute() {
        mockedInst.when(() -> InstConfUtil.getBool(ConnectionParams.ENABLE_LOG_SYSTEM_METRICS)).thenReturn(false);

        Exception targetException = new RuntimeException("test LogSystemMetricsScheduledJobTest execute");
        mockedScheduledJobsManager.when(() ->
                ScheduledJobsManager.casStateWithFinishTime(anyLong(), anyLong(), any(), any(), anyLong(), anyString()))
            .thenThrow(targetException);
        mockedScheduledJobsManager.when(() ->
                ScheduledJobsManager.updateState(anyLong(), anyLong(), any(), anyString(), anyString()))
            .thenThrow(targetException);
        boolean result = job.execute();
        // Assert
        assertFalse(result);
        mockedScheduledJobsManager.verify(() ->
            ScheduledJobsManager.updateState(eq(scheduleId), eq(fireTime), eq(FAILED), any(), any()));

        mockedInst.when(() -> InstConfUtil.getBool(ConnectionParams.ENABLE_LOG_SYSTEM_METRICS)).thenReturn(true);
        assert !job.execute();
        mockedScheduledJobsManager.when(() ->
                ScheduledJobsManager.casStateWithStartTime(eq(scheduleId), eq(fireTime), eq(QUEUED), eq(RUNNING),
                    anyLong()))
            .thenReturn(true);
        mockedGmsSyncManagerHelper.when(() ->
            GmsSyncManagerHelper.sync(new MetricSyncAction(), "polardbx", SyncScope.CURRENT_ONLY)
        ).thenReturn(Lists.newArrayList());
        mockedScheduledJobsManager.when(() ->
                ScheduledJobsManager.casStateWithFinishTime(anyLong(), anyLong(), any(), any(), anyLong(), anyString()))
            .thenAnswer((Answer<Boolean>) invocation -> null);
        assert job.execute();
    }
}