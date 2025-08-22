package com.alibaba.polardbx.executor.scheduler.executor;

import com.alibaba.polardbx.gms.config.impl.MetaDbInstConfigManager;
import com.alibaba.polardbx.gms.scheduler.ScheduledJobsRecord;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ScheduleJobStarterTest {

    @Test
    public void testInitStatisticSampleSketchJob() {
        MetaDbInstConfigManager.setConfigFromMetaDb(false);
        Connection conn = mock(Connection.class);
        List< ScheduledJobsRecord> scheduledJobsRecords = new ArrayList<>();
        ScheduledJobsRecord cur = mock(ScheduledJobsRecord.class);
        when(cur.getScheduleExpr()).thenReturn("test");
        scheduledJobsRecords.add(cur);
        try(MockedStatic<MetaDbUtil> metaDbUtilMock = Mockito.mockStatic(MetaDbUtil.class);
        ) {
            metaDbUtilMock.when(() -> MetaDbUtil.getConnection()).thenReturn(conn);
            metaDbUtilMock.when(() -> MetaDbUtil.query(Mockito.anyString(), any(), any(), any())).thenReturn(scheduledJobsRecords);

            ScheduleJobStarter.initStatisticSampleSketchJob();;
        }
    }
}
