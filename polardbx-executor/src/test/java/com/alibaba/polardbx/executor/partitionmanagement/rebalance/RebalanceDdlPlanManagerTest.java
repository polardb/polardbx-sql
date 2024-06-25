package com.alibaba.polardbx.executor.partitionmanagement.rebalance;

import com.alibaba.polardbx.common.properties.BooleanConfigParam;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.properties.ConnectionProperties;
import com.alibaba.polardbx.common.properties.StringConfigParam;
import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.executor.ddl.newengine.meta.DdlJobManager;
import com.alibaba.polardbx.gms.config.impl.InstConfUtil;
import com.alibaba.polardbx.gms.config.impl.MetaDbInstConfigManager;
import com.alibaba.polardbx.gms.metadb.misc.DdlEngineRecord;
import com.alibaba.polardbx.gms.scheduler.DdlPlanAccessor;
import com.alibaba.polardbx.gms.scheduler.DdlPlanRecord;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.optimizer.config.server.DefaultServerConfigManager;
import com.alibaba.polardbx.optimizer.utils.OptimizerHelper;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.Calendar;
import java.util.GregorianCalendar;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Created by luoyanxin.
 *
 * @author luoyanxin
 */
public class RebalanceDdlPlanManagerTest {

    @Test
    // 测试INIT状态
    public void testOnInit() {
        try (MockedStatic<InstConfUtil> mockedInstConfUtil = Mockito.mockStatic(InstConfUtil.class);
            MockedStatic<MetaDbUtil> mockedMetaDbUtil = Mockito.mockStatic(MetaDbUtil.class);
            MockedConstruction<DdlPlanAccessor> mockedDdlPlanAccessor = Mockito.mockConstruction(DdlPlanAccessor.class,
                (mockObj, context) -> {
                    Mockito.when(mockObj.queryForUpdate(anyLong()))
                        .thenReturn(mock(DdlPlanRecord.class));
                });) {
            mockedInstConfUtil.when(() -> InstConfUtil.isInRebalanceMaintenanceTimeWindow())
                .thenReturn(false).thenReturn(true);
            DdlPlanRecord ddlPlanRecord =
                DdlPlanRecord.constructNewDdlPlanRecord("test", 1L, "REBALANCE", "REBALANCE DATABASE");
            RebalanceDdlPlanManager manager = spy(new RebalanceDdlPlanManager());
            manager.process(ddlPlanRecord);
            Mockito.doNothing().when(manager).onInit(any(DdlPlanRecord.class));
            manager.process(ddlPlanRecord);
        }
    }

    @Test
    // EXECUTING
    public void testOnExecuting() {
        try (MockedStatic<InstConfUtil> mockedInstConfUtil = Mockito.mockStatic(InstConfUtil.class);
            MockedStatic<MetaDbUtil> mockedMetaDbUtil = Mockito.mockStatic(MetaDbUtil.class);
        ) {
            mockedInstConfUtil.when(() -> InstConfUtil.isInRebalanceMaintenanceTimeWindow())
                .thenReturn(false).thenReturn(true);
            DdlPlanRecord ddlPlanRecord =
                DdlPlanRecord.constructNewDdlPlanRecord("test", 1L, "REBALANCE", "REBALANCE DATABASE");
            ddlPlanRecord.setJobId(123l);
            ddlPlanRecord.setState("EXECUTING");
            RebalanceDdlPlanManager manager = spy(new RebalanceDdlPlanManager());
            Mockito.doNothing().when(manager).terminateRebalanceJob(any(DdlPlanRecord.class));
            manager.process(ddlPlanRecord);
            Mockito.doNothing().when(manager).onExecuting(any(DdlPlanRecord.class));
            manager.process(ddlPlanRecord);
        }
    }

    @Test
    // PAUSE_ON_NON_MAINTENANCE_WINDOW
    public void testOnPauseOnNonMaintenanceWindow() {
        DdlEngineRecord ddlEngineRecord = new DdlEngineRecord();
        ddlEngineRecord.state = "RUNNING";
        ddlEngineRecord.jobId = 123l;
        try (MockedStatic<InstConfUtil> mockedInstConfUtil = Mockito.mockStatic(InstConfUtil.class);
            MockedStatic<MetaDbUtil> mockedMetaDbUtil = Mockito.mockStatic(MetaDbUtil.class);
            MockedStatic<OptimizerHelper> mockedOptimizerHelper = Mockito.mockStatic(OptimizerHelper.class);
            MockedConstruction<DdlJobManager> DdlJobManager = Mockito.mockConstruction(DdlJobManager.class,
                (mockObj, context) -> {
                    Mockito.when(mockObj.fetchRecordByJobId(anyLong()))
                        .thenReturn(null).thenReturn(ddlEngineRecord).thenReturn(ddlEngineRecord);
                });) {
            mockedOptimizerHelper.when(() -> OptimizerHelper.getServerConfigManager())
                .thenReturn(mock(DefaultServerConfigManager.class));
            mockedInstConfUtil.when(() -> InstConfUtil.isInRebalanceMaintenanceTimeWindow())
                .thenReturn(false).thenReturn(false).thenReturn(true).thenReturn(false);
            DdlPlanRecord ddlPlanRecord =
                DdlPlanRecord.constructNewDdlPlanRecord("test", 1L, "REBALANCE", "REBALANCE DATABASE");
            ddlPlanRecord.setJobId(123l);
            ddlPlanRecord.setState("PAUSE_ON_NON_MAINTENANCE_WINDOW");
            RebalanceDdlPlanManager manager = spy(new RebalanceDdlPlanManager());
            manager.process(ddlPlanRecord);
            manager.process(ddlPlanRecord);
            Mockito.doNothing().when(manager).onExecuting(any(DdlPlanRecord.class));
            manager.process(ddlPlanRecord);
            ddlPlanRecord.setJobId(-1l);
            manager.process(ddlPlanRecord);
        }
    }

    @Test
    public void testIsInMaintenanceTimeWindow() {
        try (MockedStatic<InstConfUtil> mockedInstConfUtil = Mockito.mockStatic(InstConfUtil.class);) {
            mockedInstConfUtil.when(() -> InstConfUtil.isInRebalanceMaintenanceTimeWindow()).thenCallRealMethod();
            mockedInstConfUtil.when(() -> InstConfUtil.isInRebalanceMaintenanceTimeWindow(any(StringConfigParam.class),
                any(StringConfigParam.class), any(
                    BooleanConfigParam.class))).thenCallRealMethod();
            mockedInstConfUtil.when(() -> InstConfUtil.isInMaintenanceTimeWindow(any(GregorianCalendar.class),
                any(StringConfigParam.class), any(StringConfigParam.class))).thenCallRealMethod();

            mockedInstConfUtil.when(() -> InstConfUtil.getBool(any())).thenReturn(false).thenReturn(true);
            Assert.assertTrue(InstConfUtil.isInRebalanceMaintenanceTimeWindow());
            InstConfUtil.isInRebalanceMaintenanceTimeWindow();
            InstConfUtil.isInMaintenanceTimeWindow();

            Assert.assertTrue(
                InstConfUtil.isInRebalanceMaintenanceTimeWindow(ConnectionParams.REBALANCE_MAINTENANCE_TIME_START,
                    ConnectionParams.REBALANCE_MAINTENANCE_TIME_END, ConnectionParams.REBALANCE_MAINTENANCE_ENABLE));

            Assert.assertTrue(
                InstConfUtil.isInRebalanceMaintenanceTimeWindow(ConnectionParams.REBALANCE_MAINTENANCE_TIME_START,
                    ConnectionParams.REBALANCE_MAINTENANCE_TIME_END,
                    new BooleanConfigParam("REBALANCE_MAINTENANCE_ENABLE", false, true)));

            Assert.assertTrue(
                InstConfUtil.isInRebalanceMaintenanceTimeWindow(ConnectionParams.REBALANCE_MAINTENANCE_TIME_START,
                    ConnectionParams.REBALANCE_MAINTENANCE_TIME_END,
                    new BooleanConfigParam("REBALANCE_MAINTENANCE_ENABLE", true, true)));

            MetaDbInstConfigManager.setConfigFromMetaDb(false);

            mockedInstConfUtil.when(() -> InstConfUtil.getOriginVal(any())).thenCallRealMethod();
            mockedInstConfUtil.when(() -> InstConfUtil.getBool(any())).thenCallRealMethod();
            mockedInstConfUtil.when(() -> InstConfUtil.getMinute(any())).thenCallRealMethod();

            Assert.assertTrue(
                !InstConfUtil.isInRebalanceMaintenanceTimeWindow(
                    new StringConfigParam(ConnectionProperties.REBALANCE_MAINTENANCE_TIME_START, "00:00:00", true),
                    new StringConfigParam(ConnectionProperties.REBALANCE_MAINTENANCE_TIME_END, "xxx", true)
                    , new BooleanConfigParam("REBALANCE_MAINTENANCE_ENABLE", true, true)));

            Calendar calendar = Calendar.getInstance();
            InstConfUtil.isInMaintenanceTimeWindow(calendar,
                new StringConfigParam(ConnectionProperties.REBALANCE_MAINTENANCE_TIME_START, "00:00:00", true),
                new StringConfigParam(ConnectionProperties.REBALANCE_MAINTENANCE_TIME_END, "-01:00:00", true));
        }
    }
}