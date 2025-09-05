package com.alibaba.polardbx.executor.scheduler;

import com.alibaba.polardbx.common.columnar.ColumnarUtils;
import com.alibaba.polardbx.common.properties.DynamicConfig;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.executor.utils.ExecUtils;
import com.alibaba.polardbx.gms.util.SyncUtil;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.Trigger;
import org.quartz.TriggerKey;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.mockito.ArgumentMatchers.any;

public class AutoSnapshotManagerTest {
    @Test
    public void test()
        throws InterruptedException, NoSuchMethodException, InvocationTargetException, IllegalAccessException,
        NoSuchFieldException, SchedulerException {
        try (MockedStatic<ExecUtils> execUtilsMockedStatic = Mockito.mockStatic(ExecUtils.class);
            MockedStatic<ColumnarUtils> columnarUtilsMockedStatic = Mockito.mockStatic(ColumnarUtils.class);
            MockedStatic<DynamicConfig> dynamicConfigMockedStatic = Mockito.mockStatic(DynamicConfig.class);
            MockedStatic<ConfigDataMode> configDataModeMockedStatic = Mockito.mockStatic(ConfigDataMode.class);
            MockedStatic<SyncUtil> syncUtilMockedStatic = Mockito.mockStatic(SyncUtil.class)) {
            AtomicInteger counter = new AtomicInteger(0);
            Map<String, String> config = new HashMap<>();
            config.put("ZONE_ID", "+08:00");
            config.put("CRON_EXPR", "* * * * * ?");
            execUtilsMockedStatic.when(ExecUtils::getColumnarAutoSnapshotConfig).thenReturn(config);

            Method scan = AutoSnapshotManager.class.getDeclaredMethod("scan");
            scan.setAccessible(true);
            Field cronExpr = AutoSnapshotManager.class.getDeclaredField("cronExpr");
            cronExpr.setAccessible(true);
            Field zoneId = AutoSnapshotManager.class.getDeclaredField("zoneId");
            zoneId.setAccessible(true);
            Field scheduler = AutoSnapshotManager.class.getDeclaredField("scheduler");
            scheduler.setAccessible(true);
            Scheduler scheduler0 = (Scheduler) scheduler.get(null);
            Trigger old = scheduler0.getTrigger(new TriggerKey("AutoSnapshotTrigger"));

            //  case 1: init with config
            Assert.assertNull(old);
            Assert.assertNull(cronExpr.get(null));
            Assert.assertNull(zoneId.get(null));
            scan.invoke(null);
            Assert.assertEquals("* * * * * ?", cronExpr.get(null));
            Assert.assertEquals("+08:00", zoneId.get(null));
            old = scheduler0.getTrigger(new TriggerKey("AutoSnapshotTrigger"));
            Assert.assertNotNull(old);

            // case 2: change config
            config.put("ZONE_ID", "+06:00");
            config.put("CRON_EXPR", "0/1 * * * * ?");
            scan.invoke(null);
            Assert.assertEquals("0/1 * * * * ?", cronExpr.get(null));
            Assert.assertEquals("+06:00", zoneId.get(null));
            // compare address
            Assert.assertTrue(old != scheduler0.getTrigger(new TriggerKey("AutoSnapshotTrigger")));

            AutoSnapshotManager.ColumnarFlushJob job = new AutoSnapshotManager.ColumnarFlushJob();
            columnarUtilsMockedStatic.when(() -> ColumnarUtils.AddCDCMarkEventForColumnar(any(), any())).then(
                invocation -> {
                    counter.incrementAndGet();
                    return null;
                }
            );
            DynamicConfig dynamicConfig = Mockito.mock(DynamicConfig.class);
            dynamicConfigMockedStatic.when(DynamicConfig::getInstance).thenReturn(dynamicConfig);

            // case 3: current instance is leader of master instance, should run task.
            Mockito.when(dynamicConfig.isEnableColumnarReadInstanceAutoGenerateSnapshot()).thenReturn(false);
            execUtilsMockedStatic.when(() -> ExecUtils.hasLeadership(any())).thenReturn(true);
            int before = counter.get();
            job.execute(null);
            Assert.assertTrue(counter.get() > before);

            // case 4: current instance is non-leader of master instance, should not run task.
            Mockito.when(dynamicConfig.isEnableColumnarReadInstanceAutoGenerateSnapshot()).thenReturn(false);
            execUtilsMockedStatic.when(() -> ExecUtils.hasLeadership(any())).thenReturn(false);
            before = counter.get();
            job.execute(null);
            Assert.assertEquals(counter.get(), before);

            // case 5: current instance is leader of master instance, but option on, should not run task.
            Mockito.when(dynamicConfig.isEnableColumnarReadInstanceAutoGenerateSnapshot()).thenReturn(true);
            configDataModeMockedStatic.when(ConfigDataMode::isColumnarMode).thenReturn(false);
            execUtilsMockedStatic.when(() -> ExecUtils.hasLeadership(any())).thenReturn(true);
            before = counter.get();
            job.execute(null);
            Assert.assertEquals(counter.get(), before);

            // case 6: current instance is leader of columnar instance, and option on, should run task.
            Mockito.when(dynamicConfig.isEnableColumnarReadInstanceAutoGenerateSnapshot()).thenReturn(true);
            configDataModeMockedStatic.when(ConfigDataMode::isColumnarMode).thenReturn(true);
            syncUtilMockedStatic.when(SyncUtil::isNodeWithSmallestId).thenReturn(true);
            execUtilsMockedStatic.when(() -> ExecUtils.hasLeadership(any())).thenReturn(false);
            before = counter.get();
            job.execute(null);
            Assert.assertTrue(counter.get() > before);

            // case 7: current instance is leader of columnar instance, but option off, should not run task.
            Mockito.when(dynamicConfig.isEnableColumnarReadInstanceAutoGenerateSnapshot()).thenReturn(true);
            configDataModeMockedStatic.when(ConfigDataMode::isColumnarMode).thenReturn(true);
            syncUtilMockedStatic.when(SyncUtil::isNodeWithSmallestId).thenReturn(false);
            execUtilsMockedStatic.when(() -> ExecUtils.hasLeadership(any())).thenReturn(false);
            before = counter.get();
            job.execute(null);
            Assert.assertEquals(counter.get(), before);

            AutoSnapshotManager.init();

        }
    }
}
