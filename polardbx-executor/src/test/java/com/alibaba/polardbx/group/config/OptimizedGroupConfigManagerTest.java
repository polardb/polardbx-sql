package com.alibaba.polardbx.group.config;

import com.alibaba.polardbx.atom.TAtomDataSource;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.gms.ha.HaSwitchParams;
import com.alibaba.polardbx.gms.ha.HaSwitcher;
import com.alibaba.polardbx.gms.util.GroupInfoUtil;
import com.alibaba.polardbx.group.jdbc.DataSourceWrapper;
import com.alibaba.polardbx.group.jdbc.TGroupDataSource;
import com.alibaba.polardbx.server.SwitchoverManager;
import com.alibaba.polardbx.server.response.OnDnChangeLeaderAction;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.Map;

import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

public class OptimizedGroupConfigManagerTest {

    @Test
    public void testPolarDBXSourceHolderInMasterMode() {
        TGroupDataSource groupDataSource = Mockito.mock(TGroupDataSource.class);
        when(groupDataSource.getSchemaName()).thenReturn("app");
        OptimizedGroupConfigManager configManager = new OptimizedGroupConfigManager(groupDataSource);

        initDataSourceWrapper(1, 0, configManager);
        configManager.resetPolarDBXSourceHolder(true);
        GroupDataSourceHolder groupDataSourceHolder = configManager.getGroupDataSourceHolder();
        Assert.assertTrue(groupDataSourceHolder instanceof MasterFailedSlaveGroupDataSourceHolder);

        configManager.resetPolarDBXSourceHolder(false);
        groupDataSourceHolder = configManager.getGroupDataSourceHolder();
        Assert.assertTrue(groupDataSourceHolder instanceof MasterOnlyGroupDataSourceHolder);

        initDataSourceWrapper(1, 1, configManager);
        configManager.resetPolarDBXSourceHolder(true);
        groupDataSourceHolder = configManager.getGroupDataSourceHolder();
        Assert.assertTrue(groupDataSourceHolder instanceof MasterSlaveGroupDataSourceHolder);
    }

    @Test
    public void testPolarDBXSourceHolderInSlaveMode() {
        try (MockedStatic<ConfigDataMode> configDataMode = Mockito.mockStatic(ConfigDataMode.class)) {
            configDataMode.when(() -> ConfigDataMode.isReadOnlyMode()).thenReturn(true);
            TGroupDataSource groupDataSource = Mockito.mock(TGroupDataSource.class);
            when(groupDataSource.getSchemaName()).thenReturn("app");
            OptimizedGroupConfigManager configManager = new OptimizedGroupConfigManager(groupDataSource);

            initDataSourceWrapper(1, 0, configManager);
            configManager.resetPolarDBXSourceHolder(true);
            GroupDataSourceHolder groupDataSourceHolder = configManager.getGroupDataSourceHolder();
            Assert.assertTrue(groupDataSourceHolder instanceof MasterFailedSlaveGroupDataSourceHolder);

            configManager.resetPolarDBXSourceHolder(false);
            groupDataSourceHolder = configManager.getGroupDataSourceHolder();
            Assert.assertTrue(groupDataSourceHolder instanceof MasterFailedSlaveGroupDataSourceHolder);

            initDataSourceWrapper(1, 1, configManager);
            configManager.resetPolarDBXSourceHolder(true);
            groupDataSourceHolder = configManager.getGroupDataSourceHolder();
            Assert.assertTrue(groupDataSourceHolder instanceof MasterSlaveGroupDataSourceHolder);
        }

    }

    private void initDataSourceWrapper(int masterNum, int slaveNum, OptimizedGroupConfigManager configManager) {
        Map<String, DataSourceWrapper> dataSourceWrapperMap = configManager.getDataSourceWrapperMap();
        dataSourceWrapperMap.clear();
        for (int i = 0; i < masterNum; i++) {
            TAtomDataSource tAtomDataSource = Mockito.mock(TAtomDataSource.class);
            when(tAtomDataSource.isFollowerDB()).thenReturn(false);
            when(tAtomDataSource.getDnId()).thenReturn("m" + i);
            DataSourceWrapper dataSourceWrapper = new DataSourceWrapper(
                "m" + i, "a", GroupInfoUtil.buildWeightStr(10, 10), tAtomDataSource, 0);
            dataSourceWrapperMap.put("master" + i, dataSourceWrapper);
        }

        for (int i = 0; i < slaveNum; i++) {
            TAtomDataSource tAtomDataSource = Mockito.mock(TAtomDataSource.class);
            when(tAtomDataSource.isFollowerDB()).thenReturn(false);
            when(tAtomDataSource.getDnId()).thenReturn("m" + i);
            DataSourceWrapper dataSourceWrapper = new DataSourceWrapper(
                "s" + i, "a", GroupInfoUtil.buildWeightStr(10, 0), tAtomDataSource, 0);
            dataSourceWrapperMap.put("slave" + i, dataSourceWrapper);
        }
    }

    @Test
    public void testSwitchover0() {
        final TGroupDataSource tGroupDataSource = new TGroupDataSource("abc", "def",
            "gh", "ijk");
        final OptimizedGroupConfigManager optimizedGroupConfigManager =
            new OptimizedGroupConfigManager(tGroupDataSource);
        final HaSwitcher sw = optimizedGroupConfigManager.getGroupDsSwithcer();
        sw.doHaSwitch(new HaSwitchParams());
    }

    @Test
    public void testSwitchover1() {
        final TGroupDataSource tGroupDataSource = new TGroupDataSource("abc", "def",
            "gh", "ijk");
        final OptimizedGroupConfigManager optimizedGroupConfigManager =
            new OptimizedGroupConfigManager(tGroupDataSource);
        final HaSwitcher sw = optimizedGroupConfigManager.getGroupDsSwithcer();

        try (final MockedStatic<SwitchoverManager> switchoverManagerMockedStatic = mockStatic(SwitchoverManager.class);
            final MockedStatic<OnDnChangeLeaderAction> onDnChangeLeaderActionMockedStatic = mockStatic(
                OnDnChangeLeaderAction.class)) {
            switchoverManagerMockedStatic.when(SwitchoverManager::rescheduleAll).thenAnswer(i -> null);
            onDnChangeLeaderActionMockedStatic.when(() -> OnDnChangeLeaderAction.onDnLeaderChanging(anyBoolean()))
                .thenAnswer(i -> null);

            sw.doHaSwitch(new HaSwitchParams());
        }
    }
}
