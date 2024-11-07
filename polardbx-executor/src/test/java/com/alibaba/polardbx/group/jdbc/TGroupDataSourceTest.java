package com.alibaba.polardbx.group.jdbc;

import com.alibaba.polardbx.atom.TAtomDataSource;
import com.alibaba.polardbx.gms.listener.ConfigManager;
import com.alibaba.polardbx.group.config.OptimizedGroupConfigManager;
import com.alibaba.polardbx.group.config.Weight;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.HashMap;
import java.util.Map;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TGroupDataSourceTest {
    @Test
    public void testHashCode() {
        TGroupDataSource tGroupDataSource = new TGroupDataSource("abc", "def",
            "gh", "ijk");
        Map<String, DataSourceWrapper> dataSourceWrapperMap = new HashMap<>();

        // Mock the configManager and its behavior
        OptimizedGroupConfigManager mockConfigManager = Mockito.mock(OptimizedGroupConfigManager.class);

        // Assume DataSourceWrapper class has a constructor that accepts TAtomDataSource
        DataSourceWrapper mockWrapper = Mockito.mock(DataSourceWrapper.class);

        Weight mockWeight = Mockito.mock(Weight.class);
        when(mockWrapper.getWeight()).thenReturn(mockWeight);

        TAtomDataSource mockAtomDataSource = mock(TAtomDataSource.class);
        when(mockAtomDataSource.getHost()).thenReturn("localhost");
        when(mockAtomDataSource.getPort()).thenReturn("3306");

        dataSourceWrapperMap.put("data_source_1", mockWrapper);

        when(mockConfigManager.getDataSourceWrapperMap()).thenReturn(dataSourceWrapperMap);

        tGroupDataSource.setConfigManager(mockConfigManager);
        Assert.assertEquals(98475778L, tGroupDataSource.hashCode());
    }

    @Test
    public void testHashCode2() {
        TGroupDataSource tGroupDataSource = new TGroupDataSource(null, null,
            "gh", "ijk");
        Map<String, DataSourceWrapper> dataSourceWrapperMap = new HashMap<>();

        OptimizedGroupConfigManager mockConfigManager = Mockito.mock(OptimizedGroupConfigManager.class);

        when(mockConfigManager.getDataSourceWrapperMap()).thenReturn(dataSourceWrapperMap);

        tGroupDataSource.setConfigManager(mockConfigManager);
        Assert.assertEquals(29791, tGroupDataSource.hashCode());
    }
}
