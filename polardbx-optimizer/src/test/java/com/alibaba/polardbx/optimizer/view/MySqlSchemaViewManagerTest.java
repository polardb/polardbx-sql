package com.alibaba.polardbx.optimizer.view;

import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.config.ConfigDataMode;
import org.junit.Test;
import org.mockito.MockedStatic;

import static org.mockito.Mockito.mockStatic;

public class MySqlSchemaViewManagerTest {

    @Test
    public void testViewMangerContainsProcsPriv() {
        MysqlSchemaViewManager mysqlSchemaViewManager = MysqlSchemaViewManager.getInstance();
        try (MockedStatic<ConfigDataMode> configDataModeMockedStatic = mockStatic(ConfigDataMode.class)) {
            configDataModeMockedStatic.when(() -> ConfigDataMode.isPolarDbX()).thenReturn(true);
            mysqlSchemaViewManager.doInit();
            Assert.assertTrue(mysqlSchemaViewManager.select("procs_priv") != null);
        }
    }

    @Test
    public void testUserViewContainsPasswordColumn() {
        MysqlSchemaViewManager mysqlSchemaViewManager = MysqlSchemaViewManager.getInstance();
        try (MockedStatic<ConfigDataMode> configDataModeMockedStatic = mockStatic(ConfigDataMode.class)) {
            configDataModeMockedStatic.when(() -> ConfigDataMode.isPolarDbX()).thenReturn(true);
            mysqlSchemaViewManager.doInit();
            Assert.assertTrue(mysqlSchemaViewManager.select("user").getColumnList().contains("password"));
        }
    }
}
