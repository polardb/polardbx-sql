package com.alibaba.polardbx.config.loader;

import com.alibaba.polardbx.CobarConfig;
import com.alibaba.polardbx.CobarServer;
import com.alibaba.polardbx.common.properties.ConnectionProperties;
import com.alibaba.polardbx.config.SystemConfig;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.Properties;

public class ClusterLoaderTest {
    private ClusterLoader clusterLoader;
    private MockedStatic<MetaDbUtil> metaDbUtilMockedStatic;
    private MockedStatic<CobarServer> cobarServerMockedStatic;
    private MockedConstruction<ServerLoader> serverLoaderMockedCtor;

    @Before
    public void setUp() throws Exception {
        SystemConfig systemConfig = new SystemConfig();
        systemConfig.setClusterName("test_cluster");
        systemConfig.setUnitName("test_unit");

        clusterLoader = new GmsClusterLoader(systemConfig);
        metaDbUtilMockedStatic = Mockito.mockStatic(MetaDbUtil.class);
        metaDbUtilMockedStatic.when(MetaDbUtil::getConnection).thenReturn(null);
        serverLoaderMockedCtor = Mockito.mockConstruction(ServerLoader.class, (mock, context) -> {
            Mockito.when(mock.getSystem()).thenReturn(systemConfig);
        });

        CobarServer cobarServer = Mockito.mock(CobarServer.class);
        CobarConfig cobarConfig = Mockito.mock(CobarConfig.class);
        Mockito.when(cobarServer.getConfig()).thenReturn(cobarConfig);
        Mockito.when(cobarConfig.getSystem()).thenReturn(systemConfig);
        cobarServerMockedStatic = Mockito.mockStatic(CobarServer.class);
        cobarServerMockedStatic.when(CobarServer::getInstance).thenReturn(cobarServer);
    }

    @After
    public void tearDown() throws Exception {
        if (clusterLoader != null) {
            clusterLoader.destroy();
        }
        if (metaDbUtilMockedStatic != null) {
            metaDbUtilMockedStatic.close();
        }
        if (cobarServerMockedStatic != null) {
            cobarServerMockedStatic.close();
        }
    }

    @Test
    public void testResetCsvCacheSize() {
        Properties p = new Properties();
        p.setProperty(ConnectionProperties.CSV_CACHE_SIZE, "1024");
        clusterLoader.applyProperties(p);
        p.setProperty(ConnectionProperties.CSV_CACHE_SIZE, "2048");
        clusterLoader.applyProperties(p);
    }
}