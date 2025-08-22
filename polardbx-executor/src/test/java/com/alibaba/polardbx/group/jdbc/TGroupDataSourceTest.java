package com.alibaba.polardbx.group.jdbc;

import com.alibaba.polardbx.atom.TAtomDataSource;
import com.alibaba.polardbx.atom.config.TAtomDsConfDO;
import com.alibaba.polardbx.atom.config.gms.TAtomDsGmsConfigHelper;
import com.alibaba.polardbx.common.exception.TddlNestableRuntimeException;
import com.alibaba.polardbx.common.jdbc.IConnection;
import com.alibaba.polardbx.common.jdbc.MasterSlave;
import com.alibaba.polardbx.common.properties.ConnectionProperties;
import com.alibaba.polardbx.common.properties.DynamicConfig;
import com.alibaba.polardbx.gms.config.impl.ConnPoolConfig;
import com.alibaba.polardbx.group.config.OptimizedGroupConfigManager;
import com.alibaba.polardbx.group.config.Weight;
import com.alibaba.polardbx.rpc.compatible.XDataSource;
import com.alibaba.polardbx.rpc.pool.XConnection;
import com.google.common.collect.ImmutableList;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
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

    public final static String SERVER_ADDR = "11.167.60.147:14120";
    public final static int SERVER_PORT = 42120;
    public final static String SERVER_USR = "diamond";
    public final static String SERVER_PSW_ENC = "STA0HLmViwmos2woaSzweB9QM13NjZgrOtXTYx+ZzLw=";
    public final static String SERVER_DB = "test";

    private TAtomDataSource atomDS(TAtomDataSource.AtomSourceFrom sourceFrom, String dnId, int xport) {
        ConnPoolConfig storageInstConfig = new ConnPoolConfig();
        storageInstConfig.minPoolSize = 1;
        storageInstConfig.maxPoolSize = 1;
        storageInstConfig.maxWaitThreadCount = 1;
        storageInstConfig.idleTimeout = 60000;
        storageInstConfig.blockTimeout = 5000;
        storageInstConfig.connProps = "";
        storageInstConfig.xprotoStorageDbPort = 0;
        TAtomDsConfDO atomDsConf =
            TAtomDsGmsConfigHelper.buildAtomDsConfByGms(SERVER_ADDR, xport, SERVER_USR, SERVER_PSW_ENC, SERVER_DB,
                storageInstConfig, SERVER_DB);
        atomDsConf.setXport(xport);
        atomDsConf.setCharacterEncoding("utf8mb4");
        TAtomDataSource atomDs = new TAtomDataSource(sourceFrom, dnId);
        atomDs.init("app", "group" + dnId, "dsKey" + dnId, "", atomDsConf);
        return atomDs;
    }

    @Test
    public void testGroupConnection() throws SQLException {
        final TGroupDataSource tGroupDataSource = new TGroupDataSource("abc", "def",
            "gh", "ijk");
        final TAtomDataSource master = atomDS(TAtomDataSource.AtomSourceFrom.MASTER_DB, "dn_real", SERVER_PORT);

        // Mock the configManager and its behavior
        final OptimizedGroupConfigManager mockConfigManager = Mockito.mock(OptimizedGroupConfigManager.class);
        when(mockConfigManager.getDataSource(any())).thenReturn(master);
        tGroupDataSource.setConfigManager(mockConfigManager);

        try (final Connection conn = tGroupDataSource.getConnection(MasterSlave.MASTER_ONLY, null)) {
            Assert.assertEquals(master.getDataSource(), conn.unwrap(XConnection.class).getDataSource());
        }
        try (final Connection conn = tGroupDataSource.getConnection(MasterSlave.MASTER_ONLY, ImmutableList.of())) {
            Assert.assertEquals(master.getDataSource(), conn.unwrap(XConnection.class).getDataSource());
        }

        DynamicConfig.getInstance().loadValue(null, ConnectionProperties.SWITCHOVER_WAIT_TIMEOUT_IN_MILLIS, "1000");

        master.getDataSource().unwrap(XDataSource.class).getClientPool().markChangingLeader();
        try (final IConnection exist = tGroupDataSource.getConnection(MasterSlave.MASTER_ONLY, null)) {
            try (final Connection conn = tGroupDataSource.getConnection(MasterSlave.MASTER_ONLY,
                ImmutableList.of(exist))) {
                Assert.assertEquals(master.getDataSource(), conn.unwrap(XConnection.class).getDataSource());
            }
        }

        final TGroupDirectConnection mockConn = Mockito.mock(TGroupDirectConnection.class);
        when(mockConn.isWrapperFor(any())).thenThrow(new RuntimeException("mock throw"));
        try (final Connection conn = tGroupDataSource.getConnection(MasterSlave.MASTER_ONLY,
            ImmutableList.of(mockConn))) {
            Assert.assertEquals(master.getDataSource(), conn.unwrap(XConnection.class).getDataSource());
        }

        Thread.currentThread().interrupt();
        try (final Connection conn = tGroupDataSource.getConnection(MasterSlave.MASTER_ONLY, ImmutableList.of())) {
            Assert.assertEquals(master.getDataSource(), conn.unwrap(XConnection.class).getDataSource());
        } catch (TddlNestableRuntimeException e) {
            Assert.assertTrue(e.getCause() instanceof InterruptedException);
        }
    }
}
