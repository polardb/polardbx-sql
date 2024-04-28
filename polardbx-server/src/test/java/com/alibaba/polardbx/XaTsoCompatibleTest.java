package com.alibaba.polardbx;

import com.alibaba.polardbx.common.properties.ConnectionProperties;
import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.gms.metadb.MetaDbDataSource;
import com.alibaba.polardbx.gms.topology.VariableConfigAccessor;
import com.alibaba.polardbx.gms.topology.VariableConfigRecord;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import org.junit.Test;
import org.junit.jupiter.api.condition.DisabledOnJre;
import org.junit.jupiter.api.condition.EnabledOnJre;
import org.junit.jupiter.api.condition.JRE;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.mockito.ArgumentMatchers.any;

public class XaTsoCompatibleTest {
    @Test
    @DisabledOnJre(JRE.JAVA_8)
    public void testHotSpotOn() throws Exception {
        List<VariableConfigRecord> records = new ArrayList<>();
        VariableConfigRecord record0 = new VariableConfigRecord();
        record0.paramKey = "hotspot";
        record0.paramValue = "on";
        records.add(record0);
        try (MockedConstruction<CobarServer> mocked = Mockito.mockConstruction(CobarServer.class);
            MockedStatic<MetaDbDataSource> metaDbDataSourceMockedStatic = Mockito.mockStatic(MetaDbDataSource.class);
            MockedStatic<MetaDbUtil> metaDbUtilMockedStatic = Mockito.mockStatic(MetaDbUtil.class);
            MockedConstruction<VariableConfigAccessor> variableConfigAccessorMockedConstruction =
                Mockito.mockConstruction(VariableConfigAccessor.class, (accessor, context) -> {
                    Mockito.when(accessor.queryAll()).thenReturn(records);
                })) {

            AtomicBoolean disableXaTso = new AtomicBoolean(false);
            metaDbDataSourceMockedStatic.when(MetaDbDataSource::getInstance).thenReturn(new MockedMetaDbInstance());
            metaDbUtilMockedStatic.when(() -> MetaDbUtil.setGlobal(any(Properties.class))).then(
                args -> {
                    Properties properties = args.getArgument(0);
                    String enableXaTso = properties.getProperty(ConnectionProperties.ENABLE_XA_TSO);
                    if (null != enableXaTso && !Boolean.parseBoolean(enableXaTso)) {
                        disableXaTso.set(true);
                    }
                    return null;
                }
            );

            CobarServer.checkCompatible();

            Assert.assertTrue(disableXaTso.get());
        }
    }

    @Test
    @DisabledOnJre(JRE.JAVA_8)
    public void testHotSpotLockTypeOn() throws Exception {
        List<VariableConfigRecord> records = new ArrayList<>();
        VariableConfigRecord record0 = new VariableConfigRecord();
        record0.paramKey = "hotspot_lock_type";
        record0.paramValue = "on";
        records.add(record0);
        try (MockedConstruction<CobarServer> mocked = Mockito.mockConstruction(CobarServer.class);
            MockedStatic<MetaDbDataSource> metaDbDataSourceMockedStatic = Mockito.mockStatic(MetaDbDataSource.class);
            MockedStatic<MetaDbUtil> metaDbUtilMockedStatic = Mockito.mockStatic(MetaDbUtil.class);
            MockedConstruction<VariableConfigAccessor> variableConfigAccessorMockedConstruction =
                Mockito.mockConstruction(VariableConfigAccessor.class, (accessor, context) -> {
                    Mockito.when(accessor.queryAll()).thenReturn(records);
                })) {

            AtomicBoolean disableXaTso = new AtomicBoolean(false);
            metaDbDataSourceMockedStatic.when(MetaDbDataSource::getInstance).thenReturn(new MockedMetaDbInstance());
            metaDbUtilMockedStatic.when(() -> MetaDbUtil.setGlobal(any(Properties.class))).then(
                args -> {
                    Properties properties = args.getArgument(0);
                    String enableXaTso = properties.getProperty(ConnectionProperties.ENABLE_XA_TSO);
                    if (null != enableXaTso && !Boolean.parseBoolean(enableXaTso)) {
                        disableXaTso.set(true);
                    }
                    return null;
                }
            );

            CobarServer.checkCompatible();

            Assert.assertTrue(disableXaTso.get());
        }
    }

    @Test
    @DisabledOnJre(JRE.JAVA_8)
    public void testHotSpotOff() throws Exception {
        List<VariableConfigRecord> records = new ArrayList<>();
        VariableConfigRecord record0 = new VariableConfigRecord();
        record0.paramKey = "hotspot";
        record0.paramValue = "off";
        records.add(record0);
        try (MockedConstruction<CobarServer> mocked = Mockito.mockConstruction(CobarServer.class);
            MockedStatic<MetaDbDataSource> metaDbDataSourceMockedStatic = Mockito.mockStatic(MetaDbDataSource.class);
            MockedStatic<MetaDbUtil> metaDbUtilMockedStatic = Mockito.mockStatic(MetaDbUtil.class);
            MockedConstruction<VariableConfigAccessor> variableConfigAccessorMockedConstruction =
                Mockito.mockConstruction(VariableConfigAccessor.class, (accessor, context) -> {
                    Mockito.when(accessor.queryAll()).thenReturn(records);
                })) {

            AtomicBoolean disableXaTso = new AtomicBoolean(false);
            metaDbDataSourceMockedStatic.when(MetaDbDataSource::getInstance).thenReturn(new MockedMetaDbInstance());
            metaDbUtilMockedStatic.when(() -> MetaDbUtil.setGlobal(any(Properties.class))).then(
                args -> {
                    Properties properties = args.getArgument(0);
                    String enableXaTso = properties.getProperty(ConnectionProperties.ENABLE_XA_TSO);
                    if (null != enableXaTso && !Boolean.parseBoolean(enableXaTso)) {
                        disableXaTso.set(true);
                    }
                    return null;
                }
            );

            CobarServer.checkCompatible();

            Assert.assertTrue(!disableXaTso.get());
        }
    }

    @Test
    @DisabledOnJre(JRE.JAVA_8)
    public void testNoHotSpot() throws Exception {
        List<VariableConfigRecord> records = new ArrayList<>();
        VariableConfigRecord record0 = new VariableConfigRecord();
        record0.paramKey = "a";
        record0.paramValue = "b";
        records.add(record0);
        try (MockedConstruction<CobarServer> mocked = Mockito.mockConstruction(CobarServer.class);
            MockedStatic<MetaDbDataSource> metaDbDataSourceMockedStatic = Mockito.mockStatic(MetaDbDataSource.class);
            MockedStatic<MetaDbUtil> metaDbUtilMockedStatic = Mockito.mockStatic(MetaDbUtil.class);
            MockedConstruction<VariableConfigAccessor> variableConfigAccessorMockedConstruction =
                Mockito.mockConstruction(VariableConfigAccessor.class, (accessor, context) -> {
                    Mockito.when(accessor.queryAll()).thenReturn(records);
                })) {

            AtomicBoolean disableXaTso = new AtomicBoolean(false);
            metaDbDataSourceMockedStatic.when(MetaDbDataSource::getInstance).thenReturn(new MockedMetaDbInstance());
            metaDbUtilMockedStatic.when(() -> MetaDbUtil.setGlobal(any(Properties.class))).then(
                args -> {
                    Properties properties = args.getArgument(0);
                    String enableXaTso = properties.getProperty(ConnectionProperties.ENABLE_XA_TSO);
                    if (null != enableXaTso && !Boolean.parseBoolean(enableXaTso)) {
                        disableXaTso.set(true);
                    }
                    return null;
                }
            );

            CobarServer.checkCompatible();

            Assert.assertTrue(!disableXaTso.get());
        }
    }

    static class MockedMetaDbInstance extends MetaDbDataSource {
        protected MockedMetaDbInstance() {
            super("addrListStr", "dbName", "properties", "user", "passwd");
        }

        public Connection getConnection() {
            return new MockedConnection();
        }
    }

    static class MockedConnection implements Connection {

        @Override
        public Statement createStatement() throws SQLException {
            return null;
        }

        @Override
        public PreparedStatement prepareStatement(String sql) throws SQLException {
            return null;
        }

        @Override
        public CallableStatement prepareCall(String sql) throws SQLException {
            return null;
        }

        @Override
        public String nativeSQL(String sql) throws SQLException {
            return null;
        }

        @Override
        public void setAutoCommit(boolean autoCommit) throws SQLException {

        }

        @Override
        public boolean getAutoCommit() throws SQLException {
            return false;
        }

        @Override
        public void commit() throws SQLException {

        }

        @Override
        public void rollback() throws SQLException {

        }

        @Override
        public void close() throws SQLException {

        }

        @Override
        public boolean isClosed() throws SQLException {
            return false;
        }

        @Override
        public DatabaseMetaData getMetaData() throws SQLException {
            return null;
        }

        @Override
        public void setReadOnly(boolean readOnly) throws SQLException {

        }

        @Override
        public boolean isReadOnly() throws SQLException {
            return false;
        }

        @Override
        public void setCatalog(String catalog) throws SQLException {

        }

        @Override
        public String getCatalog() throws SQLException {
            return null;
        }

        @Override
        public void setTransactionIsolation(int level) throws SQLException {

        }

        @Override
        public int getTransactionIsolation() throws SQLException {
            return 0;
        }

        @Override
        public SQLWarning getWarnings() throws SQLException {
            return null;
        }

        @Override
        public void clearWarnings() throws SQLException {

        }

        @Override
        public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
            return null;
        }

        @Override
        public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency)
            throws SQLException {
            return null;
        }

        @Override
        public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency)
            throws SQLException {
            return null;
        }

        @Override
        public Map<String, Class<?>> getTypeMap() throws SQLException {
            return null;
        }

        @Override
        public void setTypeMap(Map<String, Class<?>> map) throws SQLException {

        }

        @Override
        public void setHoldability(int holdability) throws SQLException {

        }

        @Override
        public int getHoldability() throws SQLException {
            return 0;
        }

        @Override
        public Savepoint setSavepoint() throws SQLException {
            return null;
        }

        @Override
        public Savepoint setSavepoint(String name) throws SQLException {
            return null;
        }

        @Override
        public void rollback(Savepoint savepoint) throws SQLException {

        }

        @Override
        public void releaseSavepoint(Savepoint savepoint) throws SQLException {

        }

        @Override
        public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability)
            throws SQLException {
            return null;
        }

        @Override
        public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency,
                                                  int resultSetHoldability) throws SQLException {
            return null;
        }

        @Override
        public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency,
                                             int resultSetHoldability) throws SQLException {
            return null;
        }

        @Override
        public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
            return null;
        }

        @Override
        public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
            return null;
        }

        @Override
        public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
            return null;
        }

        @Override
        public Clob createClob() throws SQLException {
            return null;
        }

        @Override
        public Blob createBlob() throws SQLException {
            return null;
        }

        @Override
        public NClob createNClob() throws SQLException {
            return null;
        }

        @Override
        public SQLXML createSQLXML() throws SQLException {
            return null;
        }

        @Override
        public boolean isValid(int timeout) throws SQLException {
            return false;
        }

        @Override
        public void setClientInfo(String name, String value) throws SQLClientInfoException {

        }

        @Override
        public void setClientInfo(Properties properties) throws SQLClientInfoException {

        }

        @Override
        public String getClientInfo(String name) throws SQLException {
            return null;
        }

        @Override
        public Properties getClientInfo() throws SQLException {
            return null;
        }

        @Override
        public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
            return null;
        }

        @Override
        public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
            return null;
        }

        @Override
        public void setSchema(String schema) throws SQLException {

        }

        @Override
        public String getSchema() throws SQLException {
            return null;
        }

        @Override
        public void abort(Executor executor) throws SQLException {

        }

        @Override
        public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {

        }

        @Override
        public int getNetworkTimeout() throws SQLException {
            return 0;
        }

        @Override
        public <T> T unwrap(Class<T> iface) throws SQLException {
            return null;
        }

        @Override
        public boolean isWrapperFor(Class<?> iface) throws SQLException {
            return false;
        }
    }
}
