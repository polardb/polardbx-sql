package com.alibaba.polardbx.executor.common;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.jdbc.IConnection;
import com.alibaba.polardbx.common.jdbc.IDataSource;
import com.alibaba.polardbx.common.model.Group;
import com.alibaba.polardbx.common.model.Group.GroupType;
import com.alibaba.polardbx.common.model.Matrix;
import com.alibaba.polardbx.executor.spi.IGroupExecutor;
import com.alibaba.polardbx.gms.topology.DbGroupInfoManager;
import com.alibaba.polardbx.group.jdbc.TGroupDataSource;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.lang.reflect.Field;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class StorageInfoManagerTest {
    @Mock
    private IGroupExecutor groupExecutor;
    @Mock
    private Group group;
    @Mock
    private IDataSource dataSource;

    @Before
    public void setUp() {
        MockitoAnnotations.openMocks(this);
        // Initialize mocks here if needed
    }

    @Test
    public void testDoInitWithDefaultValue() throws NoSuchFieldException, IllegalAccessException {
        TopologyHandler topologyHandler = new TopologyHandler("test", "test", "test", new ExecutorContext());
        StorageInfoManager storageInfoManager = new StorageInfoManager(topologyHandler);
        when(groupExecutor.getDataSource()).thenReturn(dataSource);
        when(group.getType()).thenReturn(GroupType.MYSQL_JDBC);

        storageInfoManager.doInit();

        // modify isInited through reflection
        Class<?> parentClass = storageInfoManager.getClass().getSuperclass();
        Field field = parentClass.getDeclaredField("isInited");
        if (field.isAccessible() == false) {
            field.setAccessible(true);
        }
        field.set(storageInfoManager, true);

        // Verify the expected value here
        Assert.assertTrue(storageInfoManager.supportXA());
        Assert.assertTrue(storageInfoManager.supportTso());
        Assert.assertTrue(!storageInfoManager.supportPurgeTso());
        Assert.assertTrue(!storageInfoManager.isLessMy56Version());
        Assert.assertTrue(storageInfoManager.isMysql80());
        Assert.assertTrue(!storageInfoManager.supportTsoHeartbeat());
        Assert.assertTrue(storageInfoManager.supportCtsTransaction());
        Assert.assertTrue(storageInfoManager.supportAsyncCommit());
        Assert.assertTrue(storageInfoManager.supportLizard1PCTransaction());
        Assert.assertTrue(storageInfoManager.supportDeadlockDetection());
        Assert.assertTrue(storageInfoManager.supportMdlDeadlockDetection());
        Assert.assertTrue(storageInfoManager.supportsBloomFilter());
        Assert.assertTrue(storageInfoManager.supportSharedReadView());
        Assert.assertTrue(storageInfoManager.supportOpenSSL());
        Assert.assertTrue(storageInfoManager.supportsHyperLogLog());
        Assert.assertTrue(storageInfoManager.supportsXxHash());
        Assert.assertTrue(storageInfoManager.supportsReturning());
        Assert.assertTrue(storageInfoManager.supportsBackfillReturning());
        Assert.assertTrue(storageInfoManager.supportsAlterType());
        Assert.assertTrue(!storageInfoManager.isReadOnly());
        Assert.assertTrue(storageInfoManager.isLowerCaseTableNames());
        Assert.assertTrue(storageInfoManager.supportFastChecker());
        Assert.assertTrue(storageInfoManager.supportChangeSet());
        Assert.assertTrue(storageInfoManager.supportXOptForAutoSp());
        Assert.assertTrue(storageInfoManager.supportXRpc());
        Assert.assertTrue(storageInfoManager.isSupportMarkDistributed());
        Assert.assertTrue(storageInfoManager.supportXOptForPhysicalBackfill());
    }

    @Test
    public void testDoInit() {
        StorageInfoManager.StorageInfo storageInfo = new StorageInfoManager.StorageInfo(
            "5.7",
            false,
            false,
            false,
            false,
            false,
            false,
            false,
            false,
            false,
            false,
            1,
            false,
            false,
            false,
            false,
            false,
            false,
            false,
            false,
            false,
            false,
            false,
            false,
            false,
            false,
            true,
            true
        );

        Group group = mock(Group.class);
        when(group.getType()).thenReturn(Group.GroupType.MYSQL_JDBC);
        when(group.getName()).thenReturn("test_group");
        List<Group> groups = new ArrayList<>();
        groups.add(group);

        IGroupExecutor groupExecutor = mock(IGroupExecutor.class);
        IDataSource dataSource = mock(TGroupDataSource.class);
        when(groupExecutor.getDataSource()).thenReturn(dataSource);
        Matrix matrix = mock(Matrix.class);
        TopologyHandler topologyHandler = mock(TopologyHandler.class);
        when(topologyHandler.getMatrix()).thenReturn(matrix);
        when(matrix.getGroups()).thenReturn(groups);
        when(topologyHandler.get(any())).thenReturn(groupExecutor);

        try (MockedStatic<StorageInfoManager.StorageInfo> storageInfoMock =
            Mockito.mockStatic(StorageInfoManager.StorageInfo.class);
            MockedStatic<DbGroupInfoManager> dbGroupInfoManagerMock =
                Mockito.mockStatic(DbGroupInfoManager.class)) {
            storageInfoMock.when(() -> StorageInfoManager.StorageInfo.create(any()))
                .thenReturn(storageInfo);
            dbGroupInfoManagerMock.when(() -> DbGroupInfoManager.isNormalGroup(any()))
                .thenReturn(true);

            StorageInfoManager storageInfoManager = new StorageInfoManager(topologyHandler);
            storageInfoManager.doInit();
            assertTrue(storageInfoManager.isSupportSyncPoint());
        }
    }

    @Test
    public void checkSupportSyncPointTest() throws SQLException {
        IDataSource dataSource = mock(IDataSource.class);
        IConnection connection = mock(IConnection.class);
        Statement statement = mock(Statement.class);
        ResultSet rs = mock(ResultSet.class);
        when(dataSource.getConnection()).thenReturn(connection);
        when(connection.createStatement()).thenReturn(statement);
        when(statement.executeQuery(any())).thenReturn(rs);
        when(rs.next()).thenReturn(true);
        assertTrue(StorageInfoManager.checkSupportSyncPoint(dataSource));

        when(rs.next()).thenReturn(false);
        assertFalse(StorageInfoManager.checkSupportSyncPoint(dataSource));

        when(statement.executeQuery(any())).thenThrow(new SQLException("test"));
        try {
            StorageInfoManager.checkSupportSyncPoint(dataSource);
        } catch (TddlRuntimeException e) {
            assertTrue(e.getMessage().contains("Failed to check sync point support: test"));
        }
    }

    @Test
    public void checkSupportFlashbackAreaTest() throws SQLException {
        IDataSource dataSource = mock(IDataSource.class);
        IConnection connection = mock(IConnection.class);
        Statement statement = mock(Statement.class);
        ResultSet rs = mock(ResultSet.class);
        when(dataSource.getConnection()).thenReturn(connection);
        when(connection.createStatement()).thenReturn(statement);
        when(statement.executeQuery(any())).thenReturn(rs);
        when(rs.next()).thenReturn(true);
        assertTrue(StorageInfoManager.checkSupportFlashbackArea(dataSource));

        when(rs.next()).thenReturn(false);
        assertFalse(StorageInfoManager.checkSupportFlashbackArea(dataSource));

        when(statement.executeQuery(any())).thenThrow(new SQLException("test"));
        try {
            StorageInfoManager.checkSupportFlashbackArea(dataSource);
        } catch (TddlRuntimeException e) {
            assertTrue(e.getMessage().contains("Failed to check flashback area support: test"));
        }
    }
}
