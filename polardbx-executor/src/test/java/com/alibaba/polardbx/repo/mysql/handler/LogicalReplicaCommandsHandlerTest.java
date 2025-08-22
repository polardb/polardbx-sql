package com.alibaba.polardbx.repo.mysql.handler;

import java.io.IOException;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.utils.PooledHttpHelper;
import com.alibaba.polardbx.executor.handler.LogicalCancelReplicaCheckTableHandler;
import com.alibaba.polardbx.executor.handler.LogicalChangeMasterHandler;
import com.alibaba.polardbx.executor.handler.LogicalChangeReplicationFilterHandler;
import com.alibaba.polardbx.executor.handler.LogicalContinueReplicaCheckTableHandler;
import com.alibaba.polardbx.executor.handler.LogicalPauseReplicaCheckTableHandler;
import com.alibaba.polardbx.executor.handler.LogicalResetReplicaCheckTableHandler;
import com.alibaba.polardbx.executor.handler.LogicalResetSlaveHandler;
import com.alibaba.polardbx.executor.handler.LogicalShowReplicaCheckDiffHandler;
import com.alibaba.polardbx.executor.handler.LogicalShowReplicaCheckProgressHandler;
import com.alibaba.polardbx.executor.handler.LogicalShowSlaveStatusHandler;
import com.alibaba.polardbx.executor.handler.LogicalStartReplicaCheckTableHandler;
import com.alibaba.polardbx.executor.handler.LogicalStartSlaveHandler;
import com.alibaba.polardbx.executor.handler.LogicalStopSlaveHandler;
import com.alibaba.polardbx.net.util.CdcTargetUtil;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.dal.LogicalDal;
import org.apache.calcite.sql.SqlCancelReplicaCheck;
import org.apache.calcite.sql.SqlChangeMaster;
import org.apache.calcite.sql.SqlChangeReplicationFilter;
import org.apache.calcite.sql.SqlContinueReplicaCheck;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlPauseReplicaCheck;
import org.apache.calcite.sql.SqlResetReplicaCheck;
import org.apache.calcite.sql.SqlResetSlave;
import org.apache.calcite.sql.SqlShowReplicaCheckDiff;
import org.apache.calcite.sql.SqlShowReplicaCheckProgress;
import org.apache.calcite.sql.SqlShowSlaveStatus;
import org.apache.calcite.sql.SqlStartReplicaCheck;
import org.apache.calcite.sql.SqlStartSlave;
import org.apache.calcite.sql.SqlStopSlave;
import org.apache.http.entity.ContentType;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.MockitoAnnotations;
import java.util.HashMap;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

public class LogicalReplicaCommandsHandlerTest {

    @InjectMocks
    private LogicalShowSlaveStatusHandler handler1;

    @InjectMocks
    private LogicalCancelReplicaCheckTableHandler handler2;

    @InjectMocks
    private LogicalChangeMasterHandler handler3;

    @InjectMocks
    private LogicalChangeReplicationFilterHandler handler4;

    @InjectMocks
    private LogicalContinueReplicaCheckTableHandler handler5;

    @InjectMocks
    private LogicalPauseReplicaCheckTableHandler handler6;

    @InjectMocks
    private LogicalResetReplicaCheckTableHandler handler7;

    @InjectMocks
    private LogicalResetSlaveHandler handler8;

    @InjectMocks
    private LogicalShowReplicaCheckDiffHandler handler9;

    @InjectMocks
    private LogicalShowReplicaCheckProgressHandler handler10;

    @InjectMocks
    private LogicalStartReplicaCheckTableHandler handler11;

    @InjectMocks
    private LogicalStartSlaveHandler handler12;

    @InjectMocks
    private LogicalStopSlaveHandler handler13;

    @Mock
    private LogicalDal logicalDal;

    @Mock
    private ExecutionContext executionContext;

    @Before
    public void setUp() {
        MockitoAnnotations.initMocks(this);
    }


    @Test(expected = TddlRuntimeException.class)
    public void handle_HttpPostFailure_ThrowsException1() {
        SqlShowSlaveStatus sqlNode = mock(SqlShowSlaveStatus.class);
        when(sqlNode.getParams()).thenReturn(new HashMap<>());
        when(logicalDal.getNativeSqlNode()).thenReturn(sqlNode);
        String mockDaemonEndpoint = "http://mock-daemon-endpoint";

        try(MockedStatic<CdcTargetUtil> mockCdcTargetUtil = mockStatic(CdcTargetUtil.class);
            MockedStatic<PooledHttpHelper> mockPooledHttpHelper = mockStatic(PooledHttpHelper.class)) {
            mockCdcTargetUtil.when(CdcTargetUtil::getReplicaDaemonMasterTarget).thenReturn(mockDaemonEndpoint);
            mockPooledHttpHelper.when(() -> PooledHttpHelper.doPost(anyString(), any(ContentType.class), anyString()
                    , anyInt()))
                .thenThrow(new IOException("Connection failed"));
            handler1.handle(logicalDal, executionContext);
        }
    }

    @Test(expected = TddlRuntimeException.class)
    public void handle_HttpPostFailure_ThrowsException2() {
        SqlCancelReplicaCheck sqlNode = mock(SqlCancelReplicaCheck.class);
        SqlNode dbName = mock(SqlNode.class);
        when(sqlNode.getDbName()).thenReturn(dbName);
        when(sqlNode.getTableName()).thenReturn(null);
        when(logicalDal.getNativeSqlNode()).thenReturn(sqlNode);
        String mockDaemonEndpoint = "http://mock-daemon-endpoint";

        try(MockedStatic<CdcTargetUtil> mockCdcTargetUtil = mockStatic(CdcTargetUtil.class);
            MockedStatic<PooledHttpHelper> mockPooledHttpHelper = mockStatic(PooledHttpHelper.class)) {
            mockCdcTargetUtil.when(CdcTargetUtil::getReplicaDaemonMasterTarget).thenReturn(mockDaemonEndpoint);
            mockPooledHttpHelper.when(() -> PooledHttpHelper.doPost(anyString(), any(ContentType.class), anyString()
                    , anyInt()))
                .thenThrow(new IOException("Connection failed"));
            handler2.handle(logicalDal, executionContext);
        }
    }

    @Test(expected = TddlRuntimeException.class)
    public void handle_HttpPostFailure_ThrowsException3() {
        SqlChangeMaster sqlNode = mock(SqlChangeMaster.class);
        when(sqlNode.getParams()).thenReturn(new HashMap<>());

        when(logicalDal.getNativeSqlNode()).thenReturn(sqlNode);

        String mockDaemonEndpoint = "http://mock-daemon-endpoint";

        try(MockedStatic<CdcTargetUtil> mockCdcTargetUtil = mockStatic(CdcTargetUtil.class);
            MockedStatic<PooledHttpHelper> mockPooledHttpHelper = mockStatic(PooledHttpHelper.class)) {
            mockCdcTargetUtil.when(CdcTargetUtil::getReplicaDaemonMasterTarget).thenReturn(mockDaemonEndpoint);
            mockPooledHttpHelper.when(() -> PooledHttpHelper.doPost(anyString(), any(ContentType.class), anyString()
                    , anyInt()))
                .thenThrow(new IOException("Connection failed"));
            handler3.handle(logicalDal, executionContext);
        }
    }

    @Test(expected = TddlRuntimeException.class)
    public void handle_HttpPostFailure_ThrowsException4() {
        SqlChangeReplicationFilter sqlNode = mock(SqlChangeReplicationFilter.class);
        when(sqlNode.getParams()).thenReturn(new HashMap<>());

        when(logicalDal.getNativeSqlNode()).thenReturn(sqlNode);

        String mockDaemonEndpoint = "http://mock-daemon-endpoint";

        try(MockedStatic<CdcTargetUtil> mockCdcTargetUtil = mockStatic(CdcTargetUtil.class);
            MockedStatic<PooledHttpHelper> mockPooledHttpHelper = mockStatic(PooledHttpHelper.class)) {
            mockCdcTargetUtil.when(CdcTargetUtil::getReplicaDaemonMasterTarget).thenReturn(mockDaemonEndpoint);
            mockPooledHttpHelper.when(() -> PooledHttpHelper.doPost(anyString(), any(ContentType.class), anyString()
                    , anyInt()))
                .thenThrow(new IOException("Connection failed"));
            handler4.handle(logicalDal, executionContext);
        }
    }

    @Test(expected = TddlRuntimeException.class)
    public void handle_HttpPostFailure_ThrowsException5() {
        SqlContinueReplicaCheck sqlNode = mock(SqlContinueReplicaCheck.class);
        SqlNode dbName = mock(SqlNode.class);
        when(sqlNode.getDbName()).thenReturn(dbName);
        when(sqlNode.getTableName()).thenReturn(null);
        when(logicalDal.getNativeSqlNode()).thenReturn(sqlNode);
        String mockDaemonEndpoint = "http://mock-daemon-endpoint";

        try(MockedStatic<CdcTargetUtil> mockCdcTargetUtil = mockStatic(CdcTargetUtil.class);
            MockedStatic<PooledHttpHelper> mockPooledHttpHelper = mockStatic(PooledHttpHelper.class)) {
            mockCdcTargetUtil.when(CdcTargetUtil::getReplicaDaemonMasterTarget).thenReturn(mockDaemonEndpoint);
            mockPooledHttpHelper.when(() -> PooledHttpHelper.doPost(anyString(), any(ContentType.class), anyString()
                    , anyInt()))
                .thenThrow(new IOException("Connection failed"));
            handler5.handle(logicalDal, executionContext);
        }
    }

    @Test(expected = TddlRuntimeException.class)
    public void handle_HttpPostFailure_ThrowsException6() {
        SqlPauseReplicaCheck sqlNode = mock(SqlPauseReplicaCheck.class);
        SqlNode dbName = mock(SqlNode.class);
        when(sqlNode.getDbName()).thenReturn(dbName);
        when(sqlNode.getTableName()).thenReturn(null);
        when(logicalDal.getNativeSqlNode()).thenReturn(sqlNode);
        String mockDaemonEndpoint = "http://mock-daemon-endpoint";

        try(MockedStatic<CdcTargetUtil> mockCdcTargetUtil = mockStatic(CdcTargetUtil.class);
            MockedStatic<PooledHttpHelper> mockPooledHttpHelper = mockStatic(PooledHttpHelper.class)) {
            mockCdcTargetUtil.when(CdcTargetUtil::getReplicaDaemonMasterTarget).thenReturn(mockDaemonEndpoint);
            mockPooledHttpHelper.when(() -> PooledHttpHelper.doPost(anyString(), any(ContentType.class), anyString()
                    , anyInt()))
                .thenThrow(new IOException("Connection failed"));
            handler6.handle(logicalDal, executionContext);
        }
    }

    @Test(expected = TddlRuntimeException.class)
    public void handle_HttpPostFailure_ThrowsException7() {
        SqlResetReplicaCheck sqlNode = mock(SqlResetReplicaCheck.class);
        SqlNode dbName = mock(SqlNode.class);
        when(sqlNode.getDbName()).thenReturn(dbName);
        when(sqlNode.getTableName()).thenReturn(null);
        when(logicalDal.getNativeSqlNode()).thenReturn(sqlNode);
        String mockDaemonEndpoint = "http://mock-daemon-endpoint";

        try(MockedStatic<CdcTargetUtil> mockCdcTargetUtil = mockStatic(CdcTargetUtil.class);
            MockedStatic<PooledHttpHelper> mockPooledHttpHelper = mockStatic(PooledHttpHelper.class)) {
            mockCdcTargetUtil.when(CdcTargetUtil::getReplicaDaemonMasterTarget).thenReturn(mockDaemonEndpoint);
            mockPooledHttpHelper.when(() -> PooledHttpHelper.doPost(anyString(), any(ContentType.class), anyString()
                    , anyInt()))
                .thenThrow(new IOException("Connection failed"));
            handler7.handle(logicalDal, executionContext);
        }
    }

    @Test(expected = TddlRuntimeException.class)
    public void handle_HttpPostFailure_ThrowsException8() {
        SqlResetSlave sqlNode = mock(SqlResetSlave.class);
        when(sqlNode.getParams()).thenReturn(new HashMap<>());
        when(logicalDal.getNativeSqlNode()).thenReturn(sqlNode);
        String mockDaemonEndpoint = "http://mock-daemon-endpoint";

        try(MockedStatic<CdcTargetUtil> mockCdcTargetUtil = mockStatic(CdcTargetUtil.class);
            MockedStatic<PooledHttpHelper> mockPooledHttpHelper = mockStatic(PooledHttpHelper.class)) {
            mockCdcTargetUtil.when(CdcTargetUtil::getReplicaDaemonMasterTarget).thenReturn(mockDaemonEndpoint);
            mockPooledHttpHelper.when(() -> PooledHttpHelper.doPost(anyString(), any(ContentType.class), anyString()
                    , anyInt()))
                .thenThrow(new IOException("Connection failed"));
            handler8.handle(logicalDal, executionContext);
        }
    }

    @Test(expected = TddlRuntimeException.class)
    public void handle_HttpPostFailure_ThrowsException9() {
        SqlShowReplicaCheckDiff sqlNode = mock(SqlShowReplicaCheckDiff.class);
        SqlNode dbName = mock(SqlNode.class);
        when(sqlNode.getDbName()).thenReturn(dbName);
        when(sqlNode.getTableName()).thenReturn(null);
        when(logicalDal.getNativeSqlNode()).thenReturn(sqlNode);

        String mockDaemonEndpoint = "http://mock-daemon-endpoint";

        try(MockedStatic<CdcTargetUtil> mockCdcTargetUtil = mockStatic(CdcTargetUtil.class);
            MockedStatic<PooledHttpHelper> mockPooledHttpHelper = mockStatic(PooledHttpHelper.class)) {
            mockCdcTargetUtil.when(CdcTargetUtil::getReplicaDaemonMasterTarget).thenReturn(mockDaemonEndpoint);
            mockPooledHttpHelper.when(() -> PooledHttpHelper.doPost(anyString(), any(ContentType.class), anyString()
                    , anyInt()))
                .thenThrow(new IOException("Connection failed"));
            handler9.handle(logicalDal, executionContext);
        }
    }

    @Test(expected = TddlRuntimeException.class)
    public void handle_HttpPostFailure_ThrowsException10() {
        SqlShowReplicaCheckProgress sqlNode = mock(SqlShowReplicaCheckProgress.class);
        SqlNode dbName = mock(SqlNode.class);
        when(sqlNode.getDbName()).thenReturn(dbName);
        when(sqlNode.getTableName()).thenReturn(null);
        when(logicalDal.getNativeSqlNode()).thenReturn(sqlNode);
        String mockDaemonEndpoint = "http://mock-daemon-endpoint";

        try(MockedStatic<CdcTargetUtil> mockCdcTargetUtil = mockStatic(CdcTargetUtil.class);
            MockedStatic<PooledHttpHelper> mockPooledHttpHelper = mockStatic(PooledHttpHelper.class)) {
            mockCdcTargetUtil.when(CdcTargetUtil::getReplicaDaemonMasterTarget).thenReturn(mockDaemonEndpoint);
            mockPooledHttpHelper.when(() -> PooledHttpHelper.doPost(anyString(), any(ContentType.class), anyString()
                    , anyInt()))
                .thenThrow(new IOException("Connection failed"));
            handler10.handle(logicalDal, executionContext);
        }
    }

    @Test(expected = TddlRuntimeException.class)
    public void handle_HttpPostFailure_ThrowsException11() {
        SqlStartReplicaCheck sqlNode = mock(SqlStartReplicaCheck.class);
        SqlNode dbName = mock(SqlNode.class);
        when(sqlNode.getDbName()).thenReturn(dbName);
        when(sqlNode.getTableName()).thenReturn(null);
        when(logicalDal.getNativeSqlNode()).thenReturn(sqlNode);
        String mockDaemonEndpoint = "http://mock-daemon-endpoint";

        try(MockedStatic<CdcTargetUtil> mockCdcTargetUtil = mockStatic(CdcTargetUtil.class);
            MockedStatic<PooledHttpHelper> mockPooledHttpHelper = mockStatic(PooledHttpHelper.class)) {
            mockCdcTargetUtil.when(CdcTargetUtil::getReplicaDaemonMasterTarget).thenReturn(mockDaemonEndpoint);
            mockPooledHttpHelper.when(() -> PooledHttpHelper.doPost(anyString(), any(ContentType.class), anyString()
                    , anyInt()))
                .thenThrow(new IOException("Connection failed"));
            handler11.handle(logicalDal, executionContext);
        }
    }

    @Test(expected = TddlRuntimeException.class)
    public void handle_HttpPostFailure_ThrowsException12() {
        SqlStartSlave sqlNode = mock(SqlStartSlave.class);
        when(sqlNode.getParams()).thenReturn(new HashMap<>());
        when(logicalDal.getNativeSqlNode()).thenReturn(sqlNode);
        String mockDaemonEndpoint = "http://mock-daemon-endpoint";

        try(MockedStatic<CdcTargetUtil> mockCdcTargetUtil = mockStatic(CdcTargetUtil.class);
            MockedStatic<PooledHttpHelper> mockPooledHttpHelper = mockStatic(PooledHttpHelper.class)) {
            mockCdcTargetUtil.when(CdcTargetUtil::getReplicaDaemonMasterTarget).thenReturn(mockDaemonEndpoint);
            mockPooledHttpHelper.when(() -> PooledHttpHelper.doPost(anyString(), any(ContentType.class), anyString()
                    , anyInt()))
                .thenThrow(new IOException("Connection failed"));
            handler12.handle(logicalDal, executionContext);
        }
    }

    @Test(expected = TddlRuntimeException.class)
    public void handle_HttpPostFailure_ThrowsException13() {
        SqlStopSlave sqlNode = mock(SqlStopSlave.class);
        when(sqlNode.getParams()).thenReturn(new HashMap<>());
        when(logicalDal.getNativeSqlNode()).thenReturn(sqlNode);
        String mockDaemonEndpoint = "http://mock-daemon-endpoint";

        try(MockedStatic<CdcTargetUtil> mockCdcTargetUtil = mockStatic(CdcTargetUtil.class);
            MockedStatic<PooledHttpHelper> mockPooledHttpHelper = mockStatic(PooledHttpHelper.class)) {
            mockCdcTargetUtil.when(CdcTargetUtil::getReplicaDaemonMasterTarget).thenReturn(mockDaemonEndpoint);
            mockPooledHttpHelper.when(() -> PooledHttpHelper.doPost(anyString(), any(ContentType.class), anyString()
                    , anyInt()))
                .thenThrow(new IOException("Connection failed"));
            handler13.handle(logicalDal, executionContext);
        }
    }
}