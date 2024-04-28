package com.alibaba.polardbx.gms.metadb.columnar;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.google.common.truth.Truth;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.MockitoJUnitRunner;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.ArgumentMatchers.matches;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class ColumnarNodeInfoAccessorTest {
    public static final String TEST_DAEMON_IP = "127.0.0.1";
    public static final int TEST_DAEMON_PORT = 3007;
    @Mock
    Connection conn;
    @Mock
    ResultSet queryColumnarNodeInfoRs;

    @Test
    public void getDaemonMaster() throws SQLException {
        try (final MockedStatic<MetaDbUtil> metaDbUtilMockedStatic = mockStatic(MetaDbUtil.class);) {

            final AtomicBoolean hasPolardbxInstId = new AtomicBoolean(true);
            metaDbUtilMockedStatic.when(
                    () -> MetaDbUtil.hasColumn(ArgumentMatchers.matches("columnar_node_info"), matches("polarx_inst_id")))
                .thenAnswer(invocation -> hasPolardbxInstId.get());

            final ColumnarNodeInfoRecord columnarNodeInfoRecord = new ColumnarNodeInfoRecord();
            when(queryColumnarNodeInfoRs.getString(matches("ip"))).thenReturn(TEST_DAEMON_IP);
            when(queryColumnarNodeInfoRs.getInt(matches("daemon_port"))).thenReturn(TEST_DAEMON_PORT);
            columnarNodeInfoRecord.fill(queryColumnarNodeInfoRs);

            final AtomicBoolean queryByInstIdFailed = new AtomicBoolean(false);
            final List<ColumnarNodeInfoRecord> queryByInstIdResult = new ArrayList<>();
            queryByInstIdResult.add(columnarNodeInfoRecord);
            metaDbUtilMockedStatic.when(
                    () -> MetaDbUtil.query(anyString(), anyMap(), eq(ColumnarNodeInfoRecord.class), isNull()))
                .thenAnswer(invocation -> {
                    if (queryByInstIdFailed.get()) {
                        throw new RuntimeException("Mock query failed");
                    } else {
                        return queryByInstIdResult;
                    }
                });

            final AtomicBoolean queryFailed = new AtomicBoolean(false);
            final List<ColumnarNodeInfoRecord> queryResult = new ArrayList<>();
            queryResult.add(columnarNodeInfoRecord);
            metaDbUtilMockedStatic.when(
                    () -> MetaDbUtil.query(anyString(), eq(ColumnarNodeInfoRecord.class), isNull()))
                .thenAnswer(invocation -> {
                    if (queryFailed.get()) {
                        throw new RuntimeException("Mock query failed");
                    } else {
                        return queryResult;
                    }
                });

            // query by inst id success
            final ColumnarNodeInfoAccessor columnarNodeInfoAccessor = new ColumnarNodeInfoAccessor();
            Optional<ColumnarNodeInfoRecord> daemonMaster;
            daemonMaster = columnarNodeInfoAccessor.getDaemonMaster();
            Truth.assertThat(daemonMaster.isPresent()).isTrue();
            Truth.assertThat(daemonMaster.get().getIp()).isEqualTo(TEST_DAEMON_IP);
            Truth.assertThat(daemonMaster.get().getDaemonPort()).isEqualTo(TEST_DAEMON_PORT);

            // has no polardbx inst id
            hasPolardbxInstId.set(false);
            queryByInstIdResult.clear();
            daemonMaster = columnarNodeInfoAccessor.getDaemonMaster();
            Truth.assertThat(daemonMaster.isPresent()).isTrue();
            Truth.assertThat(daemonMaster.get().getIp()).isEqualTo(TEST_DAEMON_IP);
            Truth.assertThat(daemonMaster.get().getDaemonPort()).isEqualTo(TEST_DAEMON_PORT);

            // empty result for query by polardbx inst id
            hasPolardbxInstId.set(true);
            queryByInstIdResult.clear();
            daemonMaster = columnarNodeInfoAccessor.getDaemonMaster();
            Truth.assertThat(daemonMaster.isPresent()).isTrue();
            Truth.assertThat(daemonMaster.get().getIp()).isEqualTo(TEST_DAEMON_IP);
            Truth.assertThat(daemonMaster.get().getDaemonPort()).isEqualTo(TEST_DAEMON_PORT);

            // query by inst id failed
            hasPolardbxInstId.set(true);
            queryByInstIdFailed.set(true);
            try {
                daemonMaster = columnarNodeInfoAccessor.getDaemonMaster();
                Assert.fail("expected throw an exception");
            } catch (TddlRuntimeException re) {
                Truth.assertThat(re.getMessage()).contains("Mock query failed");
            }

            // query without inst id failed
            hasPolardbxInstId.set(false);
            queryFailed.set(true);
            try {
                daemonMaster = columnarNodeInfoAccessor.getDaemonMaster();
                Assert.fail("expected throw an exception");
            } catch (TddlRuntimeException re) {
                Truth.assertThat(re.getMessage()).contains("Mock query failed");
            }

            hasPolardbxInstId.set(true);
            queryByInstIdFailed.set(false);
            queryByInstIdResult.clear();
            queryFailed.set(true);
            try {
                daemonMaster = columnarNodeInfoAccessor.getDaemonMaster();
                Assert.fail("expected throw an exception");
            } catch (TddlRuntimeException re) {
                Truth.assertThat(re.getMessage()).contains("Mock query failed");
            }
        }

    }
}