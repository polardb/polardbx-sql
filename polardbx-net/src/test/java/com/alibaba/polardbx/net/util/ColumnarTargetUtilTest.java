package com.alibaba.polardbx.net.util;

import com.alibaba.polardbx.common.exception.TddlNestableRuntimeException;
import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.gms.metadb.columnar.ColumnarNodeInfoRecord;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.google.common.truth.Truth;
import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.MockitoJUnitRunner;

import java.sql.Connection;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.matches;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class ColumnarTargetUtilTest {
    public static final String TEST_DAEMON_IP = "127.0.0.1";
    public static final int TEST_DAEMON_PORT = 3007;
    @Mock
    Connection conn;
    @Mock
    ResultSet queryColumnarNodeInfoRs;

    private MockedStatic<MetaDbUtil> mockMetaDbUtil;

    @Test
    public void testGetDaemonMasterTarget() throws Exception {
        mockMetaDbUtil = mockStatic(MetaDbUtil.class);
        final AtomicBoolean getConnectionFailed = new AtomicBoolean(false);
        final AtomicBoolean queryFailed = new AtomicBoolean(false);

        mockMetaDbUtil.when(MetaDbUtil::getConnection).thenAnswer(invocation -> {
            if (getConnectionFailed.get()) {
                throw new RuntimeException("Mock get connection failed");
            } else {
                return conn;
            }
        });
        mockMetaDbUtil.when(() -> MetaDbUtil.hasColumn("columnar_node_info", "polarx_inst_id")).thenReturn(true);

        final ColumnarNodeInfoRecord columnarNodeInfoRecord = new ColumnarNodeInfoRecord();
        when(queryColumnarNodeInfoRs.getString(matches("ip"))).thenReturn(TEST_DAEMON_IP);
        when(queryColumnarNodeInfoRs.getInt(matches("daemon_port"))).thenReturn(TEST_DAEMON_PORT);
        columnarNodeInfoRecord.fill(queryColumnarNodeInfoRs);

        final List<ColumnarNodeInfoRecord> queryResult = new ArrayList<>();
        queryResult.add(columnarNodeInfoRecord);
        mockMetaDbUtil.when(
            () -> MetaDbUtil.query(anyString(), anyMap(), eq(ColumnarNodeInfoRecord.class), eq(conn))
        ).thenAnswer(invocation -> {
            if (queryFailed.get()) {
                throw new RuntimeException("Mock query failed");
            } else {
                return queryResult;
            }
        });

        Truth.assertThat(ColumnarTargetUtil.getDaemonMasterTarget())
            .matches(TEST_DAEMON_IP + ":" + TEST_DAEMON_PORT);

        getConnectionFailed.set(true);
        try {
            ColumnarTargetUtil.getDaemonMasterTarget();
            Assert.fail("expected throw an exception");
        } catch (TddlNestableRuntimeException tner) {
            Truth.assertThat(tner.getMessage()).contains("Mock get connection failed");
        }

        getConnectionFailed.set(false);
        queryFailed.set(true);
        try {
            ColumnarTargetUtil.getDaemonMasterTarget();
            Assert.fail("expected throw an exception");
        } catch (TddlNestableRuntimeException tner) {
            Truth.assertThat(tner.getMessage()).contains("Mock query failed");
        }

        queryFailed.set(false);
        queryResult.clear();
        try {
            ColumnarTargetUtil.getDaemonMasterTarget();
            Assert.fail("expected throw an exception");
        } catch (TddlNestableRuntimeException tner) {
            Truth.assertThat(tner.getMessage()).contains("can not find columnar daemon master endpoint");
        }
    }

    @After
    public void after() {
        if (mockMetaDbUtil != null) {
            mockMetaDbUtil.close();
        }
    }
}