package com.alibaba.polardbx.gms.metadb.columnar;

import com.google.common.truth.Truth;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.sql.ResultSet;
import java.sql.SQLException;

import static org.mockito.ArgumentMatchers.matches;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class ColumnarNodeInfoRecordTest {
    public static final String TEST_DAEMON_IP = "127.0.0.1";
    public static final int TEST_DAEMON_PORT = 3007;
    @Mock
    ResultSet rs;

    @Test
    public void testFill() throws SQLException {
        when(rs.getString(matches("ip"))).thenReturn(TEST_DAEMON_IP);
        when(rs.getInt(matches("daemon_port"))).thenReturn(TEST_DAEMON_PORT);

        final ColumnarNodeInfoRecord result = new ColumnarNodeInfoRecord().fill(rs);
        Truth.assertThat(result.getIp()).isEqualTo(TEST_DAEMON_IP);
        Truth.assertThat(result.getDaemonPort()).isEqualTo(TEST_DAEMON_PORT);
    }
}