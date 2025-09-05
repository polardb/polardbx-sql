package com.alibaba.polardbx.executor.utils;

import com.alibaba.fastjson.JSON;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.optimizer.utils.MetaUtils;
import com.google.common.collect.ImmutableMap;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.mockito.ArgumentMatchers.any;

public class AutoSnapshotExecUtilsTest {
    @Test
    public void testGetColumnarAutoSnapshotConfig() throws SQLException {
        try (MockedStatic<MetaDbUtil> metaUtilsMockedStatic = Mockito.mockStatic(MetaDbUtil.class)) {
            Connection mockConn = Mockito.mock(Connection.class);
            PreparedStatement mockStmt = Mockito.mock(PreparedStatement.class);
            ResultSet resultSet = Mockito.mock(ResultSet.class);
            metaUtilsMockedStatic.when(MetaDbUtil::getConnection).thenReturn(mockConn);
            metaUtilsMockedStatic.when(() -> MetaDbUtil.query(any(), any(), any(), any())).thenCallRealMethod();
            Mockito.when(mockConn.prepareStatement(any())).thenReturn(mockStmt);
            Mockito.when(mockStmt.executeQuery()).thenReturn(resultSet);
            AtomicBoolean full = new AtomicBoolean(false);
            // mock only one row
            Mockito.when(resultSet.next()).then(
                invocation -> {
                    if (full.get()) {
                        return false;
                    } else {
                        full.set(true);
                        return true;
                    }
                }
            );
            Map<String, String> config = ImmutableMap.of("Test1", "Test2");
            Mockito.when(resultSet.getString("config_value")).thenReturn(JSON.toJSONString(config));

            Map<String, String> res = ExecUtils.getColumnarAutoSnapshotConfig();
            Assert.assertEquals(config, res);

            res = ExecUtils.getColumnarAutoSnapshotConfig();
            Assert.assertNull(res);
        }
    }
}
