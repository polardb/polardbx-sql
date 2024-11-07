package com.alibaba.polardbx.executor.handler;

import com.alibaba.polardbx.common.cdc.RplConstants;
import lombok.SneakyThrows;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlStartReplicaCheck;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.junit.Test;
import org.mockito.Mockito;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.when;

/**
 * @author yudong
 * @since 2024/6/25 14:54
 **/
public class LogicalStartReplicaCheckTableHandlerTest {

    @SneakyThrows
    @SuppressWarnings("unchecked")
    @Test
    public void testBuildParamsWithAllParametersPresent() {
        LogicalStartReplicaCheckTableHandler mockHandler = Mockito.mock(LogicalStartReplicaCheckTableHandler.class);
        SqlStartReplicaCheck sqlStartReplicaCheck = Mockito.mock(SqlStartReplicaCheck.class);
        when(sqlStartReplicaCheck.getDbName()).thenReturn(new SqlIdentifier("test_db", new SqlParserPos(0, 0)));
        when(sqlStartReplicaCheck.getTableName()).thenReturn(new SqlIdentifier("test_table", new SqlParserPos(0, 0)));
        when(sqlStartReplicaCheck.getChannel()).thenReturn(new SqlIdentifier("test_channel", new SqlParserPos(0, 0)));
        when(sqlStartReplicaCheck.getMode()).thenReturn(new SqlIdentifier("snapshot", new SqlParserPos(0, 0)));

        Method method =
            LogicalStartReplicaCheckTableHandler.class.getDeclaredMethod("buildParams", SqlStartReplicaCheck.class);
        method.setAccessible(true);
        Map<String, String> result = (Map<String, String>) method.invoke(mockHandler, sqlStartReplicaCheck);

        assertEquals("test_db", result.get(RplConstants.RPL_FULL_VALID_DB));
        assertEquals("test_table", result.get(RplConstants.RPL_FULL_VALID_TB));
        assertEquals("test_channel", result.get(RplConstants.CHANNEL));
        assertEquals("snapshot", result.get(RplConstants.RPL_FULL_VALID_MODE));
    }

    @SneakyThrows
    @SuppressWarnings("unchecked")
    @Test
    public void testBuildParamsWithoutMode() {
        LogicalStartReplicaCheckTableHandler mockHandler = Mockito.mock(LogicalStartReplicaCheckTableHandler.class);
        SqlStartReplicaCheck sqlStartReplicaCheck = Mockito.mock(SqlStartReplicaCheck.class);
        when(sqlStartReplicaCheck.getDbName()).thenReturn(new SqlIdentifier("test_db", new SqlParserPos(0, 0)));
        when(sqlStartReplicaCheck.getTableName()).thenReturn(new SqlIdentifier("test_table", new SqlParserPos(0, 0)));
        when(sqlStartReplicaCheck.getChannel()).thenReturn(new SqlIdentifier("test_channel", new SqlParserPos(0, 0)));
        when(sqlStartReplicaCheck.getMode()).thenReturn(null);

        Method method =
            LogicalStartReplicaCheckTableHandler.class.getDeclaredMethod("buildParams", SqlStartReplicaCheck.class);
        method.setAccessible(true);
        Map<String, String> result = (Map<String, String>) method.invoke(mockHandler, sqlStartReplicaCheck);

        assertEquals("test_db", result.get(RplConstants.RPL_FULL_VALID_DB));
        assertEquals("test_table", result.get(RplConstants.RPL_FULL_VALID_TB));
        assertEquals("test_channel", result.get(RplConstants.CHANNEL));
        assertEquals("snapshot", result.get(RplConstants.RPL_FULL_VALID_MODE));
    }

    @Test(expected = InvocationTargetException.class)
    @SneakyThrows
    public void testBuildParamsWithFalseMode() {
        LogicalStartReplicaCheckTableHandler mockHandler = Mockito.mock(LogicalStartReplicaCheckTableHandler.class);
        SqlStartReplicaCheck sqlStartReplicaCheck = Mockito.mock(SqlStartReplicaCheck.class);
        when(sqlStartReplicaCheck.getDbName()).thenReturn(new SqlIdentifier("test_db", new SqlParserPos(0, 0)));
        when(sqlStartReplicaCheck.getTableName()).thenReturn(new SqlIdentifier("test_table", new SqlParserPos(0, 0)));
        when(sqlStartReplicaCheck.getChannel()).thenReturn(new SqlIdentifier("test_channel", new SqlParserPos(0, 0)));
        when(sqlStartReplicaCheck.getMode()).thenReturn(new SqlIdentifier("auto", new SqlParserPos(0, 0)));

        Method method =
            LogicalStartReplicaCheckTableHandler.class.getDeclaredMethod("buildParams", SqlStartReplicaCheck.class);

        method.setAccessible(true);
        method.invoke(mockHandler, sqlStartReplicaCheck);
    }

}
