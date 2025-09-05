package com.alibaba.polardbx.executor.handler;

import com.alibaba.fastjson.JSON;
import com.alibaba.polardbx.common.cdc.CdcConstants;
import com.alibaba.polardbx.common.cdc.ResultCode;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.utils.PooledHttpHelper;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.gms.util.InstIdUtil;
import com.alibaba.polardbx.net.util.CdcTargetUtil;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.dal.LogicalShow;
import com.alibaba.polardbx.optimizer.core.row.Row;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlShowBinlogDumpStatus;
import org.apache.http.entity.ContentType;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class LogicalShowBinlogDumpStatusHandlerTest {

    @Mock
    private IRepository repo;

    @Mock
    private LogicalShow logicalPlan;

    @Mock
    private SqlNode with;

    @Mock
    private ExecutionContext executionContext;

    @Mock
    private SqlShowBinlogDumpStatus sqlShowBinlogDumpStatus;
    MockedStatic<PooledHttpHelper> pooledHttpHelperMockedStatic;
    MockedStatic<CdcTargetUtil> cdcTargetUtilMockedStatic;

    @InjectMocks
    private LogicalShowBinlogDumpStatusHandler handler;
    private Map<String, String> params = new HashMap<>(1);
    private String url = "http://127.0.0.1:1201/dumper/showBinlogDumpStatus";

    @Before
    public void setUp() {
        // 设置模拟对象
        when(logicalPlan.getNativeSqlNode()).thenReturn(sqlShowBinlogDumpStatus);
        when(sqlShowBinlogDumpStatus.getWith()).thenReturn(with);
        when(with.toString()).thenReturn("");
        pooledHttpHelperMockedStatic = mockStatic(PooledHttpHelper.class);
        cdcTargetUtilMockedStatic = mockStatic(CdcTargetUtil.class);
        cdcTargetUtilMockedStatic.when(CdcTargetUtil::getDaemonMasterTarget).thenReturn("127.0.0.1:1201");
        handler = new LogicalShowBinlogDumpStatusHandler(repo);

        params.put("instId", InstIdUtil.getInstId());
    }

    @After
    public void clear() {
        pooledHttpHelperMockedStatic.close();
        cdcTargetUtilMockedStatic.close();
    }

    @Test
    public void handleSuccessfulResponseReturnsCursor() {
        // 模拟 HTTP 响应
        String paramJson = JSON.toJSONString(params);
        String jsonResponse =
            "[{\"Process_Id\":\"1\",\"Trace_Id\":\"2\",\"Dumper_Address\":\"3\",\"Client_Ip\":\"4\",\"Client_Port\":\"5\",\"Filename\":\"6\",\"Position\":\"7\",\"Delay\":\"8\",\"Bps\":\"9\",\"Last_Sync_Timestamp\":\"10\",\"Alive_Second\":\"11\"}]";
        ResultCode<String> resultCode = new ResultCode<>(CdcConstants.SUCCESS_CODE, "success");
        resultCode.setData(jsonResponse);
        String responseJson = JSON.toJSONString(resultCode);
        pooledHttpHelperMockedStatic.when(
                () -> PooledHttpHelper.doPost(url, ContentType.APPLICATION_JSON, paramJson, 10000))
            .thenReturn(responseJson);

        Cursor cursor = handler.handle(logicalPlan, executionContext);

        Assert.assertNotNull(cursor);
        Assert.assertTrue(cursor instanceof ArrayResultCursor);
        ArrayResultCursor arrayResultCursor = (ArrayResultCursor) cursor;
        List<Row> rows = arrayResultCursor.getRows();
        for (int i = 1; i <= 11; i++) {
            int actualVal = Integer.parseInt(rows.get(0).getString(i - 1));
            Assert.assertEquals(i, actualVal);
        }
    }

    @Test(expected = TddlRuntimeException.class)
    public void handleFailedHttpResponseThrowsException() {
        String paramJson = JSON.toJSONString(params);
        // 模拟 HTTP 响应失败
        pooledHttpHelperMockedStatic.when(
                () -> PooledHttpHelper.doPost(url, ContentType.APPLICATION_JSON, paramJson, 10000))
            .thenThrow(
                new RuntimeException("HTTP error"));

        handler.handle(logicalPlan, executionContext);
    }

    @Test(expected = TddlRuntimeException.class)
    public void handleErrorCodeInResponseThrowsException() {
        // 模拟错误响应
        String paramJson = JSON.toJSONString(params);
        ResultCode<String> resultCode = new ResultCode<>(CdcConstants.FAILURE_CODE, "Error message");
        String responseJson = JSON.toJSONString(resultCode);
        pooledHttpHelperMockedStatic.when(
                () -> PooledHttpHelper.doPost(url, ContentType.APPLICATION_JSON, paramJson, 10000))
            .thenReturn(responseJson);

        handler.handle(logicalPlan, executionContext);
    }

    @Test
    public void handleEmptyResponseReturnsEmptyCursor() {
        // 模拟空响应
        String paramJson = JSON.toJSONString(params);
        ResultCode<String> resultCode = new ResultCode<>(CdcConstants.SUCCESS_CODE, "[]");
        String responseJson = JSON.toJSONString(resultCode);
        pooledHttpHelperMockedStatic.when(
                () -> PooledHttpHelper.doPost(url, ContentType.APPLICATION_JSON, paramJson, 10000))
            .thenReturn(responseJson);

        Cursor cursor = handler.handle(logicalPlan, executionContext);

        Assert.assertNotNull(cursor);
        Assert.assertTrue(cursor instanceof ArrayResultCursor);
        ArrayResultCursor arrayResultCursor = (ArrayResultCursor) cursor;
        List<ColumnMeta> columns = arrayResultCursor.getReturnColumns();
        Assert.assertEquals(11, columns.size());
        Assert.assertEquals("Process_Id", columns.get(0).getName());
        Assert.assertEquals("Trace_Id", columns.get(1).getName());
        Assert.assertEquals("Dumper_Address", columns.get(2).getName());
        Assert.assertEquals("Client_Ip", columns.get(3).getName());
        Assert.assertEquals("Client_Port", columns.get(4).getName());
        Assert.assertEquals("Filename", columns.get(5).getName());
        Assert.assertEquals("Position", columns.get(6).getName());
        Assert.assertEquals("Delay", columns.get(7).getName());
        Assert.assertEquals("Bps", columns.get(8).getName());
        Assert.assertEquals("Last_Sync_Timestamp", columns.get(9).getName());
        Assert.assertEquals("Alive_Second", columns.get(10).getName());
    }
}
