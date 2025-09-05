package com.alibaba.polardbx.net.util;

import com.alibaba.fastjson.JSON;
import com.alibaba.polardbx.common.cdc.CdcConstants;
import com.alibaba.polardbx.common.cdc.ResultCode;
import com.alibaba.polardbx.common.utils.PooledHttpHelper;
import com.alibaba.polardbx.rpc.cdc.DumpRequest;
import lombok.SneakyThrows;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.MockedStatic;
import org.mockito.junit.MockitoJUnitRunner;

import static com.alibaba.polardbx.common.utils.PooledHttpHelper.doPost;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class CdcTargetUtilTest {

    private MockedStatic<PooledHttpHelper> pooledHttpHelper;
    private MockedStatic<CdcTargetUtil> cdcTargetUtilMockedStatic;

    @Before
    @SneakyThrows
    public void setUp() {
        // 设置模拟行为
        pooledHttpHelper = mockStatic(PooledHttpHelper.class);
        cdcTargetUtilMockedStatic = mockStatic(CdcTargetUtil.class);

        pooledHttpHelper.when(() -> doPost(any(String.class), any(), any(String.class), anyInt()))
            .thenAnswer(invocation -> {
                String params = invocation.getArgument(2);
                if (params.contains("success")) {
                    ResultCode<String> successResult =
                        new ResultCode<>(CdcConstants.SUCCESS_CODE, "success", "targetAddress");
                    return JSON.toJSONString(successResult);
                } else if (params.contains("nonSuccess")) {
                    ResultCode<String> failureResult =
                        new ResultCode<>(CdcConstants.FAILURE_CODE, "failure");
                    return JSON.toJSONString(failureResult);
                } else {
                    throw new RuntimeException("HTTP request failed");
                }
            });
        cdcTargetUtilMockedStatic.when(() -> CdcTargetUtil.getDumperTarget(any(DumpRequest.class)))
            .thenCallRealMethod();
        cdcTargetUtilMockedStatic.when(CdcTargetUtil::getDumperMasterTarget).thenReturn("dumperMasterTarget");
        cdcTargetUtilMockedStatic.when(CdcTargetUtil::getDaemonMasterTarget).thenReturn("daemonMasterTarget");
    }

    @After
    public void clear() {
        pooledHttpHelper.close();
        cdcTargetUtilMockedStatic.close();
    }

    @Test
    public void getDumperTargetSuccess() {
        DumpRequest request = DumpRequest.newBuilder()
            .setFileName("success")
            .setPosition(100)
            .build();
        String result = CdcTargetUtil.getDumperTarget(request);
        Assert.assertEquals("targetAddress", result);
    }

    @Test
    public void getDumperTargetHttpRequestFails() {
        DumpRequest request = DumpRequest.newBuilder()
            .setFileName("fail")
            .setPosition(100)
            .build();

        String result = CdcTargetUtil.getDumperTarget(request);

        Assert.assertEquals("dumperMasterTarget", result);
    }

    @Test
    public void getDumperTargetNonSuccessCode() {
        DumpRequest request = DumpRequest.newBuilder()
            .setFileName("nonSuccess")
            .setPosition(100)
            .build();
        String result = CdcTargetUtil.getDumperTarget(request);
        Assert.assertEquals("dumperMasterTarget", result);
    }
}
