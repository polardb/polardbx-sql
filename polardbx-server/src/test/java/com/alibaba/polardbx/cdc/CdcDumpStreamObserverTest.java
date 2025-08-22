package com.alibaba.polardbx.cdc;

import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.net.FrontendConnection;
import com.alibaba.polardbx.net.compress.IPacketOutputProxy;
import com.alibaba.polardbx.net.compress.PacketOutputProxyFactory;
import com.alibaba.polardbx.rpc.cdc.DumpStream;
import com.google.protobuf.ByteString;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import lombok.SneakyThrows;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.concurrent.CountDownLatch;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class CdcDumpStreamObserverTest {

    @Mock
    private FrontendConnection frontendConnection;

    @Mock
    private CountDownLatch countDownLatch;

    @InjectMocks
    private CdcDumpStreamObserver cdcDumpStreamObserver;

    @Test
    @SneakyThrows
    public void testOnNext() {
        try (MockedStatic<PacketOutputProxyFactory> proxyFactory = mockStatic(PacketOutputProxyFactory.class)) {
            PacketOutputProxyFactory packetOutputProxy = Mockito.mock(PacketOutputProxyFactory.class);
            IPacketOutputProxy proxy = Mockito.mock(IPacketOutputProxy.class);
            proxyFactory.when(PacketOutputProxyFactory::getInstance).thenReturn(packetOutputProxy);
            when(packetOutputProxy.createProxy(frontendConnection)).thenReturn(proxy);
            DumpStream dumpStream = DumpStream.newBuilder().setPayload(ByteString.copyFromUtf8("1")).build();
            cdcDumpStreamObserver.onNext(dumpStream);
            verify(proxy).writeArrayAsPacket(dumpStream.getPayload().toByteArray());
        }
    }

    @Test
    public void testOnErrorStatusRuntimeExceptionCancelled() {
        StatusRuntimeException statusRuntimeException = new StatusRuntimeException(Status.CANCELLED);
        cdcDumpStreamObserver.onError(statusRuntimeException);
        verify(countDownLatch).countDown();
    }

    @Test
    public void testOnErrorStatusRuntimeExceptionInvalidArgument() {
        StatusRuntimeException statusRuntimeException = new StatusRuntimeException(
            Status.INVALID_ARGUMENT.withDescription("{\"error_code\":1234,\"error_message\":\"Invalid argument\"}"));
        cdcDumpStreamObserver.onError(statusRuntimeException);
        verify(frontendConnection).writeErrMessage(eq(1234), eq(null), eq("Invalid argument"));
        verify(countDownLatch).countDown();
    }

    @Test
    public void testOnErrorStatusRuntimeExceptionUnavailable() {
        StatusRuntimeException statusRuntimeException = new StatusRuntimeException(Status.UNAVAILABLE);
        cdcDumpStreamObserver.onError(statusRuntimeException);
        verify(frontendConnection).writeErrMessage(eq(ErrorCode.ER_MASTER_FATAL_ERROR_READING_BINLOG),
            eq("please try later..."));
        verify(countDownLatch).countDown();
    }

    @Test
    public void testOnErrorStatusRuntimeExceptionUnknown() {
        StatusRuntimeException statusRuntimeException = new StatusRuntimeException(Status.UNKNOWN);
        cdcDumpStreamObserver.onError(statusRuntimeException);
        verify(frontendConnection).writeErrMessage(eq(ErrorCode.ER_MASTER_FATAL_ERROR_READING_BINLOG),
            eq(statusRuntimeException.getMessage()));
        verify(countDownLatch).countDown();
    }

    @Test
    public void testOnErrorThrowable() {
        Throwable throwable = new RuntimeException("Unexpected error");
        cdcDumpStreamObserver.onError(throwable);
        verify(frontendConnection).writeErrMessage(eq(ErrorCode.ER_MASTER_FATAL_ERROR_READING_BINLOG),
            eq(throwable.getMessage()));
        verify(countDownLatch).countDown();
    }

    @Test
    public void testOnCompleted() {
        cdcDumpStreamObserver.onCompleted();
        verify(countDownLatch).countDown();
    }
}
