package com.alibaba.polardbx.cdc;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.net.FrontendConnection;
import com.alibaba.polardbx.net.compress.PacketOutputProxyFactory;
import com.alibaba.polardbx.rpc.cdc.DumpStream;

import io.grpc.Status;
import io.grpc.StatusException;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;

import java.util.concurrent.CountDownLatch;

import static com.alibaba.polardbx.statistics.SQLRecorderLogger.cdcLogger;

/**
 * @author zm
 */
public class CdcDumpStreamObserver implements StreamObserver<DumpStream> {
    private final FrontendConnection connection;
    private final String host;
    private final int port;
    private final CountDownLatch countDownLatch;

    public CdcDumpStreamObserver(FrontendConnection conn, CountDownLatch countDownLatch) {
        this.connection = conn;
        this.host = conn.getHost();
        this.port = conn.getPort();
        this.countDownLatch = countDownLatch;
    }

    @Override
    public void onNext(DumpStream dumpStream) {
        PacketOutputProxyFactory.getInstance().createProxy(connection)
            .writeArrayAsPacket(dumpStream.getPayload().toByteArray());
    }

    /**
     * Receives a terminating error from the stream.
     *
     * <p>May only be called once and if called it must be the last method called. In particular if an
     * exception is thrown by an implementation of {@code onError} no further calls to any method are
     * allowed.
     *
     * <p>{@code t} should be a {@link StatusException} or {@link
     * StatusRuntimeException}, but other {@code Throwable} types are possible. Callers should
     * generally convert from a {@link Status} via {@link Status#asException()} or
     * {@link Status#asRuntimeException()}. Implementations should generally convert to a
     * {@code Status} via {@link Status#fromThrowable(Throwable)}.
     *
     * @param t the error occurred on the stream
     */
    @Override
    public void onError(Throwable t) {
        try {
            if (t instanceof StatusRuntimeException) {
                final Status status = ((StatusRuntimeException) t).getStatus();
                if (status.getCode() == Status.Code.CANCELLED && status.getCause() == null) {
                    if (cdcLogger.isInfoEnabled()) {
                        cdcLogger.info("binlog dump canceled by remote [" + host + ":" + port + "]...");
                    }
                    return;
                }
                cdcLogger.error("[" + host + ":" + port + "] binlog dump from cdc failed", t);
                if (status.getCode() == Status.Code.INVALID_ARGUMENT) {
                    final String description = status.getDescription();
                    JSONObject obj = JSON.parseObject(description);
                    cdcLogger.error(
                        "[" + host + ":" + port + "] binlog dump from cdc failed with " + obj);
                    connection.writeErrMessage((Integer) obj.get("error_code"), null,
                        (String) obj.get("error_message"));
                } else if (status.getCode() == Status.Code.UNAVAILABLE) {
                    cdcLogger.error("[" + host + ":" + port
                        + "] binlog dump from cdc failed cause of UNAVAILABLE, please try later");
                    connection.writeErrMessage(ErrorCode.ER_MASTER_FATAL_ERROR_READING_BINLOG,
                        "please try later...");
                } else {
                    cdcLogger.error("[" + host + ":" + port
                        + "] binlog dump from cdc failed cause of unknown, please try later");
                    connection.writeErrMessage(ErrorCode.ER_MASTER_FATAL_ERROR_READING_BINLOG, t.getMessage());
                }
            } else {
                cdcLogger.error("binlog dump from cdc failed", t);
                connection.writeErrMessage(ErrorCode.ER_MASTER_FATAL_ERROR_READING_BINLOG, t.getMessage());
            }
        } catch (Throwable th) {
            cdcLogger.error("binlog dump from cdc failed with Throwable", th);
            connection.writeErrMessage(ErrorCode.ER_MASTER_FATAL_ERROR_READING_BINLOG, th.getMessage());
        } finally {
            countDownLatch.countDown();
        }
    }

    /**
     * Receives a notification of successful stream completion.
     *
     * <p>May only be called once and if called it must be the last method called. In particular if an
     * exception is thrown by an implementation of {@code onCompleted} no further calls to any method
     * are allowed.
     */
    @Override
    public void onCompleted() {
        if (cdcLogger.isInfoEnabled()) {
            cdcLogger.info("binlog dump finished at this time");
        }
        countDownLatch.countDown();
    }
}
