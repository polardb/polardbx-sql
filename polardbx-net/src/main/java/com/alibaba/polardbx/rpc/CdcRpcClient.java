/*
 * Copyright [2013-2021], Alibaba Group Holding Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.polardbx.rpc;

import com.alibaba.polardbx.common.exception.TddlNestableRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.model.lifecycle.AbstractLifecycle;
import com.alibaba.polardbx.common.model.lifecycle.Lifecycle;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.gms.metadb.MetaDbDataSource;
import com.alibaba.polardbx.net.util.CdcTargetUtil;
import com.alibaba.polardbx.rpc.cdc.CdcServiceGrpc;
import com.alibaba.polardbx.rpc.cdc.CdcServiceGrpc.CdcServiceBlockingStub;
import com.alibaba.polardbx.rpc.cdc.CdcServiceGrpc.CdcServiceStub;
import com.alibaba.polardbx.rpc.cdc.DumpRequest;
import com.alibaba.polardbx.rpc.cdc.DumpStream;
import io.grpc.Context;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.stub.StreamObserver;
import org.apache.commons.lang3.StringUtils;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.TimeUnit;

public class CdcRpcClient extends AbstractLifecycle implements Lifecycle {
    protected static final Logger logger = LoggerFactory.getLogger(CdcRpcClient.class);

    private static CdcRpcClient cdcRpcClient;

    public CdcRpcClient() {
    }

    public static void buildCdcRpcClient() {
        cdcRpcClient = new CdcRpcClient();
        cdcRpcClient.init();
    }

    public CdcServiceBlockingStub getCdcServiceBlockingStub() {
        ManagedChannel channel = ManagedChannelBuilder
            .forTarget(CdcTargetUtil.getDumperMasterTarget())
            .usePlaintext()
            .maxInboundMessageSize(0xFFFFFF + 5 + 0xFF)
            .build();
        return CdcServiceGrpc.newBlockingStub(channel);
    }

    public CdcServiceBlockingStub getCdcServiceBlockingStub(String streamName) {
        ManagedChannel channel = ManagedChannelBuilder
            .forTarget(CdcTargetUtil.getDumperTarget(streamName))
            .usePlaintext()
            .maxInboundMessageSize(0xFFFFFF + 5 + 0xFF)
            .build();
        return CdcServiceGrpc.newBlockingStub(channel);
    }

    public CdcServiceStub getCdcServiceStub() {

        ManagedChannel channel = NettyChannelBuilder
            .forTarget(CdcTargetUtil.getDumperMasterTarget())
            .usePlaintext()
            .flowControlWindow(1048576 * 500)
            .maxInboundMessageSize(0xFFFFFF + 5 + 0xFF)
            .build();
        return CdcServiceGrpc.newStub(channel);
    }

    public CdcServiceStub getCdcServiceStub(String streamName) {
        if (StringUtils.isEmpty(streamName)) {
            return getCdcServiceStub();
        }
        ManagedChannel channel = NettyChannelBuilder
            .forTarget(CdcTargetUtil.getDumperTarget(streamName))
            .usePlaintext()
            .maxInboundMessageSize(0xFFFFFF + 5 + 0xFF)
            .flowControlWindow(1048576 * 500)
            .build();
        return CdcServiceGrpc.newStub(channel);
    }

    public static CdcRpcClient getCdcRpcClient() {
        return cdcRpcClient;
    }

    /**
     * 作为开关，如果没有配置cdc target，则不启用逻辑binlog相关的执行计划及handler
     *
     * @return cdc target endpoint
     */
    public static boolean useCdc() {
        if (ConfigDataMode.isPolarDbX()) {
            try (Connection connection = MetaDbDataSource.getInstance().getConnection();
                Statement stmt = connection.createStatement()) {
                try (ResultSet ignored = stmt.executeQuery("SHOW COLUMNS FROM BINLOG_NODE_INFO")) {
                    return true;
                }
            } catch (SQLException e) {
                if (e.getErrorCode() == ErrorCode.ER_NO_SUCH_TABLE.getCode()) {
                    return false;
                } else {
                    throw new TddlNestableRuntimeException("", e);
                }
            }
        } else {
            return false;
        }
    }

    public static class CdcRpcStreamingProxy {
        private final String streamName;
        private ManagedChannel channel;
        private Context.CancellableContext mListenContext;

        public CdcRpcStreamingProxy(String streamName) {
            this.streamName = streamName;
        }

        public void dump(final DumpRequest listenRequest, final StreamObserver<DumpStream> messageStream) {
            Runnable runnable = () -> {
                final CdcServiceStub cdcServiceStub = getCdcRpcClient().getCdcServiceStub(streamName);
                channel = (ManagedChannel) cdcServiceStub.getChannel();
                cdcServiceStub.dump(listenRequest, messageStream);
            };

            if (mListenContext != null && !mListenContext.isCancelled()) {
                logger.info("listen: already listening");
                return;
            }

            mListenContext = Context.current().withCancellation();
            mListenContext.run(runnable);
        }

        public void cancel() {
            if (mListenContext != null) {
                try {
                    mListenContext.cancel(null);
                    mListenContext = null;
                    if (channel != null) {
                        channel.shutdown();
                        channel.awaitTermination(1, TimeUnit.SECONDS);
                    }
                } catch (Exception e) {
                    logger.warn("channel shutdown awaitTermination fail");
                }
            }
        }
    }
}
