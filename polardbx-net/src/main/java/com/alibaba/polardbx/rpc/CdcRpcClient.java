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

import com.alibaba.polardbx.common.model.lifecycle.AbstractLifecycle;
import com.alibaba.polardbx.common.model.lifecycle.Lifecycle;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.gms.metadb.cdc.CdcDataAccessor;
import com.alibaba.polardbx.gms.metadb.cdc.CdcDumperRecord;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.rpc.cdc.CdcServiceGrpc;
import com.alibaba.polardbx.rpc.cdc.CdcServiceGrpc.CdcServiceBlockingStub;
import com.alibaba.polardbx.rpc.cdc.CdcServiceGrpc.CdcServiceStub;
import com.alibaba.polardbx.rpc.cdc.DumpRequest;
import com.alibaba.polardbx.rpc.cdc.DumpStream;
import io.grpc.Context;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import org.apache.commons.lang3.StringUtils;

import java.sql.Connection;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public class CdcRpcClient extends AbstractLifecycle implements Lifecycle {
    protected static final Logger logger = LoggerFactory.getLogger(CdcRpcClient.class);

    private static final String CDC_DUMPER_MASTER_ROLE = "M";
    private static CdcRpcClient cdcRpcClient;

    public CdcRpcClient() {
    }

    public static void buildCdcRpcClient() {
        cdcRpcClient = null;
        cdcRpcClient = new CdcRpcClient();
        cdcRpcClient.init();
    }

    private static String getCdcTargetFromMetaDb() {
        CdcDataAccessor cdcDataAccessor = new CdcDataAccessor();
        try (Connection metaDbConn = MetaDbUtil.getConnection()) {
            cdcDataAccessor.setConnection(metaDbConn);
            List<CdcDumperRecord> dumpers = cdcDataAccessor.getAllCdcDumpers();
            if (dumpers == null) {
                return null;
            }
            Optional<CdcDumperRecord> master = dumpers.stream().filter(
                d -> d != null && CDC_DUMPER_MASTER_ROLE.equals(d.getRole()))
                .findFirst();
            if (master.isPresent()) {
                CdcDumperRecord cdr = master.get();
                return cdr.getIp() + ":" + cdr.getPort();
            }

        } catch (Exception e) {
            logger.error("get cdc records fail", e);
        }
        return StringUtils.EMPTY;
    }

    public CdcServiceBlockingStub getCdcServiceBlockingStub() {
        ManagedChannel channel = ManagedChannelBuilder
            .forTarget(getCdcTargetFromMetaDb())
            .usePlaintext()
            .maxInboundMessageSize(0xFFFFFF + 5 + 0xFF)
            .build();
        return CdcServiceGrpc.newBlockingStub(channel);
    }

    public CdcServiceStub getCdcServiceStub() {

        ManagedChannel channel = ManagedChannelBuilder
            .forTarget(getCdcTargetFromMetaDb())
            .usePlaintext()
            .maxInboundMessageSize(0xFFFFFF + 5 + 0xFF)
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
        return !StringUtils.isEmpty(getCdcTargetFromMetaDb());
    }

    public static class CdcRpcStreamingProxy {
        private ManagedChannel channel;
        private Context.CancellableContext mListenContext;

        public void dump(final DumpRequest listenRequest, final StreamObserver<DumpStream> messageStream) {
            Runnable runnable = () -> {
                final CdcServiceStub cdcServiceStub = getCdcRpcClient().getCdcServiceStub();
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
                    channel.shutdown();
                    channel.awaitTermination(1, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    logger.warn("channel shutdown awaitTermination fail");
                }
            }
        }
    }
}
