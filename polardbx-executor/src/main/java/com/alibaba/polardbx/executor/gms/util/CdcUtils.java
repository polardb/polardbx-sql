/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the License);
 * you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.polardbx.executor.gms.util;

import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.rpc.CdcRpcClient;
import com.alibaba.polardbx.rpc.cdc.CdcServiceGrpc;
import com.alibaba.polardbx.rpc.cdc.FullMasterStatus;
import com.alibaba.polardbx.rpc.cdc.Request;
import io.grpc.Channel;
import io.grpc.ManagedChannel;

public class CdcUtils {
    /**
     * 获取CDC位点和延迟
     */
    public static Pair<Long, Long> getCdcTsoAndDelay() {
        //获取CDC位点和延迟
        CdcServiceGrpc.CdcServiceBlockingStub cdcServiceBlockingStub =
            CdcRpcClient.getCdcRpcClient().getCdcServiceBlockingStub();
        FullMasterStatus fullMasterStatus =
            cdcServiceBlockingStub.showFullMasterStatus(Request.newBuilder().setStreamName("").build());

        long cdcTso = getTsoTimestamp(fullMasterStatus.getLastTso());
        long cdcDelay = fullMasterStatus.getDelayTime();

        Channel channel = cdcServiceBlockingStub.getChannel();
        if (channel instanceof ManagedChannel) {
            ((ManagedChannel) channel).shutdown();
        }

        return Pair.of(cdcTso, cdcDelay);
    }

    public static Long getTsoTimestamp(String tso) {
        return Long.valueOf(tso.substring(0, 19));
    }

}
