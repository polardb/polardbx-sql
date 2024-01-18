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

package com.alibaba.polardbx.executor.handler;

import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.rel.dal.LogicalShow;
import com.alibaba.polardbx.optimizer.utils.RelUtils;
import com.alibaba.polardbx.rpc.CdcRpcClient;
import com.alibaba.polardbx.rpc.cdc.CdcServiceGrpc.CdcServiceBlockingStub;
import com.alibaba.polardbx.rpc.cdc.MasterStatus;
import com.alibaba.polardbx.rpc.cdc.Request;
import io.grpc.Channel;
import io.grpc.ManagedChannel;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlShowMasterStatus;

/**
 *
 */
public class LogicalShowMasterStatusHandler extends HandlerCommon {
    public LogicalShowMasterStatusHandler(IRepository repo) {
        super(repo);
    }

    @Override
    public Cursor handle(RelNode logicalPlan, ExecutionContext executionContext) {

        SqlShowMasterStatus sqlShowMasterStatus = (SqlShowMasterStatus) ((LogicalShow) logicalPlan).getNativeSqlNode();
        SqlNode with = sqlShowMasterStatus.getWith();
        String streamName = with == null ? "" : RelUtils.lastStringValue(with);
        CdcServiceBlockingStub cdcServiceBlockingStub =
            with == null ? CdcRpcClient.getCdcRpcClient().getCdcServiceBlockingStub() :
                CdcRpcClient.getCdcRpcClient().getCdcServiceBlockingStub(streamName);
        MasterStatus masterStatus = cdcServiceBlockingStub.showMasterStatus(
            Request.newBuilder().setStreamName(streamName).build());
        ArrayResultCursor result = new ArrayResultCursor("SHOW MASTER STATUS");
        result.addColumn("File", DataTypes.StringType);
        result.addColumn("Position", DataTypes.LongType);
        result.addColumn("Binlog_Do_DB", DataTypes.StringType);
        result.addColumn("Binlog_Ignore_DB", DataTypes.StringType);
        result.addColumn("Executed_Gtid_Set", DataTypes.StringType);
        result.initMeta();
        result.addRow(new Object[] {
            masterStatus.getFile(), masterStatus.getPosition(), masterStatus.getBinlogDoDB(),
            masterStatus.getBinlogIgnoreDB(), masterStatus.getExecutedGtidSet()});
        Channel channel = cdcServiceBlockingStub.getChannel();
        if (channel instanceof ManagedChannel) {
            ((ManagedChannel) channel).shutdown();
        }
        return result;
    }
}
