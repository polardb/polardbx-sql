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
import com.alibaba.polardbx.executor.cursor.impl.CdcResultCursor;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.rel.dal.LogicalShow;
import com.alibaba.polardbx.optimizer.utils.RelUtils;
import com.alibaba.polardbx.rpc.CdcRpcClient;
import com.alibaba.polardbx.rpc.cdc.BinaryLog;
import com.alibaba.polardbx.rpc.cdc.CdcServiceGrpc.CdcServiceBlockingStub;
import com.alibaba.polardbx.rpc.cdc.FullBinaryLog;
import com.alibaba.polardbx.rpc.cdc.Request;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlShowBinaryLogs;
import org.apache.commons.lang3.StringUtils;

import java.util.Iterator;

import static com.alibaba.polardbx.executor.utils.CdcExeUtil.tryExtractStreamNameFromUser;

/**
 * created by ziyang.lb
 */
public class LogicalShowBinaryLogsHandler extends HandlerCommon {

    public LogicalShowBinaryLogsHandler(IRepository repo) {
        super(repo);
    }

    @Override
    public Cursor handle(RelNode logicalPlan, ExecutionContext executionContext) {
        SqlShowBinaryLogs sqlShowBinaryLogs = (SqlShowBinaryLogs) ((LogicalShow) logicalPlan).getNativeSqlNode();
        SqlNode with = sqlShowBinaryLogs.getWith();
        String streamName =
            with == null ? tryExtractStreamNameFromUser(executionContext) : RelUtils.lastStringValue(with);
        CdcServiceBlockingStub cdcServiceBlockingStub =
            StringUtils.isBlank(streamName) ? CdcRpcClient.getCdcRpcClient().getCdcServiceBlockingStub() :
                CdcRpcClient.getCdcRpcClient().getCdcServiceBlockingStub(streamName);

        CdcResultCursor result;
        if (sqlShowBinaryLogs.isFull()) {
            Iterator<FullBinaryLog> fullLogs = cdcServiceBlockingStub.showFullBinaryLogs(
                Request.newBuilder().setStreamName(streamName).build());
            result = new CdcResultCursor("SHOW FULL BINARY LOGS", fullLogs, cdcServiceBlockingStub.getChannel());
            result.addColumn("Log_name", DataTypes.StringType);
            result.addColumn("File_size", DataTypes.LongType);
            result.addColumn("Create_time", DataTypes.StringType);
            result.addColumn("Last_modify_time", DataTypes.StringType);
            result.addColumn("First_event_time", DataTypes.StringType);
            result.addColumn("Last_event_time", DataTypes.StringType);
            result.addColumn("Last_tso", DataTypes.StringType);
            result.addColumn("Upload_status", DataTypes.StringType);
            result.addColumn("File_location", DataTypes.StringType);
            result.addColumn("ExtInfo", DataTypes.StringType);
        } else {
            Iterator<BinaryLog> logs = cdcServiceBlockingStub.showBinaryLogs(
                Request.newBuilder().setStreamName(streamName).build());
            result = new CdcResultCursor("SHOW BINARY LOGS", logs, cdcServiceBlockingStub.getChannel());
            result.addColumn("Log_name", DataTypes.StringType);
            result.addColumn("File_size", DataTypes.LongType);
        }

        result.initMeta();
        return result;
    }
}
