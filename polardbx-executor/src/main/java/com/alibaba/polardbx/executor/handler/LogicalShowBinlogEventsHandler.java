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
import com.alibaba.polardbx.rpc.cdc.BinlogEvent;
import com.alibaba.polardbx.rpc.cdc.CdcServiceGrpc.CdcServiceBlockingStub;
import com.alibaba.polardbx.rpc.cdc.ShowBinlogEventsRequest;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlShowBinlogEvents;
import org.apache.commons.lang3.StringUtils;

import java.util.Iterator;

import static com.alibaba.polardbx.executor.utils.CdcExeUtil.tryExtractStreamNameFromUser;

public class LogicalShowBinlogEventsHandler extends HandlerCommon {
    public LogicalShowBinlogEventsHandler(IRepository repo) {
        super(repo);
    }

    @Override
    public Cursor handle(RelNode logicalPlan, ExecutionContext executionContext) {
        SqlShowBinlogEvents sqlShowBinlogEvents = (SqlShowBinlogEvents) ((LogicalShow) logicalPlan).getNativeSqlNode();
        SqlNode offset = null;
        SqlNode rowCount = null;
        if (sqlShowBinlogEvents.getLimit() != null) {
            offset = ((SqlNodeList) sqlShowBinlogEvents.getLimit()).get(0);
            rowCount = ((SqlNodeList) sqlShowBinlogEvents.getLimit()).get(1);
        }

        String fileName = sqlShowBinlogEvents.getLogName() == null ? "" :
            RelUtils.lastStringValue(sqlShowBinlogEvents.getLogName());
        SqlNode with = sqlShowBinlogEvents.getWith();
        String streamName = with == null ?
            extractStreamName(fileName, executionContext) : RelUtils.lastStringValue(with);

        CdcServiceBlockingStub cdcServiceBlockingStub =
            StringUtils.isBlank(streamName) ? CdcRpcClient.getCdcRpcClient().getCdcServiceBlockingStub() :
                CdcRpcClient.getCdcRpcClient().getCdcServiceBlockingStub(streamName);
        Iterator<BinlogEvent> binlogEvents = cdcServiceBlockingStub.showBinlogEvents(
            ShowBinlogEventsRequest.newBuilder()
                .setLogName(fileName)
                .setPos(sqlShowBinlogEvents.getPos() == null ? -1
                    : RelUtils.longValue(sqlShowBinlogEvents.getPos()).intValue())
                .setOffset(offset == null ? -1 : RelUtils.longValue(offset).intValue())
                .setRowCount(rowCount == null ? -1 : RelUtils.longValue(rowCount).intValue())
                .setStreamName(streamName)
                .build());
        CdcResultCursor result = new CdcResultCursor("SHOW BINLOG EVENTS", binlogEvents,
            cdcServiceBlockingStub.getChannel());
        result.addColumn("Log_name", DataTypes.StringType);
        result.addColumn("Pos", DataTypes.LongType);
        result.addColumn("Event_type", DataTypes.StringType);
        result.addColumn("Server_id", DataTypes.LongType);
        result.addColumn("End_log_pos", DataTypes.LongType);
        result.addColumn("Info", DataTypes.StringType);
        result.initMeta();
        return result;
    }

    private String extractStreamName(String fileName, ExecutionContext executionContext) {
        if (StringUtils.isNotBlank(fileName)) {
            int idx = fileName.lastIndexOf('_');
            if (idx == -1) {
                return "";
            } else {
                return fileName.substring(0, idx);
            }
        } else {
            return tryExtractStreamNameFromUser(executionContext);
        }
    }
}
