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

import com.alibaba.polardbx.common.exception.TddlNestableRuntimeException;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.gms.metadb.cdc.BinlogStreamAccessor;
import com.alibaba.polardbx.gms.metadb.cdc.BinlogStreamRecord;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.statistics.SQLRecorderLogger;
import com.alibaba.polardbx.optimizer.core.rel.dal.LogicalShow;
import com.alibaba.polardbx.optimizer.utils.RelUtils;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlShowBinaryStreams;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

public class LogicalShowBinaryStreamsHandler extends HandlerCommon {
    private static final Logger cdcLogger = SQLRecorderLogger.cdcLogger;

    public LogicalShowBinaryStreamsHandler(IRepository repo) {
        super(repo);
    }

    @Override
    public Cursor handle(RelNode logicalPlan, ExecutionContext executionContext) {
        SqlShowBinaryStreams sqlShowBinaryStreams =
            (SqlShowBinaryStreams) ((LogicalShow) logicalPlan).getNativeSqlNode();
        SqlNode with = sqlShowBinaryStreams.getWith();
        String groupName = with == null ? null : RelUtils.lastStringValue(with);

        ArrayResultCursor result = new ArrayResultCursor("SHOW BINARY STREAMS");
        result.addColumn("Group", DataTypes.StringType);
        result.addColumn("Stream", DataTypes.StringType);
        result.addColumn("File", DataTypes.StringType);
        result.addColumn("Position", DataTypes.LongType);
        result.initMeta();

        BinlogStreamAccessor binlogStreamAccessor = new BinlogStreamAccessor();
        try (Connection metaDbConn = MetaDbUtil.getConnection()) {
            binlogStreamAccessor.setConnection(metaDbConn);
            List<BinlogStreamRecord> streams;
            if (groupName == null) {
                streams = binlogStreamAccessor.listAllStream();
            } else {
                streams = binlogStreamAccessor.listStreamInGroup(groupName);
            }
            if (streams == null) {
                throw new TddlNestableRuntimeException("binlog multi stream is not support...");
            }
            for (BinlogStreamRecord stream : streams) {
                result.addRow(new Object[] {
                    stream.getGroupName(), stream.getStreamName(), stream.getFileName(), stream.getPosition()});
            }
        } catch (SQLException e) {
            cdcLogger.error("get binlog x stream fail", e);
        }
        return result;
    }
}
