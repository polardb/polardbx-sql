package com.alibaba.polardbx.executor.handler;

import com.alibaba.polardbx.common.exception.TddlNestableRuntimeException;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.gms.metadb.cdc.BinlogStreamAccessor;
import com.alibaba.polardbx.gms.metadb.cdc.BinlogStreamRecord;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import org.apache.calcite.rel.RelNode;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

public class LogicalShowBinaryStreamsHandler extends HandlerCommon {
    private static final Logger logger = LoggerFactory.getLogger(LogicalShowBinaryStreamsHandler.class);

    public LogicalShowBinaryStreamsHandler(IRepository repo) {
        super(repo);
    }

    @Override
    public Cursor handle(RelNode logicalPlan, ExecutionContext executionContext) {
        ArrayResultCursor result = new ArrayResultCursor("SHOW BINARY STREAMS");
        result.addColumn("Group", DataTypes.StringType);
        result.addColumn("Stream", DataTypes.StringType);
        result.addColumn("File", DataTypes.StringType);
        result.addColumn("Position", DataTypes.LongType);
        result.initMeta();

        BinlogStreamAccessor binlogStreamAccessor = new BinlogStreamAccessor();
        try (Connection metaDbConn = MetaDbUtil.getConnection()) {
            binlogStreamAccessor.setConnection(metaDbConn);
            List<BinlogStreamRecord> streams = binlogStreamAccessor.listAllStream();
            if (streams == null) {
                throw new TddlNestableRuntimeException("binlog multi stream is not support...");
            }
            for (BinlogStreamRecord stream : streams) {
                result.addRow(new Object[] {
                    stream.getGroupName(), stream.getStreamName(), stream.getFileName(), stream.getPosition()});
            }
        } catch (SQLException e) {
            logger.error("get binlog x stream fail", e);
        }
        return result;
    }
}
