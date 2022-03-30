package com.alibaba.polardbx.executor.handler;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.AffectRowCursor;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.rpc.cdc.RplCommandResponse;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import io.grpc.Channel;
import io.grpc.ManagedChannel;
import org.apache.calcite.rel.RelNode;

/**
 * @author shicai.xsc 2021/3/5 15:07
 * @desc
 * @since 5.0.0.0
 */
public class LogicalReplicationBaseHandler extends HandlerCommon {

    public LogicalReplicationBaseHandler(IRepository repo) {
        super(repo);
    }

    @Override
    public Cursor handle(RelNode logicalPlan, ExecutionContext executionContext) {
        return null;
    }

    public Cursor handleRplCommandResponse(RplCommandResponse response, Channel channel) {
        if (channel instanceof ManagedChannel) {
            ((ManagedChannel)channel).shutdown();
        }

        if (response.getResultCode() == 0) {
            return new AffectRowCursor(new int[] {0});
        } else {
            throw new TddlRuntimeException(ErrorCode.ERR_REPLICATION_RESULT, String.format(response.getError()));
        }
    }
}