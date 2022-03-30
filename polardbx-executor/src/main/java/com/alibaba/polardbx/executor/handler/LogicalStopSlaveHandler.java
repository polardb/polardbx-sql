package com.alibaba.polardbx.executor.handler;

import com.alibaba.fastjson.JSON;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.dal.LogicalDal;
import com.alibaba.polardbx.rpc.CdcRpcClient;
import com.alibaba.polardbx.rpc.cdc.CdcServiceGrpc;
import com.alibaba.polardbx.rpc.cdc.RplCommandResponse;
import com.alibaba.polardbx.rpc.cdc.StopSlaveRequest;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlStopSlave;

/**
 * @author shicai.xsc 2021/3/5 14:32
 * @desc
 * @since 5.0.0.0
 */
public class LogicalStopSlaveHandler extends LogicalReplicationBaseHandler {

    public LogicalStopSlaveHandler(IRepository repo){
        super(repo);
    }

    @Override
    public Cursor handle(RelNode logicalPlan, ExecutionContext executionContext) {
        LogicalDal dal = (LogicalDal) logicalPlan;
        SqlStopSlave sqlNode = (SqlStopSlave) dal.getNativeSqlNode();

        StopSlaveRequest request = StopSlaveRequest.newBuilder()
            .setRequest(JSON.toJSONString(sqlNode.getParams()))
            .build();

        final CdcServiceGrpc.CdcServiceBlockingStub blockingStub = CdcRpcClient.getCdcRpcClient().getCdcServiceBlockingStub();
        RplCommandResponse response = blockingStub.stopSlave(request);
        return handleRplCommandResponse(response, blockingStub.getChannel());
    }
}
