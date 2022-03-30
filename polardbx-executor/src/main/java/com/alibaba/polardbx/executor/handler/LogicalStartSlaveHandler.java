package com.alibaba.polardbx.executor.handler;

import com.alibaba.fastjson.JSON;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.dal.LogicalDal;
import com.alibaba.polardbx.rpc.CdcRpcClient;
import com.alibaba.polardbx.rpc.cdc.CdcServiceGrpc;
import com.alibaba.polardbx.rpc.cdc.RplCommandResponse;
import com.alibaba.polardbx.rpc.cdc.StartSlaveRequest;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlStartSlave;

/**
 * @author shicai.xsc 2021/3/5 14:32
 * @desc
 * @since 5.0.0.0
 */
public class LogicalStartSlaveHandler extends LogicalReplicationBaseHandler {

    public LogicalStartSlaveHandler(IRepository repo){
        super(repo);
    }

    @Override
    public Cursor handle(RelNode logicalPlan, ExecutionContext executionContext) {
        LogicalDal dal = (LogicalDal) logicalPlan;
        SqlStartSlave sqlNode = (SqlStartSlave) dal.getNativeSqlNode();

        StartSlaveRequest request = StartSlaveRequest.newBuilder()
            .setRequest(JSON.toJSONString(sqlNode.getParams()))
            .build();

        final CdcServiceGrpc.CdcServiceBlockingStub blockingStub = CdcRpcClient.getCdcRpcClient().getCdcServiceBlockingStub();
        RplCommandResponse response = blockingStub.startSlave(request);
        return handleRplCommandResponse(response, blockingStub.getChannel());
    }
}
