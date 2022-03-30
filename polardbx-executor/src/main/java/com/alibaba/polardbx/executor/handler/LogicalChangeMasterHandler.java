package com.alibaba.polardbx.executor.handler;

import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.dal.LogicalDal;
import com.alibaba.polardbx.rpc.CdcRpcClient;
import com.alibaba.polardbx.rpc.cdc.CdcServiceGrpc;
import com.alibaba.polardbx.rpc.cdc.ChangeMasterRequest;
import com.alibaba.polardbx.rpc.cdc.RplCommandResponse;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlChangeMaster;

import com.alibaba.fastjson.JSON;


/**
 *
 */
public class LogicalChangeMasterHandler extends LogicalReplicationBaseHandler {

    public LogicalChangeMasterHandler(IRepository repo){
        super(repo);
    }

    @Override
    public Cursor handle(RelNode logicalPlan, ExecutionContext executionContext) {
        LogicalDal dal = (LogicalDal) logicalPlan;
        SqlChangeMaster sqlNode = (SqlChangeMaster) dal.getNativeSqlNode();

        ChangeMasterRequest request = ChangeMasterRequest.newBuilder()
            .setRequest(JSON.toJSONString(sqlNode.getParams()))
            .build();

        final CdcServiceGrpc.CdcServiceBlockingStub blockingStub = CdcRpcClient.getCdcRpcClient().getCdcServiceBlockingStub();
        RplCommandResponse response = blockingStub.changeMaster(request);
        return handleRplCommandResponse(response, blockingStub.getChannel());
    }
}