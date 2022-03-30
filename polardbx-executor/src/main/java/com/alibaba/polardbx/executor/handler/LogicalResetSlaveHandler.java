package com.alibaba.polardbx.executor.handler;

import com.alibaba.fastjson.JSON;
import com.alibaba.polardbx.common.cdc.RplConstants;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.dal.LogicalDal;
import com.alibaba.polardbx.rpc.CdcRpcClient;
import com.alibaba.polardbx.rpc.cdc.CdcServiceGrpc;
import com.alibaba.polardbx.rpc.cdc.ResetSlaveRequest;
import com.alibaba.polardbx.rpc.cdc.RplCommandResponse;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlResetSlave;

import java.util.HashMap;
import java.util.Map;

/**
 * @author shicai.xsc 2021/5/26 16:37
 * @since 5.0.0.0
 */
public class LogicalResetSlaveHandler extends LogicalReplicationBaseHandler {

    public LogicalResetSlaveHandler(IRepository repo) {
        super(repo);
    }

    @Override
    public Cursor handle(RelNode logicalPlan, ExecutionContext executionContext) {
        LogicalDal dal = (LogicalDal) logicalPlan;
        SqlResetSlave sqlNode = (SqlResetSlave) dal.getNativeSqlNode();

        Map<String, String> params = new HashMap<>();
        params.putAll(sqlNode.getParams());
        params.put(RplConstants.IS_ALL, String.valueOf(sqlNode.isAll()));

        ResetSlaveRequest request = ResetSlaveRequest.newBuilder()
            .setRequest(JSON.toJSONString(params))
            .build();

        final CdcServiceGrpc.CdcServiceBlockingStub blockingStub =
            CdcRpcClient.getCdcRpcClient().getCdcServiceBlockingStub();
        RplCommandResponse response = blockingStub.resetSlave(request);
        return handleRplCommandResponse(response, blockingStub.getChannel());
    }
}
