package com.alibaba.polardbx.executor.handler;

import com.alibaba.fastjson.JSON;
import com.alibaba.polardbx.common.cdc.CdcConstants;
import com.alibaba.polardbx.common.cdc.ResultCode;
import com.alibaba.polardbx.common.cdc.RplConstants;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.utils.HttpClientHelper;
import com.alibaba.polardbx.common.utils.PooledHttpHelper;
import com.alibaba.polardbx.druid.util.StringUtils;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.AffectRowCursor;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.net.util.CdcTargetUtil;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.dal.LogicalDal;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlResetReplicaCheck;
import org.apache.http.entity.ContentType;

import java.util.HashMap;
import java.util.Map;

/**
 * @author yudong
 * @since 2023/11/9 10:42
 **/
public class LogicalResetReplicaCheckTableHandler extends HandlerCommon {

    private static final String API_PATTERN = "http://%s/replica/fullValidation/reset";

    public LogicalResetReplicaCheckTableHandler(IRepository repo) {
        super(repo);
    }

    @Override
    public Cursor handle(RelNode logicalPlan, ExecutionContext executionContext) {
        SqlResetReplicaCheck sqlResetReplicaCheck =
            (SqlResetReplicaCheck) ((LogicalDal) logicalPlan).getNativeSqlNode();

        String dbName = sqlResetReplicaCheck.getDbName().toString();
        if (StringUtils.isEmpty(dbName)) {
            throw new TddlRuntimeException(ErrorCode.ERR_REPLICATION_RESULT, "database cannot be empty!");
        }
        Map<String, String> params = new HashMap<>();
        params.put(RplConstants.RPL_FULL_VALID_DB, dbName);
        if (sqlResetReplicaCheck.getTableName() != null) {
            String tbName = sqlResetReplicaCheck.getTableName().toString();
            params.put(RplConstants.RPL_FULL_VALID_TB, tbName);
        }

        String daemonEndpoint = CdcTargetUtil.getDaemonMasterTarget();
        String url = String.format(API_PATTERN, daemonEndpoint);
        String res;
        try {
            res = PooledHttpHelper.doPost(url, ContentType.APPLICATION_JSON, JSON.toJSONString(params), 10000);
        } catch (Exception e) {
            throw new TddlRuntimeException(ErrorCode.ERR_REPLICATION_RESULT, e);
        }

        ResultCode<?> httpResult = JSON.parseObject(res, ResultCode.class);
        if (httpResult.getCode() != CdcConstants.SUCCESS_CODE) {
            throw new TddlRuntimeException(ErrorCode.ERR_REPLICATION_RESULT, httpResult.getMsg());
        }
        return new AffectRowCursor(0);
    }
}
