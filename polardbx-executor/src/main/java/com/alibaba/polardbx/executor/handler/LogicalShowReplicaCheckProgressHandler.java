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

import com.alibaba.fastjson.JSON;
import com.alibaba.polardbx.common.cdc.CdcConstants;
import com.alibaba.polardbx.common.cdc.ResultCode;
import com.alibaba.polardbx.common.cdc.RplConstants;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.utils.PooledHttpHelper;
import com.alibaba.polardbx.druid.util.StringUtils;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.net.util.CdcTargetUtil;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.rel.dal.LogicalDal;
import lombok.Data;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlShowReplicaCheckProgress;
import org.apache.http.entity.ContentType;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author yudong
 * @since 2023/11/9 10:41
 **/
public class LogicalShowReplicaCheckProgressHandler extends HandlerCommon {

    private static final String API_PATTERN = "http://%s/replica/fullValidation/progress";

    public LogicalShowReplicaCheckProgressHandler(IRepository repo) {
        super(repo);
    }

    @Override
    public Cursor handle(RelNode logicalPlan, ExecutionContext executionContext) {
        SqlShowReplicaCheckProgress sqlShowReplicaCheckProgress =
            (SqlShowReplicaCheckProgress) ((LogicalDal) logicalPlan).getNativeSqlNode();

        String dbName = sqlShowReplicaCheckProgress.getDbName().toString();
        if (StringUtils.isEmpty(dbName)) {
            throw new TddlRuntimeException(ErrorCode.ERR_REPLICATION_RESULT, "database cannot be empty!");
        }
        Map<String, String> params = new HashMap<>();
        params.put(RplConstants.RPL_FULL_VALID_DB, dbName);
        if (sqlShowReplicaCheckProgress.getTableName() != null) {
            String tbName = sqlShowReplicaCheckProgress.getTableName().toString();
            params.put(RplConstants.RPL_FULL_VALID_TB, tbName);
        }

        String daemonEndpoint = CdcTargetUtil.getDaemonMasterTarget();
        String url = String.format(API_PATTERN, daemonEndpoint);
        String res;
        try {
            res = PooledHttpHelper.doPost(url, ContentType.APPLICATION_JSON, JSON.toJSONString(params), 10000);
        } catch (Exception e) {
            throw new TddlRuntimeException(ErrorCode.ERR_REPLICATION_RESULT, e, e.getMessage());
        }

        ResultCode<?> httpResult = JSON.parseObject(res, ResultCode.class);
        if (httpResult.getCode() != CdcConstants.SUCCESS_CODE) {
            throw new TddlRuntimeException(ErrorCode.ERR_REPLICATION_RESULT, httpResult.getMsg());
        }

        try {
            final ArrayResultCursor result = new ArrayResultCursor("CHECK REPLICA TABLE SHOW PROGRESS");
            result.addColumn("DATABASE", DataTypes.StringType);
            result.addColumn("TABLE", DataTypes.StringType);
            result.addColumn("STAGE", DataTypes.StringType);
            result.addColumn("STATUS", DataTypes.StringType);
            result.addColumn("SUMMARY", DataTypes.StringType);
            result.initMeta();

            String jsonStr = (String) httpResult.getData();
            List<ReplicaFullValidProgressInfo> infos = JSON.parseArray(jsonStr, ReplicaFullValidProgressInfo.class);

            infos.forEach(r -> result.addRow(
                new Object[] {r.getDbName(), r.getTbName(), r.getStage(), r.getStatus(), r.getSummary()}));
            return result;
        } catch (Exception e) {
            throw new TddlRuntimeException(ErrorCode.ERR_REPLICATION_RESULT, e, e.getMessage());
        }
    }

    @Data
    private static class ReplicaFullValidProgressInfo {
        String dbName;
        String tbName;
        String stage;
        String status;
        String summary;
    }

}
