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
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.utils.PooledHttpHelper;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.AffectRowCursor;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.net.util.CdcTargetUtil;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.dal.LogicalDal;
import com.alibaba.polardbx.statistics.SQLRecorderLogger;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlChangeReplicationFilter;
import org.apache.http.entity.ContentType;

/**
 * @author shicai.xsc 2021/3/5 14:33
 * @desc
 * @since 5.0.0.0
 */
public class LogicalChangeReplicationFilterHandler extends LogicalReplicationBaseHandler {

    private static final Logger cdcLogger = SQLRecorderLogger.cdcLogger;

    public LogicalChangeReplicationFilterHandler(IRepository repo) {
        super(repo);
    }

    @Override
    public Cursor handle(RelNode logicalPlan, ExecutionContext executionContext) {
        LogicalDal dal = (LogicalDal) logicalPlan;
        SqlChangeReplicationFilter sqlNode = (SqlChangeReplicationFilter) dal.getNativeSqlNode();

        String daemonEndpoint = CdcTargetUtil.getReplicaDaemonMasterTarget();
        String res;
        try {
            res = PooledHttpHelper.doPost("http://" + daemonEndpoint + "/replica/changeReplicationFilter",
                ContentType.APPLICATION_JSON,
                JSON.toJSONString(sqlNode.getParams()), 10000);
        } catch (Exception e) {
            cdcLogger.error("change replication filter error!", e);
            throw new TddlRuntimeException(ErrorCode.ERR_REPLICATION_RESULT, e, e.getMessage());
        }
        ResultCode<?> httpResult = JSON.parseObject(res, ResultCode.class);
        if (httpResult.getCode() != CdcConstants.SUCCESS_CODE) {
            cdcLogger.warn(
                "change replication filter failed! code:" + httpResult.getCode() + ", msg:" + httpResult.getMsg());
            throw new TddlRuntimeException(ErrorCode.ERR_REPLICATION_RESULT, httpResult.getMsg());
        }
        return new AffectRowCursor(0);
    }
}
