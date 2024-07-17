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

import com.alibaba.polardbx.common.cdc.CdcDdlMarkVisibility;
import com.alibaba.polardbx.common.cdc.CdcManagerHelper;
import com.alibaba.polardbx.common.cdc.ICdcManager;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.AffectRowCursor;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.gms.metadb.MetaDbDataSource;
import com.alibaba.polardbx.gms.metadb.cdc.BinlogStreamGroupAccessor;
import com.alibaba.polardbx.gms.metadb.cdc.BinlogStreamGroupRecord;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.dal.LogicalDal;
import com.alibaba.polardbx.statistics.SQLRecorderLogger;
import com.google.common.collect.Maps;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlFlushLogs;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.commons.collections.CollectionUtils;

import java.sql.Connection;
import java.util.List;
import java.util.Map;

import static com.alibaba.polardbx.executor.ddl.job.task.cdc.CdcMarkUtil.buildExtendParameter;

/**
 * @author chengjin
 * @since 2023/7/21 14:37
 **/
public class LogicalFlushLogsHandler extends HandlerCommon {

    private static final Logger cdcLogger = SQLRecorderLogger.cdcLogger;

    public LogicalFlushLogsHandler(IRepository repo) {
        super(repo);
    }

    @Override
    public Cursor handle(RelNode logicalPlan, ExecutionContext executionContext) {
        SqlFlushLogs sqlFlushLogs = (SqlFlushLogs) ((LogicalDal) logicalPlan).getNativeSqlNode();
        SqlNode with = sqlFlushLogs.getWith();
        String groupName = "";
        if (with != null) {
            groupName = with.toString();
            try (Connection metaDbConn = MetaDbDataSource.getInstance().getConnection()) {
                BinlogStreamGroupAccessor accessor = new BinlogStreamGroupAccessor();
                accessor.setConnection(metaDbConn);
                List<BinlogStreamGroupRecord> groupRecordList = accessor.getGroup(groupName);
                if (CollectionUtils.isEmpty(groupRecordList)) {
                    throw new TddlRuntimeException(ErrorCode.ERR_CDC_GENERIC, "not found group name " + groupName);
                }
            } catch (Throwable ex) {
                if (ex instanceof TddlRuntimeException) {
                    throw (TddlRuntimeException) ex;
                }
                throw new TddlRuntimeException(ErrorCode.ERR_GMS_GENERIC, ex);
            }

        }

        Map<String, Object> extendParams = buildExtendParameter(executionContext);
        extendParams.put(ICdcManager.CDC_GROUP_NAME, groupName);
        CdcManagerHelper.getInstance().notifyDdlNew("", null, SqlKind.FLUSH_LOGS.name(),
            executionContext.getOriginSql(), null, null, null,
            CdcDdlMarkVisibility.Private, extendParams);
        return new AffectRowCursor(0);
    }
}
