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

import com.alibaba.polardbx.common.exception.TddlNestableRuntimeException;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.gms.metadb.cdc.BinlogTaskInfoAccessor;
import com.alibaba.polardbx.gms.metadb.cdc.BinlogTaskRecord;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.statistics.SQLRecorderLogger;
import org.apache.calcite.rel.RelNode;

import java.sql.Connection;
import java.util.Optional;

/**
 * @author yudong
 * @since 2023/6/9 14:52
 **/
public class LogicalShowCdcStorageHandler extends HandlerCommon {

    private static final Logger cdcLogger = SQLRecorderLogger.cdcLogger;

    public LogicalShowCdcStorageHandler(IRepository repo) {
        super(repo);
    }

    @Override
    public Cursor handle(RelNode logicalPlan, ExecutionContext executionContext) {
        ArrayResultCursor result = new ArrayResultCursor("SHOW CDC STORAGE");
        result.addColumn("STORAGE_INST_ID", DataTypes.StringType);

        BinlogTaskInfoAccessor accessor = new BinlogTaskInfoAccessor();
        try (Connection metaDbConn = MetaDbUtil.getConnection()) {
            accessor.setConnection(metaDbConn);
            Optional<BinlogTaskRecord> finalTaskInfo = accessor.getFinalTaskInfo();
            if (finalTaskInfo.isPresent()) {
                for (String storageInstId : finalTaskInfo.get().getSourcesList()) {
                    result.addRow(new Object[] {storageInstId});
                }
            }
        } catch (Exception e) {
            cdcLogger.error("show cdc storage error!", e);
            throw new TddlNestableRuntimeException(e);
        }
        return result;
    }
}
