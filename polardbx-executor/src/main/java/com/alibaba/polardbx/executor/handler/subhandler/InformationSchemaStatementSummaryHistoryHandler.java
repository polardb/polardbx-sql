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

package com.alibaba.polardbx.executor.handler.subhandler;

import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.handler.VirtualViewHandler;
import com.alibaba.polardbx.executor.sync.StatementSummarySyncAction;
import com.alibaba.polardbx.gms.topology.SystemDbHelper;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.view.InformationSchemaStatementSummaryHistory;
import com.alibaba.polardbx.optimizer.view.VirtualView;
import static com.alibaba.polardbx.executor.handler.subhandler.InformationSchemaStatementSummaryHandler.buildFinalResultFromSync;

/**
 * @author busu
 * date: 2021/11/15 8:37 下午
 */
public class InformationSchemaStatementSummaryHistoryHandler extends BaseVirtualViewSubClassHandler {
    public InformationSchemaStatementSummaryHistoryHandler(
        VirtualViewHandler virtualViewHandler) {
        super(virtualViewHandler);
    }

    @Override
    public Cursor handle(VirtualView virtualView, ExecutionContext executionContext, ArrayResultCursor cursor) {
        long timestamp = System.currentTimeMillis();
        StatementSummarySyncAction statementSummarySyncAction = new StatementSummarySyncAction();
        statementSummarySyncAction.setHistory(true);
        statementSummarySyncAction.setTimestamp(timestamp);
        String schema = SystemDbHelper.DEFAULT_DB_NAME;
        boolean local = !executionContext.getParamManager().getBoolean(ConnectionParams.ENABLE_REMOTE_SYNC_ACTION);
        buildFinalResultFromSync(statementSummarySyncAction, local, schema, cursor);
        return cursor;
    }

    @Override
    public boolean isSupport(VirtualView virtualView) {
        return virtualView instanceof InformationSchemaStatementSummaryHistory;
    }

}
