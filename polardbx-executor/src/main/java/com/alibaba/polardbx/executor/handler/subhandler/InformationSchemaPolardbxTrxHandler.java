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

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.handler.VirtualViewHandler;
import com.alibaba.polardbx.executor.sync.ISyncAction;
import com.alibaba.polardbx.executor.sync.SyncManagerHelper;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.view.InformationSchemaPolardbxTrx;
import com.alibaba.polardbx.optimizer.view.VirtualView;

import java.util.List;
import java.util.Map;

/**
 * @author yaozhili
 */
public class InformationSchemaPolardbxTrxHandler extends BaseVirtualViewSubClassHandler {

    public InformationSchemaPolardbxTrxHandler(VirtualViewHandler virtualViewHandler) {
        super(virtualViewHandler);
    }

    @Override
    public boolean isSupport(VirtualView virtualView) {
        return virtualView instanceof InformationSchemaPolardbxTrx;
    }

    private static final Class PolardbxTrxSyncActionClass;

    static {
        try {
            PolardbxTrxSyncActionClass =
                Class.forName("com.alibaba.polardbx.transaction.sync.PolardbxTrxSyncAction");
        } catch (ClassNotFoundException e) {
            throw new TddlRuntimeException(ErrorCode.ERR_CONFIG, e, e.getMessage());
        }
    }

    @Override
    public Cursor handle(VirtualView virtualView, ExecutionContext executionContext, ArrayResultCursor cursor) {
        ISyncAction syncAction;
        try {
            syncAction = (ISyncAction) PolardbxTrxSyncActionClass.getConstructor()
                .newInstance();
        } catch (Exception e) {
            throw new TddlRuntimeException(ErrorCode.ERR_CONFIG, e, e.getMessage());
        }

        final String schema = executionContext.getSchemaName();
        List<List<Map<String, Object>>> results = SyncManagerHelper.sync(syncAction, schema);

        for (List<Map<String, Object>> rs : results) {
            if (rs == null) {
                continue;
            }

            for (Map<String, Object> row : rs) {
                cursor.addRow(InformationSchemaPolardbxTrx.COLUMNS
                    .stream().map(column -> row.get(column.name)).toArray());
            }

        }

        return cursor;
    }
}
