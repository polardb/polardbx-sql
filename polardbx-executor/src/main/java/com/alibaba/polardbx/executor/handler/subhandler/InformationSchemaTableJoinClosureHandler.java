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

import com.alibaba.polardbx.common.exception.NotSupportException;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.handler.VirtualViewHandler;
import com.alibaba.polardbx.executor.sync.ISyncAction;
import com.alibaba.polardbx.executor.sync.SyncManagerHelper;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.statis.PlanAccessStat;
import com.alibaba.polardbx.optimizer.view.InformationSchemaTableJoinClosure;
import com.alibaba.polardbx.optimizer.view.VirtualView;

import java.util.List;
import java.util.Map;

/**
 * @author dylan
 */
public class InformationSchemaTableJoinClosureHandler extends BaseVirtualViewSubClassHandler {

    private static Class showTableJoinClosureSyncAction;

    static {
        // 只有server支持，这里是暂时改法，后续要将这段逻辑解耦
        try {
            showTableJoinClosureSyncAction =
                Class.forName("com.alibaba.polardbx.server.response.ShowTableJoinClosureSyncAction");
        } catch (ClassNotFoundException e) {
            throw new TddlRuntimeException(ErrorCode.ERR_CONFIG, e, e.getMessage());
        }
    }

    public InformationSchemaTableJoinClosureHandler(VirtualViewHandler virtualViewHandler) {
        super(virtualViewHandler);
    }

    @Override
    public boolean isSupport(VirtualView virtualView) {
        return virtualView instanceof InformationSchemaTableJoinClosure;
    }

    @Override
    public Cursor handle(VirtualView virtualView, ExecutionContext executionContext, ArrayResultCursor cursor) {

        ISyncAction showTableJoinClosureAction;
        if (showTableJoinClosureSyncAction == null) {
            throw new NotSupportException();
        }
        try {
            showTableJoinClosureAction = (ISyncAction) showTableJoinClosureSyncAction.getConstructor(String.class)
                .newInstance(executionContext.getSchemaName());
        } catch (Exception e) {
            throw new TddlRuntimeException(ErrorCode.ERR_CONFIG, e, e.getMessage());
        }

        List<List<Map<String, Object>>> results =
            SyncManagerHelper.sync(showTableJoinClosureAction, executionContext.getSchemaName());
        List<PlanAccessStat.PlanJoinClosureStatInfo> statInfos = PlanAccessStat.collectTableJoinClosureStat(results);
        for (int i = 0; i < statInfos.size(); i++) {
            PlanAccessStat.PlanJoinClosureStatInfo statInfo = statInfos.get(i);
            cursor.addRow(new Object[] {
                statInfo.closureKey,
                statInfo.joinTableSetStr,
                statInfo.joinClosureTableSet.size(),
                statInfo.accessCount,
                statInfo.templateCount
            });
        }
        return cursor;
    }
}

