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

package com.alibaba.polardbx.executor.handler.ddl;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.AffectRowCursor;
import com.alibaba.polardbx.executor.handler.HandlerCommon;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.executor.sync.DropViewSyncAction;
import com.alibaba.polardbx.executor.sync.SyncManagerHelper;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalDropView;
import com.alibaba.polardbx.optimizer.view.PolarDbXSystemTableView;
import com.alibaba.polardbx.optimizer.view.SystemTableView;
import org.apache.calcite.rel.RelNode;

import java.util.ArrayList;

/**
 * @author dylan
 */
public class LogicalDropViewHandler extends HandlerCommon {

    private static final Logger logger = LoggerFactory.getLogger(LogicalDropViewHandler.class);

    public LogicalDropViewHandler(IRepository repo) {
        super(repo);
    }

    @Override
    public Cursor handle(final RelNode logicalPlan, ExecutionContext executionContext) {

        LogicalDropView logicalDropView = (LogicalDropView) logicalPlan;

        String schemaName = logicalDropView.getSchemaName();
        String viewName = logicalDropView.getViewName();

        if (!logicalDropView.isIfExists()) {
            SystemTableView.Row row = OptimizerContext.getContext(schemaName).getViewManager().select(viewName);
            if (row == null) {
                throw new TddlRuntimeException(ErrorCode.ERR_VIEW, "Unknown view " + viewName);
            }
        }

        boolean success = OptimizerContext.getContext(schemaName).getViewManager().delete(viewName);

        if (!success) {
            throw new TddlRuntimeException(ErrorCode.ERR_VIEW,
                "drop view fail for " + PolarDbXSystemTableView.TABLE_NAME + " can not write");
        }

        ArrayList<String> viewList = new ArrayList<>();
        viewList.add(viewName);
        SyncManagerHelper.sync(new DropViewSyncAction(schemaName, viewList), schemaName);

        return new AffectRowCursor(new int[] {0});
    }
}

