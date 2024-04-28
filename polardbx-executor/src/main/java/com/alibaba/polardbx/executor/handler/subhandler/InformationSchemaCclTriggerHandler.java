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

import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.handler.LogicalShowCclTriggerHandler;
import com.alibaba.polardbx.executor.handler.VirtualViewHandler;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.view.InformationSchemaCclTriggers;
import com.alibaba.polardbx.optimizer.view.VirtualView;
import com.google.common.collect.Lists;
import org.apache.calcite.sql.SqlShowCclTrigger;
import org.apache.calcite.sql.SqlSpecialIdentifier;
import org.apache.calcite.sql.parser.SqlParserPos;

/**
 * @author busu
 * date: 2021/5/24 11:10 上午
 */
public class InformationSchemaCclTriggerHandler extends BaseVirtualViewSubClassHandler {

    public InformationSchemaCclTriggerHandler(VirtualViewHandler virtualViewHandler) {
        super(virtualViewHandler);
    }

    @Override
    public Cursor handle(VirtualView virtualView, ExecutionContext executionContext, ArrayResultCursor cursor) {
        LogicalShowCclTriggerHandler logicalShowCclTriggerHandler = new LogicalShowCclTriggerHandler(null);
        SqlShowCclTrigger sqlShowCclTrigger = new SqlShowCclTrigger(SqlParserPos.ZERO, Lists.newArrayList(
            SqlSpecialIdentifier.CCL_RULE), true, Lists.newArrayList());
        logicalShowCclTriggerHandler.handle(sqlShowCclTrigger, executionContext);
        return logicalShowCclTriggerHandler.handle(sqlShowCclTrigger, executionContext);
    }

    @Override
    public boolean isSupport(VirtualView virtualView) {
        return virtualView instanceof InformationSchemaCclTriggers;
    }

}
