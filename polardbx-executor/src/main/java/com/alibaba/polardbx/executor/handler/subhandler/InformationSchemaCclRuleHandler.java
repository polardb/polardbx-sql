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
import com.alibaba.polardbx.executor.handler.LogicalShowCclRuleHandler;
import com.alibaba.polardbx.executor.handler.VirtualViewHandler;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.view.InformationSchemaCclRules;
import com.alibaba.polardbx.optimizer.view.VirtualView;
import com.google.common.collect.Lists;
import org.apache.calcite.sql.SqlShowCclRule;
import org.apache.calcite.sql.SqlSpecialIdentifier;
import org.apache.calcite.sql.parser.SqlParserPos;

/**
 * @author busu
 * date: 2021/5/24 10:39 上午
 */
public class InformationSchemaCclRuleHandler extends BaseVirtualViewSubClassHandler {

    public InformationSchemaCclRuleHandler(VirtualViewHandler virtualViewHandler) {
        super(virtualViewHandler);
    }

    @Override
    public Cursor handle(VirtualView virtualView, ExecutionContext executionContext, ArrayResultCursor cursor) {

        LogicalShowCclRuleHandler logicalShowCclRuleHandler = new LogicalShowCclRuleHandler(null);
        SqlShowCclRule sqlShowCclRule =
            new SqlShowCclRule(SqlParserPos.ZERO, Lists.newArrayList(SqlSpecialIdentifier.CCL_RULE), true,
                Lists.newArrayListWithCapacity(0));
        return logicalShowCclRuleHandler.handle(sqlShowCclRule, executionContext);
    }

    @Override
    public boolean isSupport(VirtualView virtualView) {
        return virtualView instanceof InformationSchemaCclRules;
    }

}
