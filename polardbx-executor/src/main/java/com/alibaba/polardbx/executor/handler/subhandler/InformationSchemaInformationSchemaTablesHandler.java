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
import com.alibaba.polardbx.executor.handler.VirtualViewHandler;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.view.InformationSchemaInformationSchemaTables;
import com.alibaba.polardbx.optimizer.view.VirtualView;
import com.alibaba.polardbx.optimizer.view.VirtualViewType;

/**
 * @author shengyu
 */
public class InformationSchemaInformationSchemaTablesHandler extends BaseVirtualViewSubClassHandler {
    public InformationSchemaInformationSchemaTablesHandler(
        VirtualViewHandler virtualViewHandler) {
        super(virtualViewHandler);
    }

    @Override
    public boolean isSupport(VirtualView virtualView) {
        return virtualView instanceof InformationSchemaInformationSchemaTables;
    }

    @Override
    public Cursor handle(VirtualView virtualView, ExecutionContext executionContext, ArrayResultCursor cursor) {
        InformationSchemaInformationSchemaTables informationSchemaInformationSchemaTables
            = (InformationSchemaInformationSchemaTables) virtualView;
        for (VirtualViewType virtualViewType : VirtualViewType.values()) {
            cursor.addRow(new Object[] {
                "def",
                "information_schema",
                virtualViewType,
                "SYSTEM VIEW",
                "MEMORY",
                "10",
                "Fixed",
                null,
                0,
                0,
                0,
                0,
                0,
                null,
                new java.util.Date(),
                null,
                null,
                "utf8_general_ci",
                null,
                null,
                null
            });
        }
        return cursor;
    }
}
