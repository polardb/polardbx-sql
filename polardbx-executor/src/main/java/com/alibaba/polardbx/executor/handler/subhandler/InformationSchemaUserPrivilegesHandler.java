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
import com.alibaba.polardbx.gms.privilege.PolarPrivManager;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.view.InformationSchemaUserPrivileges;
import com.alibaba.polardbx.optimizer.view.VirtualView;

/**
 * @author bairui.lrj
 * @since 5.4.10
 */
public class InformationSchemaUserPrivilegesHandler extends BaseVirtualViewSubClassHandler {
    public InformationSchemaUserPrivilegesHandler(VirtualViewHandler virtualViewHandler) {
        super(virtualViewHandler);
    }

    /**
     * @param virtualView the origin virtualView to be handled
     * @param executionContext context may be useful for some handler
     * @param cursor empty cursor with types defined
     * @return the answer cursor
     */
    @Override
    public Cursor handle(VirtualView virtualView, ExecutionContext executionContext, ArrayResultCursor cursor) {
        PolarPrivManager.getInstance()
            .listUserPrivileges(executionContext.getPrivilegeContext().getPolarUserInfo())
            .forEach(cursor::addRow);
        return cursor;
    }

    @Override
    public boolean isSupport(VirtualView virtualView) {
        return virtualView instanceof InformationSchemaUserPrivileges;
    }
}
