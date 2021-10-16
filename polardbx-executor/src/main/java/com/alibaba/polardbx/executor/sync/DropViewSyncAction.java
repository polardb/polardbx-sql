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

package com.alibaba.polardbx.executor.sync;

import com.alibaba.polardbx.executor.cursor.ResultCursor;
import com.alibaba.polardbx.optimizer.OptimizerContext;

import java.util.List;

/**
 * @author dylan
 */
public class DropViewSyncAction implements ISyncAction {

    private String schemaName;
    private List<String> viewNames;

    public DropViewSyncAction() {

    }

    public DropViewSyncAction(String schemaName, List<String> viewNames) {
        this.schemaName = schemaName;
        this.viewNames = viewNames;
    }

    @Override
    public ResultCursor sync() {
        if (viewNames != null) {
            for (String viewName : viewNames) {
                OptimizerContext.getContext(schemaName).getViewManager().invalidate(viewName);
            }
        }

        return null;

    }

    public List<String> getViewNames() {
        return viewNames;
    }

    public void setViewNames(List<String> viewNames) {
        this.viewNames = viewNames;
    }

}