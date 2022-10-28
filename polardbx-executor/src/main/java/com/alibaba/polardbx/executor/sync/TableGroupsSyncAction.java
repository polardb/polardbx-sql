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
import com.alibaba.polardbx.optimizer.tablegroup.TableGroupInfoManager;

import java.util.List;

/**
 * Created by wumu.
 *
 * @author wumu
 */
public class TableGroupsSyncAction implements ISyncAction {
    private String schemaName;
    private List<String> tableGroupNameList;

    public String getSchemaName() {
        return schemaName;
    }

    public void setSchemaName(String schemaName) {
        this.schemaName = schemaName;
    }

    public List<String> getTableGroupNameList() {
        return tableGroupNameList;
    }

    public void setTableGroupNameList(List<String> tableGroupNameList) {
        this.tableGroupNameList = tableGroupNameList;
    }

    public TableGroupsSyncAction(String schemaName, List<String> tableGroupNameList) {
        this.schemaName = schemaName;
        this.tableGroupNameList = tableGroupNameList;
    }

    @Override
    public ResultCursor sync() {
        TableGroupInfoManager tableGroupInfoManager =
            OptimizerContext.getContext(schemaName).getTableGroupInfoManager();

        if (tableGroupNameList != null && !tableGroupNameList.isEmpty()) {
            tableGroupNameList.forEach(e -> tableGroupInfoManager.reloadTableGroupByGroupName(schemaName, e));
        }
        return null;
    }
}
