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

import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.executor.cursor.ResultCursor;
import com.alibaba.polardbx.executor.gms.GmsTableMetaManager;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.SchemaManager;

/**
 * 重命名表结构
 *
 * @author agapple 2015年3月26日 下午8:31:03
 * @author arnkore 2017年6月2日 下午6:51
 * @since 5.1.19
 */
public class RenameTableSyncAction implements ISyncAction {

    private String schemaName;
    private String sourceTableName;
    private String targetTableName;

    public RenameTableSyncAction() {
    }

    public RenameTableSyncAction(String schemaName, String sourceTableName, String targetTableName) {
        this.schemaName = schemaName;
        this.sourceTableName = sourceTableName;
        this.targetTableName = targetTableName;
    }

    @Override
    public ResultCursor sync() {
        SchemaManager schemaManager = OptimizerContext.getContext(schemaName).getLatestSchemaManager();
        schemaManager.invalidate(sourceTableName);
        ((GmsTableMetaManager) schemaManager).tonewversion(targetTableName);
        return null;
    }

    public String getSchemaName() {
        return schemaName;
    }

    public void setSchemaName(String schemaName) {
        this.schemaName = schemaName;
    }

    public String getSourceTableName() {
        return sourceTableName;
    }

    public void setSourceTableName(String sourceTableName) {
        this.sourceTableName = sourceTableName;
    }

    public String getTargetTableName() {
        return targetTableName;
    }

    public void setTargetTableName(String targetTableName) {
        this.targetTableName = targetTableName;
    }

}
