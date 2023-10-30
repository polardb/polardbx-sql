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

import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.executor.cursor.ResultCursor;
import com.alibaba.polardbx.executor.pl.ProcedureManager;
import com.alibaba.polardbx.optimizer.locality.StoragePoolManager;
import com.alibaba.polardbx.statistics.SQLRecorderLogger;

public class AlterStoragePoolSyncAction implements ISyncAction {
    String schemaName;
    String storagePoolName;

    private static final Logger LOGGER = SQLRecorderLogger.ddlMetaLogger;

    public AlterStoragePoolSyncAction() {
    }

    public AlterStoragePoolSyncAction(String schemaName, String storagePoolName) {
        this.schemaName = schemaName;
        this.storagePoolName = storagePoolName;
    }

    @Override
    public ResultCursor sync() {
        LOGGER.info("AlterStoragePoolSyncAction triggered! reload storage pool info from metadb");
        StoragePoolManager.getInstance().reloadStoragePoolInfoFromMetaDb();
        return null;
    }

    public String getSchemaName() {
        return schemaName;
    }

    public void setSchemaName(String schemaName) {
        this.schemaName = schemaName;
    }

    public String getStoragePoolName() {
        return storagePoolName;
    }

}
