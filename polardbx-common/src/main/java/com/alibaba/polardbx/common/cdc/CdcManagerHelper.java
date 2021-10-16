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

package com.alibaba.polardbx.common.cdc;

import com.alibaba.polardbx.common.ddl.newengine.DdlType;
import com.alibaba.polardbx.common.model.lifecycle.AbstractLifecycle;
import com.alibaba.polardbx.common.utils.extension.ExtensionLoader;

import java.util.Map;
import java.util.Set;

public class CdcManagerHelper {
    private static volatile CdcManagerHelper instance;
    private final ICdcManager cdcManager;

    public static CdcManagerHelper getInstance() {
        if (instance == null) {
            synchronized (CdcManagerHelper.class) {
                if (instance == null) {
                    instance = new CdcManagerHelper();
                }
            }
        }
        return instance;
    }

    private CdcManagerHelper() {
        cdcManager = ExtensionLoader.load(ICdcManager.class);
    }

    public void initialize() {
        if (cdcManager instanceof AbstractLifecycle) {
            ((AbstractLifecycle) cdcManager).init();
        }
    }

    public void notifyDdl(String schemaName, String tableName, String sqlKind, String ddlSql,
                          DdlVisibility visibility,
                          Map<String, Object> extendParams) {
        CdcDDLContext context = new CdcDDLContext(schemaName, tableName, sqlKind, ddlSql, visibility, null, null,
            null, extendParams, false, null);

        cdcManager.notifyDdl(context);
    }

    public void notifyDdlNew(String schemaName, String tableName, String sqlKind, String ddlSql, DdlType ddlType,
                             Long jobId,
                             Long taskId,
                             DdlVisibility visibility,
                             Map<String, Object> extendParams) {
        CdcDDLContext context =
            new CdcDDLContext(schemaName, tableName, sqlKind, ddlSql, visibility, jobId, taskId, ddlType,
                extendParams, false, null);
        cdcManager.notifyDdl(context);
    }

    public void notifyDdlNew(String schemaName, String tableName, String sqlKind, String ddlSql, DdlType ddlType,
                             Long jobId, Long taskId, DdlVisibility visibility, Map<String, Object> extendParams,
                             boolean isRefreshTableMetaInfo, Map<String, Set<String>> newTableTopology) {
        CdcDDLContext context =
            new CdcDDLContext(schemaName, tableName, sqlKind, ddlSql, visibility, jobId, taskId, ddlType,
                extendParams, isRefreshTableMetaInfo, newTableTopology);
        cdcManager.notifyDdl(context);
    }

    public void checkCdcBeforeStorageRemove(Set<String> storageInstIds, String identifier) {
        cdcManager.checkCdcBeforeStorageRemove(storageInstIds, identifier);
    }
}
