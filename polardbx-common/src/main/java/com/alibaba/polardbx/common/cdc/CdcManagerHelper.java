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

import com.alibaba.polardbx.common.ddl.Job;
import com.alibaba.polardbx.common.ddl.newengine.DdlType;
import com.alibaba.polardbx.common.model.lifecycle.AbstractLifecycle;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.common.utils.extension.ExtensionLoader;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author ziyang.lb 2020-12-05
 **/
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

    //老ddl引擎打标方法
    public void notifyDdl(String schemaName, String tableName, String sqlKind, String ddlSql, Job job,
                          CdcDdlMarkVisibility visibility,
                          Map<String, Object> extendParams) {
        Long jobId = job == null ? null : job.getId();
        DdlType ddlType = job == null ? null : typeTransfer(job.getType());
        CdcDDLContext context = new CdcDDLContext(schemaName, tableName, sqlKind, ddlSql, visibility, jobId, null,
            ddlType, false, extendParams, job, false, null, null, null, null, false);

        cdcManager.notifyDdl(context);
    }

    //新ddl引擎打标方法
    public void notifyDdlNew(String schemaName, String tableName, String sqlKind, String ddlSql, DdlType ddlType,
                             Long jobId,
                             Long taskId,
                             CdcDdlMarkVisibility visibility,
                             Map<String, Object> extendParams) {
        CdcDDLContext context = new CdcDDLContext(schemaName, tableName, sqlKind, ddlSql, visibility, jobId, taskId,
            ddlType, true, extendParams, null, false, null, null, null, null, false);
        cdcManager.notifyDdl(context);
    }

    public void notifySequenceDdl(String schemaName, String tableName, String sqlKind, String ddlSql, DdlType ddlType,
                                  Long jobId,
                                  Long taskId,
                                  CdcDdlMarkVisibility visibility,
                                  Map<String, Object> extendParams) {
        CdcDDLContext context = new CdcDDLContext(schemaName, tableName, sqlKind, ddlSql, visibility, jobId, taskId,
            ddlType, true, extendParams, null, false, null, null, null, null, true);
        cdcManager.notifyDdl(context);
    }

    public void notifyDdlWithContext(CdcDDLContext cdcDDLContext) {
        cdcManager.notifyDdl(cdcDDLContext);
    }

    public List<CdcDdlRecord> queryDdl(String schemaName,
                                       String tableName,
                                       String sqlKind,
                                       CdcDdlMarkVisibility visibility,
                                       Long versionId,
                                       DdlType ddlType,
                                       Map<String, Object> extendParams) {
        CdcDDLContext context = new CdcDDLContext(
            schemaName,
            tableName,
            sqlKind,
            null,
            visibility,
            null,
            null,
            ddlType,
            true,
            extendParams,
            null,
            false,
            null,
            null,
            versionId);
        return cdcManager.getDdlRecord(context);
    }

    public List<CdcDdlRecord> queryDdlByJobId(Long jobId) {
        CdcDDLContext context = new CdcDDLContext(
            null,
            null,
            null,
            null,
            null,
            jobId,
            null,
            null,
            true,
            null,
            null,
            false,
            null,
            null,
            null);
        return cdcManager.getDdlRecord(context);
    }

    //新ddl引擎打标方法
    public void notifyDdlNew(String schemaName, String tableName, String sqlKind, String ddlSql, DdlType ddlType,
                             Long jobId, Long taskId, CdcDdlMarkVisibility visibility, Map<String, Object> extendParams,
                             boolean isRefreshTableMetaInfo, Map<String, Set<String>> newTableTopology) {
        CdcDDLContext context = new CdcDDLContext(schemaName, tableName, sqlKind, ddlSql, visibility, jobId, taskId,
            ddlType, true, extendParams, null, isRefreshTableMetaInfo, newTableTopology, null, null, null, false);
        cdcManager.notifyDdl(context);
    }

    //新ddl引擎打标方法
    public void notifyDdlNew(String schemaName, String tableName, String sqlKind, String ddlSql, DdlType ddlType,
                             Long jobId, Long taskId, CdcDdlMarkVisibility visibility, Map<String, Object> extendParams,
                             boolean isRefreshTableMetaInfo, Map<String, Set<String>> newTableTopology,
                             Pair<String, TablesExtInfo> cdcMetaPair) {
        CdcDDLContext context = new CdcDDLContext(schemaName, tableName, sqlKind, ddlSql, visibility, jobId, taskId,
            ddlType, true, extendParams, null, isRefreshTableMetaInfo, newTableTopology, cdcMetaPair, null, null,
            false);
        cdcManager.notifyDdl(context);
    }

    private DdlType typeTransfer(Job.JobType jobType) {
        switch (jobType) {
        case CREATE_TABLE:
            return DdlType.CREATE_TABLE;
        case DROP_TABLE:
            return DdlType.DROP_TABLE;
        case ALTER_TABLE:
            return DdlType.ALTER_TABLE;
        case RENAME_TABLE:
            return DdlType.RENAME_TABLE;
        case TRUNCATE_TABLE:
            return DdlType.TRUNCATE_TABLE;

        case CREATE_INDEX:
            return DdlType.CREATE_INDEX;
        case DROP_INDEX:
            return DdlType.DROP_INDEX;

        case CREATE_GLOBAL_INDEX:
            return DdlType.CREATE_GLOBAL_INDEX;
        case CHECK_GLOBAL_INDEX:
            return DdlType.CHECK_GLOBAL_INDEX;
        case CHECK_COLUMNAR_INDEX:
            return DdlType.CHECK_COLUMNAR_INDEX;
        case RENAME_GLOBAL_INDEX:
            return DdlType.RENAME_GLOBAL_INDEX;
        case DROP_GLOBAL_INDEX:
            return DdlType.DROP_GLOBAL_INDEX;
        case ALTER_GLOBAL_INDEX:
            return DdlType.ALTER_GLOBAL_INDEX;
        case MOVE_TABLE:
            return DdlType.MOVE_DATABASE;//TODO,ziyang.lb 待确认，应该是MoveDataBase？待确认

        case UNSUPPORTED:
            return DdlType.UNSUPPORTED;
        default:
            throw new RuntimeException("unsupported ddltype :" + jobType);
        }
    }

    public void checkCdcBeforeStorageRemove(Set<String> storageInstIds, String identifier) {
        cdcManager.checkCdcBeforeStorageRemove(storageInstIds, identifier);
    }
}
