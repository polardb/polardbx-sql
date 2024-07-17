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

import com.alibaba.polardbx.atom.CacheVariables;
import com.alibaba.polardbx.executor.common.ExecutorContext;
import com.alibaba.polardbx.executor.cursor.ResultCursor;
import com.alibaba.polardbx.executor.pl.ProcedureManager;
import com.alibaba.polardbx.executor.pl.StoredFunctionManager;
import com.alibaba.polardbx.executor.utils.ReloadUtils;
import com.alibaba.polardbx.executor.utils.ReloadUtils.ReloadType;
import com.alibaba.polardbx.gms.engine.FileSystemManager;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.GsiMetaManager;
import com.alibaba.polardbx.optimizer.config.table.statistic.inf.SystemTableColumnStatistic;
import com.alibaba.polardbx.optimizer.config.table.statistic.inf.SystemTableTableStatistic;
import com.alibaba.polardbx.optimizer.config.table.statistic.StatisticManager;
import com.alibaba.polardbx.optimizer.core.expression.JavaFunctionManager;
import com.alibaba.polardbx.optimizer.view.SystemTableView;

import java.util.Optional;

public class ReloadSyncAction implements ISyncAction {

    private ReloadType type;

    private String schemaName;

    public ReloadSyncAction(ReloadType type, String schemaName) {
        this.type = type;
        this.schemaName = schemaName;
    }

    public ReloadSyncAction() {

    }

    @Override
    public ResultCursor sync() {
        if (type != null) {
            switch (type) {
            case SCHEMA:
                OptimizerContext.getContext(schemaName).getLatestSchemaManager().invalidateAll();
                GsiMetaManager.invalidateCache(schemaName);
                SystemTableTableStatistic.invalidateAll();
                SystemTableView.invalidateAll();
                OptimizerContext.getContext(schemaName).getVariableManager().invalidateAll();
                CacheVariables.invalidateAll();
                break;
            case DATASOURCES:
                ReloadUtils
                    .reloadDataSources(ExecutorContext.getContext(schemaName), OptimizerContext.getContext(schemaName));
                CacheVariables.invalidateAll();
                break;
            case USERS:
                break;
            case FILESTORAGE:
                // reset rate-limiter of oss file system
                FileSystemManager.resetRate();
                break;
            case PROCEDURES:
                ProcedureManager.getInstance().reload();
                break;
            case FUNCTIONS:
                StoredFunctionManager.getInstance().reload();
                break;
            case JAVA_FUNCTIONS:
                JavaFunctionManager.getInstance().reload();
                break;
            case STATISTICS:
                StatisticManager.getInstance().clearAndReloadData();
                break;

            case COLUMNARMANAGER:
                Optional
                    .ofNullable(ExecutorContext.getContext(schemaName))
                    .ifPresent(ExecutorContext::reloadColumnarManager);
                break;
            default:
                break;
            }
        }

        return null;
    }

    public ReloadType getType() {
        return type;
    }

    public void setType(ReloadType type) {
        this.type = type;
    }

    public String getSchemaName() {
        return schemaName;
    }

    public void setSchemaName(String schemaName) {
        this.schemaName = schemaName;
    }

}
