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

package com.alibaba.polardbx.executor.ddl.job.task.cdc;

import com.alibaba.fastjson.annotation.JSONCreator;
import com.alibaba.polardbx.common.cdc.CdcDdlMarkVisibility;
import com.alibaba.polardbx.common.cdc.CdcManagerHelper;
import com.alibaba.polardbx.common.cdc.ICdcManager;
import com.alibaba.polardbx.common.ddl.newengine.DdlType;
import com.alibaba.polardbx.executor.ddl.job.meta.TableMetaChanger;
import com.alibaba.polardbx.executor.ddl.job.task.BaseCdcTask;
import com.alibaba.polardbx.executor.ddl.job.task.BaseDdlTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.executor.utils.failpoint.FailPoint;
import com.alibaba.polardbx.gms.metadb.MetaDbDataSource;
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.optimizer.context.DdlContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import lombok.Getter;
import lombok.Setter;
import org.apache.calcite.sql.SqlKind;

import java.sql.Connection;
import java.util.Map;

import static com.alibaba.polardbx.executor.ddl.job.meta.TableMetaChanger.buildNewPartTbNamePattern;
import static com.alibaba.polardbx.executor.ddl.job.task.cdc.CdcMarkUtil.buildExtendParameter;
import static com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalRenameTable.checkTableNamePatternForRename;

/**
 * Created by ziyang.lb
 **/
@TaskName(name = "CdcTruncateWithRecycleMarkTask")
@Getter
@Setter
public class CdcTruncateWithRecycleMarkTask extends BaseCdcTask {
    public static final String CDC_RECYCLE_HINTS = "/* RECYCLEBIN_INTERMEDIATE_DDL=true */";

    private final String sourceTableName;
    private final String targetTableName;

    @JSONCreator
    public CdcTruncateWithRecycleMarkTask(String schemaName, String sourceTableName, String targetTableName) {
        super(schemaName);
        this.sourceTableName = sourceTableName;
        this.targetTableName = targetTableName;
    }

    @Override
    protected void duringTransaction(Connection metaDbConnection, ExecutionContext executionContext) {
        updateSupportedCommands(true, false, metaDbConnection);
        FailPoint.injectRandomExceptionFromHint(executionContext);
        FailPoint.injectRandomSuspendFromHint(executionContext);
        mark4RenameTable(executionContext);
    }

    protected void mark4RenameTable(ExecutionContext executionContext) {
        // 如果物理表名也发生了变化，需要将新的tablePattern作为附加参数传给cdcManager
        // 如果物理表名也发生了变更，此处所有物理表已经都完成了rename(此时用户针对该逻辑表提交的任何dml操作都会报错)，cdc打标必须先于元数据变更
        // 如果物理表名未进行变更，那么tablePattern不会发生改变，Rename是一个轻量级的操作，打标的位置放到元数据变更之前或之后，都可以
        boolean isNewPartDb = DbInfoManager.getInstance().isNewPartitionDb(schemaName);
        String newTbNamePattern;
        if (isNewPartDb) {
            newTbNamePattern = buildNewPartTbNamePattern(schemaName, sourceTableName);
        } else {
            newTbNamePattern = TableMetaChanger.buildNewTbNamePattern(executionContext, schemaName,
                sourceTableName, targetTableName, !checkTableNamePatternForRename(schemaName, sourceTableName));
        }
        Map<String, Object> params = buildExtendParameter(executionContext);
        params.put(ICdcManager.TABLE_NEW_NAME, targetTableName);
        params.put(ICdcManager.TABLE_NEW_PATTERN, newTbNamePattern);

        String renameSql =
            String.format(CDC_RECYCLE_HINTS + "RENAME TABLE `%s` TO `%s`", sourceTableName, targetTableName);
        DdlContext ddlContext = executionContext.getDdlContext();
        CdcManagerHelper.getInstance()
            .notifyDdlNew(schemaName, sourceTableName, SqlKind.RENAME_TABLE.name(), renameSql,
                DdlType.RENAME_TABLE, ddlContext.getJobId(), getTaskId(), CdcDdlMarkVisibility.Public, params);
    }
}
