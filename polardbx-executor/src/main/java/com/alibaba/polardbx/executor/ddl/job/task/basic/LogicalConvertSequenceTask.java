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

package com.alibaba.polardbx.executor.ddl.job.task.basic;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.annotation.JSONCreator;
import com.alibaba.polardbx.executor.ddl.job.task.BaseDdlTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.executor.ddl.newengine.utils.DdlHelper;
import com.alibaba.polardbx.executor.utils.DrdsToAutoSequenceUtil;
import com.alibaba.polardbx.gms.metadb.misc.DdlEngineTaskAccessor;
import com.alibaba.polardbx.gms.migration.TableMigrationTaskInfo;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import lombok.Getter;

import java.sql.Connection;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Created by zhuqiwei.
 *
 * @author zhuqiwei
 */
@Getter
@TaskName(name = "LogicalConvertSequenceTask")
public class LogicalConvertSequenceTask extends BaseDdlTask {
    final private String srcSchemaName;
    final private String dstSchemaName;
    final private List<String> needDoCreationTables;
    final private boolean onlyAlterImplictSeq;

    boolean errorHappened;
    boolean doNothing;
    Map<String, String> errorMessage;

    @JSONCreator
    public LogicalConvertSequenceTask(String srcSchemaName, String dstSchemaName, List<String> needDoCreationTables,
                                      boolean onlyAlterImplictSeq, boolean doNothing) {
        super(dstSchemaName);

        this.srcSchemaName = srcSchemaName;
        this.dstSchemaName = dstSchemaName;
        this.needDoCreationTables = needDoCreationTables;
        this.errorHappened = false;
        this.onlyAlterImplictSeq = onlyAlterImplictSeq;
        this.doNothing = doNothing;
        this.errorMessage = new TreeMap<>();
        onExceptionTryRecoveryThenRollback();
    }

    public void executeImpl() {
        if (doNothing) {
            return;
        }

        Map<String, String> sqls = new TreeMap<>();

        try {
            if (onlyAlterImplictSeq) {
                sqls = DrdsToAutoSequenceUtil.convertOnlyTableSequences(needDoCreationTables, srcSchemaName);
            } else {
                sqls = DrdsToAutoSequenceUtil.convertAllDrdsSequences(needDoCreationTables, srcSchemaName);
            }
        } catch (Throwable e) {
            this.errorHappened = true;
            this.errorMessage.put("all sequences in " + srcSchemaName, e.getMessage());
        }

        if (!this.errorHappened) {
            for (Map.Entry<String, String> entry : sqls.entrySet()) {
                String name = entry.getKey();
                String sql = entry.getValue();
                try {
                    DdlHelper.getServerConfigManager().executeBackgroundSql(sql, dstSchemaName, null);
                } catch (Throwable e) {
                    this.errorHappened = true;
                    this.errorMessage.put(name, e.getMessage());
                }
            }
        }
    }

    @Override
    protected void beforeTransaction(ExecutionContext executionContext) {
        executeImpl();
    }

    @Override
    protected void duringTransaction(Connection metaDbConnection, ExecutionContext executionContext) {
        //log the error info into system table
        DdlEngineTaskAccessor ddlEngineTaskAccessor = new DdlEngineTaskAccessor();
        ddlEngineTaskAccessor.setConnection(metaDbConnection);
        TableMigrationTaskInfo extraInfo;
        if (errorHappened) {
            extraInfo = new TableMigrationTaskInfo(
                srcSchemaName,
                dstSchemaName,
                null,
                false,
                null,
                null,
                null
            );
            extraInfo.setErrorInfoForSequence(this.errorMessage);
        } else {
            extraInfo = new TableMigrationTaskInfo(
                srcSchemaName,
                dstSchemaName,
                String.format("sequences in database [%s]", dstSchemaName),
                true,
                null,
                null,
                null
            );
            extraInfo.setErrorInfoForSequence(this.errorMessage);
        }

        ddlEngineTaskAccessor.updateExtraInfoForCreateDbAsLike(
            jobId,
            taskId,
            JSON.toJSONString(extraInfo)
        );
    }
}
