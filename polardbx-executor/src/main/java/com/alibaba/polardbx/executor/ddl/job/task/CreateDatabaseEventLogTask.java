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

package com.alibaba.polardbx.executor.ddl.job.task;

import com.alibaba.fastjson.annotation.JSONCreator;
import com.alibaba.polardbx.common.eventlogger.EventLogger;
import com.alibaba.polardbx.common.eventlogger.EventType;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.gms.metadb.misc.DdlEngineAccessor;
import com.alibaba.polardbx.gms.metadb.misc.DdlEngineRecord;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import lombok.Getter;

import java.sql.Connection;

/**
 * Created by zhuqiwei.
 *
 * @author zhuqiwei
 */

@Getter
@TaskName(name = "CreateDatabaseEventLogTask")
public class CreateDatabaseEventLogTask extends BaseDdlTask {
    final private String schemaSrc;
    final private String schemaDst;
    final private Boolean createDatabaseLike;
    final private Long convertTableNum;
    final private Long rowCount;

    @JSONCreator
    public CreateDatabaseEventLogTask(String schemaSrc, String schemaDst, Boolean createDatabaseLike,
                                      Long convertTableNum, Long rowCount) {
        super(schemaDst);
        this.schemaSrc = schemaSrc;
        this.schemaDst = schemaDst;
        this.createDatabaseLike = createDatabaseLike;
        this.convertTableNum = convertTableNum;
        this.rowCount = rowCount;
    }

    @Override
    protected void duringTransaction(Connection metaDbConnection, ExecutionContext executionContext) {
        executeImpl(metaDbConnection, executionContext);
    }

    public void executeImpl(Connection metaDbConnection, ExecutionContext executionContext) {
        long timeElapseInSeconds = -1;
        try {
            DdlEngineAccessor engineAccessor = new DdlEngineAccessor();
            engineAccessor.setConnection(metaDbConnection);
            DdlEngineRecord jobsRecord = engineAccessor.query(getJobId());
            if (jobsRecord != null) {
                timeElapseInSeconds = (System.currentTimeMillis() - jobsRecord.gmtCreated) / 1000;
            }
        } catch (Throwable t) {
            LOGGER.error(String.format(
                "error occurs while query ddl job %s", getJobId()
            ));
        }

        String timeUsed = timeElapseInSeconds == -1 ? "none" : convertSecondsToHhMmSs(timeElapseInSeconds);
        if (createDatabaseLike) {
            EventLogger.log(
                EventType.CREATE_DATABASE_LIKE_AS,
                String.format(
                    "create database like finished, srcDb [%s], dstDB [%s], create [%s] tables, time use [%s]",
                    schemaSrc,
                    schemaDst,
                    convertTableNum,
                    timeUsed)
            );
        } else {
            EventLogger.log(
                EventType.CREATE_DATABASE_LIKE_AS,
                String.format(
                    "create database as finished, srcDb [%s], dstDB [%s], create [%s] tables, backfill [%s] rows, time use [%s]",
                    schemaSrc,
                    schemaDst,
                    convertTableNum,
                    rowCount,
                    timeUsed)
            );
        }
    }

    public static String convertSecondsToHhMmSs(long seconds) {
        long s = seconds % 60;
        long m = (seconds / 60) % 60;
        long h = (seconds / (60 * 60)) % 24;
        return String.format("%d:%02d:%02d", h, m, s);
    }

}

