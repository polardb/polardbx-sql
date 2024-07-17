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

package com.alibaba.polardbx.executor.ddl.job.task.columnar;

import com.alibaba.fastjson.annotation.JSONCreator;
import com.alibaba.polardbx.executor.ddl.job.task.BaseDdlTask;
import com.alibaba.polardbx.executor.gsi.CheckerManager;
import lombok.Getter;
import org.apache.calcite.sql.SqlCheckColumnarIndex;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Optional;

/**
 * @author yaozhili
 */
@Getter
public abstract class CheckCciBaseTask extends BaseDdlTask {
    /**
     * Blank if {@link #indexName} not exists in {@link #schemaName}
     */
    protected final String tableName;
    protected final String indexName;
    protected final SqlCheckColumnarIndex.CheckCciExtraCmd extraCmd;

    @JSONCreator
    public CheckCciBaseTask(String schemaName, String tableName, String indexName,
                            SqlCheckColumnarIndex.CheckCciExtraCmd extraCmd) {
        super(schemaName);
        this.tableName = tableName;
        this.indexName = indexName;
        this.extraCmd = extraCmd;
    }

    protected CheckerManager.CheckerReport createReportRecord(CheckCciMetaTask.ReportErrorType errorType,
                                                              CheckerManager.CheckerReportStatus status,
                                                              String detail) {
        return createReportRecord(errorType, status, "--", detail, "Reporter.");
    }

    protected CheckerManager.CheckerReport createReportRecord(CheckCciMetaTask.ReportErrorType errorType,
                                                              CheckerManager.CheckerReportStatus status,
                                                              String primaryKey,
                                                              String detail,
                                                              String extra) {
        return new CheckerManager.CheckerReport(-1,
            this.jobId,
            this.schemaName,
            this.tableName,
            this.schemaName,
            this.indexName,
            "",
            "",
            Optional
                .ofNullable(errorType)
                .filter(e -> e != CheckCciMetaTask.ReportErrorType.UNKNOWN)
                .map(CheckCciMetaTask.ReportErrorType::name)
                .orElse("--"),
            new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(Calendar.getInstance().getTime()),
            status.getValue(),
            primaryKey,
            detail,
            extra,
            null
        );
    }

}
