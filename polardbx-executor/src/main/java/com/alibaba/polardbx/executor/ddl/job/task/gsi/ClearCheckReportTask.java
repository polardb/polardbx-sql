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

package com.alibaba.polardbx.executor.ddl.job.task.gsi;

/**
 * Clear checker report
 *
 * @author moyi
 * @since 2021/07
 */
public class ClearCheckReportTask extends CheckReportBaseTask {

    private int numDeleted = 0;

    public ClearCheckReportTask(String schemaName, String tableName, String indexName) {
        super(schemaName, tableName, indexName);
    }

    public void clear() {
        numDeleted = checkerManager.deleteReports(schemaName, tableName, indexName);
        finalResult = "" + numDeleted + " rows of report cleared.";
    }

    public int getNumDeleted() {
        return this.numDeleted;
    }
}
