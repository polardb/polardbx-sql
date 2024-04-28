/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the License);
 * you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.polardbx.qatest.ddl.auto.columnar;

import com.alibaba.polardbx.executor.mpp.metadata.NotNull;

public abstract class CciCaseGenerator extends CaseGeneratorBase {
    private static final String SKIP_REAL_CREATE_HINT =
        "/*+TDDL:CMD_EXTRA(SKIP_DDL_TASKS=\"WaitColumnarTableCreationTask\")*/";
    private static final String ALTER_CCI_STATUS_HINT =
        "/*+TDDL:CMD_EXTRA(ALTER_CCI_STATUS=true, ALTER_CCI_STATUS_BEFORE=CREATING, ALTER_CCI_STATUS_AFTER=PUBLIC)*/";

    private static final String ALTER_CCI_STATUS_TMPL = "ALTER TABLE `%s`\n"
        + "\t ALTER INDEX `%s` VISIBLE ;\n";
    private static final String SHOW_FULL_CREATE_TABLE_TMPL = "SHOW FULL CREATE TABLE %s;\n";
    private static final String SHOW_FULL_CREATE_TABLE_RESULT_TMPL = "Table,Create Table\n%s,%s";
    private static final String CHECK_CCI_META_TMPL = "CHECK COLUMNAR INDEX `%s` ON `%s` META;\n";
    private static final String CHECK_CCI_META_RESULT_TMPL = "CCI,error_type,status,primary_key,details\n"
        + "`%s_$` ON `part_mtr`.`%s`,SUMMARY,--,--,OK (metadata of columnar index checked) Finish time: %%\n";

    public CciCaseGenerator(@NotNull Class testClass, @NotNull String testFilePrefix) {
        super(generateTestFilePath(testClass, testFilePrefix),
            generateResultFilePath(testClass, testFilePrefix));
    }

    protected String buildCheckCciMetaTest(String tableName, String indexName) {
        return String.format(CHECK_CCI_META_TMPL, indexName, tableName);
    }

    protected String buildCheckCciMetaResult(String tableName, String indexName) {
        return String.format(CHECK_CCI_META_RESULT_TMPL, indexName, tableName);
    }

    protected String buildShowFullCreateTableTest(String tableName) {
        return String.format(SHOW_FULL_CREATE_TABLE_TMPL, tableName);
    }

    protected String buildShowFullCreateTableResult(String tableName, String createTable) {
        return String.format(SHOW_FULL_CREATE_TABLE_RESULT_TMPL, tableName, createTable);
    }

    protected String buildSkipRealCreateHint() {
        return SKIP_REAL_CREATE_HINT;
    }

    protected String buildAlterCciStatus(String table, String cci) {
        return ALTER_CCI_STATUS_HINT + String.format(ALTER_CCI_STATUS_TMPL, table, cci);
    }

}
