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

import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.executor.ddl.job.task.columnar.CheckCciMetaTask.ReportErrorType;
import com.alibaba.polardbx.qatest.DDLBaseNewDBTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.google.common.truth.Truth;
import lombok.Data;
import org.apache.calcite.linq4j.Ord;
import org.apache.commons.lang3.tuple.ImmutableTriple;
import org.apache.commons.lang3.tuple.Triple;
import org.jetbrains.annotations.NotNull;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class CheckCciTest extends DDLBaseNewDBTestCase {

    private static final String PRIMARY_TABLE_NAME1 = "check_cci_meta_test_prim_auto_1";
    private static final String PRIMARY_TABLE_NAME2 = "check_cci_meta_test_prim_auto_2";
    private static final String PRIMARY_TABLE_NAME3 = "check_cci_meta_test_prim_auto_3";
    private static final String INDEX_NAME1 = "check_cci_meta_test_cci_auto_1";
    private static final String INDEX_NAME2 = "check_cci_meta_test_cci_auto_2";
    // Create duplicated index name for test
    private static final String INDEX_NAME3 = "check_cci_meta_test_cci_auto_2";
    private static final String NONEXISTENT_NAME = "check_cci_meta_nonexistent";
    private static final String CREATE_TABLE_TEMPLATE = "CREATE TABLE `{0}` ("
        + "`pk` int(11) NOT NULL AUTO_INCREMENT,"
        + "`c1` int(11) DEFAULT NULL,"
        + "`c2` int(11) DEFAULT NULL,"
        + "`c3` int(11) DEFAULT NULL,"
        + "PRIMARY KEY (`pk`),"
        + "CLUSTERED COLUMNAR INDEX `{1}` (`c2`)"
        + ") ENGINE = InnoDB DEFAULT CHARSET = utf8mb4";

    private String indexNameWithSuffix1 = null;
    private String indexNameWithSuffix2 = null;
    private String indexNameWithSuffix3 = null;

    @Override
    public boolean usingNewPartDb() {
        return true;
    }

    @Before
    public void before() {
        indexNameWithSuffix1 = createTable(PRIMARY_TABLE_NAME1, INDEX_NAME1);
        indexNameWithSuffix2 = createTable(PRIMARY_TABLE_NAME2, INDEX_NAME2);
        indexNameWithSuffix3 = createTable(PRIMARY_TABLE_NAME3, INDEX_NAME3);
    }

    @After
    public void after() {
        dropTableIfExists(PRIMARY_TABLE_NAME1);
        dropTableIfExists(PRIMARY_TABLE_NAME2);
        dropTableIfExists(PRIMARY_TABLE_NAME3);
    }

    @Test
    public void testCheckCciMetaWithOriginIndexName() {
        // CHECK COLUMNAR INDEX check_cci_meta_test_cci_auto_1 ON check_cci_meta_test_prim_auto_1 META
        checkWithIndexAndTableName(INDEX_NAME1, PRIMARY_TABLE_NAME1);

        // CHECK COLUMNAR INDEX check_cci_meta_test_cci_auto_1 ON ddl1_3343801_new.check_cci_meta_test_prim_auto_1 META
        checkWithIndexDatabaseTableName(INDEX_NAME1, tddlDatabase1, PRIMARY_TABLE_NAME1);
    }

    @Test
    public void testCheckCciMetaWithDuplicatedIndexName() {
        // CHECK COLUMNAR INDEX check_cci_meta_test_cci_auto_1 META
        checkWithIndexNameOnly(INDEX_NAME1);

        // CHECK COLUMNAR INDEX check_cci_meta_test_cci_auto_2 ON check_cci_meta_test_prim_auto_2 META
        checkWithIndexAndTableName(INDEX_NAME2, PRIMARY_TABLE_NAME2);

        // CHECK COLUMNAR INDEX check_cci_meta_test_cci_auto_2 ON ddl1_3343801_new.check_cci_meta_test_prim_auto_3 META
        checkWithIndexDatabaseTableName(INDEX_NAME3, tddlDatabase1, PRIMARY_TABLE_NAME3);
    }

    @Test
    public void testCheckCciMetaWithUniqueIndexName() {
        // CHECK COLUMNAR INDEX check_cci_meta_test_cci_auto_1_$df5a META
        checkWithIndexNameOnly(indexNameWithSuffix1);

        // CHECK COLUMNAR INDEX check_cci_meta_test_cci_auto_1_$df5a ON check_cci_meta_test_prim_auto_1 META
        checkWithIndexAndTableName(indexNameWithSuffix1, PRIMARY_TABLE_NAME1);

        // CHECK COLUMNAR INDEX check_cci_meta_test_cci_auto_1_$df5a ON ddl1_3343801_new.check_cci_meta_test_prim_auto_1 META
        checkWithIndexDatabaseTableName(indexNameWithSuffix1, tddlDatabase1, PRIMARY_TABLE_NAME1);

        // CHECK COLUMNAR INDEX check_cci_meta_test_cci_auto_2_$df5a META
        checkWithIndexNameOnly(indexNameWithSuffix2);

        // CHECK COLUMNAR INDEX check_cci_meta_test_cci_auto_2_$df5a ON check_cci_meta_test_prim_auto_2 META
        checkWithIndexAndTableName(indexNameWithSuffix2, PRIMARY_TABLE_NAME2);

        // CHECK COLUMNAR INDEX check_cci_meta_test_cci_auto_2_$df5a ON ddl1_3343801_new.check_cci_meta_test_prim_auto_3 META
        checkWithIndexDatabaseTableName(indexNameWithSuffix3, tddlDatabase1, PRIMARY_TABLE_NAME3);
    }

    @Test
    public void testCheckCciMetaErrorTableNotExists() {
        // CHECK COLUMNAR INDEX {indexName|indexNameWithSuffix} META

        String sqlCheckCciMeta = MessageFormat.format("CHECK COLUMNAR INDEX {0} META;", NONEXISTENT_NAME);
        checkCciFailed(sqlCheckCciMeta, String.format("CCI '%s' doesn't exist.", NONEXISTENT_NAME));

        sqlCheckCciMeta = MessageFormat.format("CHECK COLUMNAR INDEX {0} META;", INDEX_NAME2);
        checkCciFailed(sqlCheckCciMeta, String.format("Multiple CCI named '%s' found.", INDEX_NAME2));

        // CHECK COLUMNAR INDEX {indexName} ON {tableName} META

        sqlCheckCciMeta =
            MessageFormat.format("CHECK COLUMNAR INDEX {0} ON {1} META;", NONEXISTENT_NAME, NONEXISTENT_NAME);
        checkCciFailed(sqlCheckCciMeta, String.format("Table '%s' doesn't exist.", NONEXISTENT_NAME));

        sqlCheckCciMeta = MessageFormat.format("CHECK COLUMNAR INDEX {0} ON {1} META;", INDEX_NAME1, NONEXISTENT_NAME);
        checkCciFailed(sqlCheckCciMeta, String.format("Table '%s' doesn't exist.", NONEXISTENT_NAME));

        sqlCheckCciMeta =
            MessageFormat.format("CHECK COLUMNAR INDEX {0} ON {1} META;", indexNameWithSuffix1, NONEXISTENT_NAME);
        checkCciFailed(sqlCheckCciMeta, String.format("Table '%s' doesn't exist.", NONEXISTENT_NAME));

        sqlCheckCciMeta =
            MessageFormat.format("CHECK COLUMNAR INDEX {0} ON {1} META;", NONEXISTENT_NAME, PRIMARY_TABLE_NAME1);
        checkCciFailed(sqlCheckCciMeta,
            String.format("CCI '%s' on table '%s' doesn't exist.", NONEXISTENT_NAME, PRIMARY_TABLE_NAME1));

        sqlCheckCciMeta =
            MessageFormat.format("CHECK COLUMNAR INDEX {0} ON {1} META;", indexNameWithSuffix2, PRIMARY_TABLE_NAME1);
        checkCciFailed(sqlCheckCciMeta,
            String.format("CCI '%s' on table '%s' doesn't exist.", indexNameWithSuffix2, PRIMARY_TABLE_NAME1));

        // CHECK COLUMNAR INDEX {indexName} ON {schemaName}.{tableName} META

        sqlCheckCciMeta =
            MessageFormat.format("CHECK COLUMNAR INDEX {0} ON {1}.{2} META;", NONEXISTENT_NAME, NONEXISTENT_NAME,
                NONEXISTENT_NAME);
        checkCciFailed(sqlCheckCciMeta, String.format("Schema '%s' not found", NONEXISTENT_NAME));

        sqlCheckCciMeta =
            MessageFormat.format("CHECK COLUMNAR INDEX {0} ON {1}.{2} META;", INDEX_NAME1, NONEXISTENT_NAME,
                NONEXISTENT_NAME);
        checkCciFailed(sqlCheckCciMeta, String.format("Schema '%s' not found", NONEXISTENT_NAME));

        sqlCheckCciMeta =
            MessageFormat.format("CHECK COLUMNAR INDEX {0} ON {1}.{2} META;", indexNameWithSuffix1, NONEXISTENT_NAME,
                NONEXISTENT_NAME);
        checkCciFailed(sqlCheckCciMeta, String.format("Schema '%s' not found", NONEXISTENT_NAME));

        sqlCheckCciMeta = MessageFormat.format("CHECK COLUMNAR INDEX {0} ON {1}.{2} META;", INDEX_NAME1, tddlDatabase1,
            NONEXISTENT_NAME);
        checkCciFailed(sqlCheckCciMeta, String.format("Table '%s' doesn't exist.", NONEXISTENT_NAME));

        sqlCheckCciMeta =
            MessageFormat.format("CHECK COLUMNAR INDEX {0} ON {1}.{2} META;", indexNameWithSuffix1, tddlDatabase1,
                NONEXISTENT_NAME);
        checkCciFailed(sqlCheckCciMeta, String.format("Table '%s' doesn't exist.", NONEXISTENT_NAME));

        sqlCheckCciMeta =
            MessageFormat.format("CHECK COLUMNAR INDEX {0} ON {1}.{2} META;", INDEX_NAME1, NONEXISTENT_NAME,
                PRIMARY_TABLE_NAME1);
        checkCciFailed(sqlCheckCciMeta, String.format("Schema '%s' not found", NONEXISTENT_NAME));

        sqlCheckCciMeta =
            MessageFormat.format("CHECK COLUMNAR INDEX {0} ON {1}.{2} META;", indexNameWithSuffix1, NONEXISTENT_NAME,
                PRIMARY_TABLE_NAME1);
        checkCciFailed(sqlCheckCciMeta, String.format("Schema '%s' not found", NONEXISTENT_NAME));

    }

    @Test
    public void testCheckCciWithCovering() {
        JdbcUtil.executeUpdateSuccess(
            tddlConnection,
            String.format("drop index %s on %s", INDEX_NAME1, PRIMARY_TABLE_NAME1));

        createCciSuccess(String.format(
            "create clustered columnar index %s on %s(c1) covering(c3)",
            INDEX_NAME1,
            PRIMARY_TABLE_NAME1));

        final List<CheckCciResult> checkCciResults = checkWithIndexAndTableName(INDEX_NAME1, PRIMARY_TABLE_NAME1);
        Truth.assertThat(checkCciResults).hasSize(1);
        Truth.assertThat(checkCciResults.get(0).errorType).isEqualTo(ReportErrorType.SUMMARY);
        Truth.assertThat(checkCciResults.get(0).details).startsWith("OK (metadata of columnar index checked)");
    }

    @Test
    public void testCheckCciShow() {
        // Generate report
        testCheckCciMetaWithUniqueIndexName();

        final String sqlCheckCciMeta = MessageFormat.format("CHECK COLUMNAR INDEX {0} SHOW;", indexNameWithSuffix1);
        final List<CheckCciResult> checkCciResults = getCheckReport(sqlCheckCciMeta);

        // Check report not empty
        Truth.assertWithMessage("No result for %s", sqlCheckCciMeta)
            .that(checkCciResults)
            .isNotEmpty();

        validateOkReport(checkCciResults);
    }

    @Test
    public void testCheckCciClear() {
        // Generate report
        testCheckCciMetaWithUniqueIndexName();

        final String sqlCheckCciMeta = MessageFormat.format("CHECK COLUMNAR INDEX {0} CLEAR;", indexNameWithSuffix1);
        final List<CheckCciResult> checkCciResults = getCheckReport(sqlCheckCciMeta);

        // Check clear result not empty
        Truth.assertWithMessage("Unexpected clear result size %s", sqlCheckCciMeta)
            .that(checkCciResults)
            .hasSize(1);

        final CheckCciResult checkCciResult = checkCciResults.get(0);
        Truth.assertWithMessage("Missing summary row")
            .that(checkCciResult.getErrorType())
            .isEqualTo(ReportErrorType.SUMMARY);
        Truth.assertThat(checkCciResult.getDetails()).contains("rows of report cleared.");
    }

    /**
     * Check & get report, or just show latest check report
     *
     * @param sql : CHECK COLUMNAR INDEX , CHECK COLUMNAR INDEX META , CHECK COLUMNAR INDEX SHOW
     * @return reports
     */
    @NotNull
    private List<CheckCciResult> getCheckReport(String sql) {
        final List<CheckCciResult> checkCciResults = new ArrayList<>();
        try {
            final ResultSet rs = JdbcUtil.executeQuery(sql, tddlConnection);
            while (rs.next()) {
                final CheckCciResult checkCciResult = CheckCciResult.create(rs);
                checkCciResults.add(checkCciResult);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return checkCciResults;
    }

    private void checkCciFailed(String sql, String errMsg) {
        JdbcUtil.executeQueryFaied(tddlConnection, sql, errMsg);
    }

    private static void validateOkReport(List<CheckCciResult> checkCciResults) {
        boolean summaryFound = false;
        String summaryDetail = "";

        for (Ord<CheckCciResult> o : Ord.zip(checkCciResults)) {
            final CheckCciResult checkResult = o.e;

            // Check 'CCI' column in report
            final Triple<String, String, String> schemaTableIndexName = checkResult.decodeIndexName();
            Truth.assertWithMessage("Empty 'CCI' column found in result row[%s]: %s", o.i, checkResult.toString())
                .that(schemaTableIndexName.getRight())
                .isNotNull();

            // Check 'ERROR_TYPE' and 'DETAILS' column in report
            switch (checkResult.errorType) {
            case SUMMARY:
                Truth.assertWithMessage(
                        "More than one summary row found: first[%s] second[%s]",
                        summaryDetail,
                        checkResult.details)
                    .that(summaryFound)
                    .isFalse();

                summaryFound = true;
                summaryDetail = checkResult.details;

                // Check 'DETAILS' column in report
                Truth.assertWithMessage("Unexpected error message: %s", summaryDetail)
                    .that(summaryDetail)
                    .ignoringCase()
                    .contains("OK (metadata of columnar index checked) Finish time:");
                break;
            default:
                Truth.assertWithMessage("Unexpected error type: %s", checkResult.errorType).fail();
            }
        }

        Truth.assertWithMessage("Missing summary row").that(summaryFound).isTrue();
    }

    /**
     * Check columnar index meta with index, database and table name
     *
     * @param indexName index name, with or without suffix
     * @param databaseName database name
     * @param tableName table name
     */
    private List<CheckCciResult> checkWithIndexDatabaseTableName(String indexName, String databaseName,
                                                                 String tableName) {
        final String sqlCheckCciMeta =
            MessageFormat.format("CHECK COLUMNAR INDEX {0} ON {1}.{2} META;", indexName, databaseName, tableName);
        final List<CheckCciResult> checkCciResults = getCheckReport(sqlCheckCciMeta);
        checkResult(sqlCheckCciMeta, checkCciResults);
        return checkCciResults;
    }

    /**
     * Check columnar index meta with index and table name
     *
     * @param indexName index name, with or without suffix
     * @param tableName table name
     */
    private List<CheckCciResult> checkWithIndexAndTableName(String indexName, String tableName) {
        final String sqlCheckCciMeta =
            MessageFormat.format("CHECK COLUMNAR INDEX {0} ON {1} META;", indexName, tableName);
        final List<CheckCciResult> checkCciResults = getCheckReport(sqlCheckCciMeta);
        checkResult(sqlCheckCciMeta, checkCciResults);
        return checkCciResults;
    }

    /**
     * Check columnar index meta with index name only
     *
     * @param indexName index name, with or without suffix
     */
    private List<CheckCciResult> checkWithIndexNameOnly(String indexName) {
        final String sqlCheckCciMeta = MessageFormat.format("CHECK COLUMNAR INDEX {0} META;", indexName);
        final List<CheckCciResult> checkCciResults = getCheckReport(sqlCheckCciMeta);
        checkResult(sqlCheckCciMeta, checkCciResults);
        return checkCciResults;
    }

    private static void checkResult(String sqlCheckCciMeta, List<CheckCciResult> checkCciResults) {
        Assert.assertFalse(String.format("No result for %s", sqlCheckCciMeta), checkCciResults.isEmpty());
        for (Ord<CheckCciResult> o : Ord.zip(checkCciResults)) {
            final CheckCciResult checkResult = o.e;
            final Triple<String, String, String> schemaTableIndexName = checkResult.decodeIndexName();
            Assert.assertFalse(
                String.format("Empty CCI column found in result row[%s]: %s", o.i, checkResult),
                Objects.isNull(schemaTableIndexName.getRight()));
        }
    }

    private String createTable(String tableName, String indexName) {
        dropTableIfExists(tddlConnection, tableName);
        final String sqlCreateTable1 = MessageFormat.format(CREATE_TABLE_TEMPLATE, tableName, indexName);
        createCciSuccess(sqlCreateTable1);
        return getFirstColumnarIndexNameWithSuffix(tddlConnection, tableName);
    }

    @Data
    private static class CheckCciResult {
        final String cciName;
        final ReportErrorType errorType;
        final String status;
        final String primaryKey;
        final String details;

        public static CheckCciResult create(ResultSet rs) throws SQLException {
            return new CheckCciResult(
                rs.getString(1),
                ReportErrorType.of(rs.getString(2)),
                rs.getString(3),
                rs.getString(4),
                rs.getString(5)
            );
        }

        /**
         * @return Triple<SchemaName, TableName, IndexName>
         */
        public Triple<String, String, String> decodeIndexName() {
            if (TStringUtil.isBlank(cciName)) {
                return ImmutableTriple.nullTriple();
            }
            final Pattern regex = Pattern.compile("`(.*?)` ON `(.*?)`\\.`(.*?)`");
            final Matcher matcher = regex.matcher(cciName);

            if (matcher.find()) {
                return ImmutableTriple.of(matcher.group(2), matcher.group(3), matcher.group(1));
            } else {
                return ImmutableTriple.nullTriple();
            }
        }
    }
}
