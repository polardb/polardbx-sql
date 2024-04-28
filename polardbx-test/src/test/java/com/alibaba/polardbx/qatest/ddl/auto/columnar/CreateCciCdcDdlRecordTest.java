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

import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.qatest.DDLBaseNewDBTestCase;
import com.alibaba.polardbx.qatest.ddl.cdc.ImplicitTableGroupChecker;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.google.common.collect.ImmutableList;
import com.google.common.truth.Truth;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.SQLException;
import java.util.Formatter;
import java.util.List;
import java.util.Random;

public class CreateCciCdcDdlRecordTest extends DDLBaseNewDBTestCase {
    private static final String PRIMARY_TABLE_PREFIX = "create_cci_cdc_prim";
    private static final String INDEX_PREFIX = "create_cci_cdc_cci";
    private static final String CREATE_TABLE_TMPL = "CREATE TABLE `%s` (\n"
        + "\t`id` bigint(11) NOT NULL AUTO_INCREMENT,\n"
        + "\t`order_id` varchar(20) DEFAULT NULL,\n"
        + "\t`buyer_id` varchar(20) DEFAULT NULL,\n"
        + "\t`order_snapshot` longtext,\n"
        + "\tPRIMARY KEY (`id`)"
        + "%s\n"
        + ") ENGINE = InnoDB DEFAULT CHARSET = utf8 DEFAULT COLLATE = `utf8_general_ci`";

    private static final String CREATE_TABLE_WITH_CCI_ORIGIN_DDL_TMPL = "CREATE TABLE `%s` (\n"
        + "\t`id` bigint(11) NOT NULL AUTO_INCREMENT,\n"
        + "\t`order_id` varchar(20) DEFAULT NULL,\n"
        + "\t`buyer_id` varchar(20) DEFAULT NULL,\n"
        + "\t`order_snapshot` longtext,\n"
        + "\tPRIMARY KEY (`id`)"
        + "%s\n"
        + ") ENGINE = 'InnoDB' DEFAULT CHARSET = utf8 DEFAULT COLLATE = `utf8_general_ci`";
    private static final List<String> CREATE_CCI_TMPL_LIST = ImmutableList.of(
        "create clustered columnar index `%s` on `%s` (`order_id`)",
        "create clustered columnar index `%s` on `%s` (`order_id`) partition by hash (`id`)",
        "create clustered columnar index `%s` on `%s` (`order_id`) partition by hash (`id`) partitions 5"
    );
    private static final List<Pair<String, String>> ALTER_TABLE_ADD_CCI_TMPL_LIST = ImmutableList.of(
        Pair.of("alter table %s add clustered columnar index `%s` (`order_id`)",
            "create clustered columnar index `%s` on `%s` (`order_id`)"),
        Pair.of("alter table %s add clustered columnar index `%s` (`order_id`) partition by hash (`id`)",
            "create clustered columnar index `%s` on `%s` (`order_id`) partition by hash (`id`)"),
        Pair.of("alter table %s add clustered columnar index `%s` (`order_id`) partition by hash (`id`) partitions 5",
            "create clustered columnar index `%s` on `%s` (`order_id`) partition by hash (`id`) partitions 5")
    );
    private static final List<Pair<String, String>> CREATE_TABLE_WITH_CCI_TMPL_LIST = ImmutableList.of(
        Pair.of(",\n\tclustered columnar index `%s`(`order_id`)", ""),
        Pair.of(",\n\tclustered columnar index `%s`(`order_id`) partition by hash (`id`)", ""),
        Pair.of(",\n\tclustered columnar index `%s`(`order_id`) partition by hash (`id`) partitions 5", "")
    );
    protected ImplicitTableGroupChecker implicitTableGroupChecker = new ImplicitTableGroupChecker();
    private String primaryTableName;
    private String indexName;

    @Override
    public boolean usingNewPartDb() {
        return true;
    }

    @Before
    public void before() {
        initTableAndIndexName();
    }

    private void initTableAndIndexName() {
        final Random random = new Random();
        final Formatter formatter = new Formatter();
        final String suffix = "__" + formatter.format("%04x", random.nextInt(0x10000));
        this.primaryTableName = PRIMARY_TABLE_PREFIX + suffix;
        this.indexName = INDEX_PREFIX + suffix;
    }

    @After
    public void after() {
        dropTableIfExists(primaryTableName);
    }

    @Test
    public void test1CreateCci() throws SQLException {
        // Create Table
        dropTableIfExists(primaryTableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(CREATE_TABLE_TMPL, primaryTableName, ""));

        // Create CCI
        for (String createCciTmpl : CREATE_CCI_TMPL_LIST) {
            final String sqlCreateCci = String.format(createCciTmpl, indexName, primaryTableName);
            executeDdlAndCheckCdcRecord(
                SKIP_WAIT_CCI_CREATION_HINT + sqlCreateCci,
                sqlCreateCci,
                primaryTableName,
                cdcDdlRecord -> {
                    Truth.assertThat(cdcDdlRecord.ddlSql).ignoringCase().contains(sqlCreateCci);
                    Truth.assertThat(cdcDdlRecord.ddlSql).ignoringCase().doesNotContain("DIRECT_HASH");
                },
                ddlExtInfo -> {
                    Truth.assertThat(ddlExtInfo.getDdlId()).isGreaterThan(0);
                    Truth.assertThat(ddlExtInfo.getOriginalDdl()).ignoringCase()
                        .contains(buildExpectedOriginalDdlSql1(sqlCreateCci));
                    Truth.assertThat(ddlExtInfo.getOriginalDdl()).ignoringCase().doesNotContain("DIRECT_HASH");
                });

            // Cleanup
            JdbcUtil.executeUpdateSuccess(
                tddlConnection,
                String.format("drop index %s on %s",
                    indexName,
                    primaryTableName));
        }
    }

    @Test
    public void test2AlterTableAddCci() throws SQLException {
        // Create Table
        dropTableIfExists(primaryTableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(CREATE_TABLE_TMPL, primaryTableName, ""));

        // Create CCI
        for (Pair<String, String> p : ALTER_TABLE_ADD_CCI_TMPL_LIST) {
            final String createCciTmpl = p.getKey();
            final String expectedDdlSqlTmpl = p.getValue();
            final String sqlAlterTableAddCci = String.format(createCciTmpl, primaryTableName, indexName);
            final String sqlExpectedDdlSql = String.format(expectedDdlSqlTmpl, indexName, primaryTableName);
            executeDdlAndCheckCdcRecord(
                SKIP_WAIT_CCI_CREATION_HINT + sqlAlterTableAddCci,
                sqlExpectedDdlSql,
                primaryTableName,
                cdcDdlRecord -> {
                    Truth.assertThat(cdcDdlRecord.ddlSql).ignoringCase().contains(sqlExpectedDdlSql);
                    Truth.assertThat(cdcDdlRecord.ddlSql).ignoringCase().doesNotContain("DIRECT_HASH");
                },
                ddlExtInfo -> {
                    Truth.assertThat(ddlExtInfo.getDdlId()).isGreaterThan(0);
                    Truth.assertThat(ddlExtInfo.getOriginalDdl()).ignoringCase()
                        .contains(buildExpectedOriginalDdlSql1(sqlExpectedDdlSql));
                    Truth.assertThat(ddlExtInfo.getOriginalDdl()).ignoringCase().doesNotContain("DIRECT_HASH");
                });

            // Cleanup
            JdbcUtil.executeUpdateSuccess(
                tddlConnection,
                String.format("drop index %s on %s",
                    indexName,
                    primaryTableName));
        }
    }

    @Test
    public void test3CreateTableWithCci() throws SQLException {
        for (Pair<String, String> p : CREATE_TABLE_WITH_CCI_TMPL_LIST) {
            // Init
            initTableAndIndexName();
            dropTableIfExists(primaryTableName);

            // Create table with CCI
            final String indexDef = p.getKey();
            final String indexDefInDdlSql = p.getValue();
            final String sqlCreateTableWithCci =
                String.format(CREATE_TABLE_TMPL, primaryTableName, String.format(indexDef, indexName));
            final String sqlExpectedDdlSql = String.format(
                SKIP_WAIT_CCI_CREATION_HINT.trim() + "\n" + CREATE_TABLE_TMPL,
                primaryTableName,
                indexDefInDdlSql);

            executeDdlAndCheckCdcRecord(
                SKIP_WAIT_CCI_CREATION_HINT + sqlCreateTableWithCci,
                sqlExpectedDdlSql,
                primaryTableName,
                cdcDdlRecord -> {
                    Truth.assertThat(cdcDdlRecord.ddlSql).ignoringCase().contains(sqlExpectedDdlSql);
                    Truth.assertThat(cdcDdlRecord.ddlSql).ignoringCase().doesNotContain("DIRECT_HASH");
                },
                ddlExtInfo -> {
                    Truth.assertThat(ddlExtInfo.getDdlId()).isGreaterThan(0);
                    Truth
                        .assertThat(ddlExtInfo.getOriginalDdl())
                        .ignoringCase()
                        .contains(buildExpectedOriginalDdlSql2(indexDef));
                    Truth.assertThat(ddlExtInfo.getOriginalDdl()).ignoringCase().doesNotContain("DIRECT_HASH");
                });

            // Cleanup
            dropTableIfExists(primaryTableName);
        }
    }

    private String buildExpectedOriginalDdlSql1(String createCciSql) {
        return implicitTableGroupChecker.attachImplicitTg(getDdlSchema(), primaryTableName, createCciSql);
    }

    private String buildExpectedOriginalDdlSql2(String indexDef) {
        return implicitTableGroupChecker.attachImplicitTg(getDdlSchema(), primaryTableName,
            String.format(
                CREATE_TABLE_WITH_CCI_ORIGIN_DDL_TMPL,
                primaryTableName,
                String.format(indexDef, indexName)));
    }
}
