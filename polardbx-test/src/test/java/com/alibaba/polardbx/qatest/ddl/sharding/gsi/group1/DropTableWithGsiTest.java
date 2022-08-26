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

package com.alibaba.polardbx.qatest.ddl.sharding.gsi.group1;

import com.alibaba.polardbx.qatest.DDLBaseNewDBTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.google.common.collect.ImmutableList;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runners.Parameterized.Parameters;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;

/**
 * @author chenmo.cm
 */

public class DropTableWithGsiTest extends DDLBaseNewDBTestCase {

    private static final String HINT = "/*+TDDL:cmd_extra(STORAGE_CHECK_ON_GSI=false)*/ ";
    private static final String createTableTemplate = "CREATE TABLE `{0}`( "
        + "    `a` bigint(11) NOT NULL, `b` bigint(11), `c` varchar(20), PRIMARY KEY (`a`), "
        + "    GLOBAL UNIQUE INDEX `{1}`(c) dbpartition by hash(`c`) "
        + ") ENGINE = InnoDB CHARSET = utf8 dbpartition by hash(`b`) ";
    private final String createTablePrimary;
    private final String createTableIndex;
    private final String dropTablePrimary;
    private final String dropTableIndex;
    private final String createTable;

    public DropTableWithGsiTest(String createTablePrimary, String createTableIndex, String dropTablePrimary,
                                String dropTableIndex) {
        this.createTablePrimary = createTablePrimary;
        this.createTableIndex = createTableIndex;
        this.dropTablePrimary = dropTablePrimary;
        this.dropTableIndex = dropTableIndex;
        this.createTable = MessageFormat.format(createTableTemplate, this.createTablePrimary, this.createTableIndex);
    }

    /**
     * <pre>
     * 0. primary table name for create table
     * 1. index name for create table
     * 2. name for drop primary table
     * 3. name for drop index table
     * </pre>
     */
    @Parameters(name = "{index}:ct_p={0}, ct_i={1}, dt_p={2}, dt_i={3}")
    public static List<String[]> prepareDate() {
        final String tableName = "sct_gsi_p1";
        final String indexName = "sct_gsi_i1";

        final List<String[]> params = new ArrayList<>();
        params.add(new String[] {tableName, indexName, tableName, indexName});
        params.add(new String[] {tableName.toUpperCase(), indexName.toUpperCase(), tableName, indexName});
        params.add(new String[] {tableName, indexName, tableName.toUpperCase(), indexName.toUpperCase()});

        return params;
    }

    @Before
    public void before() {

//
        dropTableWithGsi(createTablePrimary, ImmutableList.of(createTableIndex));

        JdbcUtil.executeUpdateSuccess(tddlConnection, HINT + createTable);
    }

    @After
    public void after() {

        JdbcUtil.executeUpdateSuccess(tddlConnection, "DROP TABLE IF EXISTS " + createTablePrimary);
    }

    @Test
    public void testShowCreateTableWithGsi() {
        JdbcUtil.executeQuerySuccess(tddlConnection, "SHOW CREATE TABLE " + dropTablePrimary);
        JdbcUtil.executeQuerySuccess(tddlConnection, "SHOW CREATE TABLE " + dropTableIndex);

        JdbcUtil.executeUpdateFailed(tddlConnection,
            "DROP TABLE " + dropTableIndex,
            "Table '" + dropTableIndex + "' is global secondary index table, which is forbidden to be modified.");

        JdbcUtil.executeUpdateSuccess(tddlConnection, "DROP TABLE " + dropTablePrimary);

        try (final ResultSet resultSet = JdbcUtil.executeQuerySuccess(tddlConnection,
            "SHOW TABLES LIKE " + createTablePrimary)) {
            Assert.assertFalse(resultSet.next());
        } catch (SQLException e) {
            throw new RuntimeException("", e);
        }

        try (final ResultSet resultSet = JdbcUtil.executeQuerySuccess(tddlConnection,
            "SHOW TABLES LIKE " + createTableIndex)) {
            Assert.assertFalse(resultSet.next());
        } catch (SQLException e) {
            throw new RuntimeException("", e);
        }
    }
}
