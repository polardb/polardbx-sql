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

package com.alibaba.polardbx.qatest.ddl.sharding.gsi.group3;

import com.alibaba.polardbx.qatest.DDLBaseNewDBTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.util.Litmus;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;

/**
 * @author chenmo.cm
 */

public class ShowWithGsiCaseSensitiveTest extends DDLBaseNewDBTestCase {

    private static final String HINT = "/*+TDDL:cmd_extra(STORAGE_CHECK_ON_GSI=false)*/ ";
    private static final String createTableTemplate = "CREATE TABLE `{0}`( "
        + "    `a` bigint(11) NOT NULL, `b` bigint(11), `c` varchar(20), PRIMARY KEY (`a`), "
        + "    UNIQUE GLOBAL INDEX `{1}`(c) dbpartition by hash(`c`) "
        + ") ENGINE = InnoDB CHARSET = utf8 dbpartition by hash(`b`) ";
    private final String createTablePrimary;
    private final String createTableIndex;
    private final String showCreateTablePrimary;
    private final String showCreateTableIndex;
    private final String showIndex;
    private final String createTable;
    private final String createTableLower;

    public ShowWithGsiCaseSensitiveTest(String createTablePrimary, String createTableIndex,
                                        String showCreateTablePrimary, String showCreateTableIndex, String showIndex) {
        this.createTablePrimary = createTablePrimary;
        this.createTableIndex = createTableIndex;
        this.showCreateTablePrimary = showCreateTablePrimary;
        this.showCreateTableIndex = showCreateTableIndex;
        this.showIndex = showIndex;
        this.createTable = MessageFormat.format(createTableTemplate, this.createTablePrimary, this.createTableIndex);
        this.createTableLower =
            MessageFormat.format(createTableTemplate, this.createTablePrimary.toLowerCase(), this.createTableIndex);
    }

    /**
     * <pre>
     * 0. primary table name for create table
     * 1. index name for create table
     * 2. name for show create table
     * 3. name for show create table
     * 4. name for show index from
     * </pre>
     */
    @Parameters(name = "{index}:ct_p={0}, ct_i={1}, sct_p={2}, sct_i={3}, si={4}")
    public static List<String[]> prepareDate() {
        final String tableName = "sct_gsi_p1";
        final String indexName = "sct_gsi_i1";

        final List<String[]> params = new ArrayList<>();
        params.add(new String[] {tableName, indexName, tableName, indexName, tableName});
        params.add(new String[] {tableName.toUpperCase(), indexName.toUpperCase(), tableName, indexName, tableName});
        params.add(new String[] {
            tableName.toUpperCase(), indexName.toUpperCase(), tableName, indexName,
            tableName.toUpperCase()});
        params.add(new String[] {tableName, indexName, tableName.toUpperCase(), indexName.toUpperCase(), tableName});
        params.add(new String[] {
            tableName, indexName, tableName.toUpperCase(), indexName.toUpperCase(),
            tableName.toUpperCase()});

        return params;
    }

    @Before
    public void before() {

        //JdbcUtil.executeUpdateSuccess(tddlConnection, "REMOVE DDL ALL PENDING");
        dropTableWithGsi(createTablePrimary, ImmutableList.of(createTableIndex));

        JdbcUtil.executeUpdateSuccess(tddlConnection, HINT + createTable);
    }

    @After
    public void after() {

        JdbcUtil.executeUpdateSuccess(tddlConnection, "DROP TABLE IF EXISTS " + createTablePrimary);
    }

    @Test
    public void testShowCreateTableWithGsi() {
        JdbcUtil.executeQuerySuccess(tddlConnection, "SHOW CREATE TABLE " + showCreateTablePrimary);
        JdbcUtil.executeQuerySuccess(tddlConnection, "SHOW CREATE TABLE " + showCreateTableIndex);

        final TableChecker tableChecker = getTableChecker(tddlConnection, showCreateTablePrimary);

        tableChecker.identicalTableDefinitionTo(createTableLower, true, Litmus.THROW);

        final ShowIndexChecker showIndexChecker = getShowIndexGsiChecker(tddlConnection, showIndex);

        showIndexChecker.identicalToTableDefinition(createTableLower, true, Litmus.THROW);
    }
}
