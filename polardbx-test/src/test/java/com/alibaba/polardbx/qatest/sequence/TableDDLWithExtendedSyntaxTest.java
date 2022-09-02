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

package com.alibaba.polardbx.qatest.sequence;

import com.alibaba.polardbx.qatest.BaseSequenceTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.alibaba.polardbx.qatest.util.PropertiesUtil;
import org.apache.commons.lang.StringUtils;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.List;

/**
 * Created by chensr on 2018-05-10 Test 'ALTER TABLE' with extended sequence
 * syntax
 */

public class TableDDLWithExtendedSyntaxTest extends BaseSequenceTestCase {

    public String tableName;
    public String sqlCreateTable;
    public String sqlAlterTable;
    public String SQL_CREATE_TABLE_TEMPLATE = "CREATE TABLE IF NOT EXISTS %s"
        + "(col1 INT NOT NULL, col2 INT NOT NULL AUTO_INCREMENT %s PRIMARY KEY, "
        + "col3 INT) DBPARTITION BY HASH(col1)";
    public String SQL_ALTER_TABLE_TEMPLATE = "ALTER TABLE %s MODIFY COLUMN col2 BIGINT NOT NULL AUTO_INCREMENT FIRST";

    public TableDDLWithExtendedSyntaxTest(String schema) {
        this.schema = schema;
        this.schemaPrefix = StringUtils.isBlank(schema) ? "" : schema + ".";
        this.tableName = schemaPrefix + randomTableName("testTableDDLWithExtSeqSyntax", 4);
        this.sqlCreateTable = String.format(SQL_CREATE_TABLE_TEMPLATE, tableName, "%s");
        this.sqlAlterTable = String.format(SQL_ALTER_TABLE_TEMPLATE, tableName, "%s");
    }

    @Parameterized.Parameters(name = "{index}:schema={0}")
    public static List<Object[]> initParameters() {
        return Arrays.asList(new Object[][] {{""}, {PropertiesUtil.polardbXShardingDBName2()}});
    }

    @Test
    public void testCreateTableWithGroupSeq1() {
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(sqlCreateTable, ""));
        dropTableIfExists(tableName);
    }

    @Test
    public void testCreateTableWithGroupSeq2() {
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(sqlCreateTable, "BY GROUP"));
        dropTableIfExists(tableName);
    }

    @Test
    public void testCreateTableWithCustomUnitGroupSeq1() {
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(sqlCreateTable, "UNIT COUNT 3 INDEX 1"));
        dropTableIfExists(tableName);
    }

    @Test
    public void testCreateTableWithCustomUnitGroupSeq2() {
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(sqlCreateTable, " BY GROUP UNIT COUNT 3 INDEX 2"));
        dropTableIfExists(tableName);
    }

    @Test
    public void testCreateTableWithSimpleSeq() {
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(sqlCreateTable, "BY SIMPLE"));
        dropTableIfExists(tableName);
    }

    @Ignore("Ignore this case for now because a known bug is still not fixed after sequence porting")
    @Test
    public void testAlterTableWithUnsupportedSyntax() {
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(sqlCreateTable, ""));
        JdbcUtil.executeUpdateSuccess(tddlConnection, sqlAlterTable);
        dropTableIfExists(tableName);
    }

}
