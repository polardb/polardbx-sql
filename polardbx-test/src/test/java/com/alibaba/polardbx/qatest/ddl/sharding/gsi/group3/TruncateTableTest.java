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
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.sql.SQLException;

/**
 * @author chenmo.cm
 */
public class TruncateTableTest extends DDLBaseNewDBTestCase {

    private final String gsiPrimaryTableName = "truncate_gsi_test";
    private final String gsiIndexTableName = "g_i_truncate_test";
    private final String gsiTruncateHint = "/*+TDDL:cmd_extra(TRUNCATE_TABLE_WITH_GSI=true)*/";
    private final String gsiDisableStorageCheckHint = "/*+TDDL:cmd_extra(STORAGE_CHECK_ON_GSI=false)*/";

    private boolean supportXA = false;

    @Before
    public void before() throws SQLException {
        supportXA = JdbcUtil.supportXA(tddlConnection);
    }

    /**
     * @since 5.1.22
     */
    @Test
    @Ignore("GSI does not allow use tbpartition only.")
    public void testTruncateShardTbTableWithGsi() {
        String tableName = gsiPrimaryTableName + "_1";
        String indexTableName = gsiIndexTableName + "_1";
        dropTableIfExists(tableName);
        dropTableIfExists(indexTableName);

        String sql = "create table " + tableName + " (id int primary key, name varchar(20), global index "
            + indexTableName
            + " (name) tbpartition by hash(name) tbpartitions 2) tbpartition by hash(id) tbpartitions 2";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        if (supportXA) {
            sql = "insert into " + tableName + " (id, name) values (1, \"tom\"), (2, \"simi\") ";
            JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        } else {
            sql = gsiDisableStorageCheckHint + "insert into " + tableName
                + " (id, name) values (1, \"tom\"), (2, \"simi\") ";
            JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        }

        Assert.assertEquals(2, getDataNumFromTable(tddlConnection, tableName));
        Assert.assertEquals(2, getDataNumFromTable(tddlConnection, indexTableName));

        sql = gsiTruncateHint + "truncate table " + tableName;
//        Assert.assertEquals(2, getExplainNum(sql));
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        Assert.assertEquals(0, getDataNumFromTable(tddlConnection, tableName));
        Assert.assertEquals(0, getDataNumFromTable(tddlConnection, indexTableName));

        dropTableIfExists(tableName);
        dropTableIfExists(indexTableName);
    }

    /**
     * @since 5.1.22
     */
    @Test
    public void testTruncateShardDbTableWithGsi() {
        String tableName = gsiPrimaryTableName + "_2";
        String indexTableName = gsiIndexTableName + "_2";
        dropTableIfExists(tableName);
        dropTableIfExists(indexTableName);

        String sql = "create table " + tableName + " (id int primary key, name varchar(20), global index "
            + indexTableName + " (name) dbpartition by hash(name)) dbpartition by hash(id)";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = "insert into " + tableName + " (id, name) values (1, \"tom\"), (2, \"simi\") ";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        Assert.assertEquals(2, getDataNumFromTable(tddlConnection, tableName));

        sql = gsiTruncateHint + "truncate table " + tableName;
//        Assert.assertEquals(4, getExplainNum(sql));
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        Assert.assertEquals(0, getDataNumFromTable(tddlConnection, tableName));

        dropTableIfExists(tableName);
        dropTableIfExists(indexTableName);
    }

    /**
     * @since 5.1.22
     */
    @Test
    public void testTruncateShardDbTbTableWithGsi() {
        String tableName = gsiPrimaryTableName + "_3";
        String indexTableName = gsiIndexTableName + "_3";
        dropTableIfExists(tableName);
        dropTableIfExists(indexTableName);

        String sql = "create table " + tableName + " (id int primary key, name varchar(20), global index "
            + indexTableName
            + " (name) dbpartition by hash(name) tbpartition by hash(name) tbpartitions 2) dbpartition by hash(id) tbpartition by hash(id) tbpartitions 2";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = "insert into " + tableName + " (id, name) values (1, \"tom\"), (2, \"simi\") ";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        Assert.assertEquals(2, getDataNumFromTable(tddlConnection, tableName));

        sql = gsiTruncateHint + "truncate table " + tableName;
//        Assert.assertEquals(8, getExplainNum(sql));
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        Assert.assertEquals(0, getDataNumFromTable(tddlConnection, tableName));

        dropTableIfExists(tableName);
        dropTableIfExists(indexTableName);
    }
}
