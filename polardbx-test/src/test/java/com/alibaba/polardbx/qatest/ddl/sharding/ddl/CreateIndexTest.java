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

package com.alibaba.polardbx.qatest.ddl.sharding.ddl;

import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.qatest.AsyncDDLBaseNewDBTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.apache.commons.lang.StringUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runners.Parameterized;

import java.sql.Connection;
import java.util.Arrays;
import java.util.List;

import static com.alibaba.polardbx.qatest.validator.DataValidator.assertShardDbIndexExist;
import static com.alibaba.polardbx.qatest.validator.DataValidator.assertShardDbIndexNotExist;
import static com.alibaba.polardbx.qatest.validator.DataValidator.isIndexExist;
import static com.alibaba.polardbx.qatest.validator.DataValidator.isIndexExistByInfoSchema;


public class CreateIndexTest extends AsyncDDLBaseNewDBTestCase {

    private List<Connection> phyConnectionList;

    public CreateIndexTest(boolean schema) {
        this.crossSchema = schema;
    }

    @Parameterized.Parameters(name = "{index}:crossSchema={0}")
    public static List<Object[]> initParameters() {
        return Arrays
            .asList(new Object[][] {{false}, {true}});
    }

    @Before
    public void beforeCreateIndex() {
        this.phyConnectionList =
            StringUtils.isBlank(tddlDatabase2) ? getMySQLPhysicalConnectionList(tddlDatabase1) :
                getMySQLPhysicalConnectionList(tddlDatabase2);
    }

    /**
     * @since 5.1.20
     */
    @Test
    public void testCreateIndexNoPartition() {
        String tableName = schemaPrefix + "indexTest" + "_1";
        String indexName = "index" + "_1";
        dropTableIfExists(tableName);

        String sql = "create table " + tableName + " (id int, name varchar(200))";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = "create unique index " + indexName + " on " + tableName + " (id)";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        Assert.assertTrue(isIndexExist(tableName, indexName, tddlConnection));

        clearIndex(tableName, indexName);
        Assert.assertFalse(isIndexExist(tableName, indexName, tddlConnection));

        dropTableIfExists(tableName);
    }

    /**
     * @since 5.1.20
     */
    @Test
    public void testCreateIndexSinglePartition() {
        String tableName = schemaPrefix + "indexTest" + "_2";
        String indexName = "index" + "_2";
        dropTableIfExists(tableName);

        String sql = "create table " + tableName + " (id int, name varchar(200))";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = "create unique index " + indexName + " USING hash on " + tableName + " (id)";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        Assert.assertTrue(isIndexExist(tableName, indexName, tddlConnection));

        clearIndex(tableName, indexName);
        Assert.assertFalse(isIndexExist(tableName, indexName, tddlConnection));

        dropTableIfExists(tableName);
    }

    /**
     * @since 5.1.20
     */
    @Test
    public void testCreateIndexMultiDBpartition() {
        String tableName = schemaPrefix + "indexTest" + "_3";
        String indexName = "index" + "_3";
        dropTableIfExists(tableName);

        String sql = "create table " + tableName
            + " (id int, name varchar(200)) dbpartition by hash(id) dbpartitions 2";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = "create unique index " + indexName + " USING hash on " + tableName
            + " (id) KEY_BLOCK_SIZE=10 comment '分表index'";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        Assert.assertTrue(isIndexExist(tableName, indexName, tddlConnection));

        clearIndex(tableName, indexName);
        Assert.assertFalse(isIndexExist(tableName, indexName, tddlConnection));

        dropTableIfExists(tableName);
    }

    /**
     * @since 5.1.20
     */
    @Test
    public void testCreateIndexMultiTBpartition() {
        String tableName = schemaPrefix + "indexTest" + "_4";
        String indexName = "index" + "_4";
        dropTableIfExists(tableName);

        String sql = "create table "
            + tableName
            + " (id int, name varchar(200)) dbpartition by hash(id) dbpartitions 2 tbPartition by hash(name) tbpartitions 2";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = "create unique index " + indexName + " USING btree on " + tableName
            + " (id) KEY_BLOCK_SIZE=10 comment '分表index'";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        Assert.assertTrue(isIndexExist(tableName, indexName, tddlConnection));

        clearIndex(tableName, indexName);
        Assert.assertFalse(isIndexExist(tableName, indexName, tddlConnection));

        dropTableIfExists(tableName);
    }

    /**
     * @since 5.1.20
     */
    @Test
    public void testCreateIndexException() {
        String simpleTableName = "indexTest_5_2017";
        String tableName = schemaPrefix + simpleTableName;
        String indexName = "index_5_2017";
        dropTableIfExists(tableName);

        String sql = "create table "
            + tableName
            + " (id int, name varchar(200)) dbpartition by hash(id) dbpartitions 2 tbPartition by hash(name) tbpartitions 2";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        // 在某个分库执行建索引语句
        String physicalTableName = getPhysicalTableName(tddlDatabase2, simpleTableName, 0, 1);
        sql = "create unique index " + indexName + " USING btree on " + physicalTableName
            + " (id) KEY_BLOCK_SIZE=10 comment '分表index'";
        JdbcUtil.executeUpdateSuccess(phyConnectionList.get(0), sql);

        Assert.assertFalse(isIndexExist(tableName, indexName, tddlConnection));

        // 可以成功drop index
        sql = "drop index " + indexName + " on " + tableName;
        JdbcUtil.executeUpdateFailed(tddlConnection, sql, "");
        Assert.assertFalse(isIndexExist(tableName, indexName, tddlConnection));

        sql = "create unique index " + indexName + " USING btree on " + tableName
            + " (id) KEY_BLOCK_SIZE=10 comment '分表index'";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        Assert.assertTrue(isIndexExist(tableName, indexName, tddlConnection));

        clearIndex(tableName, indexName);
        Assert.assertFalse(isIndexExist(tableName, indexName, tddlConnection));

        dropTableIfExists(tableName);
    }

    /**
     * @since 5.1.22
     */
    @Test
    public void createIndexOnBroadcastTable() {
        String simpleTableName = "indexTest" + "_6";
        String tableName = schemaPrefix + simpleTableName;
        String indexName = "index" + "_6";
        dropTableIfExists(tableName);

        String sql = "create table " + tableName + " (id int, name varchar(200)) broadcast";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = "create unique index " + indexName + " USING btree on " + tableName
            + " (id) KEY_BLOCK_SIZE=10 comment '分表index'";
//        assertThat(getExplainNum(sql)).isEqualTo(getNodeNum(tddlConnection));
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        String phyTableName = getTbNamePattern(tableName);

        Assert.assertTrue(isIndexExist(tableName, indexName, tddlConnection));
        assertShardDbIndexExist(phyConnectionList, phyTableName, indexName);

        clearIndex(tableName, indexName);
        Assert.assertFalse(isIndexExist(tableName, indexName, tddlConnection));
        assertShardDbIndexNotExist(phyConnectionList, phyTableName, indexName);

        dropTableIfExists(tableName);
    }

    @Test
    public void createIndexAutoForShardingKey() {
        String simpleTableName = "indexTest" + "_7";
        String tableName = schemaPrefix + simpleTableName;
        String indexName = "auto_shard_key_zrrdah";

        dropTableIfExists(tableName);

        // Sharding key is in a compound key.
        String sql =
            "create table " + tableName + " (id int, zrrdah int, primary key(id, zrrdah)) dbpartition by hash(zrrdah)";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        Assert.assertTrue(isIndexExist(tableName, indexName, tddlConnection));

        dropTableIfExists(tableName);

        // Sharding key is not in any key.
        sql = "create table " + tableName + " (id int, zrrdah int, primary key(id)) dbpartition by hash(zrrdah)";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        Assert.assertTrue(isIndexExist(tableName, indexName, tddlConnection));

        dropTableIfExists(tableName);

        // Sharding key is the only primary key which is separate definition.
        sql = "create table " + tableName + " (id int, zrrdah int, primary key(id)) dbpartition by hash(id)";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        Assert.assertFalse(isIndexExist(tableName, indexName, tddlConnection));

        indexName = "auto_shard_key_id";
        Assert.assertFalse(isIndexExist(tableName, indexName, tddlConnection));

        dropTableIfExists(tableName);

        // Sharding key is the only primary key which is a constraint.
        sql = "create table " + tableName + " (id int primary key, zrrdah int) dbpartition by hash(id)";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        Assert.assertFalse(isIndexExist(tableName, indexName, tddlConnection));

        dropTableIfExists(tableName);
    }

    @Test
    public void testCreateDropIndexWithMeta() {
        String schemaName = tddlDatabase2;
        if (TStringUtil.isEmpty(schemaName)) {
            schemaName = tddlDatabase1;
        }
        String simpleTableName = "indexTest" + "_meta";
        String tableName = schemaPrefix + simpleTableName;
        String indexName1 = "index_meta_1";
        String indexName2 = "index_meta_2";

        dropTableIfExists(tableName);

        String sql = "create table " + tableName + " (c1 int, c2 int, c3 int) dbpartition by hash(c1)";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = "create index " + indexName1 + " on " + tableName + "(c2)";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = "alter table " + tableName + " add index " + indexName2 + "(c3)";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        Assert.assertTrue(isIndexExist(tableName, indexName1, tddlConnection));
        Assert.assertTrue(isIndexExist(tableName, indexName2, tddlConnection));

        Assert.assertTrue(
            isIndexExistByInfoSchema(schemaName, simpleTableName, indexName1, tddlConnection));
        Assert.assertTrue(
            isIndexExistByInfoSchema(schemaName, simpleTableName, indexName2, tddlConnection));

        clearIndex(tableName, indexName1);
        Assert.assertFalse(isIndexExist(tableName, indexName1, tddlConnection));
        Assert.assertFalse(
            isIndexExistByInfoSchema(schemaName, simpleTableName, indexName1, tddlConnection));

        clearIndexByAlterTable(tableName, indexName2);
        Assert.assertFalse(isIndexExist(tableName, indexName2, tddlConnection));
        Assert.assertFalse(
            isIndexExistByInfoSchema(schemaName, simpleTableName, indexName2, tddlConnection));

        dropTableIfExists(tableName);
    }

    @Test
    public void createTableWithIndexOnNonexistentColumn() {
        String simpleTableName = "indexTest" + "_nonexistent_column";
        String tableName = schemaPrefix + simpleTableName;
        dropTableIfExists(tableName);

        String sql = "create table " + tableName + " (id int not null, unique key (nonexistent_column))";
        JdbcUtil.executeUpdateFailed(tddlConnection, sql, "Key column 'nonexistent_column' doesn't exist");

        dropTableIfExists(tableName);
    }

}
