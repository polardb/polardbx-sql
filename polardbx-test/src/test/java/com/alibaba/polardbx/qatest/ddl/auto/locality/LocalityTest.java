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

package com.alibaba.polardbx.qatest.ddl.auto.locality;

import com.alibaba.polardbx.qatest.util.JdbcUtil;
import net.jcip.annotations.NotThreadSafe;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.After;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

@NotThreadSafe
public class LocalityTest extends LocalityTestBase {

    private static final Log logger = LogFactory.getLog(LocalityTest.class);

    private final String databaseName = "`test_locality_db`";
    private final String tableName = "`test_table`";

    @After
    public void dropDb() {
        JdbcUtil.dropDatabase(tddlConnection, databaseName);
    }

//    @Test
//    public void testCreateDatabaseLocality() {
//        final String dn = chooseDatanode(true);
//        final String localitySql = " LOCALITY=\"dn=" + dn + "\"";
//        final String createDbSql = "create database " + databaseName + localitySql;
//        final String dropDatabaseSql = "drop database if exists " + databaseName;
//        final String showCreateDbSql = "show create database " + databaseName;
//
//        // cleanup environment
//        JdbcUtil.executeUpdate(tddlConnection, dropDatabaseSql);
//        int localityCount = getLocalityInfo().size();
//
//        {
//            // create database with locality
//            JdbcUtil.executeUpdateSuccess(tddlConnection, createDbSql);
//            String result = JdbcUtil.executeQueryAndGetStringResult(showCreateDbSql, tddlConnection, 2);
//
//            String tmpRs = result.replaceAll(" = ", "=");
//            Assert.assertTrue(tmpRs.contains(localitySql));
//
//            // drop database
//            JdbcUtil.executeUpdate(tddlConnection, dropDatabaseSql);
//            Assert.assertEquals(localityCount, getLocalityInfo().size());
//        }
//
//        {
//            // create database on multiple datanodes
//            List<String> dnList = getDatanodes();
//            String dnListStr = StringUtils.join(dnList, ",");
//            String multiDnLocality = " LOCALITY=\"dn=" + dnListStr + "\"";
//            String createSql = "CREATE DATABASE " + databaseName + multiDnLocality;
//
//            logger.info(createSql);
//            JdbcUtil.executeUpdateSuccess(tddlConnection, createSql);
//            String result = JdbcUtil.executeQueryAndGetStringResult(showCreateDbSql, tddlConnection, 2);
//            String tmpRs = result.replaceAll(" = ", "=");
//            Assert.assertTrue(tmpRs.contains(multiDnLocality));
//
//            TreeSet<String> dnListActual = new TreeSet<>();
//            TreeSet<String> dnListExpected = new TreeSet<>();
//            dnListActual.addAll(dnList);
//            dnListExpected.addAll(getDnListOfDb(databaseName, false));
//            Assert.assertEquals(dnListActual, dnListExpected);
//
//            // drop database
//            JdbcUtil.executeUpdate(tddlConnection, dropDatabaseSql);
//            Assert.assertEquals(localityCount, getLocalityInfo().size());
//        }
//    }

    @Test
    public void testSingleTableLocality() {
        final String dn = chooseDatanode();
        final String localitySql = " LOCALITY='dn=" + dn + "'";
        final String createTableSql = "create table " + tableName + " (id int)" + localitySql;

        // run
        JdbcUtil.executeUpdateSuccess(tddlConnection, "create database if not exists " + databaseName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, "use " + databaseName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, "drop table if exists " + tableName);

        // before
        List<LocalityBean> originLocalityInfo = getLocalityInfo();

        JdbcUtil.executeUpdateSuccess(tddlConnection, createTableSql);

        // check show create table
        String showCreate = showCreateTable(tableName);
        Assert.assertTrue(showCreate.contains(localitySql));

        // check information_schema.locality_info

        // check show topology
        List<String> actualDn = getDnListOfTable(databaseName, tableName);
        Assert.assertEquals(Arrays.asList(dn), actualDn);

        // drop and check again
        JdbcUtil.executeUpdateSuccess(tddlConnection, "drop table " + tableName);
        Assert.assertEquals(originLocalityInfo.size(), getLocalityInfo().size());
    }

    @Test
    public void testCreateTableLikeWithLocality() {
        String dbName = "test_like_database";

        dropDatabase(dbName);
        createDatabase(dbName);
        useDatabase(dbName);

        String tableGroup = "single_tg2";
        String locality = String.format(" LOCALITY='dn=%s'", chooseDatanode());

        String createTableGroup = String.format("create tablegroup if not exists %s ", tableGroup);
        String createTable =
            String.format("create table if not exists %s (id int not null primary key) %s single tablegroup=%s",
                tableName, locality, tableGroup);
        String createTableLike = String.format("create table %s like %s", "test_table_like", tableName);

        execute(createTableGroup);
        execute(createTable);
        execute(createTableLike);

        useDatabase("polardbx");
        dropDatabase(dbName);
    }

    @Test
    public void testCreateTableLikeCrossSchema() {
        String dbName = "test_like_database";
        String dbName2 = "test_like_database_2";

        dropDatabase(dbName);
        dropDatabase(dbName2);
        createDatabase(dbName);
        createDatabase(dbName2);

        useDatabase(dbName2);
        String createTable = String.format("create table if not exists %s (id int not null primary key)", tableName);
        execute(createTable);

        useDatabase(dbName);
        String createTableLike = String.format("create table %s like %s.%s", "test_table_like", dbName2, tableName);
        execute(createTableLike);

        useDatabase("polardbx");
        dropDatabase(dbName2);
        dropDatabase(dbName);
    }

    private void dropDatabase(String dbName) {
        execute(String.format("drop database if exists %s", dbName));
    }

    private void createDatabase(String dbName) {
        execute(String.format("create database if not exists %s mode='auto'", dbName));
    }

    private void useDatabase(String dbName) {
        execute(String.format("use %s", dbName));
    }

    private void execute(String sql) {
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
    }

    private String showCreateTable(String tableName) {
        final String sql = "show create table " + tableName;
        return JdbcUtil.executeQueryAndGetStringResult(sql, tddlConnection, 2);
    }
}
