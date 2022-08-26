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
import java.util.List;
import java.util.TreeSet;

@NotThreadSafe
public class LocalityTest extends LocalityTestBase {

    private static final Log logger = LogFactory.getLog(LocalityTest.class);

    private final String databaseName = "`test_locality_db`";
    private final String tableName = "`test_table`";

    @After
    public void dropDb() {
        JdbcUtil.dropDatabase(tddlConnection, databaseName);
    }

    @Test
    public void testCreateDatabaseLocality() {
        final String dn = chooseDatanode();
        final String localitySql = " LOCALITY=\"dn=" + dn + "\"";
        final String createDbSql = "create database " + databaseName + localitySql;
        final String dropDatabaseSql = "drop database if exists " + databaseName;
        final String showCreateDbSql = "show create database " + databaseName;

        // cleanup environment
        JdbcUtil.executeUpdate(tddlConnection, dropDatabaseSql);
        int localityCount = getLocalityInfo().size();

        {
            // create database with locality
            JdbcUtil.executeUpdateSuccess(tddlConnection, createDbSql);
            String result = JdbcUtil.executeQueryAndGetStringResult(showCreateDbSql, tddlConnection, 2);

            String tmpRs = result.replaceAll(" = ", "=");
            Assert.assertTrue(tmpRs.contains(localitySql));

            List<LocalityBean> localityInfoList = getLocalityInfo();
            Assert.assertEquals(localityCount + 1, localityInfoList.size());
            LocalityBean dbLocality = localityInfoList.get(0);
            Assert.assertEquals("database", dbLocality.objectType);
            Assert.assertEquals(databaseName.replaceAll("`", ""), dbLocality.objectName);
            Assert.assertEquals("dn=" + dn, dbLocality.locality);
            List<String> dnList = getDnListOfDb(databaseName, false);
            Assert.assertEquals(Arrays.asList(dn), dnList);

            // drop database
            JdbcUtil.executeUpdate(tddlConnection, dropDatabaseSql);
            Assert.assertEquals(localityCount, getLocalityInfo().size());
        }

        {
            // create database on multiple datanodes
            List<String> dnList = getDatanodes();
            String dnListStr = StringUtils.join(dnList, ",");
            String multiDnLocality = " LOCALITY=\"dn=" + dnListStr + "\"";
            String createSql = "CREATE DATABASE " + databaseName + multiDnLocality;

            logger.info(createSql);
            JdbcUtil.executeUpdateSuccess(tddlConnection, createSql);
            String result = JdbcUtil.executeQueryAndGetStringResult(showCreateDbSql, tddlConnection, 2);
            String tmpRs = result.replaceAll(" = ", "=");
            Assert.assertTrue(tmpRs.contains(multiDnLocality));
            List<LocalityBean> localityInfoList = getLocalityInfo();
            Assert.assertEquals(localityCount + 1, localityInfoList.size());
            LocalityBean dbLocality = localityInfoList.get(0);
            Assert.assertEquals("database", dbLocality.objectType);
            Assert.assertEquals(databaseName.replaceAll("`", ""), dbLocality.objectName);
            Assert.assertEquals("dn=" + dnListStr, dbLocality.locality);

            TreeSet<String> dnListActual = new TreeSet<>();
            TreeSet<String> dnListExpected = new TreeSet<>();
            dnListActual.addAll(dnList);
            dnListExpected.addAll(getDnListOfDb(databaseName, false));
            Assert.assertEquals(dnListActual, dnListExpected);

            // drop database
            JdbcUtil.executeUpdate(tddlConnection, dropDatabaseSql);
            Assert.assertEquals(localityCount, getLocalityInfo().size());
        }
    }

    @Test
    @Ignore("fix by ???")
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
        List<LocalityBean> localityInfoList = getLocalityInfo();
        Assert.assertEquals(originLocalityInfo.size() + 1, localityInfoList.size());
        LocalityBean
            tableLocality = localityInfoList.get(localityInfoList.size() - 1);
        Assert.assertEquals("table", tableLocality.objectType);
        Assert.assertEquals(tableName.replaceAll("`", ""), tableLocality.objectName);
        Assert.assertEquals("dn=" + dn, tableLocality.locality);

        // check show topology
        List<String> actualDn = getDnListOfTable(databaseName, tableName);
        Assert.assertEquals(Arrays.asList(dn), actualDn);

        // drop and check again
        JdbcUtil.executeUpdateSuccess(tddlConnection, "drop table " + tableName);
        Assert.assertEquals(originLocalityInfo.size(), getLocalityInfo().size());
    }

    private String showCreateTable(String tableName) {
        final String sql = "show create table " + tableName;
        return JdbcUtil.executeQueryAndGetStringResult(sql, tddlConnection, 2);
    }
}
