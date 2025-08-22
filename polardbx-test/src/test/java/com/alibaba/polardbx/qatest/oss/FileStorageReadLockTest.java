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

package com.alibaba.polardbx.qatest.oss;

import com.alibaba.polardbx.common.Engine;
import com.alibaba.polardbx.qatest.BaseTestCase;
import com.alibaba.polardbx.qatest.oss.utils.FileStorageTestUtil;
import com.alibaba.polardbx.qatest.util.ConnectionManager;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.alibaba.polardbx.qatest.util.PropertiesUtil;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Map;

/**
 * test the read lock of database of expire TTL
 */
public class FileStorageReadLockTest extends BaseTestCase {

    static String baseDB = "fileStorageReadLockDataBase";
    static String db1 = baseDB + "1";
    static String db2 = baseDB + "2";
    static String db3 = baseDB + "3";

    private static Engine engine = PropertiesUtil.engine();

    @BeforeClass
    public static void initTestDatabase() {
        try (Connection tmpConnection = ConnectionManager.getInstance().getDruidPolardbxConnection()) {
            JdbcUtil.executeUpdate(tmpConnection, String.format("drop database if exists %s ", db1));
            JdbcUtil.executeUpdate(tmpConnection, String.format("create database %s mode = 'auto'", db1));

            JdbcUtil.executeUpdate(tmpConnection, String.format("drop database if exists %s ", db2));
            JdbcUtil.executeUpdate(tmpConnection, String.format("create database %s mode = 'auto'", db2));

            JdbcUtil.executeUpdate(tmpConnection, String.format("drop database if exists %s ", db3));
            JdbcUtil.executeUpdate(tmpConnection, String.format("create database %s mode = 'auto'", db3));
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
    }

    @AfterClass
    public static void dropDatabase() {
        try (Connection tmpConnection = ConnectionManager.getInstance().getDruidPolardbxConnection()) {
            JdbcUtil.executeUpdate(tmpConnection,
                String.format("/*+TDDL:cmd_extra(ALLOW_DROP_DATABASE_FORCE=true)*/drop database if exists %s ", db1));
            JdbcUtil.executeUpdate(tmpConnection,
                String.format("/*+TDDL:cmd_extra(ALLOW_DROP_DATABASE_FORCE=ture)*/drop database if exists %s ", db2));
            JdbcUtil.executeUpdate(tmpConnection,
                String.format("/*+TDDL:cmd_extra(ALLOW_DROP_DATABASE_FORCE=ture)*/drop database if exists %s ", db3));
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
    }

    @Test
    public void testLock() {
        String x1 = "x1";
        String x2 = "x2";
        String y1 = "y1";
        String y2 = "y2";
        String z1 = "z1";

        /* database and table info
        db1     db2     db3
        --------------------
        x1      y1      oss_x2
        x2      y2      oss_y2
        oss_y1  oss_x1  z1
                        oss_z1
         */

        List<String> tbs = Lists.newArrayList(x1, x2, y1, y2, z1);
        // map innodb table to oss table
        Map<String, String> tbToOssTb = Maps.newTreeMap(String::compareToIgnoreCase);
        // map table to its schema
        Map<String, String> tbToDb = Maps.newTreeMap(String::compareToIgnoreCase);
        Map<String, Connection> dbToConn = Maps.newTreeMap(String::compareToIgnoreCase);
        Map<String, Long> tbToJobId = Maps.newTreeMap(String::compareToIgnoreCase);
        for (String tb : tbs) {
            tbToOssTb.put(tb, "oss_" + tb);
        }
        tbToDb.put(x1, db1);
        tbToDb.put(tbToOssTb.get(x1), db2);
        tbToDb.put(x2, db1);
        tbToDb.put(tbToOssTb.get(x2), db3);
        tbToDb.put(y1, db2);
        tbToDb.put(tbToOssTb.get(y1), db1);
        tbToDb.put(y2, db2);
        tbToDb.put(tbToOssTb.get(y2), db3);
        tbToDb.put(z1, db3);
        tbToDb.put(tbToOssTb.get(z1), db3);

        try (Connection conn1 = getPolardbxConnection(db1);
            Connection conn2 = getPolardbxConnection(db2);
            Connection conn3 = getPolardbxConnection(db3)) {
            dbToConn.put(db1, conn1);
            dbToConn.put(db2, conn2);
            dbToConn.put(db3, conn3);
            // build innodb table
            for (String tb : tbs) {
                FileStorageTestUtil.prepareInnoTable(dbToConn.get(tbToDb.get(tb)), tb, 100, false);
            }
            // bind oss table
            for (String tb : tbs) {
                String oss = tbToOssTb.get(tb);
                FileStorageTestUtil.prepareTTLTable(dbToConn.get(tbToDb.get(oss)), tbToDb.get(oss), oss, tbToDb.get(tb),
                    tb, engine);
            }
            // expire ttl tables
            String ttlSql = "/*+TDDL:cmd_extra(OSS_ORC_INDEX_STRIDE=5,OSS_MAX_ROWS_PER_FILE=4,"
                + "ENABLE_EXPIRE_FILE_STORAGE_PAUSE=true, ENABLE_EXPIRE_FILE_STORAGE_TEST_PAUSE=true)*/ "
                + "alter table %s expire local partition";
            for (String tb : tbs) {
                JdbcUtil.executeUpdateFailed(dbToConn.get(tbToDb.get(tb)), String.format(ttlSql, tb),
                    "The DDL job has been cancelled or interrupted");
            }
            // find jobId of each ddl
            for (String tb : tbs) {
                tbToJobId.put(tb,
                    FileStorageTestUtil.fetchJobId(dbToConn.get(tbToDb.get(tb)), tbToDb.get(tb), tb));
            }
            // drop should fail
            String dropDB = "drop database if exists %s";
            for (String db : dbToConn.keySet()) {
                JdbcUtil.executeUpdateFailed(dbToConn.get(db), String.format(dropDB, db), "using 'show ddl");
            }
            // cancel ddl of x2, db3 can't be dropped
            JdbcUtil.executeUpdate(dbToConn.get(tbToDb.get(x2)), String.format("cancel ddl %d", tbToJobId.get(x2)));
            JdbcUtil.executeUpdateFailed(dbToConn.get(tbToDb.get(tbToOssTb.get(x2))),
                String.format(dropDB, tbToDb.get(tbToOssTb.get(x2))), "using 'show ddl");

            // cancel ddl of y2, db3 can be dropped
            JdbcUtil.executeUpdate(dbToConn.get(tbToDb.get(y2)), String.format("cancel ddl %d", tbToJobId.get(y2)));
            JdbcUtil.executeUpdate(dbToConn.get(db3), String.format(dropDB, db3));

            // cancel ddl of y1, db1 and db2  can be dropped
            JdbcUtil.executeUpdate(dbToConn.get(tbToDb.get(y1)), String.format("cancel ddl %d", tbToJobId.get(y1)));
            JdbcUtil.executeUpdate(dbToConn.get(db1), String.format(dropDB, db1));
            JdbcUtil.executeUpdate(dbToConn.get(db2), String.format(dropDB, db2));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
