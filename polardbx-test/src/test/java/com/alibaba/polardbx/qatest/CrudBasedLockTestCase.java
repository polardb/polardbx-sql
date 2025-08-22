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

package com.alibaba.polardbx.qatest;

import com.alibaba.polardbx.qatest.data.ColumnDataGenerator;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.alibaba.polardbx.qatest.util.PropertiesUtil;
import org.junit.After;
import org.junit.Before;

import java.sql.Connection;
import java.sql.ResultSet;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicBoolean;

public class CrudBasedLockTestCase extends BaseTestCase {

    public final static Set<String> tableSets = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
    public final static String CREATE_TABLE_LIKE_SQL = "create table %s like %s";
    public final static String DROP_TABLE_SQL = "drop table if exists %s";

    protected static long RANDOM_ID = 1;
    protected Connection mysqlConnection;
    protected Connection tddlConnection;
    protected String baseOneTableName;
    protected String baseTwoTableName;
    protected String baseThreeTableName;
    protected String baseFourTableName;
    protected String baseFiveTableName;
    protected String polardbxOneDB;
    protected String mysqlOneDB;
    protected ColumnDataGenerator columnDataGenerator;
//    private final static ReentrantLock lock = new ReentrantLock();

    protected AtomicBoolean lockSuccessful = new AtomicBoolean(false);

    @Before
    public void beforeLockTableSet() {
        this.lock();
        this.polardbxOneDB = PropertiesUtil.polardbXDBName1(usingNewPartDb());
        this.mysqlOneDB = PropertiesUtil.mysqlDBName1();
        this.mysqlConnection = getMysqlConnection();
        this.tddlConnection = getPolardbxConnection();
        this.columnDataGenerator = new ColumnDataGenerator();
    }

    private void lock() {
        try {
            while (true) {
                if (addTables()) {
                    lockSuccessful.set(true);
                    return;
                } else {
                    Thread.sleep(2 * 100);
                }
            }
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
    }

    private void unlock() {
        if (this.lockSuccessful.compareAndSet(true, false)) {
            synchronized (tableSets) {
                String[] addTableNames =
                    new String[] {
                        baseOneTableName, baseTwoTableName, baseThreeTableName, baseFourTableName, baseFiveTableName};
                int index = 0;
                for (; index < addTableNames.length; index++) {
                    if (addTableNames[index] != null) {
                        tableSets.remove(addTableNames[index]);
                    }
                }
            }
        }
    }

    private boolean addTables() {
        synchronized (tableSets) {
            boolean bSuccessful = true;
            String[] addTableNames =
                new String[] {
                    baseOneTableName, baseTwoTableName, baseThreeTableName, baseFourTableName, baseFiveTableName};
            int index = 0;
            for (; index < addTableNames.length; index++) {
                if (addTableNames[index] != null && bSuccessful) {
                    boolean ret = tableSets.add(addTableNames[index]);
                    bSuccessful &= ret;
                }
                if (!bSuccessful) {
                    break;
                }
            }
            if (!bSuccessful) {
                for (int i = 0; i < index; i++) {
                    if (addTableNames[i] != null) {
                        tableSets.remove(addTableNames[i]);
                    }
                }
            }
            return bSuccessful;
        }
    }

    @After
    public void afterLockTableSets() {
        unlock();
    }

    public void setSqlMode(String mode, Connection conn) {
        String sql = "SET session sql_mode = '" + mode + "'";
        JdbcUtil.updateDataTddl(conn, sql, null);
    }

    protected static String buildCmdExtra(String... params) {
        if (0 == params.length) {
            return "";
        }
        return "/*+TDDL:CMD_EXTRA(" + String.join(",", params) + ")*/";
    }
}

