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

package com.alibaba.polardbx.qatest.ddl.auto.tablegroup;

import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.qatest.DDLBaseNewDBTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Created by zhuqiwei.
 *
 * @author zhuqiwei
 */
public class ListDefaultShowTableGroupTest extends DDLBaseNewDBTestCase {
    private static final String dbName = "test_list_default_show_db";

    @Before
    public void prepareDb() {
        JdbcUtil.executeUpdateSuccess(tddlConnection, "create database if not exists "
            + dbName
            + " mode=auto");
        JdbcUtil.executeUpdateSuccess(tddlConnection, "use " + dbName);
    }

    @After
    public void dropDb() {
        JdbcUtil.executeUpdateSuccess(tddlConnection, "drop database if exists " + dbName);
    }

    @Test
    public void testList() {
        final String tgName = "tg_list_default_show1";
        final String createTable =
            "create table testShowTgTable("
                + "id int, "
                + "name varchar(20)) "
                + "partition by list(id) ( "
                + "partition p0 values in(1,2), "
                + "partition p1 values in(3,4), "
                + "partition pd values in(default)) "
                + "tablegroup=" + tgName;
        final String createTg = "create tablegroup " + tgName;
        final String showTableGroup = "show full tablegroup";
        JdbcUtil.executeUpdateSuccess(tddlConnection, createTg);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable);

        boolean containDefault = false;
        try (ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, showTableGroup)) {
            while (rs.next()) {
                String name = rs.getString(3);
                if (!tgName.equalsIgnoreCase(name)) {
                    continue;
                } else {
                    String partInfo = rs.getString(7);
                    if (partInfo != null && partInfo.contains("DEFAULT")) {
                        containDefault = true;
                    }
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        Assert.assertTrue(containDefault);
    }

    public void testListColumns() {
        final String tgName = "tg_list_default_show2";
        final String createTable =
            "create table testShowTgTable2("
                + "id int, "
                + "name varchar(20)) "
                + "partition by list(id, name) ("
                + "partition p0 values in((1,'aaa'),(2,'bbb')), "
                + "partition p1 values in((3,'ccc'),(4,'ddd')), "
                + "partition pd values in(default)) "
                + "tablegroup=" + tgName;
        final String createTg = "create tablegroup " + tgName;
        final String showTableGroup = "show full tablegroup";
        JdbcUtil.executeUpdateSuccess(tddlConnection, createTg);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable);

        boolean containDefault = false;
        try (ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, showTableGroup)) {
            while (rs.next()) {
                String name = rs.getString(3);
                if (!tgName.equalsIgnoreCase(name)) {
                    continue;
                } else {
                    String partInfo = rs.getString(7);
                    if (partInfo != null && partInfo.contains("DEFAULT")) {
                        containDefault = true;
                    }
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        Assert.assertTrue(containDefault);
    }

}
