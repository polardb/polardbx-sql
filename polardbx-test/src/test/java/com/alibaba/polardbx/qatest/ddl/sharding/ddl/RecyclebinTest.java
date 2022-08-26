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

import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.qatest.DDLBaseNewDBTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.Test;

import java.sql.ResultSet;
import java.sql.SQLException;

public class RecyclebinTest extends DDLBaseNewDBTestCase {

    private static final String ALLOW_ALTER_GSI_INDIRECTLY_HINT =
        "/*+TDDL:cmd_extra(ALLOW_ALTER_GSI_INDIRECTLY=true)*/";

    private static final String MULTI_TABLE_CREATEMENT = " create table if not exists %s (    \n"
        + "    id int NOT NULL AUTO_INCREMENT,\n"
        + "    order_id varchar(32) NOT NULL, \n"
        + "    create_time datetime DEFAULT NULL,\n"
        + "    primary key(id)\n"
        + ") ENGINE=InnoDB DEFAULT CHARSET=utf8 \n"
        + "dbpartition by STR_HASH(`order_id`, 2, 7) \n"
        + "tbpartition by STR_HASH(`order_id`, 2, 7) tbpartitions 2;";

    private static final String SINGLE_TABLE_CREATEMENT = " create table if not exists %s (    \n"
        + "    id int NOT NULL AUTO_INCREMENT,\n"
        + "    order_id varchar(32) NOT NULL, \n"
        + "    create_time datetime DEFAULT NULL,\n"
        + "    primary key(id)\n"
        + ") ENGINE=InnoDB DEFAULT CHARSET=utf8 \n";

    private static final String BROADCAST_TABLE_CREATEMENT = " CREATE TABLE if not exists %s (\n"
        + "  id bigint not null auto_increment, \n"
        + "  name varchar(30), \n"
        + "  primary key(id)\n"
        + ") ENGINE=InnoDB DEFAULT CHARSET=utf8 BROADCAST;";

    @Test
    public void testCreateAndDrop() throws Exception {
        String name = "test_recyclebin_tb";
        // clean env
        purge();

        String sql = String.format(ALLOW_ALTER_GSI_INDIRECTLY_HINT + SINGLE_TABLE_CREATEMENT, name);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        sql = ALLOW_ALTER_GSI_INDIRECTLY_HINT + "/!TDDL:ENABLE_RECYCLEBIN=true*/drop table " + name;
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        String binName = findTableInBin(name);
        Assert.assertTrue(binName != null);

        // test drop
        purgeTable(binName);
        Assert.assertTrue(findTableInBin(name) == null);
    }

    @Test
    public void testCreateAndDropMultiTable() throws Exception {
        String name = "test_recycle_multi_tb";
        // clean env
        purge();

        String sql = String.format(ALLOW_ALTER_GSI_INDIRECTLY_HINT + MULTI_TABLE_CREATEMENT, name);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        sql = ALLOW_ALTER_GSI_INDIRECTLY_HINT + "/!TDDL:ENABLE_RECYCLEBIN=true*/drop table " + name;
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        String binName = findTableInBin(name);
        Assert.assertTrue(binName != null);

        // test drop
        purgeTable(binName);
        Assert.assertTrue(findTableInBin(name) == null);
    }

    @Test
    public void testCreateAndFlashbackMultiTable() throws Exception {
        String name = "test_recycle_multi_tb";
        // clean env
        purge();

        String sql = String.format(ALLOW_ALTER_GSI_INDIRECTLY_HINT + MULTI_TABLE_CREATEMENT, name);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        sql = ALLOW_ALTER_GSI_INDIRECTLY_HINT + "/!TDDL:ENABLE_RECYCLEBIN=true*/drop table " + name;
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        String binName = findTableInBin(name);
        Assert.assertTrue(binName != null);

        // test drop
        flashBackTable(binName, name);
        Assert.assertTrue(findTableInBin(name) == null);
        Assert.assertTrue(findTable(name));
    }

    @Test
    public void testCreateAndFlashbackSingleTable() throws Exception {
        String name = "test_recyclebin_tb";
        // clean env
        purge();

        String sql = String.format(ALLOW_ALTER_GSI_INDIRECTLY_HINT + SINGLE_TABLE_CREATEMENT, name);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        sql = ALLOW_ALTER_GSI_INDIRECTLY_HINT + "/!TDDL:ENABLE_RECYCLEBIN=true*/drop table " + name;
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        String binName = findTableInBin(name);
        Assert.assertTrue(binName != null);

        // test flash back
        flashBackTable(binName, name);
        Assert.assertTrue(findTableInBin(name) == null);
        Assert.assertTrue(findTable(name));
    }

    @Test
    public void testCreateAndFlashbackBroadCastTable() throws Exception {
        String name = "test_recycle_broadcast_tb";
        // clean env
        purge();

        String sql = String.format(ALLOW_ALTER_GSI_INDIRECTLY_HINT + BROADCAST_TABLE_CREATEMENT, name);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        sql = ALLOW_ALTER_GSI_INDIRECTLY_HINT + "/!TDDL:ENABLE_RECYCLEBIN=true*/drop table " + name;
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        String binName = findTableInBin(name);
        Assert.assertTrue(binName != null);

        // test flash back
        flashBackTable(binName, name);
        Assert.assertTrue(findTableInBin(name) == null);
        Assert.assertTrue(findTable(name));
    }

    @Test
    public void testTruncateAndFlashbackMultiTable() throws Exception {

        String name = "test_recycle_multi_tb";
        // clean env
        purge();
        purgeDropTable(name);
        purgeDropTable(name + "_tmp");

        String sql = String.format(ALLOW_ALTER_GSI_INDIRECTLY_HINT + MULTI_TABLE_CREATEMENT, name);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        sql = ALLOW_ALTER_GSI_INDIRECTLY_HINT + "/!TDDL:ENABLE_RECYCLEBIN=true*/truncate table " + name;
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        String binName = findTableInBin(name);
        Assert.assertTrue(binName != null);

        // test flash back
        flashBackTable(binName, name + "_tmp");
        Assert.assertTrue(findTableInBin(name) == null);
        Assert.assertTrue(findTable(name + "_tmp"));

        // clean env
        purgeDropTable(name);
        purgeDropTable(name + "_tmp");
    }

    @Test
    public void testTruncateAndFlashbackSingleTable() throws Exception {

        String name = "test_recyclebin_tb";
        // clean env
        purge();
        purgeDropTable(name);
        purgeDropTable(name + "_tmp");

        String sql = String.format(ALLOW_ALTER_GSI_INDIRECTLY_HINT + SINGLE_TABLE_CREATEMENT, name);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        sql = ALLOW_ALTER_GSI_INDIRECTLY_HINT + "/!TDDL:ENABLE_RECYCLEBIN=true*/Truncate table " + name;
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        String binName = findTableInBin(name);
        Assert.assertTrue(binName != null);

        // test flash back
        flashBackTable(binName, name + "_tmp");
        Assert.assertTrue(findTableInBin(name) == null);
        Assert.assertTrue(findTable(name + "_tmp"));

        // clean env
        purgeDropTable(name);
        purgeDropTable(name + "_tmp");
    }

    @Test
    public void testTruncateAndFlashbackBroadCastTable() throws Exception {

        String name = "test_recycle_broadcast_tb";
        // clean env
        purge();
        purgeDropTable(name);
        purgeDropTable(name + "_tmp");

        String sql = String.format(ALLOW_ALTER_GSI_INDIRECTLY_HINT + BROADCAST_TABLE_CREATEMENT, name);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        sql = ALLOW_ALTER_GSI_INDIRECTLY_HINT + "/!TDDL:ENABLE_RECYCLEBIN=true*/Truncate table " + name;
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        String binName = findTableInBin(name);
        Assert.assertTrue(binName != null);

        // test flash back
        flashBackTable(binName, name + "_tmp");
        Assert.assertTrue(findTableInBin(name) == null);
        Assert.assertTrue(findTable(name + "_tmp"));

        // clean env
        purgeDropTable(name);
        purgeDropTable(name + "_tmp");
    }

    public String findTableInBin(String name) throws SQLException {
        String sql = "show recyclebin";
        ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, sql);
        try {
            while (rs.next()) {
                String itrName = rs.getString("ORIGINAL_NAME");
                if (name.equalsIgnoreCase(itrName)) {
                    return rs.getString("NAME");
                }
            }
        } finally {
            rs.close();
        }
        return null;
    }

    public boolean findTable(String name) throws SQLException {
        String sql = "show tables";
        ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, sql);
        try {
            while (rs.next()) {
                String itrName = rs.getString(1);
                if (name.equalsIgnoreCase(itrName)) {
                    return true;
                }
            }
        } finally {
            rs.close();
        }
        return false;
    }

    public void purgeDropTable(String name) {
        String sql = "drop table if exists " + name + " purge";
        JdbcUtil.executeUpdate(tddlConnection, sql);
    }

    public void purgeTable(String name) {
        String sql = "purge table " + name;
        JdbcUtil.executeUpdate(tddlConnection, sql);
    }

    public void flashBackTable(String binname, String originalName) {
        String sql = "FLASHBACK TABLE " + binname + " TO BEFORE DROP RENAME TO " + originalName;
        JdbcUtil.executeUpdate(tddlConnection, sql);
    }

    public void purge() {
        String sql = "purge recyclebin";
        JdbcUtil.executeUpdate(tddlConnection, sql);
    }

}
