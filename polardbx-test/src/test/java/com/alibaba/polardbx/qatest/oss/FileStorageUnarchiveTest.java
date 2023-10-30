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
import com.alibaba.polardbx.qatest.util.ConnectionManager;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.alibaba.polardbx.qatest.util.PropertiesUtil;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDate;

/**
 * test ddl on origin table when it has archive table
 *
 * @author Shi Yuxuan
 */
public class FileStorageUnarchiveTest extends BaseTestCase {

    private static String testDataBase = "fileStorageUnarchiveDatabase";

    private static String originTable = "originTable";
    private static String archiveTable = "archiveTable";

    private static String unarhiveTable =
        String.format("please execute \"unarchive table %s.%s\" first", testDataBase, originTable);

    Connection conn;

    private static Engine engine = PropertiesUtil.engine();

    @BeforeClass
    public static void initDb() throws SQLException {
        try (Connection tmpConnection = ConnectionManager.getInstance().getDruidPolardbxConnection();
            Statement stmt = tmpConnection.createStatement()) {
            stmt.execute(String.format("drop database if exists %s ", testDataBase));
            stmt.execute(String.format("create database %s mode = 'auto'", testDataBase));
        }
    }

    @AfterClass
    public static void deleteDb() throws SQLException {
        try (Connection tmpConnection = ConnectionManager.getInstance().getDruidPolardbxConnection();
            Statement stmt = tmpConnection.createStatement()) {
            stmt.execute(String.format("drop database if exists %s ", testDataBase));
        }
    }

    @Before
    public void before() {
        this.conn = getPolardbxConnection(testDataBase);
        LocalDate now = LocalDate.now();
        LocalDate startWithDate = now.minusMonths(12L);
        JdbcUtil.executeUpdate(conn, String.format("drop table if exists %s ", originTable));
        JdbcUtil.executeUpdate(conn,
            String.format("CREATE TABLE %s (\n" +
                "    id bigint NOT NULL AUTO_INCREMENT,\n" +
                "    gmt_modified DATETIME NOT NULL,\n" +
                "    gg int(11) NOT NULL,\n" +
                "    PRIMARY KEY (id, gmt_modified)\n" +
                ")ENGINE = InnoDB DEFAULT CHARSET = utf8mb4\n" +
                "PARTITION BY RANGE(id)" +
                "(PARTITION p0 VALUES LESS THAN (1990) ENGINE = InnoDB," +
                "PARTITION p1 VALUES LESS THAN (2000) ENGINE = InnoDB," +
                "PARTITION p2 VALUES LESS THAN (2010) ENGINE = InnoDB," +
                "PARTITION p3 VALUES LESS THAN (2020) ENGINE = InnoDB)" +
                "LOCAL PARTITION BY RANGE (gmt_modified)\n" +
                "STARTWITH '%s'\n" +
                "INTERVAL 1 MONTH\n" +
                "EXPIRE AFTER 1\n" +
                "PRE ALLOCATE 3\n" +
                "PIVOTDATE NOW();", originTable, startWithDate));

        JdbcUtil.executeUpdate(conn, String.format("drop table if exists %s ", archiveTable));
        JdbcUtil.executeUpdate(conn,
            String.format("create table %s like %s engine = '%s' archive_mode = 'ttl'", archiveTable, originTable,
                engine.name()));

    }

    @Test
    public void testColumnDDL() {

//        JdbcUtil.executeUpdateFailed(conn, String.format("alter table %s add column t1 int(11)", originTable),
//            unarhiveTable);
//        JdbcUtil.executeUpdateFailed(conn,
//            String.format("alter table %s MODIFY COLUMN gg VARCHAR(255) NOT NULL DEFAULT '{}';", originTable),
//            unarhiveTable);
//        JdbcUtil.executeUpdateFailed(conn, String.format("alter table %s MODIFY COLUMN gg VARCHAR(254);", originTable),
//            unarhiveTable);
//        JdbcUtil.executeUpdateFailed(conn, String.format("alter table %s change gg g1 int(11)", originTable),
//            unarhiveTable);
//        JdbcUtil.executeUpdateFailed(conn, String.format("alter table %s drop column gg", originTable), unarhiveTable);

        JdbcUtil.executeUpdate(conn, String.format(("unarchive table %s"), originTable));

        JdbcUtil.executeUpdateSuccess(conn, String.format("alter table %s add column t1 int(11)", originTable));
        JdbcUtil.executeUpdateSuccess(conn,
            String.format("alter table %s MODIFY COLUMN gg VARCHAR(255) NOT NULL DEFAULT '{}';", originTable));
        JdbcUtil.executeUpdateSuccess(conn,
            String.format("alter table %s MODIFY COLUMN gg VARCHAR(254);", originTable));
        JdbcUtil.executeUpdateSuccess(conn, String.format("alter table %s change gg g1 int(11)", originTable));
        JdbcUtil.executeUpdateSuccess(conn, String.format("alter table %s drop column g1", originTable));
    }

    @Test
    public void testTableGroupDDL() {
        String tableGroup;
        try {
            ResultSet rs = JdbcUtil.executeQuery("show tablegroup where TABLE_GROUP_NAME not like \"oss%\"", conn);
            rs.next();
            tableGroup = rs.getString("TABLE_GROUP_NAME");
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        String unarhiveTableGroup = String.format("please execute \"unarchive tablegroup %s\" first", tableGroup);
        JdbcUtil.executeUpdateFailed(conn,
            String.format("ALTER TABLEGROUP %s ADD PARTITION (PARTITION p7 VALUES LESS THAN (3000))", tableGroup,
                originTable),
            unarhiveTableGroup);
        JdbcUtil.executeUpdateFailed(conn, String.format(
                "ALTER TABLEGROUP %s split PARTITION p2 into (PARTITION p40 VALUES LESS THAN (2005), PARTITION p41 VALUES LESS THAN (2010))",
                tableGroup, originTable),
            unarhiveTableGroup);
        JdbcUtil.executeUpdateFailed(conn,
            String.format("ALTER TABLEGROUP %s merge PARTITIONS p0,p1 to p1", tableGroup, originTable),
            unarhiveTableGroup);

        JdbcUtil.executeUpdate(conn, String.format(("unarchive tablegroup %s"), tableGroup));

        JdbcUtil.executeUpdateSuccess(conn,
            String.format("ALTER TABLEGROUP %s ADD PARTITION (PARTITION p7 VALUES LESS THAN (3000))", tableGroup,
                originTable));
        JdbcUtil.executeUpdateSuccess(conn, String.format(
            "ALTER TABLEGROUP %s split PARTITION p2 into (PARTITION p20 VALUES LESS THAN (2005), PARTITION p21 VALUES LESS THAN (2010))",
            tableGroup, originTable));
        JdbcUtil.executeUpdateSuccess(conn,
            String.format("ALTER TABLEGROUP %s merge PARTITIONS p0,p1 to p1", tableGroup, originTable));
    }
}
