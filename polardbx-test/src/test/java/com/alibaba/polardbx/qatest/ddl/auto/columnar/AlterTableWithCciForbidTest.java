/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the License);
 * you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.polardbx.qatest.ddl.auto.columnar;

import com.alibaba.polardbx.qatest.DDLBaseNewDBTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class AlterTableWithCciForbidTest extends DDLBaseNewDBTestCase {
    private static final String FORBID_DDL_WITH_CCI = "FORBID_DDL_WITH_CCI=TRUE";
    private static final String PRIMARY_TABLE_NAME1 = "alter_table_ddl_with_cci_err_prim_pt_1";
    private static final String INDEX_NAME1 = "alter_table_ddl_with_cci_err_cci_pt_1";
    private static final String PRIMARY_TABLE_NAME2 = "alter_table_ddl_with_cci_err_prim_pt_2";
    private static final String INDEX_NAME2 = "alter_table_ddl_with_cci_err_cci_pt_2";
    private static final String BACK_FILL = "PHYSICAL_BACKFILL_ENABLE=false";

    private static final String creatTableTmpl = "CREATE TABLE `%s` ( \n"
        + "    `id` bigint(11) NOT NULL AUTO_INCREMENT BY GROUP, \n"
        + "    `order_id` varchar(20) DEFAULT NULL, \n"
        + "    `buyer_id` varchar(20) DEFAULT NULL, \n"
        + "    `order_snapshot` longtext, \n"
        + "    PRIMARY KEY (`id`)"
        + ") ENGINE = InnoDB CHARSET = utf8 PARTITION BY KEY(`order_id`);\n";
    private static final String createCciTmpl =
        "ALTER TABLE %s ADD CLUSTERED clustered columnar index %s(`buyer_id`) PARTITION BY KEY(`ID`)";

    public boolean usingNewPartDb() {
        return true;
    }

    @Before
    public void before() {
        dropTableIfExists(PRIMARY_TABLE_NAME1);
        createTableAndCci();
    }

    @After
    public void after() {
        dropTableIfExists(PRIMARY_TABLE_NAME1);
    }

    private static String buildCmdExtra(String... params) {
        if (0 == params.length) {
            return "";
        }
        return "/*+TDDL:CMD_EXTRA(" + String.join(",", params) + ")*/";
    }

    private void createTableAndCci() {
        try {
            // Create table
            final String sqlCreateTable1 = String.format(
                creatTableTmpl,
                PRIMARY_TABLE_NAME1);
            JdbcUtil.executeUpdateSuccess(tddlConnection, sqlCreateTable1);

            // Create cci success
            final String sqlCreateCci1 = String.format(createCciTmpl, PRIMARY_TABLE_NAME1, INDEX_NAME1);
            createCciSuccess(sqlCreateCci1);
        } catch (Exception e) {
            throw new RuntimeException("CREATE TABLE statement execution failed!", e);
        }
    }

    static List<String> unsupportedTypes =
        Arrays.asList(
            "decimal(10,2)",
            "float",
            "double",
            "numeric(10,2)",
            "json",
            "point",
            "enum(\"a\",\"b\",\"c\")",
            "set(\"a\",\"b\",\"c\")",
            "geometry"
        );

    @Test
    public void createTableWithCciForbidTest() {
        // Create table with cci failed
        for (String type : unsupportedTypes) {
            dropTableIfExists(PRIMARY_TABLE_NAME2);
            String sqlCreateTable1 = String.format("CREATE TABLE `%s` ( \n"
                    + "    `id` bigint(11) NOT NULL AUTO_INCREMENT BY GROUP, \n"
                    + "    `order_id` varchar(20) DEFAULT NULL, \n"
                    + "    `buyer_id` %s DEFAULT NULL, \n"
                    + "    `order_snapshot` longtext, \n"
                    + "    clustered columnar index %s (`buyer_id`) PARTITION BY KEY(`ID`),\n"
                    + "    PRIMARY KEY (`id`)\n"
                    + ") ENGINE = InnoDB CHARSET = utf8 PARTITION BY KEY(`order_id`);", PRIMARY_TABLE_NAME2, type,
                INDEX_NAME2);
            createCciFailed(sqlCreateTable1, "The Sort Key 'buyer_id' in the type of", "The DDL job has been rollback");

            sqlCreateTable1 = String.format("CREATE TABLE `%s` ( \n"
                    + "    `id` bigint(11) NOT NULL AUTO_INCREMENT BY GROUP, \n"
                    + "    `order_id` %s DEFAULT NULL, \n"
                    + "    `buyer_id` varchar(20) DEFAULT NULL, \n"
                    + "    `order_snapshot` longtext, \n"
                    + "    clustered columnar index %s (`buyer_id`) PARTITION BY KEY(`order_id`),\n"
                    + "    PRIMARY KEY (`id`)\n"
                    + ") ENGINE = InnoDB CHARSET = utf8 PARTITION BY KEY(`buyer_id`);", PRIMARY_TABLE_NAME2, type,
                INDEX_NAME2);
            createCciFailed(sqlCreateTable1, "", "The DDL job has been rollback");

            sqlCreateTable1 = String.format("CREATE TABLE `%s` ( \n"
                    + "    `id` %s NOT NULL, \n"
                    + "    `order_id` varchar(20) DEFAULT NULL, \n"
                    + "    `buyer_id` varchar(20) DEFAULT NULL, \n"
                    + "    `order_snapshot` longtext, \n"
                    + "    clustered columnar index %s (`buyer_id`) PARTITION BY KEY(`order_id`),\n"
                    + "    PRIMARY KEY (`id`)\n"
                    + ") ENGINE = InnoDB CHARSET = utf8 PARTITION BY KEY(`order_id`);", PRIMARY_TABLE_NAME2, type,
                INDEX_NAME2);
            createCciFailed(sqlCreateTable1, "The Primary Key 'id' in the type of", "The DDL job has been rollback");
        }
    }

    @Test
    public void createTableAndCreateCciForbidTest() {
        // Create table with cci failed
        for (String type : unsupportedTypes) {
            dropTableIfExists(PRIMARY_TABLE_NAME2);
            String sqlCreateTable1 = String.format("CREATE TABLE `%s` ( \n"
                + "    `id` bigint(11) NOT NULL AUTO_INCREMENT BY GROUP, \n"
                + "    `order_id` varchar(20) DEFAULT NULL, \n"
                + "    `buyer_id` %s DEFAULT NULL, \n"
                + "    `order_snapshot` longtext, \n"
                + "    PRIMARY KEY (`id`)\n"
                + ") ENGINE = InnoDB CHARSET = utf8 PARTITION BY KEY(`order_id`);", PRIMARY_TABLE_NAME2, type);
            JdbcUtil.executeUpdateSuccess(tddlConnection, sqlCreateTable1);
            // Create cci failed
            String sqlCreateCci1 =
                String.format("ALTER TABLE %s ADD clustered columnar index %s (`buyer_id`) PARTITION BY KEY(`ID`)",
                    PRIMARY_TABLE_NAME2, INDEX_NAME2);
            createCciFailed(sqlCreateCci1, "The Sort Key 'buyer_id' in the type of", "The DDL job has been rollback");

            dropTableIfExists(PRIMARY_TABLE_NAME2);
            sqlCreateTable1 = String.format("CREATE TABLE `%s` ( \n"
                + "    `id` bigint(11) NOT NULL AUTO_INCREMENT BY GROUP, \n"
                + "    `order_id` %s DEFAULT NULL, \n"
                + "    `buyer_id` varchar(20) DEFAULT NULL, \n"
                + "    `order_snapshot` longtext, \n"
                + "    PRIMARY KEY (`id`)\n"
                + ") ENGINE = InnoDB CHARSET = utf8 PARTITION BY KEY(`buyer_id`);", PRIMARY_TABLE_NAME2, type);
            JdbcUtil.executeUpdateSuccess(tddlConnection, sqlCreateTable1);
            // Create cci failed
            sqlCreateCci1 = String.format(
                "ALTER TABLE %s ADD clustered columnar index %s (`buyer_id`) PARTITION BY KEY(`order_id`)",
                PRIMARY_TABLE_NAME2, INDEX_NAME2);
            createCciFailed(sqlCreateCci1, "", "The DDL job has been rollback");

            // not support primary key in type json/geometry
            if (type.equals("json") || type.equals("geometry")) {
                continue;
            }
            //8.0 : Spatial indexes can't be primary or unique indexes
            if (isMySQL80() && (type.equals("point"))) {
                continue;
            }
            dropTableIfExists(PRIMARY_TABLE_NAME2);
            sqlCreateTable1 = String.format("CREATE TABLE `%s` ( \n"
                + "    `id` %s NOT NULL, \n"
                + "    `order_id` varchar(20) DEFAULT NULL, \n"
                + "    `buyer_id` varchar(20) DEFAULT NULL, \n"
                + "    `order_snapshot` longtext, \n"
                + "    PRIMARY KEY (`id`)\n"
                + ") ENGINE = InnoDB CHARSET = utf8 PARTITION BY KEY(`order_id`);", PRIMARY_TABLE_NAME2, type);
            JdbcUtil.executeUpdateSuccess(tddlConnection, sqlCreateTable1);
            // Create cci failed
            sqlCreateCci1 = String.format(
                "ALTER TABLE %s ADD clustered columnar index %s (`buyer_id`) PARTITION BY KEY(`order_id`)",
                PRIMARY_TABLE_NAME2, INDEX_NAME2);
            createCciFailed(sqlCreateCci1, "The Primary Key 'id' in the type of", "The DDL job has been rollback");
        }
    }

    @Test
    public void testAddColumn() {
        String hint = buildCmdExtra(FORBID_DDL_WITH_CCI);
        final String sql1 = String.format("ALTER TABLE %s ADD COLUMN `new_col` varchar(20) DEFAULT NULL",
            PRIMARY_TABLE_NAME1);
        final String sql2 = String.format("ALTER TABLE %s ADD COLUMN `new_col` varchar(20) DEFAULT NULL FIRST",
            PRIMARY_TABLE_NAME1);
        final String sql3 =
            String.format("ALTER TABLE %s ADD COLUMN `new_col` varchar(20) DEFAULT NULL AFTER `order_id`",
                PRIMARY_TABLE_NAME1);
        try {
            JdbcUtil.executeUpdateFailed(tddlConnection, hint + sql1,
                "Do not support current ddl [ADD_COLUMN] with cci");
            JdbcUtil.executeUpdateFailed(tddlConnection, hint + sql2,
                "Do not support current ddl [ADD_COLUMN] with cci");
            JdbcUtil.executeUpdateFailed(tddlConnection, hint + sql3,
                "Do not support current ddl [ADD_COLUMN] with cci");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testDropColumn() {
        String hint = buildCmdExtra(FORBID_DDL_WITH_CCI);
        final String sql1 = String.format("ALTER TABLE %s DROP COLUMN `order_snapshot`",
            PRIMARY_TABLE_NAME1);
        final String sql2 = String.format("ALTER TABLE %s DROP COLUMN `buyer_id`",
            PRIMARY_TABLE_NAME1);
        final String sql3 = String.format("ALTER TABLE %s DROP COLUMN `id`",
            PRIMARY_TABLE_NAME1);
        try {
            JdbcUtil.executeUpdateFailed(tddlConnection, hint + sql1,
                "Do not support current ddl [DROP_COLUMN] with cci");
            JdbcUtil.executeUpdateFailed(tddlConnection, hint + sql2,
                "Do not support drop sort key of clustered columnar index");
            JdbcUtil.executeUpdateFailed(tddlConnection, hint + sql3,
                "Do not support drop primary key of clustered columnar index");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testModifyColumn() {
        String hint = buildCmdExtra(FORBID_DDL_WITH_CCI);
        final String sql1 = String.format("ALTER TABLE %s MODIFY COLUMN `order_snapshot` varchar(64)",
            PRIMARY_TABLE_NAME1);
        final String sql2 = String.format("ALTER TABLE %s MODIFY COLUMN `order_snapshot` varchar(64) FIRST",
            PRIMARY_TABLE_NAME1);
        final String sql3 = String.format("ALTER TABLE %s MODIFY COLUMN `order_snapshot` varchar(64) AFTER `order_id`",
            PRIMARY_TABLE_NAME1);
        final String sql4 = String.format("ALTER TABLE %s MODIFY COLUMN `buyer_id` varchar(64)",
            PRIMARY_TABLE_NAME1);
        final String sql5 = String.format("ALTER TABLE %s MODIFY COLUMN `id` BIGINT",
            PRIMARY_TABLE_NAME1);
        try {
            JdbcUtil.executeUpdateFailed(tddlConnection, hint + sql1,
                "Do not support current ddl [MODIFY_COLUMN] with cci");
            JdbcUtil.executeUpdateFailed(tddlConnection, hint + sql2,
                "Do not support current ddl [MODIFY_COLUMN] with cci");
            JdbcUtil.executeUpdateFailed(tddlConnection, hint + sql3,
                "Do not support current ddl [MODIFY_COLUMN] with cci");
            JdbcUtil.executeUpdateFailed(tddlConnection, hint + sql4,
                "Do not support modify sort key of clustered columnar index");
            JdbcUtil.executeUpdateFailed(tddlConnection, hint + sql5,
                "Do not support modify primary key of clustered columnar index");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testChangeColumn() {
        String hint = buildCmdExtra(FORBID_DDL_WITH_CCI);
        final String sql1 =
            String.format("ALTER TABLE %s CHANGE COLUMN `order_snapshot` `order_snapshot_new` varchar(64)",
                PRIMARY_TABLE_NAME1);
        final String sql2 = String.format("ALTER TABLE %s CHANGE COLUMN `buyer_id` `buyer_id_new` varchar(64)",
            PRIMARY_TABLE_NAME1);
        final String sql3 = String.format("ALTER TABLE %s CHANGE COLUMN `id` `id_new` int",
            PRIMARY_TABLE_NAME1);
        try {
            JdbcUtil.executeUpdateFailed(tddlConnection, hint + sql1,
                "Do not support current ddl [CHANGE_COLUMN] with cci");
            JdbcUtil.executeUpdateFailed(tddlConnection, hint + sql2,
                "Do not support change sort key of clustered columnar index");
            JdbcUtil.executeUpdateFailed(tddlConnection, hint + sql3,
                "Do not support change primary key of clustered columnar index");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testAlterColumnDefaultValue() {
        String hint = buildCmdExtra(FORBID_DDL_WITH_CCI);
        final String sql1 = String.format("ALTER TABLE %s ALTER COLUMN `order_snapshot` SET DEFAULT 'abc'",
            PRIMARY_TABLE_NAME1);
        final String sql2 = String.format("ALTER TABLE %s ALTER COLUMN `buyer_id` SET DEFAULT 'abc'",
            PRIMARY_TABLE_NAME1);
        final String sql3 = String.format("ALTER TABLE %s ALTER COLUMN `id` SET DEFAULT 123",
            PRIMARY_TABLE_NAME1);

        final String sql4 = String.format("ALTER TABLE %s ALTER COLUMN `order_snapshot` DROP DEFAULT",
            PRIMARY_TABLE_NAME1);
        final String sql5 = String.format("ALTER TABLE %s ALTER COLUMN `buyer_id` DROP DEFAULT",
            PRIMARY_TABLE_NAME1);
        final String sql6 = String.format("ALTER TABLE %s ALTER COLUMN `id` DROP DEFAULT",
            PRIMARY_TABLE_NAME1);
        try {
            JdbcUtil.executeUpdateFailed(tddlConnection, hint + sql1,
                "Do not support current ddl [ALTER_COLUMN_DEFAULT_VAL] with cci");
            JdbcUtil.executeUpdateFailed(tddlConnection, hint + sql2,
                "Do not support drop or set default value of sort key in clustered columnar index");
            JdbcUtil.executeUpdateFailed(tddlConnection, hint + sql3,
                "Do not support drop or set default value of primary key in clustered columnar index");
            JdbcUtil.executeUpdateFailed(tddlConnection, hint + sql4,
                "Do not support current ddl [ALTER_COLUMN_DEFAULT_VAL] with cci");
            JdbcUtil.executeUpdateFailed(tddlConnection, hint + sql5,
                "Do not support drop or set default value of sort key in clustered columnar index");
            JdbcUtil.executeUpdateFailed(tddlConnection, hint + sql6,
                "Do not support drop or set default value of primary key in clustered columnar index");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Ignore
    @Test
    public void testOmc() {
        final String sql1 =
            String.format(
                "ALTER TABLE %s CHANGE COLUMN `order_snapshot` `order_snapshot_new` varchar(64), ALGORITHM=OMC",
                PRIMARY_TABLE_NAME1);
        final String sql2 = String.format("ALTER TABLE %s MODIFY COLUMN `order_snapshot` varchar(64), ALGORITHM=OMC",
            PRIMARY_TABLE_NAME1);
        try {
            JdbcUtil.executeUpdateFailed(tddlConnection, sql1,
                "Do not support ALGORITHM=OMC on table with cci");
            JdbcUtil.executeUpdateFailed(tddlConnection, sql2,
                "Do not support ALGORITHM=OMC on table with cci");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testRenameTable() {
        String hint = buildCmdExtra(FORBID_DDL_WITH_CCI);
        final String sql1 = String.format("ALTER TABLE %s RENAME TO %s",
            PRIMARY_TABLE_NAME1, PRIMARY_TABLE_NAME1 + "_new");
        final String sql2 = String.format("RENAME TABLE %s TO %s",
            PRIMARY_TABLE_NAME1, PRIMARY_TABLE_NAME1 + "_new");
        try {
            JdbcUtil.executeUpdateFailed(tddlConnection, hint + sql1,
                "Do not support current ddl [RENAME_TABLE] with cci");
            JdbcUtil.executeUpdateFailed(tddlConnection, hint + sql2,
                "Do not support current ddl [RENAME_TABLE] with cci");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Ignore
    @Test
    public void testRenameIndex() {
        String hint = buildCmdExtra(FORBID_DDL_WITH_CCI);
        final String sql1 = String.format("ALTER TABLE %s RENAME INDEX %s TO %s",
            PRIMARY_TABLE_NAME1, INDEX_NAME1, INDEX_NAME1 + "_new");
        try {
            JdbcUtil.executeUpdateFailed(tddlConnection, hint + sql1, "Do not support rename cci");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testRepartition() {
        String hint = buildCmdExtra(FORBID_DDL_WITH_CCI);
        final String sql1 = String.format("ALTER TABLE %s SINGLE", PRIMARY_TABLE_NAME1);
        final String sql2 = String.format("ALTER TABLE %s BROADCAST", PRIMARY_TABLE_NAME1);
        final String sql3 = String.format("ALTER TABLE %s PARTITION BY KEY(`id`)", PRIMARY_TABLE_NAME1);
        try {
            JdbcUtil.executeUpdateFailed(tddlConnection, hint + sql1,
                "Do not support current ddl [ALTER_TABLE] with cci");
            JdbcUtil.executeUpdateFailed(tddlConnection, hint + sql2,
                "Do not support current ddl [ALTER_TABLE] with cci");
            JdbcUtil.executeUpdateFailed(tddlConnection, hint + sql3,
                "Do not support current ddl [ALTER_TABLE] with cci");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testModifyPartitions() {
        String hint = buildCmdExtra(FORBID_DDL_WITH_CCI);

        String autoPartitionTable = "CREATE TABLE t_order (\n"
            + "  `id` bigint(11) NOT NULL AUTO_INCREMENT,\n"
            + "  `order_id` varchar(20) DEFAULT NULL,\n"
            + "  `buyer_id` varchar(20) DEFAULT NULL,\n"
            + "  `seller_id` varchar(20) DEFAULT NULL,\n"
            + "  `order_snapshot` longtext DEFAULT NULL,\n"
            + "  `order_detail` longtext DEFAULT NULL,\n"
            + "  PRIMARY KEY (`id`),\n"
            + "  KEY `l_i_order` (`order_id`)\n"
            + ") ENGINE=InnoDB DEFAULT CHARSET=utf8;";
        JdbcUtil.executeUpdateSuccess(tddlConnection, autoPartitionTable);
        final String sqlCreateCci1 = String.format(createCciTmpl, "t_order", INDEX_NAME1);
        createCciSuccess(sqlCreateCci1);

        final String sql1 = "ALTER TABLE t_order PARTITIONS 32;";
        final String sql2 = String.format("ALTER TABLE %s REMOVE PARTITIONING;", PRIMARY_TABLE_NAME1);
        final String sql3 =
            String.format("ALTER TABLE %s PARTITION BY KEY(order_id, buyer_id) PARTITIONS 8;", PRIMARY_TABLE_NAME1);
        try {
            JdbcUtil.executeUpdateFailed(tddlConnection, hint + sql1,
                "Do not support current ddl [ALTER_TABLE] with cci");
            JdbcUtil.executeUpdateFailed(tddlConnection, hint + sql2,
                "Do not support current ddl [ALTER_TABLE] with cci");
            JdbcUtil.executeUpdateFailed(tddlConnection, hint + sql3,
                "Do not support current ddl [ALTER_TABLE] with cci");
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            dropTableIfExists("t_order");
        }
    }

//    @Test
//    public void testAddDropPartitions() {
//        String hint = buildCmdExtra(FORBID_DDL_WITH_CCI);
//
//        String autoPartitionTable = "CREATE TABLE t_order (\n"
//            + "  `id` bigint(11) NOT NULL AUTO_INCREMENT,\n"
//            + "  `order_id` varchar(20) DEFAULT NULL,\n"
//            + "  `buyer_id` varchar(20) DEFAULT NULL,\n"
//            + "  `seller_id` varchar(20) DEFAULT NULL,\n"
//            + "  `order_snapshot` longtext DEFAULT NULL,\n"
//            + "  `order_detail` longtext DEFAULT NULL,\n"
//            + "  PRIMARY KEY (`id`),\n"
//            + "  KEY `l_i_order` (`order_id`)\n"
//            + ") PARTITION BY RANGE(id)\n"
//            + " (PARTITION p1 VALUES LESS THAN(20),\n"
//            + "  PARTITION p2 VALUES LESS THAN(40))";
//        JdbcUtil.executeUpdateSuccess(tddlConnection, autoPartitionTable);
//        final String createCciTmpl =
//            "ALTER TABLE t_order ADD CLUSTERED clustered columnar index %s(`buyer_id`) PARTITION BY RANGE(`ID`)(PARTITION p1 VALUES LESS THAN(20),\n"
//                + "  PARTITION p2 VALUES LESS THAN(40))";
//        final String sqlCreateCci1 = String.format(createCciTmpl, INDEX_NAME1);
//        createCciSuccess(sqlCreateCci1);
//
//        final String sql1 = "ALTER TABLE t_order ADD PARTITION (PARTITION p3 VALUES LESS THAN(60),\n"
//            + "                              PARTITION p4 VALUES LESS THAN(80))";
//        final String sql2 = "ALTER TABLE t_order DROP PARTITION p2";
//        try {
//            JdbcUtil.executeUpdateFailed(tddlConnection, hint + sql1,
//                "Do not support current ddl [ADD_PARTITION] with cci");
//            JdbcUtil.executeUpdateFailed(tddlConnection, hint + sql2,
//                "Do not support current ddl [DROP_PARTITION] with cci");
//        } catch (Exception e) {
//            throw new RuntimeException(e);
//        } finally {
//            dropTableIfExists("t_order");
//        }
//    }

    @Test
    public void testSplitPartition() {
        String hint = buildCmdExtra(FORBID_DDL_WITH_CCI);

        // RANGE
        String rangePartitionTable = "CREATE TABLE t_order_1 (\n"
            + "  `id` bigint(11) NOT NULL AUTO_INCREMENT,\n"
            + "  `order_id` varchar(20) DEFAULT NULL,\n"
            + "  `buyer_id` varchar(20) DEFAULT NULL,\n"
            + "  `seller_id` varchar(20) DEFAULT NULL,\n"
            + "  `order_snapshot` longtext DEFAULT NULL,\n"
            + "  `order_detail` longtext DEFAULT NULL,\n"
            + "  PRIMARY KEY (`id`),\n"
            + "  KEY `l_i_order` (`order_id`)\n"
            + ") PARTITION BY RANGE(id)\n"
            + " (PARTITION p1 VALUES LESS THAN(20),\n"
            + "  PARTITION p2 VALUES LESS THAN(100))";
        JdbcUtil.executeUpdateSuccess(tddlConnection, rangePartitionTable);
        String createCciTmpl =
            "ALTER TABLE t_order_1 ADD CLUSTERED clustered columnar index %s(`buyer_id`) PARTITION BY RANGE(`ID`)(PARTITION p1 VALUES LESS THAN(20),\n"
                + "  PARTITION p2 VALUES LESS THAN(100))";
        String sqlCreateCci1 = String.format(createCciTmpl, INDEX_NAME1);
        createCciSuccess(sqlCreateCci1);

        final String sql1 = "ALTER TABLE t_order_1 SPLIT PARTITION p1 INTO\n"
            + "(PARTITION p10 VALUES LESS THAN (8),\n"
            + "PARTITION p11 VALUES LESS THAN(15),\n"
            + "PARTITION p12 VALUES LESS THAN(20))";
        final String sql2 = "ALTER TABLE t_order_1 SPLIT PARTITION p1 AT(9) INTO (partition p11, partition p12)";

        // LIST
        String listPartitionTable = "CREATE TABLE t_order_2 (\n"
            + "  `id` bigint(11) NOT NULL AUTO_INCREMENT,\n"
            + "  `order_id` varchar(20) DEFAULT NULL,\n"
            + "  `buyer_id` varchar(20) DEFAULT NULL,\n"
            + "  `seller_id` varchar(20) DEFAULT NULL,\n"
            + "  `order_snapshot` longtext DEFAULT NULL,\n"
            + "  `order_detail` longtext DEFAULT NULL,\n"
            + "  PRIMARY KEY (`id`),\n"
            + "  KEY `l_i_order` (`order_id`)\n"
            + ") PARTITION BY LIST(id)\n"
            + " (PARTITION p1 VALUES IN(1, 2, 3, 4, 5, 6),\n"
            + "  PARTITION p2 VALUES IN(7,8,9),\n"
            + "  PARTITION p3 VALUES IN(default))";
        JdbcUtil.executeUpdateSuccess(tddlConnection, listPartitionTable);
        createCciTmpl =
            "ALTER TABLE t_order_2 ADD CLUSTERED clustered columnar index %s(`buyer_id`) PARTITION BY LIST(id)\n"
                + " (PARTITION p1 VALUES IN(1, 2, 3, 4, 5, 6),\n"
                + "  PARTITION p2 VALUES IN(7,8,9),\n"
                + "  PARTITION p3 VALUES IN(default))";
        sqlCreateCci1 = String.format(createCciTmpl, INDEX_NAME1);
        createCciSuccess(sqlCreateCci1);

        final String sql3 = "ALTER TABLE t_order_2 SPLIT PARTITION p1 INTO\n"
            + "(PARTITION p10 VALUES IN (1,3,5),\n"
            + "PARTITION p11 VALUES IN (2,4),\n"
            + "PARTITION p12 VALUES IN (6))";

        // HASH
        final String sql4 = String.format("ALTER TABLE %s SPLIT PARTITION p1;", PRIMARY_TABLE_NAME1);

        try {
            JdbcUtil.executeUpdateFailed(tddlConnection, hint + sql1,
                "Do not support current ddl [SPLIT_PARTITION] with cci");
            JdbcUtil.executeUpdateFailed(tddlConnection, hint + sql2,
                "Do not support current ddl [SPLIT_PARTITION] with cci");
            JdbcUtil.executeUpdateFailed(tddlConnection, hint + sql3,
                "Do not support current ddl [SPLIT_PARTITION] with cci");
            JdbcUtil.executeUpdateFailed(tddlConnection, hint + sql4,
                "Do not support current ddl [SPLIT_PARTITION] with cci");
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            dropTableIfExists("t_order_1");
            dropTableIfExists("t_order_2");
        }
    }

    @Test
    public void testOtherPartitionOperations() {
        String hint = buildCmdExtra(FORBID_DDL_WITH_CCI);

        // RANGE
        String rangePartitionTable = "CREATE TABLE t_order_1 (\n"
            + "  `id` bigint(11) NOT NULL AUTO_INCREMENT,\n"
            + "  `order_id` varchar(20) DEFAULT NULL,\n"
            + "  `buyer_id` varchar(20) DEFAULT NULL,\n"
            + "  `seller_id` varchar(20) DEFAULT NULL,\n"
            + "  `order_snapshot` longtext DEFAULT NULL,\n"
            + "  `order_detail` longtext DEFAULT NULL,\n"
            + "  PRIMARY KEY (`id`),\n"
            + "  KEY `l_i_order` (`order_id`)\n"
            + ") PARTITION BY RANGE(id)\n"
            + " (PARTITION p1 VALUES LESS THAN(20),\n"
            + "  PARTITION p2 VALUES LESS THAN(100))";
        JdbcUtil.executeUpdateSuccess(tddlConnection, rangePartitionTable);
        String createCciTmpl =
            "ALTER TABLE t_order_1 ADD CLUSTERED clustered columnar index %s(`buyer_id`) PARTITION BY RANGE(`ID`)(PARTITION p1 VALUES LESS THAN(20),\n"
                + "  PARTITION p2 VALUES LESS THAN(100))";
        String sqlCreateCci1 = String.format(createCciTmpl, INDEX_NAME1);
        createCciSuccess(sqlCreateCci1);

        // LIST
        String listPartitionTable = "CREATE TABLE t_order_2 (\n"
            + "  `id` bigint(11) NOT NULL AUTO_INCREMENT,\n"
            + "  `order_id` varchar(20) DEFAULT NULL,\n"
            + "  `buyer_id` varchar(20) DEFAULT NULL,\n"
            + "  `seller_id` varchar(20) DEFAULT NULL,\n"
            + "  `order_snapshot` longtext DEFAULT NULL,\n"
            + "  `order_detail` longtext DEFAULT NULL,\n"
            + "  PRIMARY KEY (`id`),\n"
            + "  KEY `l_i_order` (`order_id`)\n"
            + ") PARTITION BY LIST(id)\n"
            + " (PARTITION p1 VALUES IN(1, 2, 3, 4, 5, 6),\n"
            + "  PARTITION p2 VALUES IN(7,8,9),\n"
            + "  PARTITION p3 VALUES IN(default))";
        JdbcUtil.executeUpdateSuccess(tddlConnection, listPartitionTable);
        createCciTmpl =
            "ALTER TABLE t_order_2 ADD CLUSTERED clustered columnar index %s(`buyer_id`) PARTITION BY LIST(id)\n"
                + " (PARTITION p1 VALUES IN(1, 2, 3, 4, 5, 6),\n"
                + "  PARTITION p2 VALUES IN(7,8,9),\n"
                + "  PARTITION p3 VALUES IN(default))";
        sqlCreateCci1 = String.format(createCciTmpl, INDEX_NAME1);
        createCciSuccess(sqlCreateCci1);

        // MERGE
        final String sql1 = "ALTER TABLE t_order_1 MERGE PARTITIONS p1,p2 to p12";
        // RENAME
        final String sql2 = "ALTER TABLE t_order_1 RENAME PARTITION p1 to p10, p2 to p20";
        // MODIFY
        final String sql3 = "ALTER TABLE t_order_2 MODIFY PARTITION p2 DROP VALUES(9)";
        final String sql4 = "ALTER TABLE t_order_2 MODIFY PARTITION p2 ADD VALUES(13,14)";

        try {
            JdbcUtil.executeUpdateFailed(tddlConnection, hint + sql1,
                "Do not support current ddl [MERGE_PARTITION] with cci");
            JdbcUtil.executeUpdateFailed(tddlConnection, hint + sql2,
                "Do not support current ddl [RENAME_PARTITION] with cci");
            JdbcUtil.executeUpdateFailed(tddlConnection, hint + sql3,
                "not support");
            JdbcUtil.executeUpdateFailed(tddlConnection, hint + sql4,
                "Do not support current ddl [REORGANIZE_PARTITION] with cci");
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            dropTableIfExists("t_order_1");
//            dropTableIfExists("t_order_2");
        }
    }

    @Test
    public void testTtlLocalPartitions() {
        String hint = buildCmdExtra(FORBID_DDL_WITH_CCI);

        String autoPartitionTable = "CREATE TABLE t_order (\n"
            + "  `id` bigint NOT NULL AUTO_INCREMENT,\n"
            + "  `order_id` varchar(20) DEFAULT NULL,\n"
            + "  `buyer_id` varchar(20) DEFAULT NULL,\n"
            + "  `seller_id` varchar(20) DEFAULT NULL,\n"
            + "  `order_snapshot` longtext DEFAULT NULL,\n"
            + "  `order_detail` longtext DEFAULT NULL,\n"
            + "   `gmt_modified` DATETIME NOT NULL,\n"
            + "   PRIMARY KEY (id, gmt_modified)\n"
            + ")\n"
            + "PARTITION BY HASH(id)\n"
            + "PARTITIONS 16\n"
            + "LOCAL PARTITION BY RANGE (gmt_modified)\n"
            + "INTERVAL 1 MONTH\n"
            + "EXPIRE AFTER 12\n"
            + "PRE ALLOCATE 3;";
        JdbcUtil.executeUpdateSuccess(tddlConnection, autoPartitionTable);
        final String sqlCreateCci1 = String.format(createCciTmpl, "t_order", INDEX_NAME1);
        createCciSuccess(sqlCreateCci1);

        final String sql1 = "ALTER TABLE t_order EXPIRE LOCAL PARTITION p20210401";
        final String sql2 = "ALTER TABLE t_order REMOVE LOCAL PARTITIONING;";
        final String sql3 = "ALTER TABLE t_order ALLOCATE LOCAL PARTITION";
        final String sql4 = "ALTER TABLE t_order SINGLE";
        try {
            JdbcUtil.executeUpdateFailed(tddlConnection, hint + sql1,
                "Do not support current ddl [LOCAL_PARTITION] with cci");
            JdbcUtil.executeUpdateFailed(tddlConnection, hint + sql2,
                "Do not support current ddl [LOCAL_PARTITION] with cci");
            JdbcUtil.executeUpdateFailed(tddlConnection, hint + sql3,
                "Do not support current ddl [LOCAL_PARTITION] with cci");
            JdbcUtil.executeUpdateFailed(tddlConnection, hint + sql4,
                "Do not support current ddl [ALTER_TABLE] with cci");
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            dropTableIfExists("t_order");
        }
    }

    private static final String SELECT_FROM_TABLE_DETAIL =
        "select storage_inst_id,table_group_name from information_schema.table_detail where table_schema='%s' and table_name='%s' and partition_name='%s'";

    @Test
    public void testMovePartition() throws SQLException {
        String hint = buildCmdExtra(FORBID_DDL_WITH_CCI, BACK_FILL);

        // RANGE
        String rangePartitionTable = "CREATE TABLE t_order_1 (\n"
            + "  `id` bigint(11) NOT NULL AUTO_INCREMENT,\n"
            + "  `order_id` varchar(20) DEFAULT NULL,\n"
            + "  `buyer_id` varchar(20) DEFAULT NULL,\n"
            + "  `seller_id` varchar(20) DEFAULT NULL,\n"
            + "  `order_snapshot` longtext DEFAULT NULL,\n"
            + "  `order_detail` longtext DEFAULT NULL,\n"
            + "  PRIMARY KEY (`id`),\n"
            + "  KEY `l_i_order` (`order_id`)\n"
            + ") PARTITION BY RANGE(id)\n"
            + " (PARTITION p1 VALUES LESS THAN(20),\n"
            + "  PARTITION p2 VALUES LESS THAN(100))";
        JdbcUtil.executeUpdateSuccess(tddlConnection, rangePartitionTable);
        String createCciTmpl =
            "ALTER TABLE t_order_1 ADD CLUSTERED clustered columnar index %s(`buyer_id`) PARTITION BY RANGE(`ID`)(PARTITION p1 VALUES LESS THAN(20),\n"
                + "  PARTITION p2 VALUES LESS THAN(100))";
        String sqlCreateCci1 = String.format(createCciTmpl, INDEX_NAME1);
        createCciSuccess(sqlCreateCci1);

        String curInstId;
        Set<String> instIds = new HashSet<>();
        String sql = String.format(SELECT_FROM_TABLE_DETAIL, tddlDatabase1, "t_order_1", "p1");

        ResultSet rs = JdbcUtil.executeQuery(sql, tddlConnection);
        if (rs.next()) {
            curInstId = rs.getString("STORAGE_INST_ID");
        } else {
            throw new RuntimeException(
                String.format("not find database table %s.%s", tddlDatabase1, "t_order_1"));
        }
        rs.close();

        sql = String.format("show ds where db='%s'", tddlDatabase1);
        rs = JdbcUtil.executeQuery(sql, tddlConnection);
        while (rs.next()) {
            if (!curInstId.equalsIgnoreCase(rs.getString("STORAGE_INST_ID"))) {
                instIds.add(rs.getString("STORAGE_INST_ID"));
            }
        }
        rs.close();

        if (!instIds.isEmpty()) {
            JdbcUtil.executeUpdateSuccess(tddlConnection,
                String.format("%s alter table t_order_1 move partitions p1 to '%s'", hint, instIds.iterator().next()));
        }
    }
}
