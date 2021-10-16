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

package com.alibaba.polardbx.druid.bvt.sql.mysql.schema;

import com.alibaba.polardbx.druid.sql.repository.SchemaObject;
import com.alibaba.polardbx.druid.sql.repository.SchemaRepository;
import com.alibaba.polardbx.druid.util.JdbcConstants;
import junit.framework.TestCase;

/**
 * @version 1.0
 * @ClassName Test_1_for_rds2histore
 * @description
 * @Author zzy
 * @Date 2019-05-13 16:11
 */
public class Test_1_for_rds2histore extends TestCase {

    public void test_0() throws Exception {
        String sql = "create table `test_all` (\n"
                + "  `id` bigint(20) NOT NULL AUTO_INCREMENT,\n"
                + "`id2` bigint(20) NOT NULL AUTO_INCREMENT,\n"
                + "`c_bit_1` bit(1) DEFAULT NULL,\n"
                + "`c_bit_8` bit(8) DEFAULT NULL,\n"
                + "`c_bit_16` bit(16) DEFAULT NULL,\n"
                + "`field1` text COLLATE utf8_unicode_ci NOT NULL COMMENT '字段1' ,\n"
                + "`c_bit_32` bit(32) DEFAULT NULL,  \n"
                + "  PRIMARY KEY (`id`),\n"
                + "UNIQUE KEY `uk_bit`(`c_bit_32`),\n"
                + "  KEY `name` (`name`),\n"
                + "CONSTRAINT `fk_id` FOREIGN KEY (id2) REFERENCES Persons(id) \n"
                + ") ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4 COMMENT='testaaa' ";

        SchemaRepository repository = new SchemaRepository(JdbcConstants.MYSQL);
        repository.console(sql);

        String drop = "alter table `test_all` drop foreign key `fk_id`";
        repository.console(drop);

        assertEquals("CREATE TABLE `test_all` (\n" +
                        "\t`id` bigint(20) NOT NULL AUTO_INCREMENT,\n" +
                        "\t`id2` bigint(20) NOT NULL AUTO_INCREMENT,\n" +
                        "\t`c_bit_1` bit(1) DEFAULT NULL,\n" +
                        "\t`c_bit_8` bit(8) DEFAULT NULL,\n" +
                        "\t`c_bit_16` bit(16) DEFAULT NULL,\n" +
                        "\t`field1` text COLLATE utf8_unicode_ci NOT NULL COMMENT '字段1',\n" +
                        "\t`c_bit_32` bit(32) DEFAULT NULL,\n" +
                        "\tPRIMARY KEY (`id`),\n" +
                        "\tUNIQUE KEY `uk_bit` (`c_bit_32`),\n" +
                        "\tKEY `name` (`name`)\n" +
                        ") ENGINE = InnoDB AUTO_INCREMENT = 1 DEFAULT CHARSET = utf8mb4 COMMENT 'testaaa'",
                repository.findTable("test_all").getStatement().toString());

    }

    /**
     * Test create table xx select xx form xx.
     */
    public void test_1() throws Exception {
        SchemaRepository repository = new SchemaRepository(JdbcConstants.MYSQL);

        String sql = "CREATE TABLE `tb` (\n" +
                "  `a` char(4) CHARACTER SET utf8 COLLATE utf8_unicode_ci DEFAULT '',\n" +
                "  `b` varchar(12) CHARACTER SET utf8 NOT NULL DEFAULT '',\n" +
                "  UNIQUE INDEX `idx` (`a`(3),`b`(12))\n" +
                ") ENGINE=InnoDB DEFAULT CHARSET=utf8";
        // System.out.println(sql);
        repository.console(sql);

        sql = "create TABLE `tbx` select b as xx, a as yy from tb;";
        repository.console(sql);

        SchemaObject table = repository.findTable("tbx");
        // System.out.println(table.getStatement().toString());

        assertEquals("CREATE TABLE `tbx` (\n" +
                        "\txx varchar(12) CHARACTER SET utf8 NOT NULL DEFAULT '',\n" +
                        "\tyy char(4) CHARACTER SET utf8 COLLATE utf8_unicode_ci DEFAULT ''\n" +
                        ")",
                table.getStatement().toString());
    }

    /**
     * Test change column in PK, UK & IDX. Extend data length.
     */
    public void test_2() throws Exception {
        SchemaRepository repository = new SchemaRepository(JdbcConstants.MYSQL);

        String sql = "CREATE TABLE `tb` (\n" +
                "  `a` char(4) CHARACTER SET utf8 COLLATE utf8_unicode_ci DEFAULT '',\n" +
                "  `b` varchar(12) CHARACTER SET utf8 NOT NULL DEFAULT '',\n" +
                "  PRIMARY KEY (`b`),\n" +
                "  UNIQUE INDEX `uk` (`a`(3),`b`(10)),\n" +
                "  KEY `idx` (`a`(3),`b`(10))\n" +
                ") ENGINE=InnoDB DEFAULT CHARSET=utf8";
        // System.out.println(sql);
        repository.console(sql);

        sql = "alter table tb change `b` `c` varchar(64) first;";
        repository.console(sql);

        SchemaObject table = repository.findTable("tb");
        // System.out.println(table.getStatement().toString());

        assertEquals("CREATE TABLE `tb` (\n" +
                        "\t`c` varchar(64),\n" +
                        "\t`a` char(4) CHARACTER SET utf8 COLLATE utf8_unicode_ci DEFAULT '',\n" +
                        "\tPRIMARY KEY (`c`),\n" +
                        "\tUNIQUE INDEX `uk` (`a`(3), `c`(10)),\n" +
                        "\tKEY `idx` (`a`(3), `c`(10))\n" +
                        ") ENGINE = InnoDB DEFAULT CHARSET = utf8",
                table.getStatement().toString());
    }

    /**
     * Test change column in PK, UK & IDX. Truncate data length.
     */
    public void test_3() throws Exception {
        SchemaRepository repository = new SchemaRepository(JdbcConstants.MYSQL);

        String sql = "CREATE TABLE `tb` (\n" +
                "  `a` char(4) CHARACTER SET utf8 COLLATE utf8_unicode_ci DEFAULT '',\n" +
                "  `b` varbinary(12) CHARACTER SET utf8 NOT NULL DEFAULT '',\n" +
                "  PRIMARY KEY (`b`),\n" +
                "  UNIQUE INDEX `uk` (`a`(3),`b`(10)),\n" +
                "  KEY `idx` (`a`(3),`b`(10))\n" +
                ") ENGINE=InnoDB DEFAULT CHARSET=utf8";
        // System.out.println(sql);
        repository.console(sql);

        sql = "alter table tb change `b` `c` varbinary(8) first;";
        repository.console(sql);

        SchemaObject table = repository.findTable("tb");
        // System.out.println(table.getStatement().toString());

        assertEquals("CREATE TABLE `tb` (\n" +
                        "\t`c` varbinary(8),\n" +
                        "\t`a` char(4) CHARACTER SET utf8 COLLATE utf8_unicode_ci DEFAULT '',\n" +
                        "\tPRIMARY KEY (`c`),\n" +
                        "\tUNIQUE INDEX `uk` (`a`(3), `c`),\n" +
                        "\tKEY `idx` (`a`(3), `c`)\n" +
                        ") ENGINE = InnoDB DEFAULT CHARSET = utf8",
                table.getStatement().toString());
    }

    /**
     * Test change column in PK, UK & IDX. To no length.
     */
    public void test_4() throws Exception {
        SchemaRepository repository = new SchemaRepository(JdbcConstants.MYSQL);

        String sql = "CREATE TABLE `tb` (\n" +
                "  `a` char(4) CHARACTER SET utf8 COLLATE utf8_unicode_ci DEFAULT '',\n" +
                "  `b` varchar(12) CHARACTER SET utf8 NOT NULL DEFAULT '',\n" +
                "  PRIMARY KEY (`b`),\n" +
                "  UNIQUE INDEX `uk` (`a`(3),`b`(10)),\n" +
                "  KEY `idx` (`a`(3),`b`(10))\n" +
                ") ENGINE=InnoDB DEFAULT CHARSET=utf8";
        // System.out.println(sql);
        repository.console(sql);

        sql = "alter table tb change `b` `c` int first;";
        repository.console(sql);

        SchemaObject table = repository.findTable("tb");
        // System.out.println(table.getStatement().toString());

        assertEquals("CREATE TABLE `tb` (\n" +
                        "\t`c` int,\n" +
                        "\t`a` char(4) CHARACTER SET utf8 COLLATE utf8_unicode_ci DEFAULT '',\n" +
                        "\tPRIMARY KEY (`c`),\n" +
                        "\tUNIQUE INDEX `uk` (`a`(3), `c`),\n" +
                        "\tKEY `idx` (`a`(3), `c`)\n" +
                        ") ENGINE = InnoDB DEFAULT CHARSET = utf8",
                table.getStatement().toString());
    }


    /**
     * Test modify column in PK, UK & IDX. Extend data length.
     */
    public void test_5() throws Exception {
        SchemaRepository repository = new SchemaRepository(JdbcConstants.MYSQL);

        String sql = "CREATE TABLE `tb` (\n" +
                "  `a` char(4) CHARACTER SET utf8 COLLATE utf8_unicode_ci DEFAULT '',\n" +
                "  `b` varchar(12) CHARACTER SET utf8 NOT NULL DEFAULT '',\n" +
                "  PRIMARY KEY (`b`),\n" +
                "  UNIQUE INDEX `uk` (`a`(3),`b`(10)),\n" +
                "  KEY `idx` (`a`(3),`b`(10))\n" +
                ") ENGINE=InnoDB DEFAULT CHARSET=utf8";
        // System.out.println(sql);
        repository.console(sql);

        sql = "alter table tb modify `b` varchar(64) first;";
        repository.console(sql);

        SchemaObject table = repository.findTable("tb");
        // System.out.println(table.getStatement().toString());

        assertEquals("CREATE TABLE `tb` (\n" +
                        "\t`b` varchar(64),\n" +
                        "\t`a` char(4) CHARACTER SET utf8 COLLATE utf8_unicode_ci DEFAULT '',\n" +
                        "\tPRIMARY KEY (`b`),\n" +
                        "\tUNIQUE INDEX `uk` (`a`(3), `b`(10)),\n" +
                        "\tKEY `idx` (`a`(3), `b`(10))\n" +
                        ") ENGINE = InnoDB DEFAULT CHARSET = utf8",
                table.getStatement().toString());
    }

    /**
     * Test modify column in PK, UK & IDX. Truncate data length.
     */
    public void test_6() throws Exception {
        SchemaRepository repository = new SchemaRepository(JdbcConstants.MYSQL);

        String sql = "CREATE TABLE `tb` (\n" +
                "  `a` char(4) CHARACTER SET utf8 COLLATE utf8_unicode_ci DEFAULT '',\n" +
                "  `b` varbinary(12) CHARACTER SET utf8 NOT NULL DEFAULT '',\n" +
                "  PRIMARY KEY (`b`),\n" +
                "  UNIQUE INDEX `uk` (`a`(3),`b`(10)),\n" +
                "  KEY `idx` (`a`(3),`b`(10))\n" +
                ") ENGINE=InnoDB DEFAULT CHARSET=utf8";
        // System.out.println(sql);
        repository.console(sql);

        sql = "alter table tb modify `b` varbinary(8) first;";
        repository.console(sql);

        SchemaObject table = repository.findTable("tb");
        // System.out.println(table.getStatement().toString());

        assertEquals("CREATE TABLE `tb` (\n" +
                        "\t`b` varbinary(8),\n" +
                        "\t`a` char(4) CHARACTER SET utf8 COLLATE utf8_unicode_ci DEFAULT '',\n" +
                        "\tPRIMARY KEY (`b`),\n" +
                        "\tUNIQUE INDEX `uk` (`a`(3), `b`),\n" +
                        "\tKEY `idx` (`a`(3), `b`)\n" +
                        ") ENGINE = InnoDB DEFAULT CHARSET = utf8",
                table.getStatement().toString());
    }

    /**
     * Test modify column in PK, UK & IDX. To no length.
     */
    public void test_7() throws Exception {
        SchemaRepository repository = new SchemaRepository(JdbcConstants.MYSQL);

        String sql = "CREATE TABLE `tb` (\n" +
                "  `a` char(4) CHARACTER SET utf8 COLLATE utf8_unicode_ci DEFAULT '',\n" +
                "  `b` varchar(12) CHARACTER SET utf8 NOT NULL DEFAULT '',\n" +
                "  PRIMARY KEY (`b`),\n" +
                "  UNIQUE INDEX `uk` (`a`(3),`b`(10)),\n" +
                "  KEY `idx` (`a`(3),`b`(10))\n" +
                ") ENGINE=InnoDB DEFAULT CHARSET=utf8";
        // System.out.println(sql);
        repository.console(sql);

        sql = "alter table tb modify `b` int first;";
        repository.console(sql);

        SchemaObject table = repository.findTable("tb");
        // System.out.println(table.getStatement().toString());

        assertEquals("CREATE TABLE `tb` (\n" +
                        "\t`b` int,\n" +
                        "\t`a` char(4) CHARACTER SET utf8 COLLATE utf8_unicode_ci DEFAULT '',\n" +
                        "\tPRIMARY KEY (`b`),\n" +
                        "\tUNIQUE INDEX `uk` (`a`(3), `b`),\n" +
                        "\tKEY `idx` (`a`(3), `b`)\n" +
                        ") ENGINE = InnoDB DEFAULT CHARSET = utf8",
                table.getStatement().toString());
    }
}
