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

package com.alibaba.polardbx.qatest.ddl.sharding.gsi.group3;

import com.alibaba.polardbx.qatest.DDLBaseNewDBTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.google.common.collect.ImmutableList;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * @version 1.0
 */
public class UgsiTest extends DDLBaseNewDBTestCase {

    private static final String UGSI_TABLE_NAME = "ugsi_test";
    private static final String UGSI_INDEX_NAME = "ug_i_test";

    @Before
    public void before() {
        dropTableWithGsi(UGSI_TABLE_NAME, ImmutableList.of(UGSI_INDEX_NAME));
    }

    @After
    public void after() {
        dropTableWithGsi(UGSI_TABLE_NAME, ImmutableList.of(UGSI_INDEX_NAME));
    }

    @Test
    public void ugsiDuplicateTest() throws Exception {
        final String createTable = "CREATE TABLE IF NOT EXISTS `" + UGSI_TABLE_NAME + "` (\n"
            + "  `id` bigint(11) NOT NULL DEFAULT '1',\n"
            + "  `c1` bigint(20) NOT NULL DEFAULT '2',\n"
            + "  `c2` bigint(20) DEFAULT NULL,\n"
            + "  PRIMARY KEY(`id`),\n"
            + "  UNIQUE GLOBAL INDEX " + UGSI_INDEX_NAME
            + "(`c2`) COVERING(`c5`) DBPARTITION BY HASH(`c2`) TBPARTITION BY HASH(`c2`) TBPARTITIONS 3\n"
            + ") ENGINE=InnoDB DEFAULT CHARSET=utf8";
        final String partitionDef = " dbpartition by hash(`c1`) tbpartition by hash(`c1`) tbpartitions 7";

        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable + partitionDef);

        String insert = "insert into " + UGSI_TABLE_NAME + "(id, c1, c2) values (1, 1, 1), (7, 1, 7), (8, 1, 8)";
        JdbcUtil.executeUpdateSuccess(tddlConnection, insert);

        // Duplicate on primary.
        insert = "insert into " + UGSI_TABLE_NAME + "(id, c1, c2) values (1, 1, 2)";
        JdbcUtil.executeUpdateFailed(tddlConnection, insert, "duplicate entry '1' for key 'primary'");

        // Duplicate on UGSI.
        insert = "insert into " + UGSI_TABLE_NAME + "(id, c1, c2) values (2, 2, 1)";
        JdbcUtil.executeUpdateFailed(tddlConnection, insert, "duplicate entry '1' for key ugsi 'ug_i_test'");

        // Duplicate on UGSI multi values.
        insert = "insert into " + UGSI_TABLE_NAME + "(id, c1, c2) values (2, 2, 1), (3, 3, 2), (4, 4, 1)";
        JdbcUtil.executeUpdateFailed(tddlConnection, insert, "duplicate entry '1' for key ugsi 'ug_i_test'");

        // Duplicate on UGSI when update.
        String update = "update " + UGSI_TABLE_NAME + " set c2 = 1 where id = 7";
        JdbcUtil.executeUpdateFailed(tddlConnection, update, "duplicate entry '1' for key ugsi 'ug_i_test'");

        // Duplicate on UGSI when multi update.
        update = "update " + UGSI_TABLE_NAME + " set c2 = 1 where c1 = 1";
        JdbcUtil.executeUpdateFailed(tddlConnection, update, "duplicate entry '1' for key ugsi 'ug_i_test'");
    }

    @Test
    public void ugsiDuplicateCompositeKeyTest() throws Exception {
        final String createTable = "CREATE TABLE IF NOT EXISTS `" + UGSI_TABLE_NAME + "` (\n"
            + "  `id` bigint(11) NOT NULL DEFAULT '1',\n"
            + "  `c1` bigint(20) NOT NULL DEFAULT '2',\n"
            + "  `c2` bigint(20) DEFAULT NULL,\n"
            + "  `c3` bigint(20) DEFAULT NULL,\n"
            + "  PRIMARY KEY(`id`),\n"
            + "  UNIQUE GLOBAL INDEX " + UGSI_INDEX_NAME
            + "(`c2`, `c3`) DBPARTITION BY HASH(`c2`) TBPARTITION BY HASH(`c2`) TBPARTITIONS 3\n"
            + ") ENGINE=InnoDB DEFAULT CHARSET=utf8";
        final String partitionDef = " dbpartition by hash(`c1`) tbpartition by hash(`c1`) tbpartitions 7";

        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable + partitionDef);

        String insert = "insert into " + UGSI_TABLE_NAME
            + "(id, c1, c2, c3) values (1, 1, 1, 1), (7, 1, 7, 1), (8, 1, 8, 1), (9, 2, 1, 9), (10, 2, 1, 10)";
        JdbcUtil.executeUpdateSuccess(tddlConnection, insert);

        // Duplicate on primary.
        insert = "insert into " + UGSI_TABLE_NAME + "(id, c1, c2, c3) values (1, 1, 2, 2)";
        JdbcUtil.executeUpdateFailed(tddlConnection, insert, "duplicate entry '1' for key 'primary'");

        // Duplicate on UGSI.
        insert = "insert into " + UGSI_TABLE_NAME + "(id, c1, c2, c3) values (2, 2, 1, 1)";
        JdbcUtil.executeUpdateFailed(tddlConnection, insert, "duplicate entry '1-1' for key ugsi 'ug_i_test'");

        // Duplicate on UGSI multi values.
        insert = "insert into " + UGSI_TABLE_NAME + "(id, c1, c2, c3) values (2, 2, 1, 1), (3, 3, 2, 2), (4, 4, 1, 1)";
        JdbcUtil.executeUpdateFailed(tddlConnection, insert, "duplicate entry '1-1' for key ugsi 'ug_i_test'");

        // Duplicate on UGSI when update.
        String update = "update " + UGSI_TABLE_NAME + " set c2 = 1 where id = 7";
        JdbcUtil.executeUpdateFailed(tddlConnection, update, "duplicate entry '1-1' for key ugsi 'ug_i_test'");

        // Duplicate on UGSI when multi update.
        update = "update " + UGSI_TABLE_NAME + " set c2 = 1 where c1 = 1";
        JdbcUtil.executeUpdateFailed(tddlConnection, update, "duplicate entry '1-1' for key ugsi 'ug_i_test'");

        // Duplicate on UGSI non partition key when update.
        update = "update " + UGSI_TABLE_NAME + " set c3 = 1 where id = 9";
        JdbcUtil.executeUpdateFailed(tddlConnection, update, "duplicate entry '1-1' for key ugsi 'ug_i_test'");

        // Duplicate on UGSI non partition key when multi update.
        update = "update " + UGSI_TABLE_NAME + " set c3 = 1 where c1 = 2";
        JdbcUtil.executeUpdateFailed(tddlConnection, update, "duplicate entry '1-1' for key ugsi 'ug_i_test'");
    }

}
