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

package com.alibaba.polardbx.qatest.ddl.sharding.gsi.group2;

import com.alibaba.polardbx.qatest.DDLBaseNewDBTestCase;
import com.alibaba.polardbx.qatest.data.ExecuteTableSelect;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.util.Litmus;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runners.Parameterized.Parameters;

import java.util.List;

import static com.alibaba.polardbx.qatest.data.ExecuteTableSelect.GSI_PRIMARY_TABLE_NAME;
import static com.alibaba.polardbx.qatest.util.PropertiesUtil.isMySQL80;

/**
 * @author chenmo.cm
 */

public class CreateGsiFullTest extends DDLBaseNewDBTestCase {

    private static final String CREATE_TABLE_BASE = "CREATE TABLE IF NOT EXISTS `"
        + GSI_PRIMARY_TABLE_NAME
        + "` (\n"
        + "\t`pk` int(11) NOT NULL AUTO_INCREMENT BY GROUP,\n"
        + "\t`c2` int(20) DEFAULT NULL,\n"
        + "\t`tint` tinyint(10) UNSIGNED ZEROFILL DEFAULT NULL,\n"
        + "\t`sint` smallint(6) DEFAULT '1000',\n"
        + "\t`mint` mediumint(9) DEFAULT NULL,\n"
        + "\t`bint` bigint(20) DEFAULT NULL COMMENT ' bigint',\n"
        + "\t`dble` double(10, 2) DEFAULT NULL,\n"
        + "\t`fl` float(10, 2) DEFAULT NULL,\n"
        + "\t`dc` decimal(10, 2) DEFAULT NULL,\n"
        + "\t`num` decimal(10, 2) DEFAULT NULL,\n"
        + "\t`dt` date DEFAULT NULL,\n"
        + "\t`ti` time DEFAULT NULL,\n"
        + "\t`tis` timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),\n"
        + "\t`ts` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,\n"
        + "\t`dti` datetime DEFAULT NULL,\n"
        + "\t`vc` varchar(100) CHARACTER SET utf8 COLLATE utf8_bin DEFAULT NULL,\n"
        + "\t`vc2` varchar(100) CHARACTER SET utf8 COLLATE utf8_bin NOT NULL,\n"
        + "\t`tb` tinyblob,\n" + "\t`bl` blob,\n"
        + "\t`mb` mediumblob,\n" + "\t`lb` longblob,\n"
        + "\t`tt` tinytext CHARACTER SET utf8 COLLATE utf8_bin,\n"
        + "\t`mt` mediumtext CHARACTER SET utf8 COLLATE utf8_bin,\n"
        + "\t`lt` longtext CHARACTER SET utf8 COLLATE utf8_bin,\n"
        + "\t`en` enum('1', '2') CHARACTER SET utf8 COLLATE utf8_bin NOT NULL,\n"
        + "\t`st` set('5', '6') CHARACTER SET utf8 COLLATE utf8_bin DEFAULT NULL,\n"
        + "\t`id1` int(11) DEFAULT NULL,\n"
        + "\t`id2` int(11) DEFAULT NULL,\n"
        + "\t`id3` varchar(100) CHARACTER SET utf8mb4 DEFAULT NULL,\n"
        + "\t`vc1` varchar(100) CHARACTER SET utf8 COLLATE utf8_bin DEFAULT NULL,\n"
        + "\t`vc3` varchar(100) CHARACTER SET utf8 COLLATE utf8_bin DEFAULT NULL,\n"
        + "\tPRIMARY KEY (`pk`),\n"
        + "\tUNIQUE `idx3` USING BTREE (`vc1`(20)),\n"
        + "\tKEY `idx1` USING HASH (`id1`),\n"
        + "\tKEY `idx2` USING HASH (`id2`),\n"
        + "\tFULLTEXT KEY `idx4` (`id3`)";
    private static final String CREATE_TABLE_TAIL_DB =
        "\n\t) ENGINE = INNODB auto_increment = 2 avg_row_length = 100 DEFAULT CHARACTER SET utf8 COLLATE utf8_bin "
            + "CHECKSUM = 0 COMMENT = \"abcd\" dbpartition BY HASH ( id1 );\n";

    private static final String CREATE_TABLE_TAIL_TB =
        "\n\t) ENGINE = INNODB auto_increment = 2 avg_row_length = 100 DEFAULT CHARACTER SET utf8 COLLATE utf8_bin "
            + "CHECKSUM = 0 COMMENT = \"abcd\" dbpartition BY HASH ( id1 ) tbpartition BY HASH (id1) tbpartitions 3;\n";

    private static final String CREATE_PRIMARY_TABLE = HINT_CREATE_GSI + CREATE_TABLE_BASE + CREATE_TABLE_TAIL_TB;

    private static final String CREATE_TABLE_BASE_8 = "CREATE TABLE IF NOT EXISTS `" + GSI_PRIMARY_TABLE_NAME
        + "` (\n" + "\t`pk` int(11) NOT NULL AUTO_INCREMENT BY GROUP,\n"
        + "\t`c2` int(20) DEFAULT NULL,\n"
        + "\t`tint` tinyint(10) UNSIGNED ZEROFILL DEFAULT NULL,\n"
        + "\t`sint` smallint(6) DEFAULT '1000',\n"
        + "\t`mint` mediumint(9) DEFAULT NULL,\n"
        + "\t`bint` bigint(20) DEFAULT NULL COMMENT ' bigint',\n"
        + "\t`dble` double(10, 2) DEFAULT NULL,\n"
        + "\t`fl` float(10, 2) DEFAULT NULL,\n"
        + "\t`dc` decimal(10, 2) DEFAULT NULL,\n"
        + "\t`num` decimal(10, 2) DEFAULT NULL,\n"
        + "\t`dt` date DEFAULT NULL,\n" + "\t`ti` time DEFAULT NULL,\n"
        + "\t`tis` timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),\n"
        + "\t`ts` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,\n"
        + "\t`dti` datetime DEFAULT NULL,\n"
        + "\t`vc` varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin DEFAULT NULL,\n"
        + "\t`vc2` varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,\n"
        + "\t`tb` tinyblob,\n" + "\t`bl` blob,\n"
        + "\t`mb` mediumblob,\n" + "\t`lb` longblob,\n"
        + "\t`tt` tinytext CHARACTER SET utf8mb4 COLLATE utf8mb4_bin,\n"
        + "\t`mt` mediumtext CHARACTER SET utf8mb4 COLLATE utf8mb4_bin,\n"
        + "\t`lt` longtext CHARACTER SET utf8mb4 COLLATE utf8mb4_bin,\n"
        + "\t`en` enum('1', '2') CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,\n"
        + "\t`st` set('5', '6') CHARACTER SET utf8mb4 COLLATE utf8mb4_bin DEFAULT NULL,\n"
        + "\t`id1` int(11) DEFAULT NULL,\n"
        + "\t`id2` int(11) DEFAULT NULL,\n"
        + "\t`id3` varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci DEFAULT NULL,\n"
        + "\t`vc1` varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin DEFAULT NULL,\n"
        + "\t`vc3` varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin DEFAULT NULL,\n"
        + "\tPRIMARY KEY (`pk`),\n"
        + "\tUNIQUE `idx3` USING BTREE (`vc1`(20)),\n"
        + "\tKEY `idx1` USING BTREE (`id1`),\n"
        + "\tKEY `idx2` USING BTREE (`id2`),\n"
        + "\tFULLTEXT KEY `idx4` (`id3`)";
    private static final String CREATE_TABLE_TAIL_DB_8 =
        "\n\t) ENGINE = INNODB auto_increment = 2 avg_row_length = 100 DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_bin "
            + "CHECKSUM = 0 COMMENT = \"abcd\" dbpartition BY HASH ( id1 );\n";

    private static final String CREATE_TABLE_TAIL_TB_8 =
        "\n\t) ENGINE = INNODB auto_increment = 2 avg_row_length = 100 DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_bin "
            + "CHECKSUM = 0 COMMENT = \"abcd\" dbpartition BY HASH ( id1 ) tbpartition BY HASH (id1) tbpartitions 3;\n";

    private static final String CREATE_PRIMARY_TABLE_8 = HINT_CREATE_GSI + CREATE_TABLE_BASE_8 + CREATE_TABLE_TAIL_TB_8;

    private final String sqlCreateTable;
    private final String sqlCreateIndex;
    private final String sqlAlterCreateIndex;

    public CreateGsiFullTest(String createGsiDef, String alterCreateGsiDef) {
        sqlCreateTable = buildCreateTableSql(alterCreateGsiDef).toString();
        sqlCreateIndex = buildCreateIndexSql(createGsiDef).toString();
        sqlAlterCreateIndex = buildAlterCreateIndexSql(alterCreateGsiDef).toString();
    }

    private static StringBuilder buildCreateTableSql(String gsiDef) {
        final StringBuilder sqlBuilder = new StringBuilder(isMySQL80() ? CREATE_TABLE_BASE_8 : CREATE_TABLE_BASE);

        sqlBuilder.append(",\n\t").append(gsiDef);

        sqlBuilder.append(isMySQL80() ? CREATE_TABLE_TAIL_TB_8 : CREATE_TABLE_TAIL_TB);
        return sqlBuilder;
    }

    private static StringBuilder buildCreateIndexSql(String gsiDef) {
        final StringBuilder sqlBuilder = new StringBuilder("CREATE ");

        sqlBuilder.append(gsiDef);

        return sqlBuilder;
    }

    private static StringBuilder buildAlterCreateIndexSql(String alterCreateGsiDef) {
        final StringBuilder sqlBuilder = new StringBuilder("ALTER TABLE ");

        sqlBuilder.append(GSI_PRIMARY_TABLE_NAME).append(" ADD ").append(alterCreateGsiDef);

        return sqlBuilder;
    }

    @Parameters(name = "{index}:createGsiDef={0}, alterCreateGsiDef={1}")
    public static List<String[]> prepareDate() {
        return ExecuteTableSelect.supportedGsiDefCreateAndAlter(GSI_PRIMARY_TABLE_NAME);
    }

    @Before
    public void init() {

        dropTableWithGsi(GSI_PRIMARY_TABLE_NAME, ImmutableList.of("gsi_id2"));
        JdbcUtil.executeUpdateSuccess(tddlConnection, isMySQL80() ? CREATE_PRIMARY_TABLE_8 : CREATE_PRIMARY_TABLE);

        // supportXA = supportXA(polarDbXConnection);
    }

    @After
    public void clean() {

        dropTableWithGsi(GSI_PRIMARY_TABLE_NAME, ImmutableList.of("gsi_id2"));
    }

    @Test
    public void createIndex() {
        JdbcUtil.executeUpdateSuccess(tddlConnection, HINT_CREATE_GSI + sqlCreateIndex);

        // CREATE GLOBAL INDEX
        final TableChecker tableChecker = getTableChecker(tddlConnection, GSI_PRIMARY_TABLE_NAME);
        tableChecker.identicalTableDefinitionTo(sqlCreateTable, true, Litmus.THROW);

        JdbcUtil.executeUpdateSuccess(tddlConnection,
            HINT_CREATE_GSI + "alter table " + GSI_PRIMARY_TABLE_NAME + " drop index gsi_id2");

        // ALTER TABLE ADD GLOBAL INDEX
        JdbcUtil.executeUpdateSuccess(tddlConnection, HINT_CREATE_GSI + sqlAlterCreateIndex);

        final TableChecker alterTableChecker = getTableChecker(tddlConnection, GSI_PRIMARY_TABLE_NAME);
        alterTableChecker.identicalTableDefinitionTo(sqlCreateTable, true, Litmus.THROW);

        // SHOW INDEX
        final ShowIndexChecker showIndexChecker = getShowIndexGsiChecker(tddlConnection, GSI_PRIMARY_TABLE_NAME);
        showIndexChecker.identicalToTableDefinition(sqlCreateTable, true, Litmus.THROW);
    }
}
