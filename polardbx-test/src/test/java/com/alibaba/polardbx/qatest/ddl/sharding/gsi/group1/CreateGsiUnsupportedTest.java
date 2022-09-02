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

package com.alibaba.polardbx.qatest.ddl.sharding.gsi.group1;

import com.alibaba.polardbx.qatest.DDLBaseNewDBTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.util.Litmus;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runners.Parameterized.Parameters;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static com.alibaba.polardbx.qatest.util.PropertiesUtil.isMySQL80;

/**
 * @author chenmo.cm
 */

public class CreateGsiUnsupportedTest extends DDLBaseNewDBTestCase {

    private static final String HINT =
        "/*+TDDL:CMD_EXTRA(STORAGE_CHECK_ON_GSI=false, ALLOW_ADD_GSI=true, GSI_DEFAULT_CURRENT_TIMESTAMP=false, GSI_ON_UPDATE_CURRENT_TIMESTAMP=false)*/";
    private static final String gsiTestTableName = "gsi_test_table";
    private static final String gsiTestUkName = "g_i_test_buyer";
    private static final Map<String, String> unsupportedColumns = new LinkedHashMap<>();
    private static final Map<String, String> supportedColumns = new LinkedHashMap<>();
    private static final String gsiTestTablePart1 = "CREATE TABLE `"
        + gsiTestTableName
        + "` (\n"
        + "\t`id` bigint(11) NOT NULL AUTO_INCREMENT BY GROUP,\n"
        + "\t`order_id` varchar(20) DEFAULT NULL,\n"
        + "\t`buyer_id` varchar(20) DEFAULT NULL,\n"
        + "\t`seller_id` varchar(20) DEFAULT NULL,\n"
        + "\t`order_snapshot` longtext,\n";
    private static final String gsiTestTablePart2 = "\t`gmt_create` timestamp DEFAULT \"2019-01-27 18:23:00\",\n"
        + "\tPRIMARY KEY (`id`)\n"
        + ") ENGINE = InnoDB CHARSET = utf8 dbpartition by hash(`order_id`) tbpartition by week(`gmt_create`) tbpartitions 2;\n";
    private static final String expectedGsiTestTablePart2 =
        "\t`gmt_create` timestamp NOT NULL DEFAULT \"2019-01-27 18:23:00\",\n"
            + "\tPRIMARY KEY (`id`),\n"
            + "\tUNIQUE GLOBAL "
            + gsiTestUkName
            + "(`buyer_id`) COVERING (`order_snapshot`, `gmt_modified`) DBPARTITION BY HASH (`buyer_id`)\n"
            + ") ENGINE = InnoDB CHARSET = utf8 dbpartition by hash(`order_id`) tbpartition by week(`gmt_create`) tbpartitions 2;\n";
    private static final String expectedGsiTestTablePart2_8 =
        "\t`gmt_create` timestamp NOT NULL DEFAULT \"2019-01-27 18:23:00\",\n"
            + "\tPRIMARY KEY (`id`),\n"
            + "\tUNIQUE GLOBAL " + gsiTestUkName
            + "(`buyer_id`) COVERING (`order_snapshot`, `gmt_modified`) DBPARTITION BY HASH (`buyer_id`)\n"
            + ") ENGINE = InnoDB CHARSET = utf8 dbpartition by hash(`order_id`) tbpartition by week(`gmt_create`) tbpartitions 2;\n";

    static {
        unsupportedColumns
            .put("\t`gmt_modified` timestamp not null default current_timestamp on update current_timestamp,\n",
                "Unsupported index table structure, cannot use DEFAULT CURRENT_TIMESTAMP on index or covering column.");

        unsupportedColumns.put(
            "\t`gmt_modified` timestamp(3) not null default current_timestamp(3) on update current_timestamp(3),\n",
            "Unsupported index table structure, cannot use DEFAULT CURRENT_TIMESTAMP(3) on index or covering column.");

        unsupportedColumns.put("\t`gmt_modified` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,\n",
            "Unsupported index table structure, cannot use DEFAULT CURRENT_TIMESTAMP on index or covering column.");

        unsupportedColumns.put("\t`gmt_modified` timestamp DEFAULT CURRENT_TIMESTAMP,\n",
            "Unsupported index table structure, cannot use DEFAULT CURRENT_TIMESTAMP on index or covering column.");

        unsupportedColumns.put("\t`gmt_modified` timestamp NULL ON UPDATE CURRENT_TIMESTAMP,\n",
            "Unsupported index table structure, cannot use ON UPDATE CURRENT_TIMESTAMP on index or covering column.");

        if (isMySQL80()) {
            supportedColumns.put("\t`gmt_modified` timestamp NULL,\n", "success");
        } else {
            unsupportedColumns.put("\t`gmt_modified` timestamp,\n",
                "Unsupported index table structure, cannot use DEFAULT CURRENT_TIMESTAMP on index or covering column.");

            supportedColumns.put("\t`gmt_modified` timestamp NOT NULL DEFAULT 0,\n", "success");
            supportedColumns.put("\t`gmt_modified` timestamp NOT NULL DEFAULT \"000-00-00 00:00:00\",\n", "success");
        }
        supportedColumns.put("\t`gmt_modified` timestamp NULL DEFAULT NULL,\n", "success");
        supportedColumns.put("\t`gmt_modified` timestamp NULL,\n", "success");

    }

    private final String columnDef;
    private final String errorMsg;
    private final String sqlCreateTable;
    private final String sqlExpectedCreateTable;
    private final String sqlCreateIndex;
    private final String sqlAlterTableAddIndex;

    public CreateGsiUnsupportedTest(String columnDef, String errorMsg) {
        this.columnDef = columnDef;
        this.errorMsg = errorMsg;
        this.sqlCreateTable = gsiTestTablePart1 + columnDef + gsiTestTablePart2;
        this.sqlExpectedCreateTable =
            gsiTestTablePart1 + columnDef + (isMySQL80() ? expectedGsiTestTablePart2_8 : expectedGsiTestTablePart2);
        this.sqlCreateIndex = "CREATE UNIQUE GLOBAL INDEX "
            + gsiTestUkName
            + " ON "
            + gsiTestTableName
            + "(`buyer_id`) COVERING (`order_snapshot`, `gmt_modified`) DBPARTITION BY HASH (`buyer_id`)\n";

        this.sqlAlterTableAddIndex = "ALTER TABLE "
            + gsiTestTableName
            + " ADD UNIQUE GLOBAL INDEX "
            + gsiTestUkName
            + "(`buyer_id`) COVERING (`order_snapshot`, `gmt_modified`) DBPARTITION BY HASH (`buyer_id`)\n";
    }

    @Parameters(name = "{index}:columnDef={0},errorMsg={1}")
    public static List<String[]> prepareDate() {
        List<String[]> result = new ArrayList<>();

        unsupportedColumns.forEach((columnDef, errorMsg) -> result.add(new String[] {columnDef, errorMsg}));
        supportedColumns.forEach((columnDef, errorMsg) -> result.add(new String[] {columnDef, errorMsg}));

        return result;
    }

    @Before
    public void init() {

        dropTableWithGsi(gsiTestTableName, ImmutableList.of(gsiTestUkName));

        // supportXA = supportXA(polarDbXConnection);
    }

    @After
    public void clean() {

        JdbcUtil.executeUpdateSuccess(tddlConnection, "DROP TABLE IF EXISTS " + gsiTestTableName);
    }

    @Test
    public void createIndex() {
        try {
            JdbcUtil.executeUpdateSuccess(tddlConnection, HINT + HINT_CREATE_GSI + sqlCreateTable);
            if (unsupportedColumns.containsKey(columnDef)) {
                JdbcUtil.executeUpdateFailed(tddlConnection, HINT + HINT_CREATE_GSI + sqlCreateIndex,
                    unsupportedColumns.get(columnDef));
                JdbcUtil.executeUpdateFailed(tddlConnection,
                    HINT + HINT_CREATE_GSI + sqlAlterTableAddIndex,
                    unsupportedColumns.get(columnDef));
            } else {
                // CREATE INDEX
                JdbcUtil.executeUpdateSuccess(tddlConnection, HINT + HINT_CREATE_GSI + sqlCreateIndex);

                final TableChecker createIndexChecker = getTableChecker(tddlConnection, gsiTestTableName);

                createIndexChecker.identicalTableDefinitionTo(sqlExpectedCreateTable, true, Litmus.THROW);

                JdbcUtil.executeUpdateSuccess(tddlConnection, "DROP TABLE IF EXISTS " + gsiTestTableName);
                JdbcUtil.executeUpdateSuccess(tddlConnection, HINT + HINT_CREATE_GSI + sqlCreateTable);

                // ALTER TABLE ADD INDEX
                JdbcUtil.executeUpdateSuccess(tddlConnection, HINT + HINT_CREATE_GSI + sqlAlterTableAddIndex);

                final TableChecker alterTableAddIndexCheck = getTableChecker(tddlConnection, gsiTestTableName);

                alterTableAddIndexCheck.identicalTableDefinitionTo(sqlExpectedCreateTable, true, Litmus.THROW);
            }
        } catch (Exception e) {
            throw new RuntimeException("CREATE INDEX statement execution failed!", e);
        }
    }
}
