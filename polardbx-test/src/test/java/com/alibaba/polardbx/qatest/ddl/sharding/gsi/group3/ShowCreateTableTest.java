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

import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.qatest.DDLBaseNewDBTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.List;

import static com.alibaba.polardbx.qatest.util.PropertiesUtil.isMySQL80;

/**
 * @version 1.0
 */

public class ShowCreateTableTest extends DDLBaseNewDBTestCase {

    private String testTable = "showCreateTableTest";
    private String dataType = null;

    private static Log log = LogFactory.getLog(ShowCreateTableTest.class);

    @Parameterized.Parameters(name = "{index}:sql={0}")
    public static List<String[]> prepare() {
        String[][] type = {
            {"datetime DEFAULT CURRENT_TIMESTAMP"}, {"timestamp DEFAULT CURRENT_TIMESTAMP"},
            {"timestamp DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP"},
            {"datetime DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP"}, {"timestamp DEFAULT 0"},
            {"datetime DEFAULT 0"}, {"timestamp DEFAULT 0 ON UPDATE CURRENT_TIMESTAMP"},
            {"datetime DEFAULT 0 ON UPDATE CURRENT_TIMESTAMP"}, {"timestamp NULL ON UPDATE CURRENT_TIMESTAMP"},
            {"datetime NULL ON UPDATE CURRENT_TIMESTAMP"}, {"timestamp ON UPDATE CURRENT_TIMESTAMP"},
            {"datetime ON UPDATE CURRENT_TIMESTAMP"}, {"timestamp NOT NULL ON UPDATE CURRENT_TIMESTAMP"},
            {"datetime NOT NULL ON UPDATE CURRENT_TIMESTAMP"}, {"timestamp DEFAULT '2000-01-01 00:00:00'"},
            {"datetime DEFAULT '2000-01-01 00:00:00'"}
        };
        String[][] type_8 = {
            {"datetime DEFAULT CURRENT_TIMESTAMP"}, {"timestamp DEFAULT CURRENT_TIMESTAMP"},
            {"timestamp DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP"},
            {"datetime DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP"},
            // { "timestamp DEFAULT 0" },
            // { "datetime DEFAULT 0" },
            // { "timestamp DEFAULT 0 ON UPDATE CURRENT_TIMESTAMP" },
            // { "datetime DEFAULT 0 ON UPDATE CURRENT_TIMESTAMP" },
            {"timestamp NULL ON UPDATE CURRENT_TIMESTAMP"},
            {"datetime NULL ON UPDATE CURRENT_TIMESTAMP"}, {"timestamp ON UPDATE CURRENT_TIMESTAMP"},
            {"datetime ON UPDATE CURRENT_TIMESTAMP"}, {"timestamp NOT NULL ON UPDATE CURRENT_TIMESTAMP"},
            {"datetime NOT NULL ON UPDATE CURRENT_TIMESTAMP"}, {"timestamp DEFAULT '2000-01-01 00:00:00'"},
            {"datetime DEFAULT '2000-01-01 00:00:00'"}};

        return Arrays.asList(isMySQL80() ? type_8 : type);
    }

    public ShowCreateTableTest(String type) {
        this.dataType = type;
    }

    @Test
    public void testAutoIncrement() {
        for (String schema : new String[] {tddlDatabase1}) {
            if (!TStringUtil.equalsIgnoreCase(dataType, "datetime DEFAULT CURRENT_TIMESTAMP")
                || TStringUtil.isNotEmpty(schema)) {
                return;
            }
            String schemaPrefix = StringUtils.isBlank(schema) ? "" : schema + ".";
            String simplePrimaryTableName = testTable + "_2";
            String primaryTableName = schemaPrefix + simplePrimaryTableName;
            String simpleGsiTableName = testTable + "_2_gsi";
            String gsiTableName = schemaPrefix + simpleGsiTableName;

            dropTableIfExists(primaryTableName);

            String primarySql = "create table " + primaryTableName + "(`id` int not null auto_increment by simple, "
                + "`name` varchar(10), primary key(`id`)) dbpartition by hash(`id`)";
            log.info("Original Primary SQL: " + primarySql);

            String gsiSql = "create global index " + gsiTableName + " on " + primaryTableName
                + "(`name`,`id`) dbpartition by hash(`name`)";
            log.info("Original GSI SQL: " + gsiSql);

            JdbcUtil.executeUpdateSuccess(tddlConnection, primarySql);
            JdbcUtil.executeUpdateSuccess(tddlConnection, gsiSql);

            String showCreateTableString = showCreateTable(tddlConnection, primaryTableName.toLowerCase());
            log.info("show create table: " + showCreateTableString);

            Assert.assertTrue(showCreateTableString.toLowerCase().contains("auto_increment by simple"));

            dropTableIfExists(primaryTableName);
        }
    }

}
