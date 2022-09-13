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
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.google.common.collect.ImmutableList;
import org.junit.Before;
import org.junit.Test;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.MessageFormat;

import static com.alibaba.polardbx.qatest.validator.DataOperator.executeOnMysqlAndTddl;
import static com.alibaba.polardbx.qatest.validator.DataValidator.selectContentSameAssert;

/**
 * @author chenmo.cm
 */
public class GsiExplicitDefaultValueTest extends DDLBaseNewDBTestCase {

    private static final String PRIMARY_NAME = "explicit_default_test_primary";
    private static final String GSI_NAME = "g_i_explicit_default";

    private static final String HINT =
        "/*+TDDL:CMD_EXTRA(GSI_DEFAULT_CURRENT_TIMESTAMP=TRUE, GSI_ON_UPDATE_CURRENT_TIMESTAMP=TRUE)*/";
    private static final String CREATE_TMPL = HINT + "CREATE TABLE `" + PRIMARY_NAME + "` ("
        + " `pk` int(11) NOT NULL AUTO_INCREMENT,"
        + " `c1` int(11) NOT NULL,"
        + " `c2` int(11) NOT NULL DEFAULT 0,"
        + " `c3` varchar(128) DEFAULT NULL,"
        + " {0},"
        + " `pad` varchar(256) DEFAULT NULL,"
        + " {1},"
        + " PRIMARY KEY (`pk`)"
        + ") ENGINE = InnoDB DEFAULT CHARSET = utf8 {2}";

    @Before
    public void before() {

        // Drop table
        JdbcUtil.executeUpdateSuccess(mysqlConnection, "DROP TABLE IF EXISTS " + PRIMARY_NAME);
//      // JdbcUtil.executeUpdateSuccess(tddlConnection, "DROP TABLE IF EXISTS " + PRIMARY_NAME);
        dropTableWithGsi(PRIMARY_NAME, ImmutableList.of(GSI_NAME));
    }

    /**
     * NOT NULL 但是没有 DEFAULT VALUE，校验 INSERT 报错信息
     */
    @Test
    public void testErrorForColumnWithoutDefault() {
        final String createTableWithoutGsi = MessageFormat
            .format(CREATE_TMPL, "`c4` varchar(128) not null", "key `" + GSI_NAME + "`(c4)", "dbpartition by hash(pk)");

        JdbcUtil.executeUpdateSuccess(tddlConnection, createTableWithoutGsi);

        JdbcUtil.executeUpdateFailed(tddlConnection, "INSERT INTO " + PRIMARY_NAME + "(c1, c4) VALUES(DEFAULT, 'a')",
            "Column 'c1' has no default value");

        JdbcUtil.executeUpdateFailed(tddlConnection, "INSERT INTO " + PRIMARY_NAME + "(c1) VALUES(DEFAULT)",
            "Column 'c1' has no default value");
    }

    /**
     * 拆分键为 NOT NULL 但是没有 DEFAULT VALUE，校验 INSERT 报错信息
     */
    @Test
    public void testErrorForPartitionKeyWithoutDefault() {
        final String createTable = MessageFormat
            .format(CREATE_TMPL, "`c4` varchar(128) null", "key `" + GSI_NAME + "`(c4)", "dbpartition by hash(c1)");

        final String mysqlCreateTable = MessageFormat
            .format(CREATE_TMPL, "`c4` varchar(128) null", "key `" + GSI_NAME + "`(c4)", "");

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, mysqlCreateTable, createTable, null, true);

        JdbcUtil.executeUpdateFailed(tddlConnection, "INSERT INTO " + PRIMARY_NAME + "(c1) VALUES(DEFAULT)",
            "Column 'c1' has no default value");

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, "INSERT INTO " + PRIMARY_NAME + "(c1) VALUES(0)",
            null);

        selectContentSameAssert("SELECT C1, C2, C3, C4, PAD FROM " + PRIMARY_NAME, null, mysqlConnection,
            tddlConnection);
    }

    /**
     * 拆分键为 NOT NULL 但是没有 DEFAULT VALUE，校验 INSERT 报错信息
     */
    @Test
    public void testErrorForPartitionKeyWithoutDefault1() {
        final String createTable = MessageFormat
            .format(CREATE_TMPL, "`c4` varchar(128) not null",
                "global index `" + GSI_NAME + "`(c4) covering(pad) dbpartition by hash(c4)", "dbpartition by hash(c1)");

        final String mysqlCreateTable = MessageFormat
            .format(CREATE_TMPL, "`c4` varchar(128) not null", "key `" + GSI_NAME + "`(c4, pad)", "");

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, mysqlCreateTable, createTable, null, true);

        JdbcUtil.executeUpdateFailed(tddlConnection, "INSERT INTO " + PRIMARY_NAME + "(c1) VALUES(DEFAULT)",
            "Column 'c1' has no default value");

        JdbcUtil.executeUpdateFailed(tddlConnection, "INSERT INTO " + PRIMARY_NAME + "(c1, c4) VALUES(0, DEFAULT)",
            "Column 'c4' has no default value");

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection,
            "INSERT INTO " + PRIMARY_NAME + "(c1, c4) VALUES(0, 'a')",
            null);

        selectContentSameAssert("SELECT C1, C2, C3, C4, PAD FROM " + PRIMARY_NAME, null, mysqlConnection,
            tddlConnection);
    }

    /**
     * Statement like DEFAULT(c4)
     */
    @Test
    public void testErrorForDefaultWithParam() {
        final String createTable = MessageFormat
            .format(CREATE_TMPL, "`c4` varchar(128) not null default 'a'", "key `" + GSI_NAME + "`(c4)",
                "dbpartition by hash(c2)");

        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable);

        JdbcUtil
            .executeUpdateFailed(tddlConnection, "INSERT INTO " + PRIMARY_NAME + "(c1, c4) VALUES(DEFAULT(c1), 'a')",
                "Do not support DEFAULT with parameter");
    }

    /**
     * NOT NULL 没有 DEFAULT VALUE 但是有 AUTO_INCREMENT，校验 INSERT 执行结果
     */
    @Test
    public void testColumnWithAutoIncrement() {
        final String createTable = MessageFormat
            .format(CREATE_TMPL, "`c4` varchar(128) not null", "key `" + GSI_NAME + "`(c4)", "dbpartition by hash(c2)");

        final String mysqlCreateTable = MessageFormat
            .format(CREATE_TMPL, "`c4` varchar(128) not null", "key `" + GSI_NAME + "`(c4)", "");

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, mysqlCreateTable, createTable, null, true);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection,
            "INSERT INTO " + PRIMARY_NAME + " (c1, c4) VALUES(1, 'a')", null);

        selectContentSameAssert("SELECT C1, C2, C3, C4, PAD FROM " + PRIMARY_NAME, null, mysqlConnection,
            tddlConnection);
    }

    /**
     * NOT NULL 但是没有 DEFAULT VALUE，校验 INSERT 报错信息
     */
    @Test
    public void testColumnWithoutDefaultNoStrictSqlMode() throws Exception {
        String sqlMode = "";
        try (ResultSet resultSet = JdbcUtil.executeQuery("show variables like 'sql_mode'", tddlConnection);) {
            resultSet.next();
            sqlMode = resultSet.getString(2);
        } catch (SQLException e) {
            throw e;
        }

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, "set sql_mode='NO_ENGINE_SUBSTITUTION'", null);

        try {
            final String createTableWithoutGsi = MessageFormat
                .format(CREATE_TMPL, "`c4` varchar(128) not null", "key `" + GSI_NAME + "`(c4)",
                    "dbpartition by hash(pk)");
            final String mysqlCreateTable = MessageFormat
                .format(CREATE_TMPL, "`c4` varchar(128) not null", "key `" + GSI_NAME + "`(c4)", "");

            executeOnMysqlAndTddl(mysqlConnection, tddlConnection, mysqlCreateTable, createTableWithoutGsi, null,
                true);

            executeOnMysqlAndTddl(mysqlConnection, tddlConnection,
                "INSERT INTO " + PRIMARY_NAME + "(c1) VALUES(DEFAULT)", null);

            selectContentSameAssert("SELECT C1, C2, C3, C4, PAD FROM " + PRIMARY_NAME, null, mysqlConnection,
                tddlConnection);
        } finally {
            executeOnMysqlAndTddl(mysqlConnection, tddlConnection, "set sql_mode='" + sqlMode + "'", null);
        }
    }
}
