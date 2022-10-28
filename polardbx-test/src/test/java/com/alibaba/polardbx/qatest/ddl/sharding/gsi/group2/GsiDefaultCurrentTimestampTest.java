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

import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.qatest.DDLBaseNewDBTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.util.Litmus;
import org.junit.Before;
import org.junit.Test;

import java.sql.SQLException;
import java.text.MessageFormat;
import java.util.UUID;

import static com.alibaba.polardbx.qatest.validator.DataOperator.executeOnMysqlAndTddl;
import static com.alibaba.polardbx.qatest.validator.DataValidator.selectContentSameAssert;

/**
 * @author chenmo.cm
 */
public class GsiDefaultCurrentTimestampTest extends DDLBaseNewDBTestCase {
    private static final Logger log = LoggerFactory.getLogger(
        GsiDefaultCurrentTimestampTest.class);

    private static final String PRIMARY_NAME = "current_timestamp_test_primary";
    private static final String GSI_NAME = "g_i_current_timestamp";

    private static final String HINT =
        "/*+TDDL:CMD_EXTRA(GSI_DEFAULT_CURRENT_TIMESTAMP=TRUE, GSI_ON_UPDATE_CURRENT_TIMESTAMP=TRUE)*/";
    private static final String CREATE_TMPL = HINT + "CREATE TABLE `" + PRIMARY_NAME + "` ("
        + " `pk` int(11) NOT NULL AUTO_INCREMENT,"
        + " `c1` int(11) DEFAULT NULL,"
        + " `c2` int(11) DEFAULT NULL,"
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

    public void insertOrReplace(boolean isReplace) {
        final String head = isReplace ? "REPLACE" : "INSERT";
        String sql = head + " INTO " + PRIMARY_NAME + "(pad) values('" + UUID.randomUUID().toString() + "')";
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, null);

        sql = head + " INTO " + PRIMARY_NAME + "(pad) values('" + UUID.randomUUID().toString() + "'),('" + UUID
            .randomUUID()
            .toString() + "')";
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, null);
    }

    public void insertOrReplaceWithDefault(boolean isReplace) {
        final String head = isReplace ? "REPLACE" : "INSERT";
        String sql =
            head + " INTO " + PRIMARY_NAME + "(c_timestamp, pad) values(default, '" + UUID.randomUUID().toString()
                + "')";
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, null);

        sql = head + " INTO " + PRIMARY_NAME + "(c_timestamp, pad) values(default, '" + UUID.randomUUID().toString()
            + "'),(default, '" + UUID
            .randomUUID()
            .toString() + "')";
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, null);
    }

    public void insertOrReplaceSelect(boolean isReplace) {
        final String head = isReplace ? "REPLACE" : "INSERT";
        String sql =
            head + " INTO " + PRIMARY_NAME + "(pad) select'" + UUID.randomUUID().toString() + "' FROM " + PRIMARY_NAME;
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, null);
    }

    public void compareDataWithMySql(String selectTmplForGsi) {
        selectContentSameAssert("SELECT c1, c2, c3, pad FROM " + PRIMARY_NAME, null, mysqlConnection,
            tddlConnection);
        selectContentSameAssert(selectTmplForGsi + PRIMARY_NAME,
            selectTmplForGsi + GSI_NAME, null, mysqlConnection,
            tddlConnection);
    }

    public void writeAndCheck(String selectTmplForGsi) throws SQLException {
        insertOrReplace(false);
        // Compare data in mysql and drds
        compareDataWithMySql(selectTmplForGsi);
        // Compare data in primary and gsi
        checkGsi(tddlConnection, GSI_NAME);

//        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, "delete from " + PRIMARY_NAME, null);
//
//        insertOrReplace(true);
//        // Compare data in mysql and drds
//        compareDataWithMySql(selectTmplForGsi);
//        // Compare data in primary and gsi
//        checkGsi(tddlConnection, GSI_NAME);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, "delete from " + PRIMARY_NAME, null);

        insertOrReplaceWithDefault(false);
        // Compare data in mysql and drds
        compareDataWithMySql(selectTmplForGsi);
        // Compare data in primary and gsi
        checkGsi(tddlConnection, GSI_NAME);

//        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, "delete from " + PRIMARY_NAME, null);
//
//        insertOrReplaceWithDefault(true);
//        // Compare data in mysql and drds
//        compareDataWithMySql(selectTmplForGsi);
//        // Compare data in primary and gsi
//        checkGsi(tddlConnection, GSI_NAME);

        insertOrReplaceSelect(false);
        // Compare data in mysql and drds
        compareDataWithMySql(selectTmplForGsi);
        // Compare data in primary and gsi
        checkGsi(tddlConnection, GSI_NAME);

//        insertOrReplaceSelect(true);
//        // Compare data in mysql and drds
//        compareDataWithMySql(selectTmplForGsi);
//        // Compare data in primary and gsi
//        checkGsi(tddlConnection, GSI_NAME);
    }

    /**
     * c_timestamp 为索引表拆分键
     */
    @Test
    public void createTableWithGsi() throws SQLException {
        final String createTableWithGsi =
            MessageFormat.format(CREATE_TMPL, "`c_timestamp` timestamp not null default current_timestamp",
                "global index `" + GSI_NAME + "`(c_timestamp) covering(pad) dbpartition by YYYYMM_OPT(`c_timestamp`)",
                "dbpartition by hash(pk)");

        final String mysqlCreateTable =
            MessageFormat.format(CREATE_TMPL, "`c_timestamp` timestamp not null default current_timestamp",
                "key `" + GSI_NAME + "`(c_timestamp, pad)", "");

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, mysqlCreateTable, createTableWithGsi, null, true);

        writeAndCheck("SELECT pad FROM ");

        // Compare table structure
        final ShowIndexChecker showIndexChecker = getShowIndexGsiChecker(tddlConnection, PRIMARY_NAME);
        showIndexChecker.identicalToTableDefinition(createTableWithGsi, true, Litmus.THROW);
    }

    /**
     * c_timestamp 为索引表拆分键
     */
    @Test
    public void createTableWithGsiWithoutDefault() throws SQLException {
        if (isMySQL80()) {
            // MySQL 8.0 will not convert "`c_timestamp` timestamp ON UPDATE current_timestamp"
            // to "`c_timestamp` timestamp NOT NULL DEFAULT '0000-00-00 00:00:00' ON UPDATE CURRENT_TIMESTAMP" as MySQL 5.7 does,
            // which will cause route error
            return;
        }
        final String createTableWithGsi =
            MessageFormat.format(CREATE_TMPL, "`c_timestamp` timestamp not null default current_timestamp",
                "global index `" + GSI_NAME + "`(c_timestamp) covering(pad) dbpartition by YYYYMM_OPT(`c_timestamp`)",
                "dbpartition by hash(pk)");

        final String mysqlCreateTable =
            MessageFormat.format(CREATE_TMPL, "`c_timestamp` timestamp not null default current_timestamp",
                "key `" + GSI_NAME + "`(c_timestamp, pad)", "");

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, mysqlCreateTable, createTableWithGsi, null, true);

        writeAndCheck("SELECT pad FROM ");

        // Compare table structure
        final ShowIndexChecker showIndexChecker = getShowIndexGsiChecker(tddlConnection, PRIMARY_NAME);
        showIndexChecker.identicalToTableDefinition(createTableWithGsi, true, Litmus.THROW);
    }

    /**
     * c_timestamp 为索引表拆分键
     */
    @Test
    public void createTableWithGsiCurrentTimestamp() throws SQLException {
        final String createTableWithGsi =
            MessageFormat.format(CREATE_TMPL, "`c_timestamp` timestamp not null default current_timestamp()",
                "global index `" + GSI_NAME + "`(c_timestamp) covering(pad) dbpartition by YYYYMM_OPT(`c_timestamp`)",
                "dbpartition by hash(pk)");

        final String mysqlCreateTable =
            MessageFormat.format(CREATE_TMPL, "`c_timestamp` timestamp not null default current_timestamp",
                "key `" + GSI_NAME + "`(c_timestamp, pad)", "");

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, mysqlCreateTable, createTableWithGsi, null, true);

        writeAndCheck("SELECT pad FROM ");

        // Compare table structure
        final ShowIndexChecker showIndexChecker = getShowIndexGsiChecker(tddlConnection, PRIMARY_NAME);
        showIndexChecker.identicalToTableDefinition(createTableWithGsi, true, Litmus.THROW);
    }

    /**
     * c_timestamp 为索引表拆分键
     */
    @Test
    public void createTableWithGsiOnUpdateCurrentTimestamp() throws SQLException {
        if (isMySQL80()) {
            // MySQL 8.0 will not convert "`c_timestamp` timestamp ON UPDATE current_timestamp"
            // to "`c_timestamp` timestamp NOT NULL DEFAULT '0000-00-00 00:00:00' ON UPDATE CURRENT_TIMESTAMP" as MySQL 5.7 does,
            // which will cause route error
            return;
        }
        final String createTableWithGsi =
            MessageFormat.format(HINT + CREATE_TMPL, "`c_timestamp` timestamp not null ON UPDATE current_timestamp",
                "global index `" + GSI_NAME + "`(c_timestamp) covering(pad) dbpartition by YYYYMM_OPT(`c_timestamp`)",
                "dbpartition by hash(pk)");

        final String mysqlCreateTable =
            MessageFormat.format(CREATE_TMPL, "`c_timestamp` timestamp not null ON UPDATE current_timestamp",
                "key `" + GSI_NAME + "`(c_timestamp, pad)", "");

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, mysqlCreateTable, createTableWithGsi, null, true);

        writeAndCheck("SELECT pad FROM ");

        // Compare table structure
        final ShowIndexChecker showIndexChecker = getShowIndexGsiChecker(tddlConnection, PRIMARY_NAME);
        showIndexChecker.identicalToTableDefinition(createTableWithGsi, true, Litmus.THROW);
    }

    /**
     * c_timestamp 为索引表拆分键
     */
    @Test
    public void createTableWithGsiNow() throws SQLException {
        final String createTableWithGsi =
            MessageFormat.format(CREATE_TMPL, "`c_timestamp` timestamp not null default now()",
                "global index `" + GSI_NAME + "`(c_timestamp) covering(pad) dbpartition by YYYYMM_OPT(`c_timestamp`)",
                "dbpartition by hash(pk)");

        final String mysqlCreateTable =
            MessageFormat.format(CREATE_TMPL, "`c_timestamp` timestamp not null default current_timestamp",
                "key `" + GSI_NAME + "`(c_timestamp, pad)", "");

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, mysqlCreateTable, createTableWithGsi, null, true);

        writeAndCheck("SELECT pad FROM ");

        // Compare table structure
        final ShowIndexChecker showIndexChecker = getShowIndexGsiChecker(tddlConnection, PRIMARY_NAME);
        showIndexChecker.identicalToTableDefinition(createTableWithGsi, true, Litmus.THROW);
    }

    /**
     * c_timestamp 为索引表拆分键
     */
    @Test
    public void createTableWithGsiLocaltime() throws SQLException {
        final String createTableWithGsi =
            MessageFormat.format(CREATE_TMPL, "`c_timestamp` timestamp not null default localtime()",
                "global index `" + GSI_NAME + "`(c_timestamp) covering(pad) dbpartition by YYYYMM_OPT(`c_timestamp`)",
                "dbpartition by hash(pk)");

        final String mysqlCreateTable =
            MessageFormat.format(CREATE_TMPL, "`c_timestamp` timestamp not null default current_timestamp",
                "key `" + GSI_NAME + "`(c_timestamp, pad)", "");

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, mysqlCreateTable, createTableWithGsi, null, true);

        writeAndCheck("SELECT pad FROM ");

        // Compare table structure
        final ShowIndexChecker showIndexChecker = getShowIndexGsiChecker(tddlConnection, PRIMARY_NAME);
        showIndexChecker.identicalToTableDefinition(createTableWithGsi, true, Litmus.THROW);
    }

    /**
     * c_timestamp 为索引表拆分键
     */
    @Test
    public void createTableWithGsiLocaltime1() throws SQLException {
        final String createTableWithGsi =
            MessageFormat.format(CREATE_TMPL, "`c_timestamp` timestamp not null default localtime",
                "global index `" + GSI_NAME + "`(c_timestamp) covering(pad) dbpartition by YYYYMM_OPT(`c_timestamp`)",
                "dbpartition by hash(pk)");

        final String mysqlCreateTable =
            MessageFormat.format(CREATE_TMPL, "`c_timestamp` timestamp not null default current_timestamp",
                "key `" + GSI_NAME + "`(c_timestamp, pad)", "");

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, mysqlCreateTable, createTableWithGsi, null, true);

        writeAndCheck("SELECT pad FROM ");

        // Compare table structure
        final ShowIndexChecker showIndexChecker = getShowIndexGsiChecker(tddlConnection, PRIMARY_NAME);
        showIndexChecker.identicalToTableDefinition(createTableWithGsi, true, Litmus.THROW);
    }

    /**
     * c_timestamp 为索引表拆分键
     */
    @Test
    public void createTableWithGsiLocalTimestamp() throws SQLException {
        final String createTableWithGsi =
            MessageFormat.format(CREATE_TMPL, "`c_timestamp` timestamp not null default localtimestamp()",
                "global index `" + GSI_NAME + "`(c_timestamp) covering(pad) dbpartition by YYYYMM_OPT(`c_timestamp`)",
                "dbpartition by hash(pk)");

        final String mysqlCreateTable =
            MessageFormat.format(CREATE_TMPL, "`c_timestamp` timestamp not null default current_timestamp",
                "key `" + GSI_NAME + "`(c_timestamp, pad)", "");

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, mysqlCreateTable, createTableWithGsi, null, true);

        writeAndCheck("SELECT pad FROM ");

        // Compare table structure
        final ShowIndexChecker showIndexChecker = getShowIndexGsiChecker(tddlConnection, PRIMARY_NAME);
        showIndexChecker.identicalToTableDefinition(createTableWithGsi, true, Litmus.THROW);
    }

    /**
     * c_timestamp 为索引表拆分键
     */
    @Test
    public void createTableWithGsiLocaltimestamp1() throws SQLException {
        final String createTableWithGsi =
            MessageFormat.format(CREATE_TMPL, "`c_timestamp` timestamp not null default localtimestamp",
                "global index `" + GSI_NAME + "`(c_timestamp) covering(pad) dbpartition by YYYYMM_OPT(`c_timestamp`)",
                "dbpartition by hash(pk)");

        final String mysqlCreateTable =
            MessageFormat.format(CREATE_TMPL, "`c_timestamp` timestamp not null default current_timestamp",
                "key `" + GSI_NAME + "`(c_timestamp, pad)", "");

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, mysqlCreateTable, createTableWithGsi, null, true);

        writeAndCheck("SELECT pad FROM ");

        // Compare table structure
        final ShowIndexChecker showIndexChecker = getShowIndexGsiChecker(tddlConnection, PRIMARY_NAME);
        showIndexChecker.identicalToTableDefinition(createTableWithGsi, true, Litmus.THROW);
    }

    /**
     * c_timestamp 为索引表 COVERING 列
     */
    @Test
    public void createTableWithGsi1() throws SQLException {
        final String createTableWithGsi =
            MessageFormat.format(CREATE_TMPL, "`c_timestamp` timestamp not null default current_timestamp",
                "global index `" + GSI_NAME + "`(c1) covering(c_timestamp, pad) dbpartition by hash(`c1`)",
                "dbpartition by hash(pk)");

        final String mysqlCreateTable =
            MessageFormat.format(CREATE_TMPL, "`c_timestamp` timestamp not null default current_timestamp",
                "key `" + GSI_NAME + "`(c1, pad)", "");

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, mysqlCreateTable, createTableWithGsi, null, true);

        writeAndCheck("SELECT c1, pad FROM ");

        // Compare table structure
        final ShowIndexChecker showIndexChecker = getShowIndexGsiChecker(tddlConnection, PRIMARY_NAME);
        showIndexChecker.identicalToTableDefinition(createTableWithGsi, true, Litmus.THROW);
    }

    /**
     * c_timestamp 为主表拆分键
     */
    @Test
    public void createTableWithGsi2() throws SQLException {
        final String createTableWithGsi =
            MessageFormat.format(CREATE_TMPL, "`c_timestamp` timestamp not null default current_timestamp",
                "global index `" + GSI_NAME + "`(c1) covering(pad) dbpartition by hash(`c1`)",
                "dbpartition by YYYYMM_OPT(c_timestamp)");

        final String mysqlCreateTable =
            MessageFormat.format(CREATE_TMPL, "`c_timestamp` timestamp not null default current_timestamp",
                "key `" + GSI_NAME + "`(c1, pad)", "");

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, mysqlCreateTable, createTableWithGsi, null, true);

        writeAndCheck("SELECT c1, pad FROM ");

        // Compare table structure
        final ShowIndexChecker showIndexChecker = getShowIndexGsiChecker(tddlConnection, PRIMARY_NAME);
        showIndexChecker.identicalToTableDefinition(createTableWithGsi, true, Litmus.THROW);
    }
}
