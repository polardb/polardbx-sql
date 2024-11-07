package com.alibaba.polardbx.qatest.columnar.role;

import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.qatest.AutoReadBaseTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.alibaba.polardbx.qatest.util.PropertiesUtil;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.alibaba.polardbx.qatest.validator.DataValidator.explainResultMatchAssert;
import static com.alibaba.polardbx.qatest.validator.DataValidator.explainResultStrictMatchAssert;
import static com.alibaba.polardbx.qatest.validator.DataValidator.selectErrorAssert;

public class ColumnarRoleTest extends AutoReadBaseTestCase {

    @Before
    public void before() {
        this.tddlConnection = getPolardbxConnection(PropertiesUtil.polardbXDBName1(true));
    }

    @Test
    public void showCreateTable1() throws SQLException {
        explainResultMatchAssert(
            "show tables like '%table_multi_db_multi_tb%'", null, tddlConnection, "table_multi_db_multi_tb");

        try {
            explainResultStrictMatchAssert(
                "show create table table_multi_db_multi_tb;", null, tddlConnection,
                "CREATE TABLE `table_multi_db_multi_tb` (\n"
                    + "\t`create` int(11),\n"
                    + "\t`table` int(11),\n"
                    + "\t`database` int(11),\n"
                    + "\t`by` int(11),\n"
                    + "\t`desc` int(11) NOT NULL DEFAULT 0,\n"
                    + "\t`int` int(11),\n"
                    + "\t`group` int(11),\n"
                    + "\t`order` int(11),\n"
                    + "\t`primary` int(11),\n"
                    + "\t`key` int(11)\n"
                    + ") ENGINE = InnoDB DEFAULT CHARACTER SET = GBK DEFAULT COLLATE = gbk_chinese_ci", 2);
        } catch (Throwable e) {
            explainResultStrictMatchAssert(
                "show create table table_multi_db_multi_tb;", null, tddlConnection,
                "CREATE TABLE `table_multi_db_multi_tb` (\n"
                    + "\t`create` int,\n"
                    + "\t`table` int,\n"
                    + "\t`database` int,\n"
                    + "\t`by` int,\n"
                    + "\t`desc` int NOT NULL DEFAULT 0,\n"
                    + "\t`int` int,\n"
                    + "\t`group` int,\n"
                    + "\t`order` int,\n"
                    + "\t`primary` int,\n"
                    + "\t`key` int\n"
                    + ") ENGINE = InnoDB DEFAULT CHARACTER SET = GBK DEFAULT COLLATE = gbk_chinese_ci", 2);
        }

    }

    @Test
    public void showCreateTable2() throws SQLException {
        try {
            explainResultStrictMatchAssert(
                "show create table tddl_test;", null, tddlConnection,
                "CREATE TABLE `tddl_test` (\n"
                    + "\t`id` int(11) NOT NULL AUTO_INCREMENT,\n"
                    + "\t`name` varchar(255),\n"
                    + "\t`gmt_create` date,\n"
                    + "\t`gmt_modified` date,\n"
                    + "\tPRIMARY KEY (`id`)\n"
                    + ") ENGINE = InnoDB DEFAULT CHARACTER SET = GBK DEFAULT COLLATE = gbk_chinese_ci", 2);
        } catch (Throwable e) {
            explainResultStrictMatchAssert(
                "show create table tddl_test;", null, tddlConnection,
                "CREATE TABLE `tddl_test` (\n"
                    + "\t`id` int NOT NULL AUTO_INCREMENT,\n"
                    + "\t`name` varchar(255),\n"
                    + "\t`gmt_create` date,\n"
                    + "\t`gmt_modified` date,\n"
                    + "\tPRIMARY KEY (`id`)\n"
                    + ") ENGINE = InnoDB DEFAULT CHARACTER SET = GBK DEFAULT COLLATE = gbk_chinese_ci", 2);
        }

    }

    @Test
    public void setVariables() throws SQLException {
        selectErrorAssert(
            "set global table_open_cache=2000", null, tddlConnection,
            "SET GLOBAL is not supported in read only mode");
    }

    @Test
    public void simpleTest() throws SQLException {
        selectErrorAssert(
            "select * from tddl_test", null, tddlConnection,
            "server error by don't support query the table without columnar index");
    }

    @Test
    public void showCreateTableTest() throws SQLException {
        String showResult = null;
        try (Connection readOnlyConn = getPolardbxConnection(PropertiesUtil.polardbXDBName1(true))) {
            JdbcUtil.useDb(readOnlyConn, "drds_polarx1_part_qatest_app");
            ResultSet result = JdbcUtil.executeQuerySuccess(readOnlyConn,
                "show create table update_delete_base_autonic_two_multi_db_one_tb");
            while (result.next()) {
                showResult = result.getString(2);
            }
        }
        Assert.assertTrue(showResult != null);
    }

}
