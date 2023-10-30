package com.alibaba.polardbx.qatest.dql.sharding.select;

import com.alibaba.polardbx.qatest.BaseTestCase;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Assert;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Materialize optimize test for IN subquery
 * CorrelateApply rel node (transformed by in subquery) should push down and looks like `xx in (?)`
 */

public class SubqueryUserCaseTest extends BaseTestCase {
    private static final Log log = LogFactory.getLog(SubqueryUserCaseTest.class);
    private static final String testSchema = "subquery_testdb";

    @Test
    public void testSubqueryWithBroadcastAndSingleTable() throws SQLException {
        try (Connection c = getPolardbxConnection()) {
            prepareCatalog();
//            SELECT 1
//            FROM tbl_mult ana
//            WHERE ana.yljgdm IN
//            ( SELECT yljgdm
//            FROM tbl_broadcast jgdm
//            WHERE  jgdm.sjbm IN
//            ( SELECT pdm
//            FROM tbl_single xzqh
//            WHERE jgdm.sjbm = xzqh.qhdm
//            OR jgdm.qxbm = xzqh.qhdm ))
            String testSql =
                "SELECT 1\n"
                    + "FROM tbl_mult ana\n"
                    + "WHERE ana.yljgdm IN\n"
                    + "    ( SELECT yljgdm\n"
                    + "     FROM tbl_broadcast jgdm\n"
                    + "     WHERE  jgdm.sjbm IN\n"
                    + "         ( SELECT pdm\n"
                    + "          FROM tbl_single xzqh\n"
                    + "          WHERE jgdm.sjbm = xzqh.qhdm\n"
                    + "                 OR jgdm.qxbm = xzqh.qhdm ))";
            c.createStatement().execute("use " + testSchema);
            c.createStatement().executeQuery(testSql);
        } finally {
            clearCatalog();
        }
    }

    /**
     * prepare db and table for test
     */
    private void prepareCatalog() throws SQLException {
        try (Connection c = getPolardbxConnection()) {
            c.createStatement().execute("drop database if exists " + testSchema);
            c.createStatement().execute("create database " + testSchema);
            c.createStatement().execute("use " + testSchema);
            c.createStatement().execute(
                "CREATE TABLE `tbl_mult` (\n"
                    + "    `ID` varchar(128) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,\n"
                    + "    `YLJGDM` varchar(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin DEFAULT NULL,\n"
                    + "    `LASTUPDATETIME` datetime DEFAULT NULL COMMENT '最后一次更新时间',\n"
                    + "    PRIMARY KEY (`ID`)\n"
                    + ") ENGINE = InnoDB DEFAULT CHARSET = utf8mb4  dbpartition by YYYYMM(`LASTUPDATETIME`)");
            c.createStatement().execute(
                "CREATE TABLE `tbl_single` (\n"
                    + "    `ID` varchar(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,\n"
                    + "    `QHDM` varchar(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin DEFAULT NULL,\n"
                    + "    `PDM` varchar(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin DEFAULT NULL,\n"
                    + "    PRIMARY KEY (`ID`)\n"
                    + ") ENGINE = InnoDB DEFAULT CHARSET = utf8mb4;\n");
            c.createStatement().execute(
                "CREATE TABLE `tbl_broadcast` (\n"
                    + "    `YLJGDM` varchar(30) NOT NULL ,\n"
                    + "    `SJBM` varchar(64) DEFAULT NULL ,\n"
                    + "    `QXBM` varchar(64) DEFAULT NULL ,\n"
                    + "    PRIMARY KEY (`YLJGDM`)\n"
                    + ") ENGINE = InnoDB DEFAULT CHARSET = utf8mb4  broadcast;"
            );
        } finally {
            log.info(testSchema + " catalog prepared");
        }
    }

    /**
     * clear db after test
     */
    private void clearCatalog() throws SQLException {
        try (Connection c = getPolardbxConnection()) {
            c.createStatement().execute("drop database if exists " + testSchema);
        } finally {
            log.info(testSchema + " catalog was dropped");
        }
    }
}
