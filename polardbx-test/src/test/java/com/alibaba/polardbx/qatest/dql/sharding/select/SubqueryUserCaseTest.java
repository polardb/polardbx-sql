package com.alibaba.polardbx.qatest.dql.sharding.select;

import com.alibaba.polardbx.qatest.BaseTestCase;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Materialize optimize test for IN subquery
 * CorrelateApply rel node (transformed by in subquery) should push down and looks like `xx in (?)`
 */

public class SubqueryUserCaseTest extends BaseTestCase {
    private static final Log log = LogFactory.getLog(SubqueryUserCaseTest.class);
    private static final String testSchemaDrds = "subquery_testdb";
    private static final String testSchemaAuto = "subquery_testdb_auto";

    @Test
    public void testSubqueryWithBroadcastAndSingleTable() throws SQLException {
        try (Connection c = getPolardbxConnection()) {
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
            c.createStatement().execute("use " + testSchemaDrds);
            c.createStatement().executeQuery(testSql);
        }
    }

    @Test
    public void testCorrelatePushDown() throws SQLException {
        String testSql = "SELECT\n"
            + "  (SELECT  EXISTS\n"
            + "                (SELECT *\n"
            + "                 FROM single_tbl d\n"
            + "                 WHERE d.id = a.id)\n"
            + "              OR EXISTS\n"
            + "                (SELECT *\n"
            + "                 FROM single_tbl1 f\n"
            + "                 WHERE f.bid = a.bid)),\n"
            + "  (SELECT 1\n"
            + "   FROM single_tbl2 g\n"
            + "   WHERE g.name = a.name) \n"
            + "FROM single_tbl2 a";
        try (Connection c = getPolardbxConnection0(testSchemaAuto);
            Statement stmt = c.createStatement();) {
            stmt.executeQuery(testSql);

            stmt.executeQuery(
                "/*TDDL:ENABLE_DIRECT_PLAN=false ENABLE_POST_PLANNER=false ENABLE_PUSH_CORRELATE=false*/" + testSql);
            ResultSet rs =
                stmt.executeQuery(
                    "explain /*TDDL:ENABLE_DIRECT_PLAN=false ENABLE_POST_PLANNER=false ENABLE_PUSH_CORRELATE=false*/"
                        + testSql);
            StringBuilder sb = new StringBuilder();
            while (rs.next()) {
                sb.append(rs.getString(1));
            }

            assert sb.toString().contains("CorrelateApply");
        }
    }

    /**
     * prepare db and table for test
     */
    @BeforeClass
    public static void prepareCatalog() throws SQLException {
        try (Connection c = getPolardbxConnection0()) {
            c.createStatement().execute("drop database if exists " + testSchemaDrds);
            c.createStatement().execute("create database " + testSchemaDrds);
            c.createStatement().execute("use " + testSchemaDrds);
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

            c.createStatement().execute("drop database if exists " + testSchemaAuto);
            c.createStatement().execute("create database " + testSchemaAuto + " mode='auto'");
            c.createStatement().execute("use " + testSchemaAuto);

            c.createStatement().execute(
                "CREATE TABLE single_tbl(\n"
                    + " id bigint not null auto_increment, \n"
                    + " bid int, \n"
                    + " name varchar(30), \n"
                    + " primary key(id)\n"
                    + ") SINGLE;"
            );
            c.createStatement().execute(
                "CREATE TABLE single_tbl1(\n"
                    + " id bigint not null auto_increment, \n"
                    + " bid int, \n"
                    + " name varchar(30), \n"
                    + " primary key(id)\n"
                    + ") SINGLE;"
            );
            c.createStatement().execute(
                "CREATE TABLE single_tbl2(\n"
                    + " id bigint not null auto_increment, \n"
                    + " bid int, \n"
                    + " name varchar(30), \n"
                    + " primary key(id)\n"
                    + ") SINGLE;"
            );
        } finally {
            log.info(testSchemaDrds + " catalog prepared");
            log.info(testSchemaAuto + " catalog prepared");
        }
    }

    /**
     * clear db after test
     */
    @AfterClass
    public static void clearCatalog() throws SQLException {
        try (Connection c = getPolardbxConnection0()) {
            c.createStatement().execute("drop database if exists " + testSchemaDrds);
            c.createStatement().execute("drop database if exists " + testSchemaAuto);
        } finally {
            log.info(testSchemaDrds + " catalog was dropped");
            log.info(testSchemaAuto + " catalog was dropped");
        }
    }
}
