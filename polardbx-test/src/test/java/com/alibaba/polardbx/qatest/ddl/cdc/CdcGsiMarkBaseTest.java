package com.alibaba.polardbx.qatest.ddl.cdc;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Lists;
import org.apache.calcite.sql.SqlKind;
import org.apache.commons.lang.StringUtils;
import org.junit.Assert;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import static com.alibaba.polardbx.qatest.ddl.cdc.util.CdcTestUtil.removeImplicitTgSyntax;

public class CdcGsiMarkBaseTest extends CdcBaseTest {
    protected String GSI_TEST_DB = "gsi_test_drds";

    protected String CAST_TB_WITH_GSI = "CREATE TABLE `%s`(\n"
        + "  `id` bigint(11) NOT NULL AUTO_INCREMENT,\n"
        + "  `order_id` varchar(20) DEFAULT NULL,\n"
        + "  `buyer_id` varchar(20) DEFAULT NULL,\n"
        + "  `seller_id` varchar(20) DEFAULT NULL,\n"
        + "  `order_snapshot` longtext DEFAULT NULL,\n"
        + "  `order_detail` longtext DEFAULT NULL,\n"
        + "  PRIMARY KEY (`id`),\n"
        + "  UNIQUE KEY `l_i_order` (`order_id`),\n"
        + "  GLOBAL INDEX `g_i_seller` (`seller_id`) dbpartition by hash(`seller_id`) tbpartition by hash(`seller_id`),\n"
        + "  GLOBAL UNIQUE INDEX `g_i_buyer` (`buyer_id`) COVERING (order_snapshot) dbpartition by hash(`buyer_id`)\n"
        + ") ENGINE=InnoDB DEFAULT CHARSET=utf8 dbpartition by hash(`order_id`)";

    protected List<String> CONVERING_COLUMN_LIST = Lists.newArrayList("order_snapshot", "order_detail");
    protected String INDEX_COLUMN = "seller_id";
    protected String CREATE_GSI_DDL =
        "CREATE GLOBAL INDEX `%s` on `%s`(`%s`) COVERING (%s) dbpartition by hash(%s)";

    protected String ALTER_ADD_GSI = "alter table %s add GLOBAL INDEX `%s`(`%s`) COVERING (%s) dbpartition by hash(%s)";

    protected String RENAME_INDEX_DDL = "alter table %s rename index %s to %s";
    protected String DROP_COLUMN = "alter table %s drop column %s";
    protected String DROP_GSI = "alter table %s drop index %s";

    protected String MODE = "";

    protected String ADDL_CLUSTER_INDEX =
        "create clustered index %s on %s(%s) dbpartition by hash(`%s`)  tbpartition by hash(`%s`) tbpartitions 3";

    protected void prepareTestDatabase(Statement stmt) throws SQLException {
        stmt.execute("DROP DATABASE IF EXISTS " + GSI_TEST_DB);
        String tokenHints = buildTokenHints();
        String mode = "";
        if (StringUtils.isNotBlank(MODE)) {
            mode = " mode=" + MODE;
        }
        String DDL = tokenHints + "CREATE DATABASE " + GSI_TEST_DB + mode;
        stmt.execute(DDL);
    }

    protected void assertToken(Statement stmt, String token, SqlKind sqlKind, String ddl) throws SQLException {
        Long jobId = null;
        useDB(stmt, "__cdc__");
        try (
            ResultSet rs = stmt.executeQuery("select * from __cdc_ddl_record__ where ddl_sql like '%" + token + "%'")) {
            while (rs.next()) {
                Assert.assertNull(jobId);
                jobId = rs.getLong("JOB_ID");
                String sk = rs.getString("SQL_KIND");
                if (!sk.equals(SqlKind.ALTER_TABLE.name())) {
                    Assert.assertEquals(sqlKind.name(), sk);
                }
                String ddlSql = rs.getString("DDL_SQL");
                String ext = rs.getString("EXT");
                JSONObject json = JSON.parseObject(ext);
                String markSql = json.getString("originalDdl");
                if (StringUtils.isBlank(markSql)) {
                    markSql = ddl;
                }
                assertSqlEquals(ddlSql, removeImplicitTgSyntax(markSql));
            }
        }

        Assert.assertNotNull("not find cdc ddl mark for ddl : " + ddl, jobId);

        // check 1 jobId
        try (ResultSet rs = stmt.executeQuery("select count(*) from __cdc_ddl_record__ where JOB_ID = " + jobId)) {
            while (rs.next()) {
                int count = rs.getInt(1);
                Assert.assertEquals(1, count);
            }
        }

    }

    protected void useDB(Statement st, String dbName) throws SQLException {
        st.execute("use " + dbName);
    }

    protected void createTable(Statement stmt, String tableName) throws SQLException {
        String token = buildTokenHints();
        useDB(stmt, GSI_TEST_DB);
        String ddl = token + String.format(CAST_TB_WITH_GSI, tableName);
        stmt.execute(ddl);
    }

    protected void dropGSI(Statement stmt, String tableName, String indexName) throws SQLException {
        String token = buildTokenHints();
        String ddl = token + String.format(DROP_GSI, tableName, indexName);
        useDB(stmt, GSI_TEST_DB);
        stmt.execute(ddl);
        assertToken(stmt, token, SqlKind.DROP_INDEX, ddl);
    }

    protected void createGSI(Statement stmt, String tableName, String indexDefine) throws SQLException {
        String token = buildTokenHints();
        String ddl = token + String.format(CREATE_GSI_DDL, indexDefine, tableName, INDEX_COLUMN,
            StringUtils.join(CONVERING_COLUMN_LIST.toArray(), ","), INDEX_COLUMN);
        useDB(stmt, GSI_TEST_DB);
        stmt.execute(ddl);
        assertToken(stmt, token, SqlKind.CREATE_INDEX, ddl);
    }

    protected void alterAddGSI(Statement stmt, String tableName, String indexDefine) throws SQLException {
        String token = buildTokenHints();
        String ddl = token + String.format(ALTER_ADD_GSI, tableName, indexDefine, INDEX_COLUMN,
            StringUtils.join(CONVERING_COLUMN_LIST.toArray(), ","), INDEX_COLUMN);
        useDB(stmt, GSI_TEST_DB);
        stmt.execute(ddl);
        assertToken(stmt, token, SqlKind.CREATE_INDEX, ddl);
    }

    protected void rename(Statement stmt, String tableName, String beforeName, String afterName) throws SQLException {
        String token = buildTokenHints();
        String ddl = token + String.format(RENAME_INDEX_DDL, tableName, beforeName, afterName);
        useDB(stmt, GSI_TEST_DB);
        stmt.execute(ddl);
        assertToken(stmt, token, SqlKind.ALTER_RENAME_INDEX, ddl);
    }

    protected void dropConveringColumn(Statement stmt, String tableName) throws SQLException {
        String token = buildTokenHints();
        String ddl = token + String.format(DROP_COLUMN, tableName, CONVERING_COLUMN_LIST.get(0));
        useDB(stmt, GSI_TEST_DB);
        stmt.execute(ddl);
        assertToken(stmt, token, SqlKind.ALTER_TABLE, ddl);
        CONVERING_COLUMN_LIST.remove(0);
    }

    /**
     * "create clustered index %s on %s(%s) dbpartition by hash(`%s`)  tbpartition by hash(`%s`) tbpartitions 3";
     */
    protected void addClusterIndex(Statement stmt, String tableName, String indexName, String indexColumn)
        throws SQLException {
        String token = buildTokenHints();
        String ddl =
            token + String.format(ADDL_CLUSTER_INDEX, indexName, tableName, indexColumn, indexColumn, indexColumn);
        useDB(stmt, GSI_TEST_DB);
        stmt.execute(ddl);
        assertToken(stmt, token, SqlKind.CREATE_INDEX, ddl);
    }

}
