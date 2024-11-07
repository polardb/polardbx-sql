package com.alibaba.polardbx.qatest.oss;

import com.alibaba.polardbx.common.Engine;
import com.alibaba.polardbx.qatest.DDLBaseNewDBTestCase;
import com.alibaba.polardbx.qatest.oss.utils.FileStorageTestUtil;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.alibaba.polardbx.qatest.util.PropertiesUtil;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.stream.Collectors;

import static com.google.common.truth.Truth.assertWithMessage;

public class ColumnarSelectionTest extends DDLBaseNewDBTestCase {

    private static final Engine engine = PropertiesUtil.engine();

    private static String TABLE_DEFINITION_FORMAT = "CREATE TABLE `%s` (\n" +
        "\t`id` bigint(20) NOT NULL AUTO_INCREMENT,\n"
        + "\t`a` int(32) UNSIGNED DEFAULT NULL,\n"
        + "\t`b` int(32) UNSIGNED DEFAULT NULL,\n"
        + "\t`c` int(32) UNSIGNED DEFAULT NULL,\n"
        + "\tprimary key(id),\n"
        + "\tkey idx_a(a),\n"
        + "\tkey idx_b(b),\n"
        + "\tkey idx_c(c),\n"
        + "\tglobal index g_idx_c(c) partition by key(c) partitions 8\n"
        + ") partition by key(id) partitions 8";

    private static String CREATE_COL_IDX = SKIP_WAIT_CCI_CREATION_HINT
        + "create clustered columnar index `%s` on %s(`%s`) partition by hash(`%s`) partitions 4";

    private static String PUB_COL_IDX =
        "/*+TDDL:CMD_EXTRA(ALTER_CCI_STATUS=true, ALTER_CCI_STATUS_BEFORE=CREATING, ALTER_CCI_STATUS_AFTER=PUBLIC)*/" +
            "ALTER TABLE `%s` alter index `%s` VISIBLE;";

    String tb1 = "tb1";
    String tb2 = "tb2";
    String tb3 = "tb3";

    String colIdxA = "colIdx_a";
    String colIdxB = "colIdx_b";
    String colIdxC = "colIdx_c";

    String colA = "a";
    String colB = "b";
    String colC = "c";

    String tbOss = "tb_oss";

    String APHINT = "/*+TDDL:cmd_extra(WORKLOAD_TYPE=AP ENABLE_COLUMNAR_OPTIMIZER=true)*/";
    String AP_NOCOL_HINT = "/*+TDDL:cmd_extra(WORKLOAD_TYPE=AP ENABLE_COLUMNAR_OPTIMIZER=false)*/";

    @Override
    public boolean usingNewPartDb() {
        return true;
    }

    @After
    public void dropTable() {
        JdbcUtil.dropTable(getTddlConnection1(), tb1);
        JdbcUtil.dropTable(getTddlConnection1(), tb2);
        JdbcUtil.dropTable(getTddlConnection1(), tb3);
        JdbcUtil.dropTable(getTddlConnection1(), tbOss);
    }

    @Before
    public void prepareTable() {
        JdbcUtil.dropTable(getTddlConnection1(), tb1);
        JdbcUtil.dropTable(getTddlConnection1(), tb2);
        JdbcUtil.dropTable(getTddlConnection1(), tb3);
        JdbcUtil.dropTable(getTddlConnection1(), tbOss);

        JdbcUtil.executeSuccess(getTddlConnection1(), String.format(TABLE_DEFINITION_FORMAT, tb1));
        JdbcUtil.executeSuccess(getTddlConnection1(), String.format(TABLE_DEFINITION_FORMAT, tb2));
        JdbcUtil.executeSuccess(getTddlConnection1(), String.format(TABLE_DEFINITION_FORMAT, tb3));
        FileStorageTestUtil.createLoadingTable(getTddlConnection1(), tbOss, tddlDatabase1, tb1, engine);

        JdbcUtil.executeSuccess(getTddlConnection1(), String.format(CREATE_COL_IDX, colIdxA, tb1, colA, colA));
        JdbcUtil.executeSuccess(getTddlConnection1(), String.format(PUB_COL_IDX, tb1, colIdxA));
        JdbcUtil.executeSuccess(getTddlConnection1(), String.format(CREATE_COL_IDX, colIdxB, tb2, colB, colB));
        JdbcUtil.executeSuccess(getTddlConnection1(), String.format(PUB_COL_IDX, tb2, colIdxB));
        JdbcUtil.executeSuccess(getTddlConnection1(), String.format(CREATE_COL_IDX, colIdxC, tb3, colC, colC));
        JdbcUtil.executeSuccess(getTddlConnection1(), String.format(PUB_COL_IDX, tb3, colIdxC));
    }

    @Test
    public void testExplain() throws SQLException {
        String sql;

        sql = String.format("explain simple select * from %s", tb1);
        checkNoOSSTableScan(sql);
        sql = APHINT + String.format("explain simple select * from %s", tb1);
        checkAllOSSTableScan(sql);

        // dml
        sql = APHINT + String.format("explain simple delete from %s where a = 1", tb1);
        checkNoOSSTableScan(sql);
        sql = APHINT + String.format("explain simple update %s set b = 1 where a= 1", tb1);
        checkNoOSSTableScan(sql);
        sql = String.format("explain simple delete from %s force index(%s) where a = 1", tb1, colIdxA);
        checkNoOSSTableScan(sql);
//        sql = String.format("explain simple update %s force index(%s) set b = 1 where a= 1", tb1, colIdxA);
//        checkCantUse(sql);
//        sql = APHINT + String.format("explain simple select * from %s for update", tb1);
//        checkNoOSSTableScan(sql);
//        sql = APHINT + String.format("explain simple select * from %s force index(%s) for update", tb1, colA);
//        checkCantUse(sql);

        // subquery
        sql = String.format("explain simple select *,(select 1 from %s limit 1) from %s", tb1, tb2);
        checkNoOSSTableScan(sql);
        sql = APHINT + String.format("explain simple select *,(select 1 from %s limit 1) from %s", tb1, tb2);
        checkNoOSSTableScan(sql);

        // force/ignore index
        sql = String.format("explain simple select *,(select 1 from %s force index(%s) limit 1) from %s", tb1, colIdxA,
            tb2);
        checkOSSTableScanAndLogicalView(sql);
        sql =
            AP_NOCOL_HINT + String.format("explain simple select *,(select 1 from %s force index(%s) limit 1) from %s",
                tb1, colIdxA, tb2);
        checkOSSTableScanAndLogicalView(sql);
        sql = APHINT + String.format("explain simple select *,(select 1 from %s force index(%s) limit 1) from %s", tb1,
            colIdxA, tb2);
        checkOSSTableScanAndLogicalView(sql);

        sql =
            String.format("explain simple select *,(select 1 from %s force index(%s) limit 1) from %s force index(%s)",
                tb1, colIdxA, tb2, colIdxB);
        checkAllOSSTableScan(sql);
        sql = AP_NOCOL_HINT + String.format(
            "explain simple select *,(select 1 from %s force index(%s) limit 1) from %s force index(%s)", tb1, colIdxA,
            tb2, colIdxB);
        checkAllOSSTableScan(sql);
        sql = APHINT + String.format(
            "explain simple select *,(select 1 from %s force index(%s) limit 1) from %s force index(%s)", tb1, colIdxA,
            tb2, colIdxB);
        checkAllOSSTableScan(sql);

        sql = APHINT + String.format(
            "explain simple select *,(select 1 from %s force index(%s) limit 1) from %s force index(idx_c)", tb1,
            colIdxA, tb2);
        checkOSSTableScanAndLogicalView(sql);

        sql = APHINT + String.format(
            "explain simple select *,(select 1 from %s force index(%s) limit 1) from %s ignore index(%s)", tb1, colIdxA,
            tb2, colIdxB);
        checkOSSTableScanAndLogicalView(sql);
        sql = APHINT + String.format(
            "explain simple select *,(select 1 from %s force index(%s) limit 1) from %s ignore index(idx_c)", tb1,
            colIdxA, tb2);
        checkOSSTableScanAndLogicalView(sql);

        sql = APHINT + String.format(
            "explain simple select *,(select 1 from %s limit 1) from %s ignore index(idx_c)", tb1, tb2);
        checkNoOSSTableScan(sql);
        sql = APHINT + String.format(
            "explain simple select *,(select 1 from %s ignore index(%s) limit 1) from %s ignore index(idx_c)", tb1,
            colIdxA, tb2);
        checkNoOSSTableScan(sql);

        // columnar join oss table
        sql = String.format(
            "explain simple select *,(select 1 from %s force index(%s) limit 1) from %s ignore index(idx_c)", tb1,
            colIdxA, tbOss);
        checkAllOSSTableScan(sql);
        sql = APHINT + String.format(
            "explain simple select *,(select 1 from %s force index(%s) limit 1) from %s ignore index(idx_c)", tb1,
            colIdxA, tbOss);
        checkAllOSSTableScan(sql);

        sql = String.format(
            "explain simple select *,(select b from %s limit 1) from %s ignore index(idx_c)", tb1, tbOss);
        checkOSSTableScanAndLogicalView(sql);
        sql = AP_NOCOL_HINT + String.format(
            "explain simple select *,(select b from %s limit 1) from %s ignore index(idx_c)", tb1, tbOss);
        checkOSSTableScanAndLogicalView(sql);
        sql = APHINT + String.format(
            "explain simple select *,(select b from %s limit 1) from %s ignore index(idx_c)", tb1, tbOss);
        checkOSSTableScanAndLogicalView(sql);

        checkGsiView();
    }

    private void checkGsiView() throws SQLException {
        String sql =
            String.format("select SIZE_IN_MB from information_schema.GLOBAL_INDEXES where table in ('%s', '%s', '%s')",
                tb1, tb2, tb3);
        ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, sql);
        while (rs.next()) {
            System.out.println(rs.getString(1));
            Assert.assertNotNull("Got null gsi size, possibly got a columnar index.", rs.getString(1));
        }
    }

    void checkAllOSSTableScan(String sql) {
        String explain =
            JdbcUtil.getAllResult(JdbcUtil.executeQuery(sql, tddlConnection))
                .stream().flatMap(Collection::stream).map(Object::toString).collect(Collectors.joining(""));
        assertWithMessage(sql).that(explain).contains("OSSTableScan");
        assertWithMessage(sql).that(explain).doesNotContain("LogicalView");
    }

    void checkNoOSSTableScan(String sql) {
        String explain =
            JdbcUtil.getAllResult(JdbcUtil.executeQuery(sql, tddlConnection))
                .stream().flatMap(Collection::stream).map(Object::toString).collect(Collectors.joining(""));
        assertWithMessage(sql).that(explain).doesNotContain("OSSTableScan");
    }

    void checkCantUse(String sql) {
        JdbcUtil.executeQueryFaied(tddlConnection, sql, "can't use columnar");
    }

    void checkOSSTableScanAndLogicalView(String sql) {
        String explain =
            JdbcUtil.getAllResult(JdbcUtil.executeQuery(sql, tddlConnection))
                .stream().flatMap(Collection::stream).map(Object::toString).collect(Collectors.joining(""));
        assertWithMessage(sql).that(explain).contains("OSSTableScan");
        assertWithMessage(sql).that(explain).contains("LogicalView");
    }
}
