package com.alibaba.polardbx.qatest.columnar.dql;

import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.properties.ConnectionProperties;
import com.alibaba.polardbx.qatest.DDLBaseNewDBTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.google.common.truth.Truth;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

public class AnalyzeUseColumnarTest extends DDLBaseNewDBTestCase {
    private static String TABLE_DEFINITION_FORMAT = "CREATE TABLE `%s` (\n" +
        "\t`id` bigint(20) NOT NULL AUTO_INCREMENT,\n"
        + "\t`a` int(32) UNSIGNED DEFAULT NULL,\n"
        + "\t`b` int(32) UNSIGNED DEFAULT NULL,\n"
        + "\t`c` int(32) UNSIGNED DEFAULT NULL,\n"
        + "\tprimary key(id),\n"
        + "\tkey idx_a(a),\n"
        + "\tkey idx_b(b),\n"
        + "\tkey idx_c(c)\n"
        + ") partition by key(id) partitions 8";

    private static String CREATE_COL_IDX = SKIP_WAIT_CCI_CREATION_HINT +
        "create clustered columnar index `%s` on %s(`%s`) partition by hash(`%s`) partitions 4";

    String tableName = "tb1";
    String colIdxA = "colIdx_a";
    String colA = "a";

    String ANALYZE = "analyze table " + tableName;
    String ANALYZE_UPDATE =
        "/*+TDDL:cmd_extra(" + ConnectionParams.ANALYZE_TEST_UPDATE + "=true)*/ analyze table " + tableName;

    String ANALYZE_UPDATE_ROW = "/*+TDDL:cmd_extra("
        + ConnectionParams.ANALYZE_TEST_UPDATE + "=true "
        + ConnectionParams.ENABLE_MPP_NDV_USE_COLUMNAR + "=false "
        + ")*/ analyze table " + tableName;

    String TIME =
        "select max(UNIX_TIMESTAMP(gmt_modified)) from metadb.ndv_sketch_statistics where schema_name='%s' and table_name='%s'";

    String LEN =
        "select max(length(sketch_bytes)) from metadb.ndv_sketch_statistics where schema_name='%s' and table_name='%s'";

    public boolean usingNewPartDb() {
        return true;
    }

    @After
    public void dropTable() {
        JdbcUtil.dropTable(getTddlConnection1(), tableName);
    }

    @Before
    public void prepareTable() {
        JdbcUtil.dropTable(getTddlConnection1(), tableName);
        JdbcUtil.executeSuccess(getTddlConnection1(), String.format(TABLE_DEFINITION_FORMAT, tableName));
        JdbcUtil.executeSuccess(getTddlConnection1(),
            String.format("insert into %s(a,b,c) values(1,2,3),(4,5,6)", tableName));
        ColumnarUtils.createColumnarIndex(getTddlConnection1(), colIdxA, tableName, colA, colA, 4);
    }

    @Test
    public void testAnalyze() throws SQLException, InterruptedException {
        try (Connection conn = getPolardbxConnection(tddlDatabase1)) {
            JdbcUtil.executeSuccess(conn, ANALYZE);
            long time = getUpdateTime();
            Truth.assertThat(getByteLength()).isEqualTo(1L);

            JdbcUtil.executeSuccess(conn, ANALYZE_UPDATE);
            Truth.assertThat(getUpdateTime()).isEqualTo(time);
            Truth.assertThat(getByteLength()).isEqualTo(1L);

            JdbcUtil.executeSuccess(conn, "set global " + ConnectionProperties.STATISTIC_NDV_SKETCH_EXPIRE_TIME + "=1");
            Thread.sleep(3000L);

            JdbcUtil.executeSuccess(conn, ANALYZE_UPDATE);
            Truth.assertThat(getUpdateTime()).isGreaterThan(time);
            Truth.assertThat(getByteLength()).isEqualTo(1L);
            time = getUpdateTime();

            Thread.sleep(1000L);
            JdbcUtil.executeSuccess(conn, ANALYZE_UPDATE_ROW);
            Truth.assertThat(getUpdateTime()).isGreaterThan(time);
            Truth.assertThat(getByteLength()).isGreaterThan(1L);
        } finally {
            JdbcUtil.executeSuccess(tddlConnection, "set global "
                + ConnectionProperties.STATISTIC_NDV_SKETCH_EXPIRE_TIME + "="
                + ConnectionParams.STATISTIC_NDV_SKETCH_EXPIRE_TIME.getDefault());
        }

    }

    long getUpdateTime() {
        try (ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection,
            String.format(TIME, tddlDatabase1, tableName))) {
            if (rs.next()) {
                return rs.getLong(1);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return -1;
    }

    long getByteLength() {
        try (
            ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, String.format(LEN, tddlDatabase1, tableName))) {
            if (rs.next()) {
                return rs.getLong(1);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return -1;
    }

}
