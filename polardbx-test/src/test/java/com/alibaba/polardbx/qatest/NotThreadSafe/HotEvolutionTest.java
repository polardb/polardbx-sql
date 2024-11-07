package com.alibaba.polardbx.qatest.NotThreadSafe;

import com.alibaba.polardbx.common.properties.ConnectionProperties;
import com.alibaba.polardbx.optimizer.planmanager.PlanManager;
import com.alibaba.polardbx.qatest.BaseTestCase;
import com.alibaba.polardbx.qatest.util.ConnectionManager;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.google.common.collect.ImmutableList;
import com.google.common.truth.Truth;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.MessageFormat;
import java.util.List;

public class HotEvolutionTest extends BaseTestCase {
    private static final String DB = "hotEvolution";

    private static final String EVO_TABLE = "full_table_big";

    private static final String CREATE_TABLE_TEMPLATE = "CREATE TABLE {0} (\n"
        + "    `c_int_32` int(32) NOT NULL DEFAULT '0',\n"
        + "    `id` bigint(20) NOT NULL AUTO_INCREMENT,\n"
        + "    `c_bigint_64_un` bigint(64) UNSIGNED DEFAULT NULL,\n"
        + "    `c_bigint_1` bigint(1) DEFAULT NULL,\n"
        + "    `c_int_1` int(1) DEFAULT NULL,\n"
        + "    PRIMARY KEY (`id`),\n"
        + "    GLOBAL INDEX `c_bigint_1_idx` (`c_bigint_1`, `id`) PARTITION BY KEY(`c_bigint_1`, `id`) PARTITIONS 16,\n"
        + "    GLOBAL INDEX `c_bigint_64_un_c_int_1_idx` (`c_bigint_64_un`, `c_int_1`, `id`) PARTITION BY KEY(`c_bigint_64_un`, `c_int_1`, `id`) PARTITIONS 16,\n"
        + "    LOCAL KEY `_local_c_bigint_1_idx` (`c_bigint_1`),\n"
        + "    LOCAL KEY `_local_c_bigint_64_un_c_int_1_idx` (`c_bigint_64_un`, `c_int_1`)\n"
        + ") ENGINE = InnoDB AUTO_INCREMENT = 9999976 DEFAULT CHARSET = utf8mb4 PARTITION BY KEY(`id`) PARTITIONS 16";

    private static final String ENABLE_GLOBAL_HOT_GSI_EVOLUTION =
        "set global " + ConnectionProperties.ENABLE_HOT_GSI_EVOLUTION + "= true";
    private static final String DISABLE_GLOBAL_HOT_GSI_EVOLUTION =
        "set global " + ConnectionProperties.ENABLE_HOT_GSI_EVOLUTION + "= false";

    private static final String SET_HOT_GSI_EVOLUTION_THRESHOLD =
        "set " + ConnectionProperties.HOT_GSI_EVOLUTION_THRESHOLD + "= -1";

    private static final String ANALYZE_TEMPLATE = "analyze table {0}";

    private static final String SQL = "select * from {0} where c_bigint_1 = 12 and c_bigint_64_un = 14";

    private static final String CLEAR_CACHE = "clear plancache";

    private static final String SPM_CLEAR = "baseline delete_all";

    private static final String SPM_HELP = "baseline help";

    private static final String SPM_CLEAR_EVO = "baseline delete_evolved ";

    private static final String SPM_LIST = "baseline list ";

    private static final String INFO_SPM = "select * from information_schema.spm where baseline_id = '%s'";

    @Override
    public boolean usingNewPartDb() {
        return true;
    }

    @BeforeClass
    public static void initDb() throws SQLException {
        try (Connection tmpConnection = ConnectionManager.getInstance().getDruidPolardbxConnection()) {
            JdbcUtil.createPartDatabase(tmpConnection, DB);
            JdbcUtil.useDb(tmpConnection, DB);
            JdbcUtil.dropTable(tmpConnection, EVO_TABLE);
            JdbcUtil.executeUpdateSuccess(tmpConnection,
                MessageFormat.format(CREATE_TABLE_TEMPLATE, EVO_TABLE));
            JdbcUtil.executeUpdateSuccess(tmpConnection,
                MessageFormat.format(ANALYZE_TEMPLATE, EVO_TABLE));
        }
    }

    @AfterClass
    public static void deleteDb() throws SQLException {
        try (Connection tmpConnection = ConnectionManager.getInstance().getDruidPolardbxConnection()) {
            JdbcUtil.dropDatabase(tmpConnection, DB);
        }
    }

    @Test
    public void testEvolution() {
        System.out.println("testEvolution");
        String sql = MessageFormat.format(SQL, EVO_TABLE);
        String baselineId;
        try (Connection conn = getPolardbxConnection(DB)) {
            JdbcUtil.executeSuccess(conn, CLEAR_CACHE);
            JdbcUtil.executeSuccess(conn, SPM_CLEAR);
            checkExplainSource(conn, sql, PlanManager.PLAN_SOURCE.PLAN_CACHE);
            JdbcUtil.executeSuccess(conn, sql);
            checkExplainSource(conn, sql, PlanManager.PLAN_SOURCE.PLAN_CACHE);

            // no evolution since plan is cached
            JdbcUtil.executeSuccess(conn, SET_HOT_GSI_EVOLUTION_THRESHOLD);
            checkExplainSource(conn, sql, PlanManager.PLAN_SOURCE.PLAN_CACHE);

            JdbcUtil.executeSuccess(conn, CLEAR_CACHE);
            JdbcUtil.executeSuccess(conn, SPM_CLEAR);
            checkExplainSource(conn, sql, PlanManager.PLAN_SOURCE.PLAN_CACHE);

            // wait for evolution
            Thread.sleep(3000L);
            checkExplainSource(conn, sql, PlanManager.PLAN_SOURCE.SPM_ACCEPT);

            baselineId = getBaselineId(conn, sql);
            Truth.assertThat(baselineId).isNotEmpty();
            Truth.assertThat(getBaselineCount(conn, baselineId)).isEqualTo(3);

            // clear evolved
            JdbcUtil.executeSuccess(conn, SPM_CLEAR_EVO);
            Truth.assertThat(getBaselineCount(conn, baselineId)).isEqualTo(0);

            JdbcUtil.executeSuccess(conn, "baseline fix sql /*+TDDL:cmd_extra()*/ " + sql);
            checkExplainSource(conn, sql, PlanManager.PLAN_SOURCE.SPM_FIX);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testEvolutionAndFix() {
        System.out.println("testEvolutionAndFix");
        String sql = MessageFormat.format(SQL, EVO_TABLE);
        String baselineId;
        try (Connection conn = getPolardbxConnection(DB)) {
            JdbcUtil.executeSuccess(conn, CLEAR_CACHE);
            JdbcUtil.executeSuccess(conn, SPM_CLEAR);
            JdbcUtil.executeSuccess(conn, SET_HOT_GSI_EVOLUTION_THRESHOLD);
            checkExplainSource(conn, sql, PlanManager.PLAN_SOURCE.PLAN_CACHE);
            // wait for evolution
            Thread.sleep(3000L);
            checkExplainSource(conn, sql, PlanManager.PLAN_SOURCE.SPM_ACCEPT);

            baselineId = getBaselineId(conn, sql);
            Truth.assertThat(baselineId).isNotEmpty();
            Truth.assertThat(getBaselineCount(conn, baselineId)).isEqualTo(3);

            JdbcUtil.executeSuccess(conn, "baseline fix sql /*+TDDL:cmd_extra()*/ " + sql);
            checkExplainSource(conn, sql, PlanManager.PLAN_SOURCE.SPM_FIX);
            Truth.assertThat(getBaselineCount(conn, baselineId)).isEqualTo(3);

            // clear evolved
            JdbcUtil.executeSuccess(conn, SPM_CLEAR_EVO);
            Truth.assertThat(getBaselineCount(conn, baselineId)).isEqualTo(0);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testFixAndEvolution() {
        String sql = MessageFormat.format(SQL, EVO_TABLE);
        String baselineId;
        System.out.println("testFixAndEvolution");
        try (Connection conn = getPolardbxConnection(DB)) {
            JdbcUtil.executeSuccess(conn, CLEAR_CACHE);
            JdbcUtil.executeSuccess(conn, SPM_CLEAR);
            // baseline fix sql first
            JdbcUtil.executeSuccess(conn, "baseline fix sql /*+TDDL:cmd_extra()*/ " + sql);
            checkExplainSource(conn, sql, PlanManager.PLAN_SOURCE.SPM_FIX);

            // try evolve
            JdbcUtil.executeSuccess(conn, SET_HOT_GSI_EVOLUTION_THRESHOLD);
            JdbcUtil.executeSuccess(conn, CLEAR_CACHE);
            checkExplainSource(conn, sql, PlanManager.PLAN_SOURCE.SPM_FIX);
            JdbcUtil.executeSuccess(conn, sql);
            Thread.sleep(3000L);
            checkExplainSource(conn, sql, PlanManager.PLAN_SOURCE.SPM_FIX);

            // get baseline id
            baselineId = getBaselineId(conn, sql);
            Truth.assertThat(baselineId).isNotEmpty();

            // should not evolve
            Truth.assertThat(getBaselineCount(conn, baselineId)).isEqualTo(1);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testDisableEvolution() {
        String sql = MessageFormat.format(SQL, EVO_TABLE);
        String baselineId;
        System.out.println("testDisableEvolution");
        try (Connection conn = getPolardbxConnection(DB)) {
            JdbcUtil.executeSuccess(conn, CLEAR_CACHE);
            JdbcUtil.executeSuccess(conn, SPM_CLEAR);
            // try evolve
            JdbcUtil.executeSuccess(conn, SET_HOT_GSI_EVOLUTION_THRESHOLD);
            JdbcUtil.executeSuccess(conn, CLEAR_CACHE);
            checkExplainSource(conn, sql, PlanManager.PLAN_SOURCE.PLAN_CACHE);
            JdbcUtil.executeSuccess(conn, sql);
            Thread.sleep(3000L);

            checkExplainSource(conn, sql, PlanManager.PLAN_SOURCE.SPM_ACCEPT);
            baselineId = getBaselineId(conn, sql);
            Truth.assertThat(baselineId).isNotEmpty();
            Truth.assertThat(getBaselineCount(conn, baselineId)).isEqualTo(3);
            // disable hot evolution
            JdbcUtil.executeSuccess(conn, DISABLE_GLOBAL_HOT_GSI_EVOLUTION);
            Thread.sleep(3000L);
            checkExplainSource(conn, sql, PlanManager.PLAN_SOURCE.SPM_ACCEPT);
            Truth.assertThat(getBaselineCount(conn, baselineId)).isEqualTo(3);
            // clear cache
            JdbcUtil.executeSuccess(conn, CLEAR_CACHE);
            JdbcUtil.executeSuccess(conn, SPM_CLEAR);
            checkExplainSource(conn, sql, PlanManager.PLAN_SOURCE.PLAN_CACHE);
            JdbcUtil.executeSuccess(conn, sql);
            Thread.sleep(3000L);
            checkExplainSource(conn, sql, PlanManager.PLAN_SOURCE.PLAN_CACHE);
            Truth.assertThat(getBaselineCount(conn, baselineId)).isEqualTo(0);
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            try (Connection conn = getPolardbxConnection(DB)) {
                JdbcUtil.executeSuccess(conn, ENABLE_GLOBAL_HOT_GSI_EVOLUTION);
                Thread.sleep(3000L);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Test
    public void testBaselineView() {
        String sql = MessageFormat.format(SQL, EVO_TABLE);
        String baselineId;
        List<List<String>> rows;
        ResultSet rs;
        System.out.println("testBaselineView");
        try (Connection conn = getPolardbxConnection(DB)) {
            JdbcUtil.executeSuccess(conn, CLEAR_CACHE);
            JdbcUtil.executeSuccess(conn, SPM_CLEAR);
            JdbcUtil.executeSuccess(conn, SET_HOT_GSI_EVOLUTION_THRESHOLD);
            checkExplainSource(conn, sql, PlanManager.PLAN_SOURCE.PLAN_CACHE);
            // wait for evolution
            Thread.sleep(3000L);
            checkExplainSource(conn, sql, PlanManager.PLAN_SOURCE.SPM_ACCEPT);

            baselineId = getBaselineId(conn, sql);
            Truth.assertThat(baselineId).isNotEmpty();

            // check baseline help
            boolean find = false;
            rows = JdbcUtil.getAllStringResult(JdbcUtil.executeQuery(SPM_HELP, conn), false, ImmutableList.of());
            for (List<String> row : rows) {
                if (row.get(2).toLowerCase().contains(SPM_CLEAR_EVO.trim())) {
                    find = true;
                    break;
                }
            }
            Truth.assertThat(find).isTrue();

            // check hot evolved mark
            rs = JdbcUtil.executeQuery(SPM_LIST + baselineId, conn);
            while (rs.next()) {
                Truth.assertThat(rs.getString("HOT_EVOLVED")).isEqualTo("true");
            }
            rs.close();

            // check hot evolved mark
            rs = JdbcUtil.executeQuery(String.format(INFO_SPM, baselineId), conn);
            while (rs.next()) {
                Truth.assertThat(rs.getString("HOT_EVOLVED")).isEqualTo("true");
            }
            rs.close();

            JdbcUtil.executeSuccess(conn, SPM_CLEAR_EVO);
            Truth.assertThat(getBaselineCount(conn, baselineId)).isEqualTo(0);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void checkExplainSource(Connection conn, String sql, PlanManager.PLAN_SOURCE expected) {
        final List<List<String>> res =
            JdbcUtil.getAllStringResult(JdbcUtil.executeQuery("explain " + sql, conn), false, ImmutableList.of());
        String source = "SOURCE:";
        String result = "";
        for (List<String> row : res) {
            if (row.isEmpty()) {
                continue;
            }
            System.out.println(row);
            String value = row.get(0).toUpperCase();
            int sourceLoc = value.indexOf(source);
            if (sourceLoc < 0) {
                continue;
            }
            result = value.substring(sourceLoc + source.length()).trim();
            //break;
        }
        Truth.assertWithMessage("base explain " + sql).that(result).contains(expected.name().toUpperCase());
    }

    private String getBaselineId(Connection conn, String sql) {
        final List<List<String>> res =
            JdbcUtil.getAllStringResult(JdbcUtil.executeQuery("explain " + sql, conn), false, ImmutableList.of());
        String source = "BaselineInfo Id:".toUpperCase();

        for (List<String> row : res) {
            if (row.isEmpty()) {
                continue;
            }
            String value = row.get(0).toUpperCase();
            int sourceLoc = value.indexOf(source);
            if (sourceLoc < 0) {
                continue;
            }
            return value.substring(sourceLoc + source.length()).trim();
        }
        return "";
    }

    private int getBaselineCount(Connection conn, String baselineId) {
        return JdbcUtil.getAllResult(JdbcUtil.executeQuery(SPM_LIST + baselineId, conn), false)
            .size();
    }
}
