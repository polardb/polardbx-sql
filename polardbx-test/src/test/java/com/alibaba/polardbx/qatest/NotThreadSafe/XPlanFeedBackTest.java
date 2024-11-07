package com.alibaba.polardbx.qatest.NotThreadSafe;

import com.alibaba.polardbx.common.properties.ConnectionProperties;
import com.alibaba.polardbx.qatest.CdcIgnore;
import com.alibaba.polardbx.qatest.DDLBaseNewDBTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.google.common.collect.ImmutableList;
import com.google.common.truth.Truth;
import org.apache.commons.lang.StringUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.SQLException;
import java.text.MessageFormat;
import java.util.List;

public class XPlanFeedBackTest extends DDLBaseNewDBTestCase {

    private static final String XPLAN_TABLE = "XPlan_multi_key";

    private static final String XPLAN_TABLE_NO_STA = "XPlan_multi_key2";

    private static final String MULTI_KEY_TABLE_TEMPLATE = "create table {0} (\n"
        + "    pk bigint not null,\n"
        + "    x int not null,\n"
        + "    y int default null,\n"
        + "    z int default null,\n"
        + "    primary key(pk),\n"
        + "    key kpk(pk,x,y),"
        + "    key kx(x),\n"
        + "    key ky(y)\n"
        + ") dbpartition by hash(x) tbpartition by hash(x) tbpartitions 2";

    private static final String INSERT_TEMPLATE = "insert into {0} values "
        + "(1,101,1001,10001),(2,102,null,10002),(3,103,1003,null),(4,104,null,null),(5,105,1005,10005);";

    private static final String ENABLE_XPLAN_FEEDBACK =
        "set " + ConnectionProperties.ENABLE_XPLAN_FEEDBACK + "= true";
    private static final String DISABLE_XPLAN_FEEDBACK =
        "set " + ConnectionProperties.ENABLE_XPLAN_FEEDBACK + "= false";

    private static final String DISABLE_DIRECT_PLAN =
        "set " + ConnectionProperties.ENABLE_DIRECT_PLAN + "= false";
    private static final String DISABLE_POST_PLANNER =
        "set " + ConnectionProperties.ENABLE_POST_PLANNER + "= false";

    private static final String ANALYZE_TEMPLATE = "analyze table {0}";

    private static final String SK_SQL = "select * from {0} where x = 1";

    private static final String PK_SQL = "select * from {0} where pk = 1";

    private static final String SK_PK_SQL = "select * from {0} where x = 1 and pk = 1";

    private static final String CLEAR_CACHE = "clear plancache";

    private static final String CLEAR_SPM = "baseline delete_all";

    @Before
    public void initData() {
        JdbcUtil.dropTable(tddlConnection, XPLAN_TABLE);
        JdbcUtil.dropTable(tddlConnection, XPLAN_TABLE_NO_STA);
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            MessageFormat.format(MULTI_KEY_TABLE_TEMPLATE, XPLAN_TABLE));
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            MessageFormat.format(MULTI_KEY_TABLE_TEMPLATE, XPLAN_TABLE_NO_STA));
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            MessageFormat.format(INSERT_TEMPLATE, XPLAN_TABLE));
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            MessageFormat.format(ANALYZE_TEMPLATE, XPLAN_TABLE));
    }

    @After
    public void afterDDLBaseNewDBTestCase() {
        cleanDataBase();
    }

    @Test
    @CdcIgnore(ignoreReason = "暂时未查到原因，可能是并行跑各种实验室导致。本地无法复现，且对replica实验室无影响")
    public void testFeedBack() {
        if (isMySQL80()) {
            return;
        }
        JdbcUtil.executeSuccess(tddlConnection, CLEAR_CACHE);
        JdbcUtil.executeSuccess(tddlConnection, CLEAR_SPM);

        // try feedback
        try (Connection conn = getPolardbxConnection()) {
            JdbcUtil.executeSuccess(conn, ENABLE_XPLAN_FEEDBACK);
            assertExplainExecute(conn, MessageFormat.format(SK_SQL, XPLAN_TABLE), true);
            assertExplainExecute(conn, MessageFormat.format(PK_SQL, XPLAN_TABLE), false);
            assertExplainExecute(conn, MessageFormat.format(SK_PK_SQL, XPLAN_TABLE), true);

            // mock feedback
            JdbcUtil.executeSuccess(conn, "set " + ConnectionProperties.XPLAN_MAX_SCAN_ROWS + "= 0");
            JdbcUtil.executeSuccess(conn, MessageFormat.format(SK_SQL, XPLAN_TABLE));
            JdbcUtil.executeSuccess(conn, MessageFormat.format(PK_SQL, XPLAN_TABLE));
            JdbcUtil.executeSuccess(conn, MessageFormat.format(SK_PK_SQL, XPLAN_TABLE));
            JdbcUtil.executeSuccess(conn, "set " + ConnectionProperties.XPLAN_MAX_SCAN_ROWS + "= 1000");

            // xplan should be closed
            assertExplainExecute(conn, MessageFormat.format(SK_SQL, XPLAN_TABLE), false);
            assertExplainExecute(conn, MessageFormat.format(PK_SQL, XPLAN_TABLE), false);
            assertExplainExecute(conn, MessageFormat.format(SK_PK_SQL, XPLAN_TABLE), false);

            // disable feedback
            JdbcUtil.executeSuccess(conn, DISABLE_XPLAN_FEEDBACK);
            assertExplainExecute(conn, MessageFormat.format(SK_SQL, XPLAN_TABLE), true);
            assertExplainExecute(conn, MessageFormat.format(PK_SQL, XPLAN_TABLE), false);
            assertExplainExecute(conn, MessageFormat.format(SK_PK_SQL, XPLAN_TABLE), true);

            // enable ENABLE_XPLAN_FEEDBACK again
            JdbcUtil.executeSuccess(conn, ENABLE_XPLAN_FEEDBACK);
            assertExplainExecute(conn, MessageFormat.format(SK_SQL, XPLAN_TABLE), false);
            assertExplainExecute(conn, MessageFormat.format(PK_SQL, XPLAN_TABLE), false);
            assertExplainExecute(conn, MessageFormat.format(SK_PK_SQL, XPLAN_TABLE), false);

            // clear plancache and execute sql again
            JdbcUtil.executeSuccess(conn, CLEAR_CACHE);
            assertExplainExecute(conn, MessageFormat.format(SK_SQL, XPLAN_TABLE), true);
            assertExplainExecute(conn, MessageFormat.format(PK_SQL, XPLAN_TABLE), false);
            assertExplainExecute(conn, MessageFormat.format(SK_PK_SQL, XPLAN_TABLE), true);
            JdbcUtil.executeSuccess(conn, MessageFormat.format(SK_SQL, XPLAN_TABLE));
            JdbcUtil.executeSuccess(conn, MessageFormat.format(PK_SQL, XPLAN_TABLE));
            JdbcUtil.executeSuccess(conn, MessageFormat.format(SK_PK_SQL, XPLAN_TABLE));

            // no feedback should happen
            assertExplainExecute(conn, MessageFormat.format(SK_SQL, XPLAN_TABLE), true);
            assertExplainExecute(conn, MessageFormat.format(PK_SQL, XPLAN_TABLE), false);
            assertExplainExecute(conn, MessageFormat.format(SK_PK_SQL, XPLAN_TABLE), true);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        } finally {
            JdbcUtil.executeSuccess(tddlConnection, CLEAR_CACHE);
            JdbcUtil.executeSuccess(tddlConnection, CLEAR_SPM);
        }

        // try table no statistics
        // try feedback
        try (Connection conn = getPolardbxConnection()) {
            JdbcUtil.executeSuccess(conn, ENABLE_XPLAN_FEEDBACK);
            assertExplainExecute(conn, MessageFormat.format(SK_SQL, XPLAN_TABLE_NO_STA), false);
            assertExplainExecute(conn, MessageFormat.format(PK_SQL, XPLAN_TABLE_NO_STA), false);
            assertExplainExecute(conn, MessageFormat.format(SK_PK_SQL, XPLAN_TABLE_NO_STA), true);

            // mock feedback
            JdbcUtil.executeSuccess(conn, "set " + ConnectionProperties.XPLAN_MAX_SCAN_ROWS + "= 0");
            JdbcUtil.executeSuccess(conn, MessageFormat.format(SK_SQL, XPLAN_TABLE_NO_STA));
            JdbcUtil.executeSuccess(conn, MessageFormat.format(PK_SQL, XPLAN_TABLE_NO_STA));
            JdbcUtil.executeSuccess(conn, MessageFormat.format(SK_PK_SQL, XPLAN_TABLE_NO_STA));
            JdbcUtil.executeSuccess(conn, "set " + ConnectionProperties.XPLAN_MAX_SCAN_ROWS + "= 1000");

            // xplan should be closed
            assertExplainExecute(conn, MessageFormat.format(SK_SQL, XPLAN_TABLE_NO_STA), false);
            assertExplainExecute(conn, MessageFormat.format(PK_SQL, XPLAN_TABLE_NO_STA), false);
            assertExplainExecute(conn, MessageFormat.format(SK_PK_SQL, XPLAN_TABLE_NO_STA), false);

            // disable feedback opt
            JdbcUtil.executeSuccess(conn, DISABLE_XPLAN_FEEDBACK);
            assertExplainExecute(conn, MessageFormat.format(SK_SQL, XPLAN_TABLE_NO_STA), false);
            assertExplainExecute(conn, MessageFormat.format(PK_SQL, XPLAN_TABLE_NO_STA), false);
            assertExplainExecute(conn, MessageFormat.format(SK_PK_SQL, XPLAN_TABLE_NO_STA), true);

            // enable ENABLE_XPLAN_FEEDBACK again
            JdbcUtil.executeSuccess(conn, ENABLE_XPLAN_FEEDBACK);
            assertExplainExecute(conn, MessageFormat.format(SK_SQL, XPLAN_TABLE_NO_STA), false);
            assertExplainExecute(conn, MessageFormat.format(PK_SQL, XPLAN_TABLE_NO_STA), false);
            assertExplainExecute(conn, MessageFormat.format(SK_PK_SQL, XPLAN_TABLE_NO_STA), false);

            // clear plancache and execute sql again
            JdbcUtil.executeSuccess(conn, CLEAR_CACHE);
            assertExplainExecute(conn, MessageFormat.format(SK_SQL, XPLAN_TABLE_NO_STA), false);
            assertExplainExecute(conn, MessageFormat.format(PK_SQL, XPLAN_TABLE_NO_STA), false);
            assertExplainExecute(conn, MessageFormat.format(SK_PK_SQL, XPLAN_TABLE_NO_STA), true);
            JdbcUtil.executeSuccess(conn, MessageFormat.format(SK_SQL, XPLAN_TABLE_NO_STA));
            JdbcUtil.executeSuccess(conn, MessageFormat.format(PK_SQL, XPLAN_TABLE_NO_STA));
            JdbcUtil.executeSuccess(conn, MessageFormat.format(SK_PK_SQL, XPLAN_TABLE_NO_STA));

            // no feedback should happen
            assertExplainExecute(conn, MessageFormat.format(SK_SQL, XPLAN_TABLE_NO_STA), false);
            assertExplainExecute(conn, MessageFormat.format(PK_SQL, XPLAN_TABLE_NO_STA), false);
            assertExplainExecute(conn, MessageFormat.format(SK_PK_SQL, XPLAN_TABLE_NO_STA), true);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        } finally {
            JdbcUtil.executeSuccess(tddlConnection, ENABLE_XPLAN_FEEDBACK);
        }
    }

    @Test
    public void testFeedBackSpm() {
        if (isMySQL80()) {
            return;
        }
        JdbcUtil.executeSuccess(tddlConnection, CLEAR_CACHE);
        JdbcUtil.executeSuccess(tddlConnection, CLEAR_SPM);

        // try feedback
        try (Connection conn = getPolardbxConnection()) {
            JdbcUtil.executeSuccess(conn, ENABLE_XPLAN_FEEDBACK);
            JdbcUtil.executeSuccess(conn, DISABLE_DIRECT_PLAN);
            JdbcUtil.executeSuccess(conn, DISABLE_POST_PLANNER);
            JdbcUtil.executeSuccess(conn,
                "baseline add sql /*+TDDL:cmd_extra()*/ " + MessageFormat.format(SK_SQL, XPLAN_TABLE));
            JdbcUtil.executeSuccess(conn,
                "baseline add sql /*+TDDL:cmd_extra()*/ " + MessageFormat.format(PK_SQL, XPLAN_TABLE));
            JdbcUtil.executeSuccess(conn,
                "baseline add sql /*+TDDL:cmd_extra()*/ " + MessageFormat.format(SK_PK_SQL, XPLAN_TABLE));
            assertExplainExecute(conn, MessageFormat.format(SK_SQL, XPLAN_TABLE), true);
            assertExplainExecute(conn, MessageFormat.format(PK_SQL, XPLAN_TABLE), false);
            assertExplainExecute(conn, MessageFormat.format(SK_PK_SQL, XPLAN_TABLE), true);

            // mock feedback
            JdbcUtil.executeSuccess(conn, "set " + ConnectionProperties.XPLAN_MAX_SCAN_ROWS + "= 0");
            JdbcUtil.executeSuccess(conn, MessageFormat.format(SK_SQL, XPLAN_TABLE));
            JdbcUtil.executeSuccess(conn, MessageFormat.format(PK_SQL, XPLAN_TABLE));
            JdbcUtil.executeSuccess(conn, MessageFormat.format(SK_PK_SQL, XPLAN_TABLE));
            JdbcUtil.executeSuccess(conn, "set " + ConnectionProperties.XPLAN_MAX_SCAN_ROWS + "= 1000");

            // xplan should be closed
            assertExplainExecute(conn, MessageFormat.format(SK_SQL, XPLAN_TABLE), false);
            assertExplainExecute(conn, MessageFormat.format(PK_SQL, XPLAN_TABLE), false);
            assertExplainExecute(conn, MessageFormat.format(SK_PK_SQL, XPLAN_TABLE), false);

            // disable feedback
            JdbcUtil.executeSuccess(conn, DISABLE_XPLAN_FEEDBACK);
            assertExplainExecute(conn, MessageFormat.format(SK_SQL, XPLAN_TABLE), true);
            assertExplainExecute(conn, MessageFormat.format(PK_SQL, XPLAN_TABLE), false);
            assertExplainExecute(conn, MessageFormat.format(SK_PK_SQL, XPLAN_TABLE), true);

            // enable ENABLE_XPLAN_FEEDBACK again
            JdbcUtil.executeSuccess(conn, ENABLE_XPLAN_FEEDBACK);
            assertExplainExecute(conn, MessageFormat.format(SK_SQL, XPLAN_TABLE), false);
            assertExplainExecute(conn, MessageFormat.format(PK_SQL, XPLAN_TABLE), false);
            assertExplainExecute(conn, MessageFormat.format(SK_PK_SQL, XPLAN_TABLE), false);

            // clear plancache and execute sql again
            JdbcUtil.executeSuccess(conn, CLEAR_CACHE);
            assertExplainExecute(conn, MessageFormat.format(SK_SQL, XPLAN_TABLE), true);
            assertExplainExecute(conn, MessageFormat.format(PK_SQL, XPLAN_TABLE), false);
            assertExplainExecute(conn, MessageFormat.format(SK_PK_SQL, XPLAN_TABLE), true);
            JdbcUtil.executeSuccess(conn, MessageFormat.format(SK_SQL, XPLAN_TABLE));
            JdbcUtil.executeSuccess(conn, MessageFormat.format(PK_SQL, XPLAN_TABLE));
            JdbcUtil.executeSuccess(conn, MessageFormat.format(SK_PK_SQL, XPLAN_TABLE));

            // no feedback should happen
            assertExplainExecute(conn, MessageFormat.format(SK_SQL, XPLAN_TABLE), true);
            assertExplainExecute(conn, MessageFormat.format(PK_SQL, XPLAN_TABLE), false);
            assertExplainExecute(conn, MessageFormat.format(SK_PK_SQL, XPLAN_TABLE), true);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        } finally {
            JdbcUtil.executeSuccess(tddlConnection, CLEAR_CACHE);
            JdbcUtil.executeSuccess(tddlConnection, CLEAR_SPM);
        }
    }

    private boolean explainExecuteXplan(Connection conn, String sql) {
        final List<List<String>> res =
            JdbcUtil.getAllStringResult(JdbcUtil.executeQuery("explain execute " + sql, conn), false,
                ImmutableList.of());
        boolean useXplan = (!StringUtils.isEmpty(res.get(0).get(11))) && res.get(0).get(11).contains("Using XPlan");
        if (useXplan) {
            Truth.assertThat(res.get(0).get(5)).contains(res.get(0).get(6));
            Truth.assertThat(res.get(0).get(6)).isNotEmpty();
            Truth.assertThat(Double.valueOf(res.get(0).get(10))).isAtMost(100D);
            Truth.assertThat(Double.valueOf(res.get(0).get(10))).isAtLeast(0D);
        }
        return useXplan;
    }

    private boolean traceXplan(Connection conn, String sql) {
        JdbcUtil.executeQuerySuccess(conn, "trace " + sql);
        final List<List<String>> res =
            JdbcUtil.getAllStringResult(JdbcUtil.executeQuery("show trace", conn), false, ImmutableList.of());
        final String trace = res.get(0).get(11);
        return trace.contains("/*PolarDB-X Connection*/") && trace.contains("plan_digest");
    }

    private void assertExplainExecute(Connection conn, String sql, boolean expected) {
        // check trace and explain execute
        boolean explainExecute = explainExecuteXplan(conn, sql);
        boolean trace = traceXplan(conn, sql);
        Truth.assertWithMessage("Bad trace " + sql)
            .that(trace)
            .isEqualTo(expected);

        Truth.assertWithMessage("Bad explain execute " + sql)
            .that(explainExecute)
            .isEqualTo(expected);
    }
}
