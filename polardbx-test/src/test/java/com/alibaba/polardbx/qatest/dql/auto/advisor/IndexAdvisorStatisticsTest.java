package com.alibaba.polardbx.qatest.dql.auto.advisor;

import com.alibaba.polardbx.optimizer.index.IndexAdvisor;
import com.alibaba.polardbx.qatest.AsyncDDLBaseNewDBTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.ResultSet;
import java.sql.SQLException;

import static com.google.common.truth.Truth.assertWithMessage;

public class IndexAdvisorStatisticsTest extends AsyncDDLBaseNewDBTestCase {

    private static final String TABLE1 = "indexAd1";

    private static final String TABLE2 = "indexAd2";

    private static final String DDL = "create table %s(a int)";

    private static final String SQL = String.format("explain advisor select * from %s, %s", TABLE1, TABLE2);

    private static final String EXPIRED = "Statistics of tables are expired!";

    private static final String HINT = "/*+TDDL:cmd_extra(ENABLE_CHECK_STATISTICS_EXPIRE=false)*/";

    private static final String ANALYZE = "analyze table %s";

    @Before
    public void buildTable() {
        JdbcUtil.executeUpdate(tddlConnection, String.format(DDL, TABLE1));
        JdbcUtil.executeUpdate(tddlConnection, String.format(DDL, TABLE2));
    }

    @After
    public void dropTable() {
        JdbcUtil.dropTable(tddlConnection, TABLE1);
        JdbcUtil.dropTable(tddlConnection, TABLE2);
    }

    @Test
    public void testAnalyze() throws SQLException {

        String analyze1 = String.format(IndexAdvisor.ANALYZE_FORMAT, tddlDatabase1, TABLE1).toLowerCase();
        String analyze2 = String.format(IndexAdvisor.ANALYZE_FORMAT, tddlDatabase1, TABLE2).toLowerCase();
        String sql = String.format(SQL, TABLE1, TABLE2);
        ResultSet rs;
        String info;

        rs = JdbcUtil.executeQuery(sql, tddlConnection);
        rs.next();
        info = rs.getString("INFO");
        assertWithMessage(info + "\ncontains\n" + EXPIRED).that(info.contains(EXPIRED)).isTrue();
        assertWithMessage(info + "\ncontains\n" + analyze1).that(info.contains(analyze1)).isTrue();
        assertWithMessage(info + "\ncontains\n" + analyze2).that(info.contains(analyze2)).isTrue();
        rs.close();

        // use disable check hint
        rs = JdbcUtil.executeQuery(HINT + sql, tddlConnection);
        rs.next();
        info = rs.getString("INFO");
        assertWithMessage(info + "\ncontains\n" + EXPIRED).that(info.contains(EXPIRED)).isFalse();
        rs.close();

        // analyze table1
        JdbcUtil.executeUpdate(tddlConnection, String.format(ANALYZE, TABLE1));
        rs = JdbcUtil.executeQuery(sql, tddlConnection);
        rs.next();
        info = rs.getString("INFO");
        assertWithMessage(info + "\ncontains\n" + EXPIRED).that(info.contains(EXPIRED)).isTrue();
        assertWithMessage(info + "\ncontains\n" + analyze1).that(info.contains(analyze1)).isFalse();
        assertWithMessage(info + "\ncontains\n" + analyze2).that(info.contains(analyze2)).isTrue();
        rs.close();

        // use disable check hint
        rs = JdbcUtil.executeQuery(HINT + sql, tddlConnection);
        rs.next();
        info = rs.getString("INFO");
        assertWithMessage(info + "\ncontains\n" + EXPIRED).that(info.contains(EXPIRED)).isFalse();
        rs.close();

        // analyze table2
        JdbcUtil.executeUpdate(tddlConnection, String.format(ANALYZE, TABLE2));
        rs = JdbcUtil.executeQuery(sql, tddlConnection);
        rs.next();
        info = rs.getString("INFO");
        assertWithMessage(info + "\ncontains\n" + EXPIRED).that(info.contains(EXPIRED)).isFalse();
        rs.close();
    }
}
