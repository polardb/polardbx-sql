package com.alibaba.polardbx.qatest.statistic;

import com.alibaba.polardbx.qatest.DDLBaseNewDBTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.google.common.collect.ImmutableList;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.yaml.snakeyaml.Yaml;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import static com.google.common.truth.Truth.assertWithMessage;

public class IntoOutfileStatisticsTest extends DDLBaseNewDBTestCase {
    private static final String hint = "/*+TDDL:cmd_extra(SELECT_INTO_OUTFILE_STATISTICS_DUMP=true)*/";

    private static final String hintIgnoreString = "/*+TDDL:cmd_extra(SELECT_INTO_OUTFILE_STATISTICS_DUMP=true, "
        + "STATISTICS_DUMP_IGNORE_STRING=true)*/";

    private static final String GET_YML = "%s into outfile 'test.yml' statistics";

    private static final String GET_SQL = "%s into outfile 'test.sql' statistics";

    private static final String GET_YML_RESULT = hint + GET_YML;

    private static final String GET_SQL_RESULT = hint + GET_SQL;

    private static final String GET_SQL_IGNORE_STRING_RESULT = hintIgnoreString + GET_SQL;

    private static String singleTable = "t_s";

    private static String broadcastTable = "t_b";

    private static String partitionTable = "t_p";

    private static String tgName = "tg_shengyu123";

    private static String partitionTableWithTg = "t_p_tg";

    private static final String TABLE_TEMPLATE = "create table %s(pk int(11), id int(11) default null,"
        + " ex decimal(11,3) default null, attr varchar(128) default null, primary key(pk), index id_idx(id)) %s";

    private static final String INSERT = "insert into %s(pk, id, ex, attr) values (1,2,1.3,'test'),(30,20,2.4,'try')";

    private static final String ANALYZE = "analyze table %s";

    private static final String JOIN = "select * from %s a join %s b on a.pk = b.pk join %s c on a.id = c.id";

    private static final String SUBQUERY = "select * from %s a where a.id not in "
        + "(select c.pk from %s b join %s c on b.id = c.id where a.pk < b.pk)";
    private static final String POINT = "select * from %s a where id < 10";

    private static final String FORCE_INDEX = "select pk from %s force index(id_idx) a";

    private static final String VIEW_TEMPLATE =
        "create view %s as select a.pk, b.id, c.ex from %s a join %s b on a.pk = b.pk join %s c on a.id = c.id";

    public IntoOutfileStatisticsTest() {
        this.crossSchema = true;
    }

    public boolean usingNewPartDb() {
        return true;
    }

    @Before
    public void buildTables() {
        for (Connection conn : ImmutableList.of(getTddlConnection1(), getTddlConnection2())) {
            dropTableIfExists(conn, singleTable);
            dropTableIfExists(conn, broadcastTable);
            dropTableIfExists(conn, partitionTable);
            JdbcUtil.executeUpdateSuccess(conn, "drop tablegroup if exists " + tgName);

            JdbcUtil.executeSuccess(conn, String.format(TABLE_TEMPLATE, singleTable, "single"));
            JdbcUtil.executeSuccess(conn, String.format(INSERT, singleTable));
            JdbcUtil.executeSuccess(conn, String.format(ANALYZE, singleTable));

            JdbcUtil.executeSuccess(conn, String.format(TABLE_TEMPLATE, broadcastTable, "broadcast"));
            JdbcUtil.executeSuccess(conn, String.format(INSERT, broadcastTable));
            JdbcUtil.executeSuccess(conn, String.format(ANALYZE, broadcastTable));

            JdbcUtil.executeSuccess(conn, String.format(TABLE_TEMPLATE, partitionTable, ""));
            JdbcUtil.executeSuccess(conn, String.format(INSERT, partitionTable));
            JdbcUtil.executeSuccess(conn, String.format(ANALYZE, partitionTable));

            JdbcUtil.executeUpdateSuccess(conn, "create tablegroup " + tgName);
            JdbcUtil.executeSuccess(conn, String.format(TABLE_TEMPLATE, partitionTableWithTg, "tablegroup=" + tgName));
            JdbcUtil.executeSuccess(conn, String.format(INSERT, partitionTableWithTg));
            JdbcUtil.executeSuccess(conn, String.format(ANALYZE, partitionTableWithTg));
        }

    }

    @After
    public void afterDDLBaseNewDBTestCase() {
        cleanDataBase();
    }

    @Test
    public void testBase() {
        String sql;

        // point sql
        sql = String.format(POINT, singleTable);
        checkCommon(getTddlConnection1(), sql, singleTable);
        sql = String.format(POINT, broadcastTable);
        checkCommon(getTddlConnection1(), sql, broadcastTable);
        sql = String.format(POINT, partitionTable);
        checkCommon(getTddlConnection1(), sql, partitionTable);
        checkIgnoreString(getTddlConnection1(), sql);

        // force index sql
        sql = String.format(FORCE_INDEX, partitionTable);
        checkCommon(getTddlConnection1(), sql, partitionTable);

        // join sql
        sql = String.format(JOIN, singleTable, broadcastTable, partitionTable);
        checkCommon(getTddlConnection1(), sql, broadcastTable);
        sql = String.format(JOIN, singleTable, broadcastTable, tddlDatabase2 + "." + partitionTable);
        checkCommon(getTddlConnection1(), sql, partitionTable);

        // subquery sql
        sql = String.format(SUBQUERY, broadcastTable, partitionTable, partitionTable);
        checkCommon(getTddlConnection1(), sql, partitionTable);
        sql = String.format(JOIN, singleTable, broadcastTable, tddlDatabase2 + "." + partitionTable);
        checkCommon(getTddlConnection1(), sql, partitionTable);
    }

    @Test
    public void testView() {
        String sql;
        String view = "viewa";
        sql = "drop view if exists " + view;
        JdbcUtil.executeUpdateSuccess(getTddlConnection1(), sql);

        JdbcUtil.executeSuccess(getTddlConnection1(),
            String.format(VIEW_TEMPLATE, view, singleTable, tddlDatabase2 + "." + partitionTable, partitionTable));
        // point sql
        sql = String.format(POINT, tddlDatabase1 + "." + view);
        checkCommon(getTddlConnection2(), sql, partitionTable);

        // join sql
        sql = String.format(JOIN, singleTable, tddlDatabase1 + "." + view, partitionTable);
        checkCommon(getTddlConnection2(), sql, partitionTable);

        // subquery sql
        sql = String.format(SUBQUERY, broadcastTable, partitionTable, tddlDatabase1 + "." + view);
        checkCommon(getTddlConnection2(), sql, broadcastTable);
    }

    @Test
    public void testTableGroup() {
        String sql;
        // join sql
        sql = String.format(JOIN, singleTable, partitionTableWithTg, partitionTable);
        String result = getSQLResult(getTddlConnection2(), sql);
        assertWithMessage("缺少create table group").that(result).contains("CREATE TABLEGROUP IF NOT EXISTS");
        assertWithMessage("缺少table group").that(result).contains("TABLEGROUP =");
        assertWithMessage("缺少table group" + tgName).that(result).contains(tgName);
        JdbcUtil.executeSuccess(getTddlConnection2(), result);
    }

    private void checkCommon(Connection conn, String sql, String content) {
        checkYml(conn, sql);
        checkSQL(conn, sql, content);
        assertWithMessage("yml文件不长于sql文件").that(getYmlAffectRow(conn, sql))
            .isGreaterThan((getSQLAffectRow(conn, sql)));
    }

    private void checkIgnoreString(Connection conn, String sql) {
        String result = getSQLResult(conn, sql);
        String resultWithoutString = getSqlIgnoreResult(conn, sql);

        assertWithMessage("string类型未被过滤").that(result.length())
            .isGreaterThan(resultWithoutString.length());
    }

    private String getYmlResult(Connection conn, String sql) {
        return getResult(conn, GET_YML_RESULT, sql);
    }

    private String getSQLResult(Connection conn, String sql) {
        return getResult(conn, GET_SQL_RESULT, sql);
    }

    private String getSqlIgnoreResult(Connection conn, String sql) {
        return getResult(conn, GET_SQL_IGNORE_STRING_RESULT, sql);
    }

    private Long getYmlAffectRow(Connection conn, String sql) {
        return getAffectRow(conn, GET_YML, sql);
    }

    private Long getSQLAffectRow(Connection conn, String sql) {
        return getAffectRow(conn, GET_SQL, sql);
    }

    private String getResult(Connection conn, String template, String sql) {
        try (ResultSet rs = JdbcUtil.executeQuery(String.format(template, sql), conn)) {
            rs.next();
            return rs.getString("RESULT");
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private Long getAffectRow(Connection conn, String template, String sql) {
        try (ResultSet rs = JdbcUtil.executeQuery(String.format(template, sql), conn)) {
            rs.next();
            return rs.getLong("AFFECT_ROW");
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private void checkSQL(Connection conn, String targetSql, String content) {
        String result = getSQLResult(conn, targetSql);
        assertWithMessage("缺少reload").that(result).contains("reload statistics");
        assertWithMessage("缺少reload").that(result).contains(content);
    }

    private void checkYml(Connection conn, String targetSql) {
        String result = getYmlResult(conn, targetSql);
        Yaml yaml = new Yaml();
        List<Map<String, String>> list = yaml.loadAs(result, List.class);
        Map<String, String> map = list.get(0);
        String trace = map.get("STATISTIC_TRACE");
        String sql = map.get("SQL");
        String plan = map.get("PLAN");
        String catalog = map.get("CATALOG");

        // todo check yaml result in a better way
        assertWithMessage("缺少STATISTIC_TRACE").that(trace).isNotNull();
        assertWithMessage("缺少SQL").that(sql).isNotNull();
        assertWithMessage("缺少PLAN").that(plan).isNotNull();
        assertWithMessage("缺少CATALOG").that(catalog).isNotNull();
    }
}
