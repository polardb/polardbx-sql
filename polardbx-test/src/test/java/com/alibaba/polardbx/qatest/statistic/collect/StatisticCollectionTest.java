package com.alibaba.polardbx.qatest.statistic.collect;

import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.qatest.BaseTestCase;
import com.alibaba.polardbx.qatest.util.PropertiesUtil;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static com.alibaba.polardbx.optimizer.config.table.statistic.StatisticUtils.digestForStatisticTrace;

/**
 * @author fangwu
 */
@RunWith(Parameterized.class)
public class StatisticCollectionTest extends BaseTestCase {
    public static final Log log = LogFactory.getLog("root");
    static Map<String, Set<Object>> sampleObjects = Maps.newConcurrentMap();

    private final String schema;
    private final String tbl;
    private final String upLimitName;

    public StatisticCollectionTest(String schema, String tbl, String upLimitName) {
        this.schema = schema;
        this.tbl = tbl;
        this.upLimitName = upLimitName;
    }

    /**
     * manual test
     */
    @Parameterized.Parameters(name = "{index}: [{0}] {1} {2}")
    public static Iterable<Object[]> params() throws SQLException {
        List<Object[]> params = Lists.newLinkedList();

        params.add(new Object[] {"drds_polarx1_part_qatest_app", "update_delete_base_two_one_db_one_tb", ""});
        return params;
    }

    @Test
    public void testCollection() throws SQLException, IOException {
        // use druid
        PropertiesUtil.useDruid = true;
        try {
            log.info("start test statistic collection :" + schema + "," + tbl);
            sendAnalyze(schema, tbl);
            // check tbl rowcount
            checkTblRowCount(schema, tbl);
            // check column ndv
            Collection<String> columns = checkColumnNdv(schema, tbl);
            // check column null count
            checkColumnNullCount(schema, tbl, columns);
            // check column value frequency
            checkColumnValueFrequency(schema, tbl, columns);
        } catch (SQLException e) {
            log.error(e);
            throw e;
        } finally {
            log.info(AccuracyQuantifier.QUANTIFIER_TYPE.judgeAndReport(upLimitName));
            AccuracyQuantifier.QUANTIFIER_TYPE.clean(upLimitName);
        }
    }

    private void checkColumnValueFrequency(String schema, String tbl, Collection<String> columns) throws SQLException {
        // get max , min and middle value
        for (String column : columns) {
            log.info("check column value frequency, " + schema + "," + tbl + "," + column);
            // get equal values
            Set<Object> values = getFrequencyValues(schema, tbl, column);
            // check frequency accurate
            checkColumnEqualFrequency(schema, tbl, column, values);

            // check frequency range
            if (values.size() <= 1) {
                continue;
            }

            // Avoid range check for binary type column
            String type = getColumnType(schema, tbl, column);
            if (needSkip(type)) {
                continue;
            }

            // check column range value frequency
            List list = (List) values.stream().sorted(new DataComparator(type)).collect(Collectors.toList());
            for (int i = 0; i < values.size() - 1; i++) {
                checkColumnRangeFrequency(schema, tbl, column, list.get(i), list.get(i + 1));
            }
        }
    }

    private boolean needSkip(String type) {
        if (type.contains("blob")) {
            return true;
        }
        if (type.contains("clob")) {
            return true;
        }
        if (type.contains("binary")) {
            return true;
        }
        if (type.contains("text")) {
            return true;
        }
        if (type.contains("char")) {
            return true;
        }
        return false;
    }

    private String getColumnType(String schema, String tbl, String column) throws SQLException {
        if (typeCache.containsKey(schema + "_" + tbl + "_" + column)) {
            return typeCache.get(schema + "_" + tbl + "_" + column);
        } else {
            try (Connection c = this.getPolardbxConnection()) {
                c.createStatement().execute("use " + schema);
                ResultSet rs = c.createStatement().executeQuery("desc `" + tbl + "`");
                while (rs.next()) {
                    String field = rs.getString("field");
                    String type = rs.getString("type").toLowerCase(Locale.ROOT);
                    typeCache.put(schema + "_" + tbl + "_" + column, type);
                    if (field.equalsIgnoreCase(column)) {
                        return type;
                    }
                }
            }
        }
        return null;
    }

    private void checkColumnRangeFrequency(String schema, String tbl, String column, Object start, Object end) {
        if (start == null || end == null) {
            return;
        }
        log.info("check range frequency, " + schema + "," + tbl + "," + column + ":" + start + "," + end);
        try (Connection c = this.getPolardbxConnection()) {
            c.setSchema(schema);
            c.createStatement().executeQuery("use " + schema);
            PreparedStatement ps = c.prepareStatement(
                "explain cost_trace /*TDDL:ENABLE_DIRECT_PLAN=FALSE*/ select 1 from `" + tbl + "` where `" + column
                    + "` between ? and ?");
            ps.setObject(1, start.toString());
            ps.setObject(2, end.toString());
            ResultSet rs = ps.executeQuery();
            while (rs.next()) {
                String content = rs.getString(1);
                if (content.contains("STATISTIC TRACE INFO")) {
                    String startObj = getStringVal(start);
                    String endObj = getStringVal(end);
                    String targetPre =
                        "Catalog:" + schema + "," + tbl + "," + column + "," + digestForStatisticTrace(startObj) + "_"
                            + digestForStatisticTrace(endObj)
                            + "\nAction:getRangeCount\nStatisticValue:";
                    int indexStart = content.indexOf(targetPre) + targetPre.length();
                    int indexEnd = content.indexOf("\n", indexStart);

                    long statisticRs = Long.parseLong(content.substring(indexStart, indexEnd));

                    judgeRangeCount(schema, tbl, column, start, end, statisticRs);
                }

            }
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail(e.getMessage());
        }
    }

    private void judgeRangeCount(String schema, String tbl, String column, Object start, Object end, long estimate) {
        // nest connection, must use druid
        try (Connection c = this.getPolardbxConnection()) {
            c.setSchema(schema);
            c.createStatement().executeQuery("use " + schema);
            PreparedStatement ps = c.prepareStatement(
                "select count(1) from `" + tbl + "` force index(primary) where `" + column + "` between ? and ? or `"
                    + column + "` like ? or `" + column + "` like ?");
            ps.setObject(1, getStringVal(start));
            ps.setObject(2, getStringVal(end));
            ps.setObject(3, getStringVal(start));
            ps.setObject(4, getStringVal(end));
            ResultSet rs = ps.executeQuery();
            rs.next();
            long actual = rs.getLong(1);
            log.info("judge range count," + schema + "," + tbl + "," + column + "," + start + "," + end + ":" + estimate
                + "," + actual + "," + Math.abs(estimate - actual) / (double) actual);
            AccuracyQuantifier.getInstance()
                .feed(upLimitName, AccuracyQuantifier.QUANTIFIER_TYPE.FREQUENCY_RANGE, estimate, actual);
//            Assert.assertTrue(
//                Math.abs(estimate - actual) <= upperBound || Math.abs(estimate - actual) / (double) actual < 0.15D);
        } catch (SQLException e) {
            e.printStackTrace();
            Assert.fail(e.getMessage());
        }
    }

    static class DataComparator implements Comparator {
        private String type;

        public DataComparator(String type) {
            this.type = type;
        }

        @Override
        public int compare(Object o1, Object o2) {
            if (numberType(type)) {
                double d1 = Double.parseDouble(o1.toString());
                double d2 = Double.parseDouble(o2.toString());
                return Double.compare(d1, d2);
            }
            return String.CASE_INSENSITIVE_ORDER.compare(o1.toString(), o2.toString());
        }

        private boolean numberType(String type) {
            if (type.contains("int") ||
                type.contains("float") ||
                type.contains("double") ||
                type.contains("decimal")) {
                return true;
            }
            return false;
        }

    }

    private Set<Object> getFrequencyValues(String schema, String tbl, String column) {
        if (sampleObjects.containsKey(schema + ":" + tbl + ":" + column)) {
            return sampleObjects.get(schema + ":" + tbl + ":" + column);
        }
        Set<Object> values = Sets.newHashSet();
        Object max = null;
        Object min = null;
        Object middleValue = null;
        try (Connection c = this.getPolardbxConnection()) {
            c.setSchema(schema);
            c.createStatement().executeQuery("use " + schema);
            long count = 0;
            ResultSet rs = c.createStatement().executeQuery(
                "select max(`" + column + "`) as max,  min(`" + column + "`) as min, count(*) as count from `" + tbl
                    + "`");
            while (rs.next()) {
                max = rs.getObject("max");
                min = rs.getObject("min");
                count = rs.getLong("count");
                if (count == 0) {
                    return values;
                }
                if (max instanceof Boolean) {
                    if ((Boolean) max) {
                        values.add(1);
                    } else {
                        values.add(0);
                    }
                } else {
                    if (max != null) {
                        values.add(max);
                    }
                }

                if (min instanceof Boolean) {
                    if ((Boolean) min) {
                        values.add(1);
                    } else {
                        values.add(0);
                    }
                } else {
                    if (min != null) {
                        values.add(min);
                    }
                }
            }
            rs.close();
            String sql =
                "select `" + column + "` from (select `" + column
                    + "`, ROW_NUMBER() OVER() AS row_number from (select `"
                    + column + "` from `" + tbl + "` order by `" + column + "`)a)t where row_number in(FLOOR(" + count
                    + " / 2), CEIL(" + count + "/2))";
            System.out.println(sql);
            rs = c.createStatement().executeQuery(sql);
            while (rs.next()) {
                middleValue = rs.getObject(1);
                if (middleValue instanceof Boolean) {
                    if ((Boolean) middleValue) {
                        values.add(1);
                    } else {
                        values.add(0);
                    }
                } else {
                    if (middleValue != null) {
                        values.add(middleValue);
                    }
                }
            }
            rs.close();
        } catch (SQLException e) {
            e.printStackTrace();
            Assert.fail(e.getMessage());
        } finally {
            log.info(
                "get equal values:" + schema + "," + tbl + "," + column + ", max:" + max + ", min:" + min + ",middle:"
                    + middleValue);
        }
        return values;
    }

    private void checkColumnEqualFrequency(String schema, String tbl, String column, Set<Object> equalValues) {
        if (equalValues == null || equalValues.isEmpty()) {
            return;
        }
        equalValues.remove(null);
        log.info("check column equal frequency, " + schema + "," + tbl + "," + column + "," + equalValues);
        List<String> quests = Lists.newArrayList();
        equalValues.forEach(e -> quests.add("?"));
        try (Connection c = this.getPolardbxConnection()) {
            c.setSchema(schema);
            c.createStatement().executeQuery("use " + schema);
            PreparedStatement ps = c.prepareStatement(
                "explain cost_trace /*TDDL:ENABLE_DIRECT_PLAN=FALSE*/ select 1 from `" + tbl + "` where `" + column
                    + "` in (" + String.join(",", quests)
                    + ")");
            int index = 1;
            for (Object o : equalValues) {
                ps.setObject(index++, o.toString());
            }
            ResultSet rs = ps.executeQuery();
            while (rs.next()) {
                String content = rs.getString(1);
                if (content.contains("STATISTIC TRACE INFO")) {
                    for (Object o : equalValues) {
                        if (o == null) {
                            continue;
                        }
                        String obj = getStringVal(o);
                        String targetPre =
                            "Catalog:" + schema + "," + tbl + "," + column + "," + obj
                                + "\nAction:getFrequency\nStatisticValue:";
                        int indexStart = content.indexOf(targetPre) + targetPre.length();

                        long statisticRs;
                        if (indexStart == targetPre.length() - 1) {
                            // meaning uniq constrain make it equals to value count
                            statisticRs = 1;
                        } else {
                            int indexEnd = content.indexOf("\n", indexStart);
                            statisticRs = (long) Double.parseDouble(content.substring(indexStart, indexEnd));
                        }
                        if (statisticRs == -1) {
                            continue;
                        }

                        judgeEqualCount(schema, tbl, column, obj, statisticRs);
                    }

                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
            Assert.fail(e.getMessage());
        }
    }

    private void judgeEqualCount(String schema, String tbl, String column, String value, long estimate) {
        // nest connection, must use druid
        try (Connection c = this.getPolardbxConnection()) {
            c.setSchema(schema);
            c.createStatement().executeQuery("use " + schema);
            String sql;

            if (isFloatType(schema, tbl, column)) {
                sql = "select count(1) from `" + tbl + "` force index(primary) where `" + column + "` like '" + value
                    + "'";
            } else {
                sql =
                    "select count(1) from `" + tbl + "` force index(primary) where `" + column + "` = '" + value + "'";
            }
            ResultSet rs = c.createStatement().executeQuery(sql);
            rs.next();
            long actual = rs.getLong(1);

            log.info("judge equal count, " + schema + "," + tbl + "," + column + "," + value + ":" + estimate + ","
                + actual + "," + Math.abs(estimate - actual) / (double) actual);
            AccuracyQuantifier.getInstance()
                .feed(upLimitName, AccuracyQuantifier.QUANTIFIER_TYPE.FREQUENCY_EQUAL, estimate, actual);
        } catch (SQLException e) {
            e.printStackTrace();
            Assert.fail(e.getMessage());
        }
    }

    private Map<String, String> typeCache = Maps.newConcurrentMap();

    private boolean isFloatType(String schema, String tbl, String column) throws SQLException {
        String type = null;
        if (typeCache.containsKey(schema + "_" + tbl + "_" + column)) {
            type = typeCache.get(schema + "_" + tbl + "_" + column);
        } else {
            try (Connection c = this.getPolardbxConnection()) {
                c.createStatement().execute("use " + schema);
                ResultSet rs = c.createStatement().executeQuery("desc " + tbl);
                while (rs.next()) {
                    String field = rs.getString("field");
                    type = rs.getString("type").toLowerCase(Locale.ROOT);
                    typeCache.put(schema + "_" + tbl + "_" + column, type);
                    if (field.equalsIgnoreCase(column)) {
                        break;
                    }
                }
            }
        }

        if (type.contains("float")) {
            return true;
        }
        if (type.contains("double")) {
            return true;
        }
        if (type.contains("decimal")) {
            return true;
        }
        return false;
    }

    private static String getStringVal(Object o) {
        return o.toString();
    }

    private void checkColumnNullCount(String schema, String tbl, Collection<String> columns) {
        for (String column : columns) {
            log.info("check column null count, " + schema + "," + tbl + "," + column);
            try (Connection c = this.getPolardbxConnection()) {
                c.setSchema(schema);
                c.createStatement().executeQuery("use " + schema);
                ResultSet rs = c.createStatement()
                    .executeQuery(
                        "explain cost_trace /*TDDL:ENABLE_DIRECT_PLAN=FALSE*/ select 1 from `" + tbl + "` where `"
                            + column
                            + "` is null");
                while (rs.next()) {
                    String content = rs.getString(1);
                    if (content.contains("STATISTIC TRACE INFO")) {
                        String targetPre =
                            "Catalog:" + schema + "," + tbl + "," + column + "\nAction:getNullCount\nStatisticValue:";
                        int indexStart = content.indexOf(targetPre) + targetPre.length();
                        long columnNullCount;
                        if (indexStart == targetPre.length() - 1) {
                            // meaning distinct is eliminated cause of not null properties
                            columnNullCount = 0;
                        } else {
                            int indexEnd = content.indexOf("\n", indexStart);
                            columnNullCount = Long.parseLong(content.substring(indexStart, indexEnd));
                        }

                        if (columnNullCount == -1) {
                            continue;
                        }

                        judgeNullCount(schema, tbl, column, columnNullCount);
                    }
                }
            } catch (SQLException e) {
                e.printStackTrace();
                Assert.fail(e.getMessage());
            }
        }
    }

    private Collection<String> checkColumnNdv(String schema, String tbl) {
        Collection<String> columns = getColumns(schema, tbl);
        log.info("get columns, " + schema + "," + tbl + ":" + columns);
        for (String column : columns) {
            try (Connection c = this.getPolardbxConnection()) {
                c.setSchema(schema);
                c.createStatement().executeQuery("use " + schema);
                ResultSet rs = c.createStatement()
                    .executeQuery(
                        "explain cost_trace /*TDDL:ENABLE_DIRECT_PLAN=FALSE*/ select distinct `" + column + "` from `"
                            + tbl + "` limit 1");
                while (rs.next()) {
                    String content = rs.getString(1);
                    if (content.contains("STATISTIC TRACE INFO")) {
                        String targetPre =
                            "Catalog:" + schema + "," + tbl + "," + column + "\nAction:getCardinality\nStatisticValue:";
                        int indexStart = content.indexOf(targetPre) + targetPre.length();
                        if (indexStart == targetPre.length() - 1) {
                            // meaning distinct is eliminated cause of unique key
                            targetPre = "Catalog:" + schema + "," + tbl + "\nAction:getRowCount\nStatisticValue:";
                            indexStart = content.indexOf(targetPre) + targetPre.length();
                        }
                        int indexEnd = content.indexOf("\n", indexStart);

                        long columnNdv = Long.parseLong(content.substring(indexStart, indexEnd));

                        judgeNdv(schema, tbl, column, columnNdv);
                    }
                }
            } catch (SQLException e) {
                e.printStackTrace();
                Assert.fail(e.getMessage());
            }
        }
        return columns;
    }

    private void checkTblRowCount(String schema, String tbl) {
        log.info("check table rowcount :" + schema + "," + tbl);
        try (Connection c = this.getPolardbxConnection()) {
            c.setSchema(schema);
            c.createStatement().executeQuery("use " + schema);
            ResultSet rs = c.createStatement()
                .executeQuery(
                    "explain cost_trace /*TDDL:ENABLE_DIRECT_PLAN=FALSE*/ select 1 from `" + tbl + "` limit 1");
            while (rs.next()) {
                String content = rs.getString(1);
                if (content.contains("STATISTIC TRACE INFO")) {
                    String targetPre = "Catalog:" + schema + "," + tbl + "\nAction:getRowCount\nStatisticValue:";
                    int indexStart = content.indexOf(targetPre) + targetPre.length();
                    int indexEnd = content.indexOf("\n", indexStart);
                    long tblRowcount = Long.parseLong(content.substring(indexStart, indexEnd));

                    judgeRowCount(schema, tbl, tblRowcount);
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
            Assert.fail(e.getMessage());
        }
    }

    private void judgeNullCount(String schema, String tbl, String column, long estimate) {
        // nest connection, must use druid
        try (Connection c = this.getPolardbxConnection()) {
            c.setSchema(schema);
            c.createStatement().executeQuery("use " + schema);
            ResultSet rs = c.createStatement()
                .executeQuery("select count(1) from `" + tbl + "` force index(primary) where `" + column + "` is null");
            rs.next();
            long actual = rs.getLong(1);
            log.info("judge null count, " + schema + "," + tbl + "," + column + ":" + estimate + "," + actual + ","
                + Math.abs(estimate - actual) / (double) actual);
            AccuracyQuantifier.getInstance()
                .feed(upLimitName, AccuracyQuantifier.QUANTIFIER_TYPE.NULL_COUNT, estimate, actual);
        } catch (SQLException e) {
            e.printStackTrace();
            Assert.fail(e.getMessage());
        }
    }

    private void judgeNdv(String schema, String tbl, String column, long estimateNdv) {
        // nest connection, must use druid
        try (Connection c = this.getPolardbxConnection()) {
            c.setSchema(schema);
            c.createStatement().executeQuery("use " + schema);
            ResultSet rs = c.createStatement()
                .executeQuery("select count(distinct `" + column + "`) from `" + tbl + "` force index(primary)");
            rs.next();
            long actualNdv = rs.getLong(1);
            log.info("judge ndv, " + schema + "," + tbl + "," + column + ":" + estimateNdv + ", " + actualNdv + ","
                + (double) estimateNdv / actualNdv);
            AccuracyQuantifier.getInstance()
                .feed(upLimitName, AccuracyQuantifier.QUANTIFIER_TYPE.NDV, estimateNdv, actualNdv);
        } catch (SQLException e) {
            e.printStackTrace();
            Assert.fail(e.getMessage());
        }
    }

    private void judgeRowCount(String schema, String tbl, long estimate) {
        // nest connection, must use druid
        try (Connection c = this.getPolardbxConnection()) {
            c.setSchema(schema);
            c.createStatement().executeQuery("use " + schema);
            ResultSet rs = c.createStatement().executeQuery("select count(1) from `" + tbl + "` force index(primary)");
            rs.next();
            long actual = rs.getLong(1);
            log.info("judge rowcount :" + schema + "," + tbl + "," + estimate + "," + actual);
            AccuracyQuantifier.getInstance()
                .feed(upLimitName, AccuracyQuantifier.QUANTIFIER_TYPE.ROWCOUNT, estimate, actual);
        } catch (SQLException e) {
            e.printStackTrace();
            Assert.fail(e.getMessage());
        }
    }

    private void sendAnalyze(String schema, String tbl) {
        try (Connection c = this.getPolardbxConnection()) {
            c.setSchema(schema);
            c.createStatement().executeQuery("use " + schema);
            log.info("analyze table " + schema + "," + tbl);
            Assert.assertTrue(c.createStatement().execute("analyze table `" + tbl + "`"));
        } catch (SQLException e) {
            e.printStackTrace();
            Assert.fail(e.getMessage());
        }
    }

    private Collection<String> getColumns(String schema, String tbl) {
        Collection<String> columns = Sets.newHashSet();
        try (Connection c = this.getPolardbxConnection()) {
            c.setSchema(schema);
            c.createStatement().executeQuery("use " + schema);
            ResultSet resultSet = c.createStatement().executeQuery("desc `" + tbl + "`");
            while (resultSet.next()) {
                String type = resultSet.getString("type").toLowerCase(Locale.ROOT);
                // not support geom collection test yet
                if (type.contains("geom")) {
                    continue;
                }
                if (type.contains("polygon")) {
                    continue;
                }
                if (type.contains("point")) {
                    continue;
                }
                if (type.contains("linestring")) {
                    continue;
                }
                columns.add(resultSet.getString("Field"));
            }
        } catch (SQLException e) {
            e.printStackTrace();
            Assert.fail(e.getMessage());
        }
        return columns;
    }

}
