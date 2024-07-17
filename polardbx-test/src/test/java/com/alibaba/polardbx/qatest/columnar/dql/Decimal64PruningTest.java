package com.alibaba.polardbx.qatest.columnar.dql;

import com.alibaba.polardbx.qatest.AutoReadBaseTestCase;
import com.alibaba.polardbx.qatest.util.ConnectionManager;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.alibaba.polardbx.qatest.validator.DataValidator;
import org.apache.commons.lang3.StringUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static com.alibaba.polardbx.qatest.util.PropertiesUtil.polardbXAutoDBName1;

public class Decimal64PruningTest extends ColumnarReadBaseTestCase {

    private static final String TABLE_1 = "test_dec_orc_prune_1";
    private static final String CREATE_TABLE =
        "create table %s (c1 int primary key, c2 decimal(16, 2)) partition by hash(c1)";

    private static final String DROP_TABLE = "drop table if exists %s";

    private static final String INSERT_DATA = "insert into %s values (%s, '%s')";
    private static final String SELECT_TEMPLATE = "select * from %s where ";
    private static final String SELECT_PRIMARY_TEMPLATE = "select * from %s force index(primary) where ";

    private static final int COUNT = 100;
    private static final List<BigDecimal> ORDERED_VALUE_LIST = new ArrayList<>(COUNT);
    private static final Random random = new Random();

    @BeforeClass
    public static void prepare() throws SQLException {
        dropTable();
        prepareData();
    }

    @AfterClass
    public static void dropTable() throws SQLException {
        try (Connection tddlConnection = getColumnarConnection()) {
            JdbcUtil.executeSuccess(tddlConnection, String.format(DROP_TABLE, TABLE_1));
        }
    }

    private static void prepareData() throws SQLException {
        try (Connection tddlConnection = getColumnarConnection()) {
            // create table
            JdbcUtil.executeSuccess(tddlConnection, String.format(CREATE_TABLE, TABLE_1));
            // insert data
            for (int i = 0; i < 100; ++i) {
                String valStr = String.format("%d.%d", i + 100, i);
                JdbcUtil.executeSuccess(tddlConnection, String.format(INSERT_DATA, TABLE_1, i, valStr));
                BigDecimal valDec = new BigDecimal(valStr);
                if (!ORDERED_VALUE_LIST.isEmpty()) {
                    Assert.assertTrue("List should be in order",
                        ORDERED_VALUE_LIST.get(ORDERED_VALUE_LIST.size() - 1).compareTo(valDec) < 0);
                }
                ORDERED_VALUE_LIST.add(valDec);
            }
            // create columnar index
            ColumnarUtils.createColumnarIndex(tddlConnection, "col_" + TABLE_1, TABLE_1,
                "c1", "c1", 4);
        }
    }

    @Test
    public void testEqualPrune() {
        final String conditionInExplain = "WHERE (`c2` = ?)";

        BigDecimal val = ORDERED_VALUE_LIST.get(random.nextInt(COUNT));
        // exist
        String condition =
            String.format(" c2 = %s", val.toPlainString());
        checkOssScanExplainContains(condition, conditionInExplain);
        checkWithCondition(condition, false);

        // exist with higher scale, test decimal64 rounded value pruning
        condition =
            String.format(" c2 = %s", val.toPlainString() + "000");
        checkOssScanExplainContains(condition, conditionInExplain);
        checkWithCondition(condition, false);

        // exist with lower scale
        val = ORDERED_VALUE_LIST.get(0);
        int fracIdx = val.toPlainString().indexOf(".0");
        Assert.assertTrue(fracIdx > 0);
        condition =
            String.format(" c2 = %s", val.toPlainString().substring(0, fracIdx));
        checkOssScanExplainContains(condition, conditionInExplain);
        checkWithCondition(condition, false);

        // not exist
        val = ORDERED_VALUE_LIST.get(0).subtract(BigDecimal.ONE);
        condition =
            String.format(" c2 = %s", val.toPlainString());
        checkOssScanExplainContains(condition, conditionInExplain);
        checkWithCondition(condition, true);

        // not exist with higher scale, test decimal64 rounded value pruning
        condition =
            String.format(" c2 = %s", val.toPlainString() + "01");
        checkOssScanExplainContains(condition, conditionInExplain);
        checkWithCondition(condition, true);

        // not exist with lower scale
        val = ORDERED_VALUE_LIST.get(1);
        fracIdx = val.toPlainString().indexOf(".1");
        Assert.assertTrue(fracIdx > 0);
        condition =
            String.format(" c2 = %s", val.toPlainString().substring(0, fracIdx));
        checkOssScanExplainContains(condition, conditionInExplain);
        checkWithCondition(condition, true);
    }

    @Test
    public void testBetweenPrune() {
        final String conditionInExplain = "WHERE (`c2` BETWEEN ? AND ?)";

        BigDecimal val = ORDERED_VALUE_LIST.get(random.nextInt(COUNT));
        BigDecimal minVal = ORDERED_VALUE_LIST.get(0);
        BigDecimal maxVal = ORDERED_VALUE_LIST.get(ORDERED_VALUE_LIST.size() - 1);

        // exist
        String sql = String.format(" c2 between %s and %s", val.toPlainString(), maxVal.toPlainString());
        checkOssScanExplainContains(sql, conditionInExplain);
        checkWithCondition(sql, false);
        // exist
        sql = String.format(" c2 between %s and %s", minVal.toPlainString(), val.toPlainString());
        checkOssScanExplainContains(sql, conditionInExplain);
        checkWithCondition(sql, false);
        // exist with same value
        sql = String.format(" c2 between %s and %s", val.toPlainString(), val.toPlainString());
        checkOssScanExplainContains(sql, conditionInExplain);
        checkWithCondition(sql, false);
        // exist with boundary value
        sql = String.format(" c2 between %s and %s", minVal.subtract(BigDecimal.ONE).toPlainString(),
            minVal.toPlainString());
        checkOssScanExplainContains(sql, conditionInExplain);
        checkWithCondition(sql, false);
        // exist with different scale
        sql = String.format(" c2 between %s and %s", minVal.subtract(BigDecimal.ONE).toPlainString(),
            minVal.toPlainString() + "05");
        checkOssScanExplainContains(sql, conditionInExplain);
        checkWithCondition(sql, false);

        // not exist
        sql = String.format(" c2 between %s and %s", maxVal.add(BigDecimal.ONE).toPlainString(),
            maxVal.add(BigDecimal.TEN).toPlainString());
        checkOssScanExplainContains(sql, conditionInExplain);
        checkWithCondition(sql, true);
        // not exist with higher scale
        sql = String.format(" c2 between %s and %s", maxVal.toPlainString() + "01",
            maxVal.add(BigDecimal.ONE).toPlainString());
        checkOssScanExplainContains(sql, conditionInExplain);
        checkWithCondition(sql, true);
    }

    @Test
    public void testInPrune() {
        final String conditionInExplain = "WHERE (`c2` IN(?))";
        BigDecimal minVal = ORDERED_VALUE_LIST.get(0);
        BigDecimal maxVal = ORDERED_VALUE_LIST.get(ORDERED_VALUE_LIST.size() - 1);

        List<String> values = new ArrayList<>(16);
        // all exist
        for (int i = 0; i < 5; i++) {
            int idx = random.nextInt(COUNT);
            values.add(ORDERED_VALUE_LIST.get(idx).toPlainString());
        }
        String sql = String.format(" c2 in (%s)", StringUtils.join(values, ","));
        checkOssScanExplainContains(sql, conditionInExplain);
        checkWithCondition(sql, false);
        values.clear();
        // all exist with higher scale
        for (int i = 0; i < 5; i++) {
            int idx = random.nextInt(COUNT);
            values.add(ORDERED_VALUE_LIST.get(idx).toPlainString() + "000");
        }
        sql = String.format(" c2 in (%s)", StringUtils.join(values, ","));
        checkOssScanExplainContains(sql, conditionInExplain);
        checkWithCondition(sql, false);
        values.clear();

        // partially exist
        for (int i = 0; i < 10; i++) {
            int idx = random.nextInt(COUNT);
            if (i % 2 == 0) {
                values.add(ORDERED_VALUE_LIST.get(idx).toPlainString());
            } else {
                values.add(ORDERED_VALUE_LIST.get(idx).subtract(BigDecimal.ONE).toPlainString());
            }
        }
        sql = String.format(" c2 in (%s)", StringUtils.join(values, ","));
        checkOssScanExplainContains(sql, conditionInExplain);
        checkWithCondition(sql, false);
        values.clear();

        // not exist
        values.add(minVal.subtract(BigDecimal.ONE).toPlainString());
        values.add(maxVal.add(BigDecimal.ONE).toPlainString());
        sql = String.format(" c2 in (%s)", StringUtils.join(values, ","));
        checkOssScanExplainContains(sql, conditionInExplain);
        checkWithCondition(sql, true);
        values.clear();
    }

    private void checkOssScanExplainContains(String whereCondition, String msg) {
        String sql = String.format(SELECT_TEMPLATE, TABLE_1) + whereCondition;

        String explain = JdbcUtil.getExplainResult(tddlConnection, sql).toLowerCase();
        Assert.assertTrue(explain.contains("OSSTableScan".toLowerCase()));
        Assert.assertTrue(explain.contains(msg.toLowerCase()));
    }

    private void checkWithCondition(String whereCondition, boolean emptyResult) {
        String sql1 = String.format(SELECT_TEMPLATE, TABLE_1) + whereCondition;
        String sql2 = String.format(SELECT_PRIMARY_TEMPLATE, TABLE_1) + whereCondition;

        DataValidator.selectContentSameAssertWithDiffSql(sql1, sql2, null, tddlConnection, tddlConnection,
            emptyResult, false, true);
    }
}
