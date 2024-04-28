package com.alibaba.polardbx.qatest.columnar.dql.explain;

import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.qatest.columnar.dql.ColumnarReadBaseTestCase;
import com.alibaba.polardbx.qatest.columnar.dql.ColumnarUtils;
import com.alibaba.polardbx.qatest.data.ExecuteTableName;
import com.alibaba.polardbx.qatest.data.ExecuteTableSelect;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.alibaba.polardbx.qatest.util.PropertiesUtil;
import org.apache.commons.lang3.StringUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.yaml.snakeyaml.Yaml;

import java.io.InputStream;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

public class ExplainAnalyzeTest extends ColumnarReadBaseTestCase {

    private static final List<String> TABLES = new ArrayList<>();

    private static final Logger logger = LoggerFactory.getLogger(ExplainAnalyzeTest.class);

    private static final String RESOURCE_FILENAME = "columnar/dql/explain/ExplainAnalyzeTest.yml";
    private static final String KEY_SQL = "sql";
    private static final String KEY_PLAN = "plan";
    private final List<Map<String, String>> planList;

    public ExplainAnalyzeTest() {
        InputStream in = ExplainAnalyzeTest.class.getClassLoader().getResourceAsStream(RESOURCE_FILENAME);
        Yaml yaml = new Yaml();
        this.planList = yaml.loadAs(in, List.class);
    }

    @BeforeClass
    public static void prepare() throws SQLException, InterruptedException {
        initTables();
        dropTables();
        prepareData();
    }

    private static void initTables() {
        Set<String> tableSet = new HashSet<>();
        String[][] allTables = ExecuteTableSelect.selectOneTableMultiRuleMode();
        for (String[] tables : allTables) {
            Collections.addAll(tableSet, tables);
        }
        TABLES.addAll(tableSet);
    }

    @AfterClass
    public static void dropTables() throws SQLException {
        try (Connection connection = getColumnarConnection()) {
            for (String table : TABLES) {
                try {
                    JdbcUtil.dropTable(connection, table);
                } catch (Exception e) {
                    logger.error(e);
                }
            }
        }
    }

    private static void prepareData() throws SQLException, InterruptedException {
        try (Connection connection = getColumnarConnection()) {
            for (String table : TABLES) {
                String srcDb = PropertiesUtil.polardbXDBName1(true);
                String createTable = String.format("create table %s like %s.%s", table, srcDb, table);
                String insertSelect = String.format("insert into %s select * from %s.%s", table, srcDb, table);
                JdbcUtil.executeSuccess(connection, createTable);
                JdbcUtil.executeSuccess(connection, insertSelect);
                ColumnarUtils.createColumnarIndex(connection, table + "_col", table, "pk", "pk", 4);
            }
            // 防止实验室缺少统计信息导致计划不正确
            String injectStats =
                String.format("\nCatalog:%s,select_base_one_multi_db_one_tb\n"
                    + "Action:getRowCount\n"
                    + "StatisticValue:1000", DB_NAME) +
                    String.format("\nCatalog:%s,select_base_one_one_db_one_tb\n"
                        + "Action:getRowCount\n"
                        + "StatisticValue:1000", DB_NAME) +
                    String.format("\nCatalog:%s,select_base_one_multi_db_multi_tb\n"
                        + "Action:getRowCount\n"
                        + "StatisticValue:1000", DB_NAME) + "\n";
            JdbcUtil.executeSuccess(connection, String.format("set global STATISTIC_CORRECTIONS=\"%s\";", injectStats));
            Thread.sleep(1500);
        }
    }

    private static List<String> splitPlanLines(String plan) {
        String[] splitStrs = StringUtils.split(plan, "\n");
        List<String> result = new ArrayList<>(splitStrs.length);
        for (String splitStr : splitStrs) {
            if (StringUtils.isNotBlank(splitStr)) {
                result.add(splitStr);
            }
        }
        return result;
    }

    @Test
    public void explainAllSelectTest() throws SQLException {
        for (Map<String, String> map : planList) {
            String sql = map.get(KEY_SQL);
            String planPatterns = map.get(KEY_PLAN);
            List<String> patternList = splitPlanLines(planPatterns);
            try (ResultSet resultSet = JdbcUtil.executeQuery(sql, tddlConnection)) {
                String line;
                for (String pattern : patternList) {
                    Assert.assertTrue("Expect next plan line", resultSet.next());
                    line = resultSet.getString(1);
                    Assert.assertTrue("Pattern: " + pattern + " ; Actual line: " + line,
                        Pattern.matches(pattern, line));
                }
                // end of plan
                Assert.assertTrue("Expect HitCache", resultSet.next());
                line = resultSet.getString(1);
                Assert.assertTrue("Expect HitCache, but got " + line, StringUtils.startsWith(line, "HitCache"));
            } catch (Exception | AssertionError e) {
                logger.error("Failed at: " + sql, e);
                throw e;
            }
        }
    }
}
