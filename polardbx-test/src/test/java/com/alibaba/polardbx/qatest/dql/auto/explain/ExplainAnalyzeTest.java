package com.alibaba.polardbx.qatest.dql.auto.explain;

import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.qatest.AutoReadBaseTestCase;
import com.alibaba.polardbx.qatest.ColumnarIgnore;
import com.alibaba.polardbx.qatest.FileStoreIgnore;
import com.alibaba.polardbx.qatest.CdcIgnore;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.apache.commons.lang3.StringUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.yaml.snakeyaml.Yaml;

import java.io.InputStream;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

public class ExplainAnalyzeTest extends AutoReadBaseTestCase {

    private static final Logger logger = LoggerFactory.getLogger(ExplainAnalyzeTest.class);

    private static final String RESOURCE_FILENAME = "dql/auto/explain/ExplainAnalyzeTest.yml";
    private static final String KEY_SQL = "sql";
    private static final String KEY_PLAN = "plan";
    private final List<Map<String, String>> planList;

    public ExplainAnalyzeTest() {
        InputStream in = ExplainAnalyzeTest.class.getClassLoader().getResourceAsStream(RESOURCE_FILENAME);
        Yaml yaml = new Yaml();
        this.planList = yaml.loadAs(in, List.class);
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

    /**
     * 防止实验室缺少统计信息导致计划不正确
     */
    @Before
    public void before() throws InterruptedException {
        String injectStats =
            String.format("\nCatalog:%s,select_base_one_multi_db_one_tb\n"
                + "Action:getRowCount\n"
                + "StatisticValue:1000", polardbxOneDB) +
                String.format("\nCatalog:%s,select_base_one_one_db_one_tb\n"
                    + "Action:getRowCount\n"
                    + "StatisticValue:1000", polardbxOneDB) +
                String.format("\nCatalog:%s,select_base_one_multi_db_multi_tb\n"
                    + "Action:getRowCount\n"
                    + "StatisticValue:1000", polardbxOneDB) + "\n";
        JdbcUtil.executeSuccess(tddlConnection, String.format("set global STATISTIC_CORRECTIONS=\"%s\";", injectStats));
        Thread.sleep(1500);
    }

    @After
    public void after() {
        JdbcUtil.executeSuccess(tddlConnection, "set global STATISTIC_CORRECTIONS=\"\";");
    }

    @Test
    @FileStoreIgnore
    @ColumnarIgnore
    @CdcIgnore(ignoreReason = "暂时未查到原因，可能是并行跑各种实验室导致。本地无法复现，且对replica实验室无影响")
    public void explainAllSelectTest() throws SQLException {
        for (Map<String, String> map : planList) {
            String sql = map.get(KEY_SQL);
            String planPatterns = map.get(KEY_PLAN);
            List<String> patternList = splitPlanLines(planPatterns);
            try (ResultSet resultSet = JdbcUtil.executeQuery(sql, tddlConnection)) {
                String line;
                for (int i = 0; i < patternList.size(); i++) {
                    String pattern = patternList.get(i);
                    Assert.assertTrue("Expect next plan line; sql:" + sql, resultSet.next());
                    line = resultSet.getString(1);
                    Assert.assertTrue(
                        "PlanLine: " + i + " ; Pattern: " + pattern + " ; Actual line: " + line + " ; sql: " + sql,
                        Pattern.matches(pattern, line));
                }
                Assert.assertTrue("Expect HitCache, sql: " + sql, resultSet.next());
                line = resultSet.getString(1);
                Assert.assertTrue("Expect HitCache, but got " + line + "; sql: " + sql,
                    StringUtils.startsWith(line, "HitCache"));
            } catch (Exception | AssertionError e) {
                logger.error("Failed at: " + sql, e);
                throw e;
            }
        }
    }
}
