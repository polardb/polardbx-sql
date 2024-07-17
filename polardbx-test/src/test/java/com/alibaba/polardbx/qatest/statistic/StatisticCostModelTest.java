package com.alibaba.polardbx.qatest.statistic;

import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.qatest.BaseTestCase;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.yaml.snakeyaml.Yaml;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.io.UnsupportedEncodingException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.alibaba.polardbx.common.utils.GeneralUtil.decode;
import static com.alibaba.polardbx.common.utils.GeneralUtil.removeIdxSuffix;

/**
 * Cost model test framework contains two phase test:
 * statistic metadata -> statistic interface -> plan
 * <p>
 * With this test, we can make sure the optimizer always output the 'same' plan
 * in the same catalog and the same data statistic environment
 * <p>
 * test steps:
 * 1. prepare source file and check file
 * 2. source the catalog and statistic file
 * 3. check statistic interface
 * 4. check plan
 *
 * @author fangwu
 */
public abstract class StatisticCostModelTest extends BaseTestCase {
    public static final Log log = LogFactory.getLog("root");

    protected CheckStruct checkStruct;
    protected static final TestResultSum trs = new TestResultSum();

    public StatisticCostModelTest(CheckStruct checkStruct) {
        this.checkStruct = checkStruct;
    }

    @Before
    public void commonBefore() throws SQLException {
        try (Connection c = getPolardbxConnection()) {
            c.createStatement().execute("set global ENABLE_CACHELINE_COMPENSATION = false");
        }
    }

    @After
    public void commonAfter() throws SQLException {
        try (Connection c = getPolardbxConnection()) {
            c.createStatement().execute("set global ENABLE_CACHELINE_COMPENSATION = true");
        }
    }

    @Test
    public void testCostModel() throws SQLException, IOException {
        testInside();
    }

    public void testInside() throws SQLException, IOException {
        if (checkStruct.sql.isEmpty()) {
            return;
        }
        log.info(checkStruct.sql);
        log.info("link: xx.xx(" + checkStruct.taskLineInfo() + ")");
        try (Connection c = getPolardbxConnection()) {
            // prepare catalog
            for (String buildCommand : checkStruct.catalogBuildCommands) {
                c.createStatement().execute(buildCommand);
            }

            c.createStatement().execute("reload statistics");
            c.createStatement().execute("use " + checkStruct.schema);
            // prepare session env
            if (checkStruct.envSetCommands != null) {
                for (String setCommand : checkStruct.envSetCommands) {
                    if (StringUtils.isNotEmpty(setCommand)) {
                        c.createStatement().execute(setCommand);
                    }
                }
            }

            // check statistic trace
            checkTrace(c);
            // check plan
            ResultSet rs = c.createStatement().executeQuery("explain simple " + checkStruct.sql);
            String plan = getAllResultAsString(rs);
            // TODO check normalized plan
            String checkPlan = normalizePlan(checkStruct.plan);
            String realPlan = normalizePlan(plan);
            Assert.assertTrue(
                checkPlan.equals(realPlan),
                "plan mismatch:\n" + plan + "\n" + checkStruct.plan);
            trs.planCount.incrementAndGet();
        } finally {
            // clean
            try (Connection c = getPolardbxConnection()) {
                for (String cleanCommand : checkStruct.cleanCommands) {
                    if (StringUtils.isNotEmpty(cleanCommand)) {
                        c.createStatement().execute(cleanCommand);
                    }
                }
            }
            log.info("env file test result sum:" + trs);
        }
    }

    private String normalizePlan(String plan) {
        plan = plan.replaceAll(" ", "").trim();
        while (plan.contains("_$")) {
            plan = removeIdxSuffix(plan);
        }
        return plan;
    }

    protected void checkTrace(Connection c) throws SQLException, IOException {
        ResultSet rs = c.createStatement()
            .executeQuery("explain cost_trace /*TDDL:ENABLE_DIRECT_PLAN=FALSE*/ " + checkStruct.sql);
        Map<String, String> checkStatistic = null;
        while (rs.next()) {
            String statisticTraceInfo = rs.getString(1);
            if (statisticTraceInfo.startsWith("STATISTIC TRACE INFO")) {
                checkStatistic = decode(statisticTraceInfo);
            }
        }
        rs.close();
        if (checkStatistic == null) {
            throw new RuntimeException("explain cost trace return no statistic trace info");
        }

        Assert.assertTrue(checkStatistic.equals(checkStruct.statisticTraceMap),
            "statistic trace info mismatch:\n" + diff(checkStruct.statisticTraceMap, checkStatistic));
        trs.statisticsTraceMap.putAll(checkStruct.statisticTraceMap);
        trs.statisticTraceCount.addAndGet(checkStruct.statisticTraceMap.size());
    }

    protected String diff(Map<String, String> checkStatistic, Map<String, String> statisticTraceMap) {
        StringBuilder sb = new StringBuilder();
        for (Map.Entry<String, String> entry : checkStatistic.entrySet()) {
            if (!entry.getValue().equalsIgnoreCase(statisticTraceMap.get(entry.getKey()))) {
                sb.append("check:" + entry.getKey())
                    .append("\nexpected=" + entry.getValue())
                    .append("\nreal=" + statisticTraceMap.get(entry.getKey())).append("\n\n");
            }
        }

        for (Map.Entry<String, String> entry : statisticTraceMap.entrySet()) {
            if (!entry.getValue().equalsIgnoreCase(checkStatistic.get(entry.getKey()))) {
                sb.append("check:" + entry.getKey())
                    .append("\nreal=" + entry.getValue())
                    .append("\nexpected=" + checkStatistic.get(entry.getKey())).append("\n\n");
            }
        }
        return sb.toString();
    }

    /**
     * prepare test catalog
     *
     * @param c polardbx connection
     * @param envFilePath source file
     */
    protected static String[] source(Connection c, String envFilePath) throws IOException, SQLException {
        List<String> setCommands = Lists.newArrayList();
        InputStreamReader isr = new InputStreamReader(
            Objects.requireNonNull(StatisticCostModelTest.class.getResourceAsStream(envFilePath)));
        BufferedReader lineReader = new BufferedReader(isr);
        String line;
        while ((line = lineReader.readLine()) != null) {
            if (!StringUtils.isEmpty(line)) {
                c.createStatement().execute(line);
                if (line.trim().toUpperCase().startsWith("CREATE TABLE ")) {
                    trs.tblCount.incrementAndGet();
                } else if (line.trim().toUpperCase().startsWith("REPLACE INTO ")) {
                    trs.statisticMetaRowCount.incrementAndGet();
                } else if (line.trim().toUpperCase().startsWith("SET SESSION ")) {
                    trs.sessionVariablesCount.incrementAndGet();
                    setCommands.add(line.trim());
                }
            }
        }
        return setCommands.toArray(new String[0]);
    }

    protected String getAllResultAsString(ResultSet rs) throws SQLException {
        StringBuilder sb = new StringBuilder();
        while (rs.next()) {
            sb.append(rs.getString(1)).append("\n");
        }
        return sb.toString();
    }

    protected static Collection<CheckStruct> buildCheckStructFromFile(String filePath)
        throws IOException {
        Yaml yaml = new Yaml();
        List<Map<String, String>> yamlList =
            yaml.loadAs(StatisticCostModelTest.class.getResourceAsStream(filePath), List.class);

        List<CheckStruct> rs = Lists.newLinkedList();
        int i = 0;

        String content = readToString(StatisticCostModelTest.class.getResource(filePath).getPath());
        String shrinkContent = replaceBlank(content);
        for (Map<String, String> m : yamlList) {
            String catalog = m.get("CATALOG");
            String cleanCommands = m.get("CLEAN");
            if (cleanCommands == null) {
                cleanCommands = "";
            }
            CheckStruct struct =
                new CheckStruct(
                    m.get("SQL"),
                    m.get("PLAN"),
                    m.get("DETAIL_PLAN"),
                    m.get("STATISTIC_TRACE"),
                    catalog,
                    cleanCommands.split("\n"));
            struct.setFileName(filePath.substring(filePath.lastIndexOf("/") + 1));
            struct.setCaseIndex(i++);
            struct.setLineIndex(getNum(shrinkContent, replaceBlank(m.get("SQL"))));
            rs.add(struct);
        }
        return rs;
    }

    protected static String replaceBlank(String str) {
        String dest = null;
        if (str == null) {
            return dest;
        } else {
            Pattern p = Pattern.compile("[ \\t\\x0B\\f\\r]");
            Matcher m = p.matcher(str);
            dest = m.replaceAll("");
            return dest;
        }
    }

    protected static Integer getNum(String content, String matchStr) {
        return getNum(content.substring(0, content.indexOf(matchStr)));
    }

    protected static Integer getNum(String substring) {
        int index = 0;
        int count = 0;
        while (index != -1) {
            index = substring.indexOf("\n", index + 1);
            count++;
        }
        return count;
    }

    public static String readToString(String fileName) {
        String encoding = "utf-8";
        File file = new File(fileName);
        Long filelength = file.length();
        byte[] filecontent = new byte[filelength.intValue()];
        try {
            FileInputStream in = new FileInputStream(file);
            in.read(filecontent);
            in.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

        try {
            return new String(filecontent, encoding);
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
            return null;
        }
    }

    /**
     * test case unit
     */
    static class CheckStruct {
        private String fileName;
        private int lineIndex;
        private int caseIndex;

        String schema;

        List<String> schemaList;
        String sql;
        Map<String, String> statisticTraceMap;
        String plan;
        String[] envSetCommands;
        String[] catalogBuildCommands;
        String detailPlan;
        String[] cleanCommands;

        public CheckStruct(String sql, String plan, String detailPlan, String statisticTraceInfo, String catalog,
                           String[] cleanCommands)
            throws IOException {
            this.sql = sql;
            this.plan = plan;
            this.statisticTraceMap = decode(statisticTraceInfo);
            this.schemaList = Lists.newArrayList();
            this.detailPlan = detailPlan;
            this.cleanCommands = cleanCommands;
            decodeCatalog(catalog);
        }

        public void decodeCatalog(String catalog) throws IOException {
            if (StringUtils.isEmpty(catalog)) {
                return;
            }

            List<String> setCommands = Lists.newLinkedList();
            List<String> buildCommands = Lists.newLinkedList();

            BufferedReader lineReader = new BufferedReader(new StringReader(catalog));
            String line;
            while ((line = lineReader.readLine()) != null) {
                // find Key
                String originLine = line.trim();
                line = originLine.toLowerCase();
                // get schema
                if (line.startsWith("use ")) {
                    schema = line.substring(4).toLowerCase().trim();
                    schemaList.add(schema);
                    buildCommands.add(line);
                } else if (line.startsWith("reload statistics")) {
                } else if (line.startsWith("set session ")) {
                    setCommands.add(originLine);
                } else {
                    buildCommands.add(originLine);
                }
            }

            envSetCommands = setCommands.toArray(new String[0]);
            catalogBuildCommands = buildCommands.toArray(new String[0]);
            Assert.assertTrue(schema != null);
        }

        @Override
        public String toString() {
            return fileName + "[" + caseIndex + "]";
        }

        public void setLineIndex(int lineIndex) {
            this.lineIndex = lineIndex;
        }

        public String taskLineInfo() {
            return fileName + ":" + lineIndex;
        }

        public void setCaseIndex(int caseIndex) {
            this.caseIndex = caseIndex;
        }

        public String getFileName() {
            return fileName;
        }

        public void setFileName(String fileName) {
            this.fileName = fileName;
        }

        public List<String> getSchemaList() {
            return schemaList;
        }
    }

    static class TestResultSum {
        AtomicInteger planCount = new AtomicInteger();
        AtomicInteger tblCount = new AtomicInteger();
        AtomicInteger statisticMetaRowCount = new AtomicInteger();
        AtomicInteger sessionVariablesCount = new AtomicInteger();
        AtomicInteger statisticTraceCount = new AtomicInteger();
        Map<String, String> statisticsTraceMap = Maps.newTreeMap(String::compareTo);

        public String toString() {
            StringBuilder sb = new StringBuilder();
            for (Map.Entry<String, String> e : statisticsTraceMap.entrySet()) {
                sb.append(e.getKey().replaceAll("\n", " ")).append("--->").append(e.getValue()).append("\n");
            }

            sb.append("session variables count:").append(sessionVariablesCount).append("\n");
            sb.append("tbl create count:").append(tblCount).append("\n");
            sb.append("statistic Meta row count:").append(statisticMetaRowCount).append("\n");
            sb.append("plan count:").append(planCount).append("\n");
            sb.append("statistics trace check count:").append(statisticTraceCount).append("\n");
            sb.append("statistics trace item check:").append(statisticsTraceMap.size()).append("\n");
            return sb.toString();
        }
    }
}
