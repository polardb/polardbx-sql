package com.alibaba.polardbx.qatest.statistic;

import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.google.common.collect.Lists;
import org.junit.Test;
import org.junit.runners.Parameterized;
import org.yaml.snakeyaml.Yaml;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * tpch_100g_oss statistic and plan check
 *
 * @author fangwu
 */
public class StatisticsCostModelDumpIgnoreTraceTest extends StatisticCostModelTest {

    String filePath;

    @Parameterized.Parameters(name = "{0}")
    public static Iterable<String> params() throws SQLException, IOException {
        // read file from file path, build files Map
        return buildParamsFromFile("/statistics/dump/");
    }

    public StatisticsCostModelDumpIgnoreTraceTest(String filePath) {
        super(null);
        this.filePath = filePath;
    }

    @Test
    public void testCostModel() throws SQLException, IOException {
        Collection<CheckStruct> testStructs = buildCheckStructFromFile(filePath);

        for (CheckStruct testStruct : testStructs) {
            checkStruct = testStruct;

            testInside();
            try (Connection c = getPolardbxConnection()) {
                for (String schema : checkStruct.getSchemaList()) {
                    JdbcUtil.executeSuccess(c, "drop database if exists " + schema);
                }
            } finally {
                log.info("env file test result sum:" + trs);
            }
        }
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
            CheckStruct struct = new CheckStruct(
                m.get("SQL"),
                m.get("PLAN"),
                null,
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

    protected void checkTrace(Connection c) {
        // do nothing
    }

    /**
     * build test case
     *
     * @param path test config file
     */
    protected static Collection<String> buildParamsFromFile(String path) {
        Collection<String> paths = Lists.newLinkedList();
        File directory = new File(StatisticsCostModelDumpIgnoreTraceTest.class.getResource(path).getFile());
        Assert.assertTrue(directory.isDirectory());

        for (File f : directory.listFiles()) {
            if (!f.getName().endsWith("yml")) {
                continue;
            }
            paths.add(path + f.getName());
        }
        return paths;
    }
}
