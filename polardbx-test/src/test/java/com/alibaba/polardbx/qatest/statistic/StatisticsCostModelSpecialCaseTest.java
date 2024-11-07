package com.alibaba.polardbx.qatest.statistic;

import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.google.common.collect.Lists;
import org.junit.Test;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collection;

/**
 * tpch_100g_oss statistic and plan check
 *
 * @author fangwu
 */
public class StatisticsCostModelSpecialCaseTest extends StatisticCostModelTest {

    String filePath;

    @Parameterized.Parameters(name = "{0}")
    public static Iterable<String> params() throws SQLException, IOException {
        // read file from file path, build files Map
        return buildParamsFromFile("/statistics/special/");
    }

    public StatisticsCostModelSpecialCaseTest(String filePath) {
        super(null);
        this.filePath = filePath;
    }

    @Test
    public void testCostModel() throws SQLException, IOException, InterruptedException {
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

    /**
     * build test case
     *
     * @param path test config file
     */
    protected static Collection<String> buildParamsFromFile(String path) {
        Collection<String> paths = Lists.newLinkedList();
        File directory = new File(StatisticsCostModelSpecialCaseTest.class.getResource(path).getFile());
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
