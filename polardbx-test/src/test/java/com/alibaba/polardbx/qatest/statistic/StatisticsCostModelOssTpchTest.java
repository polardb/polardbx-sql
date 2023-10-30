package com.alibaba.polardbx.qatest.statistic;

import com.alibaba.polardbx.common.utils.Assert;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.util.Collection;

/**
 * tpch_100g_oss statistic and plan check
 *
 * @author fangwu
 */
public class StatisticsCostModelOssTpchTest extends StatisticCostModelTest {
    public StatisticsCostModelOssTpchTest(CheckStruct checkStruct) {
        super(checkStruct);
    }

    @Parameterized.Parameters(name = "oss_tpch_Q{index}+1")
    public static Iterable<CheckStruct> params() throws SQLException, IOException {
        return buildParamsFromFile("/statistics/tpch_oss/tpch_oss_check_file.yml",
            "/statistics/tpch_oss/env_oss.sql");
    }

    /**
     * build test case
     *
     * @param schema test schema
     * @param files test config file
     */
    protected static Collection<CheckStruct> buildParamsFromFile(String testFilePath, String catalogFilePath)
        throws SQLException, IOException {
        File testYmlFile = new File(testFilePath);
        File catalogFile = new File(catalogFilePath);
        Assert.assertTrue(!testYmlFile.isDirectory());
        Assert.assertTrue(!catalogFile.isDirectory());

        String catalog = readToString(StatisticCostModelTest.class.getResource(catalogFilePath).getPath());
        Collection<CheckStruct> testStructs = buildCheckStructFromFile(testFilePath);
        testStructs.stream().forEach(s -> {
            try {
                s.decodeCatalog(catalog);
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
        return testStructs;
    }
}
