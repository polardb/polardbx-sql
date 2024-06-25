package com.alibaba.polardbx.qatest.ddl.auto.partition;

import org.junit.runners.Parameterized;

import java.util.List;

/**
 * @version 1.0
 */

public class AutoPartition2Test extends PartitionAutoLoadSqlTestBase {

    public AutoPartition2Test(AutoLoadSqlTestCaseParams parameter) {
        super(parameter);
    }

    @Parameterized.Parameters(name = "{index}: SubTestCase {0}")
    public static List<AutoLoadSqlTestCaseParams> parameters() {
        return getParameters(AutoPartition2Test.class, 3, true);
    }
}
