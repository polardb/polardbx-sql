package com.alibaba.polardbx.qatest.ddl.auto.partition;

import org.junit.runners.Parameterized;

import java.util.List;

public class PartitionTableBkaJoinTest extends PartitionAutoLoadSqlTestBase {
    public PartitionTableBkaJoinTest(AutoLoadSqlTestCaseParams parameter) {
        super(parameter);
    }

    @Parameterized.Parameters(name = "{index}: SubTestCase {0}")
    public static List<AutoLoadSqlTestCaseParams> parameters() {
        return getParameters(PartitionTableBkaJoinTest.class, 0, false);
    }
}
