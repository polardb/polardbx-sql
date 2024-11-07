package com.alibaba.polardbx.qatest.ddl.auto.partition;

import org.junit.runners.Parameterized;

import java.util.List;

public class PartitionTableRangeScanTest extends PartitionAutoLoadSqlTestBase {

    public PartitionTableRangeScanTest(AutoLoadSqlTestCaseParams parameter) {
        super(parameter);
    }

    @Parameterized.Parameters(name = "{index}: SubTestCase {0}")
    public static List<AutoLoadSqlTestCaseParams> parameters() {
        return getParameters(PartitionTableRangeScanTest.class, 0, false);
    }

}