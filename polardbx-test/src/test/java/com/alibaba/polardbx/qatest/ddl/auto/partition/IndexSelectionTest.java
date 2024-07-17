package com.alibaba.polardbx.qatest.ddl.auto.partition;

import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.List;

@RunWith(value = Parameterized.class)
public class IndexSelectionTest extends PartitionAutoLoadSqlTestBase {
    public IndexSelectionTest(AutoLoadSqlTestCaseParams parameter) {
        super(parameter);
    }

    @Parameterized.Parameters(name = "{index}: SubTestCase {0}")
    public static List<AutoLoadSqlTestCaseParams> parameters() {
        return getParameters(IndexSelectionTest.class, 16, true);
    }

}